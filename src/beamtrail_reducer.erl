-module(beamtrail_reducer).

-export([new/0, from_events/1, from_snapshot_and_events/2, apply_event/2,
         attempt_keys/0]).

new() ->
    #{run_id => undefined,
      workflow => undefined,
      input => undefined,
      steps => [],
      status => new,
      current_step => undefined,
      completed_steps => 0,
      results => [],
      workflow_result => undefined,
      attempt_counts => #{},
      attempts => [],
      pending_attempt => undefined,
      next_retry_at => undefined,
      failure => undefined,
      parked => false,
      parked_reason => undefined,
      parked_at => undefined,
      terminal => false,
      last_event_seq => 0}.

from_events(Events) ->
    lists:foldl(fun(Event, State) -> apply_event(State, Event) end, new(), Events).

from_snapshot_and_events(SnapshotState, Events) ->
    lists:foldl(fun(Event, State) -> apply_event(State, Event) end, SnapshotState, Events).

attempt_keys() ->
    State = from_events(
              [#{run_id => <<"schema-run">>,
                 event_seq => 1,
                 event_type => 'workflow.instance.created',
                 payload => #{workflow => undefined,
                              input => undefined,
                              steps => [schema_step]},
                 occurred_at => 0},
               #{run_id => <<"schema-run">>,
                 event_seq => 2,
                 event_type => 'attempt.started',
                 step_id => schema_step,
                 step_version => 1,
                 idempotency_key => schema_key,
                 payload => #{attempt => 1},
                 occurred_at => 1},
               #{run_id => <<"schema-run">>,
                 event_seq => 3,
                 event_type => 'step.succeeded',
                 step_id => schema_step,
                 payload => #{result => schema_result},
                 occurred_at => 2}]),
    [Attempt] = maps:get(attempts, State),
    maps:keys(Attempt).

apply_event(State0, Event) ->
    State = State0#{last_event_seq => maps:get(event_seq, Event)},
    apply_event_type(maps:get(event_type, Event), State, Event).

apply_event_type('workflow.instance.created', State, Event) ->
    Payload = maps:get(payload, Event),
    Steps = maps:get(steps, Payload, []),
    State#{run_id => maps:get(run_id, Event),
           workflow => maps:get(workflow, Payload),
           input => maps:get(input, Payload),
           steps => Steps,
           status => running,
           current_step => first_step(Steps),
           completed_steps => 0,
           created_at => maps:get(occurred_at, Event),
           terminal => false};
apply_event_type('attempt.started', State, Event) ->
    StepId = maps:get(step_id, Event),
    AttemptCounts0 = maps:get(attempt_counts, State),
    AttemptNo = maps:get(StepId, AttemptCounts0, 0) + 1,
    PayloadAttempt = maps:get(attempt, maps:get(payload, Event), AttemptNo),
    Attempt =
        #{step_id => StepId,
          step_version => maps:get(step_version, Event),
          idempotency_key => maps:get(idempotency_key, Event),
          attempt => PayloadAttempt,
          status => unknown,
          started_event_seq => maps:get(event_seq, Event)},
    State#{status => running,
           next_retry_at => undefined,
           pending_attempt => Attempt,
           attempt_counts => maps:put(StepId, AttemptNo, AttemptCounts0),
           attempts => maps:get(attempts, State) ++ [Attempt]};
apply_event_type('step.succeeded', State, Event) ->
    StepId = maps:get(step_id, Event),
    CompletedSteps = maps:get(completed_steps, State) + 1,
    Steps = maps:get(steps, State),
    Payload = maps:get(payload, Event),
    Result = maps:get(result, Payload, undefined),
    State#{status => running,
           current_step => next_step(Steps, CompletedSteps),
           completed_steps => CompletedSteps,
           pending_attempt => undefined,
           attempts => update_latest_attempt(StepId, succeeded, Event, State),
           results => maps:get(results, State) ++
               [result_entry(StepId, Result, Event, State)],
           failure => undefined};
apply_event_type('step.failed', State, Event) ->
    StepId = maps:get(step_id, Event),
    Payload = maps:get(payload, Event),
    State#{status => failed,
           pending_attempt => undefined,
           failure => Payload,
           attempts => update_latest_attempt(StepId, failed, Event, State)};
apply_event_type('retry.scheduled', State, Event) ->
    Payload = maps:get(payload, Event),
    State#{status => retrying,
           current_step => maps:get(step_id, Event),
           next_retry_at => maps:get(next_retry_at, Payload),
           failure => Payload};
apply_event_type('workflow.completed', State, Event) ->
    Payload = maps:get(payload, Event, #{}),
    State#{status => completed,
           current_step => undefined,
           pending_attempt => undefined,
           terminal => true,
           workflow_result => maps:get(result, Payload, undefined),
           failure => undefined};
apply_event_type('workflow.failed', State, Event) ->
    State#{status => failed,
           current_step => undefined,
           pending_attempt => undefined,
           terminal => true,
           failure => maps:get(payload, Event)};
apply_event_type('workflow.cancelled', State, Event) ->
    Payload = maps:get(payload, Event),
    State#{status => cancelled,
           current_step => undefined,
           pending_attempt => undefined,
           terminal => true,
           failure => Payload,
           parked => false,
           parked_reason => undefined,
           parked_at => undefined};
apply_event_type('workflow.parked', State, Event) ->
    Payload = maps:get(payload, Event),
    State#{parked => true,
           parked_reason => maps:get(reason, Payload, undefined),
           parked_at => maps:get(parked_at, Payload,
                                 maps:get(occurred_at, Event, undefined))};
apply_event_type('workflow.resumed', State, _Event) ->
    State#{parked => false,
           parked_reason => undefined,
           parked_at => undefined};
apply_event_type(_Other, State, _Event) ->
    State.

first_step([]) ->
    undefined;
first_step([Step | _]) ->
    Step.

next_step(Steps, CompletedSteps) when CompletedSteps >= length(Steps) ->
    undefined;
next_step(Steps, CompletedSteps) ->
    lists:nth(CompletedSteps + 1, Steps).

update_latest_attempt(StepId, Status, Event, State) ->
    Attempts = maps:get(attempts, State),
    {Reversed, _Updated} =
        lists:foldl(
          fun(Attempt, {Acc, false}) ->
                  case maps:get(step_id, Attempt) =:= StepId andalso maps:get(status, Attempt) =:= unknown of
                      true ->
                          {[complete_attempt(Attempt, Status, Event) | Acc], true};
                      false ->
                          {[Attempt | Acc], false}
                  end;
             (Attempt, {Acc, true}) ->
                  {[Attempt | Acc], true}
          end,
          {[], false},
          lists:reverse(Attempts)),
    Reversed.

complete_attempt(Attempt, Status, Event) ->
    Payload = maps:get(payload, Event),
    Attempt#{status => Status,
             completed_event_seq => maps:get(event_seq, Event),
             result => maps:get(result, Payload, undefined),
             reason => maps:get(reason, Payload, undefined)}.

result_entry(StepId, Result, Event, State) ->
    Attempt = maps:get(pending_attempt, State, #{}),
    #{step_id => StepId,
      attempt => maps:get(attempt, Attempt, undefined),
      event_seq => maps:get(event_seq, Event),
      result => Result}.
