-module(beamtrail_reducer).

-export([new/0, from_events/1, from_snapshot_and_events/2, apply_event/2,
         attempt_keys/0]).

new() ->
    #{run_id => undefined,
      workflow => undefined,
      decider => legacy,
      decider_version => 1,
      input => undefined,
      steps => [],
      status => new,
      current_step => undefined,
      completed_steps => 0,
      results => [],
      workflow_result => undefined,
      signals => [],
      wait_reason => undefined,
      waiting_since => undefined,
      timers => #{},
      next_wake_at => undefined,
      attempt_counts => #{},
      attempts => [],
      pending_attempt => undefined,
      pending_effects => #{},
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
           decider => maps:get(decider, Payload, legacy),
           decider_version => maps:get(decider_version, Payload, 1),
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
    Payload = maps:get(payload, Event),
    PayloadAttempt = maps:get(attempt, Payload, AttemptNo),
    StepInput = maps:get(step_input, Payload, maps:get(input, State, undefined)),
    Attempt =
        #{step_id => StepId,
          step_version => maps:get(step_version, Event),
          idempotency_key => maps:get(idempotency_key, Event),
          effect_id => maps:get(effect_id, Payload, undefined),
          step_input => StepInput,
          attempt => PayloadAttempt,
          status => unknown,
          started_event_seq => maps:get(event_seq, Event)},
    State1 =
        State#{status => running,
               next_retry_at => undefined,
               pending_attempt => Attempt,
               attempt_counts => maps:put(StepId, AttemptNo, AttemptCounts0),
               attempts => maps:get(attempts, State) ++ [Attempt]},
    register_pending_effect(State1, Event, Attempt);
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
           pending_effects => clear_event_effect(Event, State),
           failure => undefined};
apply_event_type('timer.scheduled', State, Event) ->
    Payload = maps:get(payload, Event),
    TimerId = maps:get(timer_id, Payload),
    FireAtMs = maps:get(fire_at_ms, Payload),
    Timer =
        #{timer_id => TimerId,
          status => scheduled,
          fire_at_ms => FireAtMs,
          scheduled_event_seq => maps:get(event_seq, Event),
          scheduled_at => maps:get(scheduled_at, Payload,
                                   maps:get(occurred_at, Event, undefined)),
          fired_event_seq => undefined,
          fired_at => undefined},
    Timers = maps:put(TimerId, Timer, maps:get(timers, State, #{})),
    State#{status => running,
           timers => Timers,
           next_wake_at => next_wake_at(Timers),
           next_retry_at => undefined};
apply_event_type('timer.fired', State, Event) ->
    Payload = maps:get(payload, Event),
    TimerId = maps:get(timer_id, Payload),
    Timers0 = maps:get(timers, State, #{}),
    Timer0 = maps:get(TimerId, Timers0,
                      #{timer_id => TimerId,
                        status => scheduled,
                        fire_at_ms => maps:get(fire_at_ms, Payload),
                        scheduled_event_seq => undefined,
                        scheduled_at => undefined}),
    Timer = Timer0#{status => fired,
                    fired_event_seq => maps:get(event_seq, Event),
                    fired_at => maps:get(fired_at, Payload,
                                         maps:get(occurred_at, Event, undefined))},
    Timers = maps:put(TimerId, Timer, Timers0),
    State#{status => running,
           timers => Timers,
           next_wake_at => next_wake_at(Timers),
           next_retry_at => undefined};
apply_event_type('step.failed', State, Event) ->
    StepId = maps:get(step_id, Event),
    Payload = maps:get(payload, Event),
    State#{status => failed,
           pending_attempt => undefined,
           pending_effects => clear_event_effect(Event, State),
           failure => Payload,
           attempts => update_latest_attempt(StepId, failed, Event, State)};
apply_event_type('activity.scheduled', State, Event) ->
    update_pending_effect_status(State, Event, scheduled);
apply_event_type('activity.started', State, Event) ->
    update_pending_effect_status(State, Event, started);
apply_event_type('activity.succeeded', State, Event) ->
    State#{pending_effects => clear_event_effect(Event, State)};
apply_event_type('activity.failed', State, Event) ->
    State#{pending_effects => clear_event_effect(Event, State)};
apply_event_type('retry.scheduled', State, Event) ->
    Payload = maps:get(payload, Event),
    State#{status => retrying,
           current_step => maps:get(step_id, Event),
           next_retry_at => maps:get(next_retry_at, Payload),
           failure => Payload};
apply_event_type('workflow.waiting', State, Event) ->
    Payload = maps:get(payload, Event),
    State#{status => waiting,
           wait_reason => maps:get(reason, Payload, undefined),
           waiting_since => maps:get(waiting_since, Payload,
                                     maps:get(occurred_at, Event, undefined)),
           pending_attempt => undefined,
           next_retry_at => undefined};
apply_event_type('signal.received', State, Event) ->
    Payload = maps:get(payload, Event),
    Signal =
        #{name => maps:get(name, Payload),
          payload => maps:get(payload, Payload, #{}),
          event_seq => maps:get(event_seq, Event),
          received_at => maps:get(received_at, Payload,
                                  maps:get(occurred_at, Event, undefined))},
    State#{status => running,
           signals => maps:get(signals, State, []) ++ [Signal],
           next_retry_at => undefined};
apply_event_type('workflow.completed', State, Event) ->
    Payload = maps:get(payload, Event, #{}),
    State#{status => completed,
           current_step => undefined,
           pending_attempt => undefined,
           pending_effects => #{},
           terminal => true,
           workflow_result => maps:get(result, Payload, undefined),
           failure => undefined};
apply_event_type('workflow.failed', State, Event) ->
    State#{status => failed,
           current_step => undefined,
           pending_attempt => undefined,
           pending_effects => #{},
           terminal => true,
           failure => maps:get(payload, Event)};
apply_event_type('workflow.cancelled', State, Event) ->
    Payload = maps:get(payload, Event),
    State#{status => cancelled,
           current_step => undefined,
           pending_attempt => undefined,
           pending_effects => #{},
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

register_pending_effect(State, Event, Attempt) ->
    Payload = maps:get(payload, Event, #{}),
    case maps:get(effect_id, Payload, undefined) of
        undefined ->
            State;
        EffectId ->
            Pending0 = maps:get(pending_effects, State, #{}),
            Effect =
                #{effect_id => EffectId,
                  effect_type => maps:get(effect_type, Payload, call_step),
                  step_id => maps:get(step_id, Attempt),
                  step_version => maps:get(step_version, Attempt),
                  idempotency_key => maps:get(idempotency_key, Attempt),
                  attempt => maps:get(attempt, Attempt),
                  status => requested,
                  requested_event_seq => maps:get(event_seq, Event),
                  requested_at => maps:get(occurred_at, Event, undefined)},
            State#{pending_effects => maps:put(EffectId, Effect, Pending0)}
    end.

update_pending_effect_status(State, Event, Status) ->
    Payload = maps:get(payload, Event, #{}),
    case maps:get(effect_id, Payload, undefined) of
        undefined ->
            State;
        EffectId ->
            Pending0 = maps:get(pending_effects, State, #{}),
            Effect0 =
                maps:get(EffectId,
                         Pending0,
                         pending_effect_from_activity(Event, Payload, EffectId)),
            Effect1 = Effect0#{status => Status},
            Effect2 = add_effect_lifecycle_seq(Status, Event, Effect1),
            State#{pending_effects => maps:put(EffectId, Effect2, Pending0)}
    end.

pending_effect_from_activity(Event, Payload, EffectId) ->
    #{effect_id => EffectId,
      effect_type => maps:get(effect_type, Payload, call_step),
      step_id => maps:get(step_id, Event),
      step_version => maps:get(step_version, Event),
      idempotency_key => maps:get(idempotency_key, Event),
      attempt => maps:get(attempt, Payload, undefined),
      status => requested}.

add_effect_lifecycle_seq(scheduled, Event, Effect) ->
    Effect#{scheduled_event_seq => maps:get(event_seq, Event),
            scheduled_at => maps:get(occurred_at, Event, undefined)};
add_effect_lifecycle_seq(started, Event, Effect) ->
    Effect#{started_event_seq => maps:get(event_seq, Event),
            started_at => maps:get(occurred_at, Event, undefined)}.

clear_event_effect(Event, State) ->
    Payload = maps:get(payload, Event, #{}),
    case maps:get(effect_id, Payload, undefined) of
        undefined ->
            maps:get(pending_effects, State, #{});
        EffectId ->
            maps:remove(EffectId, maps:get(pending_effects, State, #{}))
    end.

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

next_wake_at(Timers) ->
    Scheduled =
        [FireAt || #{status := scheduled, fire_at_ms := FireAt}
                       <- maps:values(Timers),
                   is_integer(FireAt)],
    case Scheduled of
        [] -> undefined;
        _ -> lists:min(Scheduled)
    end.
