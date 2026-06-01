-module(beamtrail_transition).

-export([owner/0, lease_fencing_token/1,
         dispatch_locked/3, dispatch_locked/4,
         dispatch_retrying/4,
         finish_attempt/4, finish_attempt/5]).

dispatch_locked(RunId, State, Lease) ->
    dispatch_locked(RunId, State, Lease, #{lease_heartbeat => internal}).

dispatch_locked(RunId, State, Lease, Options) ->
    case lease_current(RunId, Lease) of
        false ->
            {error, stale_lease};
        true ->
            case maps:get(migration_required_for_version_change, State, false) of
                true ->
                    {error, {migration_required, State}};
                false ->
                    dispatch_ready(RunId, State, Lease, Options)
            end
    end.

dispatch_retrying(RunId, State, Lease, Options) ->
    Now = erlang:system_time(millisecond),
    case maps:get(next_retry_at, State, 0) =< Now of
        true -> dispatch_locked(RunId, State, Lease, Options);
        false -> {ok, State}
    end.

finish_attempt(RunId, Lease, Attempt, Result) when is_map(Lease), is_map(Attempt) ->
    Options = #{runner_mode => finish},
    finish_attempt_with_options(RunId, Lease, Attempt, Result, Options).

finish_attempt(RunId, Lease, Attempt, Result, State)
  when is_map(Lease), is_map(Attempt), is_map(State) ->
    Options = #{runner_mode => finish, runner_state => State},
    finish_attempt_with_options(RunId, Lease, Attempt, Result, Options).

finish_attempt_with_options(RunId, Lease, Attempt, Result, Options) ->
    case Result of
        {ok, Value} ->
            handle_step_success(RunId, Attempt, Value, Lease, Options);
        {error, Reason} ->
            handle_step_failure(RunId, Attempt, Reason, Lease, Options)
    end.

dispatch_ready(RunId, State, Lease, Options) ->
    case workflow_timeout_exceeded(State) of
        true ->
            fail_workflow_with_timeout(RunId, State, Lease);
        false ->
            case maps:get(current_step, State) of
                undefined ->
                    complete_if_needed(RunId, State, Lease);
                StepId ->
                    run_step(RunId, State, StepId, Lease, Options)
            end
    end.

workflow_timeout_exceeded(State) ->
    case maps:get(workflow, State, undefined) of
        undefined -> false;
        Workflow ->
            case workflow_timeout_ms(Workflow) of
                infinity -> false;
                undefined -> false;
                Budget when is_integer(Budget) ->
                    case maps:get(created_at, State, undefined) of
                        undefined -> false;
                        CreatedAt ->
                            erlang:system_time(millisecond) - CreatedAt > Budget
                    end
            end
    end.

workflow_timeout_ms(Workflow) ->
    _ = code:ensure_loaded(Workflow),
    case erlang:function_exported(Workflow, workflow_timeout_ms, 0) of
        true ->
            try Workflow:workflow_timeout_ms() catch _:_ -> infinity end;
        false ->
            infinity
    end.

fail_workflow_with_timeout(RunId, State, Lease) ->
    StepId = maps:get(current_step, State),
    Payload = #{reason => workflow_timeout,
                class => workflow_timeout,
                created_at => maps:get(created_at, State, undefined),
                failed_at => erlang:system_time(millisecond)},
    case append_event(RunId, maps:get(last_event_seq, State, 0), Lease,
                      'workflow.failed', StepId, undefined, undefined, Payload) of
        {ok, Event} ->
            State1 = apply_runtime_event(State, Event),
            case maybe_snapshot_state(RunId, State1, true) of
                ok -> {ok, State1};
                {error, _} = Error -> Error
            end;
        {error, _} = Error ->
            Error
    end.

complete_if_needed(RunId, State, Lease) ->
    case maps:get(status, State) of
        completed ->
            {ok, State};
        _ ->
            case append_event(
                   RunId,
                   maps:get(last_event_seq, State, 0),
                   Lease,
                   'workflow.completed',
                   undefined,
                   undefined,
                   undefined,
                   #{completed_at => erlang:system_time(millisecond)}) of
                {ok, Event} ->
                    State1 = apply_runtime_event(State, Event),
                    case maybe_snapshot_state(RunId, State1, true) of
                        ok -> {ok, State1};
                        {error, _} = Error -> Error
                    end;
                {error, _} = Error ->
                    Error
            end
    end.

run_step(RunId, State, StepId, Lease, Options) ->
    Workflow = maps:get(workflow, State),
    Input = maps:get(input, State),
    case ensure_attempt_started(RunId, Workflow, Input, State, StepId, Lease) of
        {ok, Attempt, StartedNow, State1} ->
            Options1 = Options#{runner_state => State1},
            case StartedNow of
                true ->
                    beamtrail_telemetry:execute([beamtrail, attempt, started], #{count => 1},
                                                #{run_id => RunId, step_id => StepId});
                false ->
                    ok
            end,
            case maps:get(runner_mode, Options, dispatch) of
                prepare ->
                    {ok, {execute, Attempt,
                          execution_spec(RunId, Workflow, StepId, Input, Attempt),
                          State1}};
                _ ->
                    Result = execute_attempt(RunId, Workflow, Input, Attempt, Lease),
                    case Result of
                        {ok, Value} ->
                            handle_step_success(RunId, Attempt, Value, Lease, Options1);
                        {error, Reason} ->
                            handle_step_failure(RunId, Attempt, Reason, Lease, Options1)
                    end
            end;
        {error, _} = Error ->
            Error
    end.

ensure_attempt_started(RunId, Workflow, Input, State, StepId, Lease) ->
    case maps:get(pending_attempt, State, undefined) of
        #{step_id := StepId} = Attempt ->
            {ok, Attempt, false, State};
        _ ->
            AttemptNo = maps:get(StepId, maps:get(attempt_counts, State), 0) + 1,
            StepVersion = Workflow:step_version(StepId),
            IdempotencyKey = Workflow:idempotency_key(RunId, StepId, Input),
            case append_event(
                   RunId,
                   maps:get(last_event_seq, State, 0),
                   Lease,
                   'attempt.started',
                   StepId,
                   StepVersion,
                   IdempotencyKey,
                   #{attempt => AttemptNo, owner_node => owner()}) of
                {ok, Event} ->
                    State1 = apply_runtime_event(State, Event),
                    case maybe_snapshot_state(RunId, State1, false) of
                        ok ->
                            {ok, maps:get(pending_attempt, State1), true, State1};
                        {error, _} = Error ->
                            Error
                    end;
                {error, _} = Error ->
                    Error
            end
    end.

execution_spec(RunId, Workflow, StepId, Input, Attempt) ->
    StepVersion = maps:get(step_version, Attempt),
    #{workflow => Workflow,
      step_id => StepId,
      step_version => StepVersion,
      input => Input,
      timeout_ms => Workflow:timeout_ms(StepId),
      context => execution_context(RunId, Attempt)}.

execution_context(RunId, Attempt) ->
    #{run_id => RunId,
      step_id => maps:get(step_id, Attempt),
      step_version => maps:get(step_version, Attempt),
      attempt => maps:get(attempt, Attempt),
      idempotency_key => maps:get(idempotency_key, Attempt)}.

execute_attempt(RunId, Workflow, Input, Attempt, Lease) ->
    StepId = maps:get(step_id, Attempt),
    ExecSpec = execution_spec(RunId, Workflow, StepId, Input, Attempt),
    beamtrail_runner_transition:execute_attempt(RunId, Lease, ExecSpec).

handle_step_success(RunId, Attempt, Value, Lease, Options) ->
    case maps:get(runner_state, Options, undefined) of
        undefined ->
            handle_step_success_reload(RunId, Attempt, Value, Lease, Options);
        State ->
            handle_step_success_state(RunId, State, Attempt, Value, Lease, Options)
    end.

handle_step_success_reload(RunId, Attempt, Value, Lease, Options) ->
    case beamtrail_state:load(RunId, beamtrail:storage()) of
        {ok, State} ->
            handle_step_success_state(RunId, State, Attempt, Value, Lease,
                                      Options#{runner_state => State});
        {error, _} = Error ->
            Error
    end.

handle_step_success_state(RunId, State, Attempt, Value, Lease, Options) ->
    StepId = maps:get(step_id, Attempt),
    case pending_attempt_expected_seq_from_state(State, Attempt) of
        {ok, ExpectedSeq} ->
            case append_event(
                   RunId,
                   ExpectedSeq,
                   Lease,
                   'step.succeeded',
                   StepId,
                   maps:get(step_version, Attempt),
                   maps:get(idempotency_key, Attempt),
                   #{result => Value}) of
                {ok, Event} ->
                    State1 = apply_runtime_event(State, Event),
                    case maybe_snapshot_state(RunId, State1, false) of
                        ok ->
                            case maps:get(runner_mode, Options, dispatch) of
                                finish ->
                                    {ok, State1};
                                _ ->
                                    dispatch_locked(RunId, State1, Lease,
                                                    Options#{runner_state := State1})
                            end;
                        {error, _} = Error ->
                            Error
                    end;
                {error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

handle_step_failure(RunId, Attempt, Reason, Lease, Options) ->
    case maps:get(runner_state, Options, undefined) of
        undefined ->
            handle_step_failure_reload(RunId, Attempt, Reason, Lease, Options);
        State ->
            handle_step_failure_state(RunId, State, Attempt, Reason, Lease, Options)
    end.

handle_step_failure_reload(RunId, Attempt, Reason, Lease, Options) ->
    case beamtrail_state:load(RunId, beamtrail:storage()) of
        {ok, State} ->
            handle_step_failure_state(RunId, State, Attempt, Reason, Lease,
                                      Options#{runner_state => State});
        {error, _} = Error ->
            Error
    end.

handle_step_failure_state(RunId, State, Attempt, Reason, Lease, Options) ->
    StepId = maps:get(step_id, Attempt),
    FailurePayload = #{reason => Reason, class => error_key(Reason),
                       attempt => maps:get(attempt, Attempt)},
    case pending_attempt_expected_seq_from_state(State, Attempt) of
        {ok, ExpectedSeq} ->
            case append_event(
                   RunId,
                   ExpectedSeq,
                   Lease,
                   'step.failed',
                   StepId,
                   maps:get(step_version, Attempt),
                   maps:get(idempotency_key, Attempt),
                   FailurePayload) of
                {ok, FailedEvent} ->
                    State1 = apply_runtime_event(State, FailedEvent),
                    case maybe_snapshot_state(RunId, State1, false) of
                        ok ->
                            Workflow = maps:get(workflow, State1),
                            Policy = Workflow:retry_policy(StepId),
                            case should_retry(Policy, Reason, maps:get(attempt, Attempt)) of
                                true ->
                                    schedule_retry_state(RunId, State1, Attempt, Reason,
                                                         Policy, Lease,
                                                         maps:get(event_seq, FailedEvent),
                                                         Options#{runner_state := State1});
                                false ->
                                    fail_workflow_state(RunId, State1, Attempt,
                                                        FailurePayload, Lease,
                                                        maps:get(event_seq, FailedEvent))
                            end;
                        {error, _} = Error ->
                            Error
                    end;
                {error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

schedule_retry_state(RunId, State, Attempt, Reason, Policy, Lease, ExpectedSeq, Options) ->
    StepId = maps:get(step_id, Attempt),
    BackoffMs = maps:get(backoff_ms, Policy, 0),
    NextRetryAt = erlang:system_time(millisecond) + BackoffMs,
    Payload =
        #{reason => Reason,
          class => error_key(Reason),
          attempt => maps:get(attempt, Attempt),
          next_retry_at => NextRetryAt},
    case append_event(
           RunId,
           ExpectedSeq,
           Lease,
           'retry.scheduled',
           StepId,
           maps:get(step_version, Attempt),
           maps:get(idempotency_key, Attempt),
           Payload) of
        {ok, RetryEvent} ->
            beamtrail_telemetry:execute([beamtrail, retry, scheduled], #{count => 1},
                                        #{run_id => RunId, step_id => StepId,
                                          next_retry_at => NextRetryAt}),
            State1 = apply_runtime_event(State, RetryEvent),
            case maybe_snapshot_state(RunId, State1, false) of
                ok ->
                    case BackoffMs of
                        0 ->
                            case maps:get(runner_mode, Options, dispatch) of
                                finish -> {ok, State1};
                                _ -> dispatch_retrying(RunId, State1, Lease,
                                                       Options#{runner_state := State1})
                            end;
                        _ ->
                            {ok, State1}
                    end;
                {error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

fail_workflow_state(RunId, State, Attempt, FailurePayload, Lease, ExpectedSeq) ->
    case append_event(
           RunId,
           ExpectedSeq,
           Lease,
           'workflow.failed',
           maps:get(step_id, Attempt),
           maps:get(step_version, Attempt),
           maps:get(idempotency_key, Attempt),
           FailurePayload) of
        {ok, FailedEvent} ->
            State1 = apply_runtime_event(State, FailedEvent),
            case maybe_snapshot_state(RunId, State1, true) of
                ok -> {ok, State1};
                {error, _} = Error -> Error
            end;
        {error, _} = Error ->
            Error
    end.

should_retry(Policy, Reason, AttemptNo) ->
    MaxAttempts = maps:get(max_attempts, Policy, 1),
    RetryableErrors = maps:get(retryable_errors, Policy, []),
    AttemptNo < MaxAttempts andalso lists:member(error_key(Reason), RetryableErrors).

error_key(Reason) when is_atom(Reason) ->
    Reason;
error_key(#{reason := Inner}) when is_atom(Inner) ->
    Inner;
error_key(_Reason) ->
    unknown.

pending_attempt_expected_seq_from_state(State, Attempt) ->
    case maps:get(pending_attempt, State, undefined) of
        #{step_id := StepId,
          attempt := AttemptNo,
          started_event_seq := StartedSeq} ->
            case StepId =:= maps:get(step_id, Attempt)
                andalso AttemptNo =:= maps:get(attempt, Attempt)
                andalso StartedSeq =:= maps:get(started_event_seq, Attempt) of
                true -> {ok, maps:get(last_event_seq, State, 0)};
                false -> {error, attempt_not_current}
            end;
        _ ->
            {error, attempt_not_current}
    end.

append_event(RunId, ExpectedSeq, Lease, EventType, StepId, StepVersion,
             IdempotencyKey, Payload) ->
    (beamtrail:storage()):append_event(RunId, ExpectedSeq, lease_fencing_token(Lease),
                                       EventType, StepId, StepVersion,
                                       IdempotencyKey, Payload).

maybe_snapshot_state(RunId, State, Force) ->
    beamtrail_state:maybe_snapshot(RunId, State, Force, beamtrail:storage()).

apply_runtime_event(State, Event) ->
    beamtrail_state:apply_event(State, Event).

lease_current(RunId, Lease) ->
    FencingToken = lease_fencing_token(Lease),
    Now = erlang:system_time(millisecond),
    case (beamtrail:storage()):read_lease(RunId) of
        {ok, #{fencing_token := FencingToken, lease_until := LeaseUntil}}
          when is_integer(FencingToken), LeaseUntil > Now ->
            true;
        _ ->
            false
    end.

owner() ->
    #{node => node(), pid => self()}.

lease_fencing_token(Lease) when is_map(Lease) ->
    maps:get(fencing_token, Lease, undefined);
lease_fencing_token(_) ->
    undefined.
