-module(beamtrail_transition).

-export([owner/0, dispatch_locked/3, dispatch_locked/4,
         dispatch_retrying/4,
         finish_attempt/4, finish_attempt/5,
         complete_effect/4, complete_effect/5,
         cancel_run/4, park_run/4, resume_run/3]).

dispatch_locked(RunId, State, Lease) ->
    dispatch_locked(RunId, State, Lease, #{lease_heartbeat => internal}).

dispatch_locked(RunId, State, Lease, Options) ->
    case lease_current(RunId, Lease) of
        false ->
            {error, stale_lease};
        true ->
            case maps:get(terminal, State, false) of
                true ->
                    {ok, State};
                false ->
                    dispatch_unparked_locked(RunId, State, Lease, Options)
            end
    end.

dispatch_unparked_locked(RunId, State, Lease, Options) ->
    case maps:get(parked, State, false) of
        true ->
            {ok, State};
        false ->
            case maps:get(migration_required_for_version_change, State, false) of
                true ->
                    {error, {migration_required, State}};
                false ->
                    case fire_due_timers(RunId, State, Lease) of
                        {ok, State1} ->
                            dispatch_ready(RunId, State1, Lease,
                                           Options#{runner_state => State1});
                        {error, _} = Error ->
                            Error
                    end
            end
    end.

dispatch_retrying(RunId, State, Lease, Options) ->
    Now = erlang:system_time(millisecond),
    case maps:get(next_retry_at, State, 0) =< Now of
        true -> dispatch_locked(RunId, State, Lease, Options);
        false -> {ok, State}
    end.

finish_attempt(RunId, Lease, Attempt, Result) when is_map(Lease), is_map(Attempt) ->
    complete_effect(RunId, Lease, Attempt, Result).

finish_attempt(RunId, Lease, Attempt, Result, State)
  when is_map(Lease), is_map(Attempt), is_map(State) ->
    complete_effect(RunId, Lease, Attempt, Result, State).

complete_effect(RunId, Lease, Attempt, Result) when is_map(Lease), is_map(Attempt) ->
    Options = #{runner_mode => finish},
    finish_attempt_with_options(RunId, Lease, Attempt, Result, Options).

complete_effect(RunId, Lease, Attempt, Result, State)
  when is_map(Lease), is_map(Attempt), is_map(State) ->
    Options = #{runner_mode => finish, runner_state => State},
    finish_attempt_with_options(RunId, Lease, Attempt, Result, Options).

cancel_run(_RunId, #{terminal := true}, _Lease, _Reason) ->
    {error, terminal};
cancel_run(RunId, State, Lease, Reason) ->
    append_control_event(
      RunId, State, Lease, 'workflow.cancelled',
      #{reason => Reason,
        class => cancelled,
        cancelled_at => erlang:system_time(millisecond)},
      true).

park_run(_RunId, #{terminal := true}, _Lease, _Reason) ->
    {error, terminal};
park_run(_RunId, #{parked := true, parked_reason := Reason} = State,
         _Lease, Reason) ->
    {ok, State};
park_run(RunId, State, Lease, Reason) ->
    append_control_event(
      RunId, State, Lease, 'workflow.parked',
      #{reason => Reason,
        parked_at => erlang:system_time(millisecond)},
      false).

resume_run(_RunId, #{terminal := true}, _Lease) ->
    {error, terminal};
resume_run(_RunId, #{parked := false} = State, _Lease) ->
    {ok, State};
resume_run(RunId, State, Lease) ->
    append_control_event(
      RunId, State, Lease, 'workflow.resumed',
      #{resumed_at => erlang:system_time(millisecond)},
      false).

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
            dispatch_not_timed_out(RunId, State, Lease, Options)
    end.

dispatch_not_timed_out(RunId, State, Lease, Options) ->
    case maps:get(pending_attempt, State, undefined) of
        #{step_id := StepId} = Attempt ->
            StepInput = maps:get(step_input, Attempt, maps:get(input, State)),
            run_step(RunId, State, StepId, StepInput, Lease, Options);
        _ ->
            dispatch_decision(RunId, State, Lease, Options)
    end.

dispatch_decision(RunId, State, Lease, Options) ->
    case beamtrail_decider:decide(State) of
        {ok, Command} ->
            dispatch_command(RunId, State, Lease, Options, Command);
        {error, FailurePayload} ->
            append_decider_failure(RunId, State, Lease, FailurePayload)
    end.

dispatch_command(RunId, State, Lease, Options, Command) ->
    case Command of
        complete ->
            complete_if_needed(RunId, State, Lease);
        {complete, Result} ->
            complete_with_result(RunId, State, Lease, Result);
        {fail, Reason} ->
            fail_with_decider_reason(RunId, State, Lease, Reason);
        {wait, Reason} ->
            wait_for_signal(RunId, State, Lease, Reason);
        {sleep, TimerId, DelayMs} ->
            schedule_timer(RunId, State, Lease, Options, sleep, TimerId,
                           erlang:system_time(millisecond) + DelayMs);
        {sleep_until, TimerId, FireAtMs} ->
            schedule_timer(RunId, State, Lease, Options, sleep_until, TimerId,
                           FireAtMs);
        {run_step, StepId, StepInput} ->
            run_step(RunId, State, StepId, StepInput, Lease, Options)
    end.

fire_due_timers(RunId, State, Lease) ->
    Now = erlang:system_time(millisecond),
    Due = due_timers(State, Now),
    case Due of
        [] ->
            {ok, State};
        _ ->
            RemainingWake = next_wake_after_firing(State, Due),
            EventSpecs =
                [event_spec('timer.fired', undefined, undefined, undefined,
                            #{timer_id => TimerId,
                              fire_at_ms => FireAtMs,
                              fired_at => Now,
                              next_wake_at => RemainingWake})
                 || {TimerId, FireAtMs} <- Due],
            case append_events(RunId, maps:get(last_event_seq, State, 0),
                               Lease, EventSpecs) of
                {ok, Events} when length(Events) =:= length(EventSpecs) ->
                    State1 = apply_runtime_events(State, Events),
                    _ = maybe_snapshot_state(RunId, State1, false),
                    {ok, State1};
                {ok, Events} ->
                    {error, {unexpected_append_result, Events}};
                {error, _} = Error ->
                    Error
            end
    end.

due_timers(State, Now) ->
    Timers = maps:get(timers, State, #{}),
    lists:sort(
      [{TimerId, FireAtMs}
       || {TimerId, #{status := scheduled, fire_at_ms := FireAtMs}} <- maps:to_list(Timers),
          is_integer(FireAtMs),
          FireAtMs =< Now]).

next_wake_after_firing(State, Due) ->
    DueIds = maps:from_list([{TimerId, true} || {TimerId, _FireAtMs} <- Due]),
    Pending =
        [FireAtMs
         || {TimerId, #{status := scheduled, fire_at_ms := FireAtMs}}
                <- maps:to_list(maps:get(timers, State, #{})),
            maps:is_key(TimerId, DueIds) =:= false,
            is_integer(FireAtMs)],
    case Pending of
        [] -> undefined;
        _ -> lists:min(Pending)
    end.

schedule_timer(RunId, State, Lease, Options, SourceCommand, TimerId, FireAtMs) ->
    Timers = maps:get(timers, State, #{}),
    case maps:get(TimerId, Timers, undefined) of
        undefined ->
            append_timer_scheduled(RunId, State, Lease, Options, SourceCommand,
                                   TimerId, FireAtMs);
        #{status := scheduled, fire_at_ms := FireAtMs} ->
            dispatch_decision(RunId, State, Lease, Options);
        #{status := scheduled} ->
            append_decider_failure(
              RunId, State, Lease,
              #{reason => invalid_decider_command,
                class => invalid_decider_command,
                command => {SourceCommand, TimerId, FireAtMs},
                timer_error => conflicting_timer_deadline});
        #{status := fired} ->
            append_decider_failure(
              RunId, State, Lease,
              #{reason => invalid_decider_command,
                class => invalid_decider_command,
                command => {SourceCommand, TimerId, FireAtMs},
                timer_error => timer_already_fired})
    end.

append_timer_scheduled(RunId, State, Lease, Options, SourceCommand, TimerId,
                       FireAtMs) ->
    Now = erlang:system_time(millisecond),
    NextWakeAt = next_wake_after_scheduling(State, FireAtMs),
    Payload =
        #{timer_id => TimerId,
          fire_at_ms => FireAtMs,
          scheduled_at => Now,
          source_command => SourceCommand,
          next_wake_at => NextWakeAt},
    case append_event(RunId, maps:get(last_event_seq, State, 0), Lease,
                      'timer.scheduled', undefined, undefined, undefined,
                      Payload) of
        {ok, Event} ->
            State1 = apply_runtime_event(State, Event),
            _ = maybe_snapshot_state(RunId, State1, false),
            dispatch_decision(RunId, State1, Lease,
                              Options#{runner_state => State1});
        {error, _} = Error ->
            Error
    end.

next_wake_after_scheduling(State, FireAtMs) ->
    case maps:get(next_wake_at, State, undefined) of
        Existing when is_integer(Existing), Existing =< FireAtMs ->
            Existing;
        _ ->
            FireAtMs
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
            _ = maybe_snapshot_state(RunId, State1, true),
            {ok, State1};
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
                    _ = maybe_snapshot_state(RunId, State1, true),
                    {ok, State1};
                {error, _} = Error ->
                    Error
            end
    end.

complete_with_result(RunId, State, Lease, Result) ->
    append_terminal_decision(
      RunId, State, Lease, 'workflow.completed', undefined,
      #{completed_at => erlang:system_time(millisecond),
        result => Result}).

fail_with_decider_reason(RunId, State, Lease, Reason) ->
    append_terminal_decision(
      RunId, State, Lease, 'workflow.failed', undefined,
      #{reason => Reason,
        class => error_key(Reason),
        failed_at => erlang:system_time(millisecond)}).

wait_for_signal(_RunId, #{status := waiting, wait_reason := Reason} = State,
                _Lease, Reason) ->
    {ok, State};
wait_for_signal(RunId, State, Lease, Reason) ->
    append_event_decision(
      RunId, State, Lease, 'workflow.waiting', undefined,
      #{reason => Reason,
        waiting_since => erlang:system_time(millisecond)}).

append_decider_failure(RunId, State, Lease, FailurePayload) ->
    append_terminal_decision(
      RunId, State, Lease, 'workflow.failed', undefined, FailurePayload).

append_terminal_decision(RunId, State, Lease, EventType, StepId, Payload) ->
    append_event_decision(RunId, State, Lease, EventType, StepId, Payload, true).

append_event_decision(RunId, State, Lease, EventType, StepId, Payload) ->
    append_event_decision(RunId, State, Lease, EventType, StepId, Payload, false).

append_event_decision(RunId, State, Lease, EventType, StepId, Payload, ForceSnapshot) ->
    case append_event(
           RunId,
           maps:get(last_event_seq, State, 0),
           Lease,
           EventType,
           StepId,
           undefined,
           undefined,
           Payload) of
        {ok, Event} ->
            State1 = apply_runtime_event(State, Event),
            _ = maybe_snapshot_state(RunId, State1, ForceSnapshot),
            {ok, State1};
        {error, _} = Error ->
            Error
    end.

run_step(RunId, State, StepId, StepInput, Lease, Options) ->
    Workflow = maps:get(workflow, State),
    case ensure_attempt_started(RunId, Workflow, StepInput, State, StepId, Lease) of
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
                    case execution_spec(RunId, Workflow, StepId, StepInput, Attempt) of
                        {ok, Effect} ->
                            {ok, {execute, Attempt, Effect, State1}};
                        {error, {bad_workflow_callback, Callback, CallbackError}} ->
                            append_attempt_callback_failure(
                              RunId, State1, Attempt, Callback, CallbackError, Lease)
                    end;
                _ ->
                    Result = execute_attempt(RunId, Workflow, StepInput, Attempt, Lease),
                    case Result of
                        {ok, Value} ->
                            handle_step_success(RunId, Attempt, Value, Lease, Options1);
                        {error, {bad_workflow_callback, Callback, CallbackError}} ->
                            append_attempt_callback_failure(
                              RunId, State1, Attempt, Callback, CallbackError, Lease);
                        {error, Reason} ->
                            handle_step_failure(RunId, Attempt, Reason, Lease, Options1)
                    end
            end;
        {terminal, State1} ->
            {ok, State1};
        {error, _} = Error ->
            Error
    end.

ensure_attempt_started(RunId, Workflow, Input, State, StepId, Lease) ->
    case maps:get(pending_attempt, State, undefined) of
        #{step_id := StepId} = Attempt ->
            {ok, Attempt, false, State};
        _ ->
            AttemptNo = maps:get(StepId, maps:get(attempt_counts, State), 0) + 1,
            StepInput = Input,
            case step_metadata(RunId, Workflow, StepId, StepInput) of
                {ok, StepVersion, IdempotencyKey} ->
                    EventSpecs =
                        [event_spec('attempt.started', StepId, StepVersion,
                                    IdempotencyKey,
                                    #{attempt => AttemptNo,
                                      owner_node => owner(),
                                      step_input => StepInput}),
                         activity_event_spec('activity.scheduled', StepId,
                                             StepVersion, IdempotencyKey,
                                             AttemptNo),
                         activity_event_spec('activity.started', StepId,
                                             StepVersion, IdempotencyKey,
                                             AttemptNo)],
                    case append_events(
                           RunId, maps:get(last_event_seq, State, 0), Lease,
                           EventSpecs) of
                        {ok, Events} when length(Events) =:= length(EventSpecs) ->
                            State1 = apply_runtime_events(State, Events),
                            _ = maybe_snapshot_state(RunId, State1, false),
                            {ok, maps:get(pending_attempt, State1), true, State1};
                        {ok, Events} ->
                            {error, {unexpected_append_result, Events}};
                        {error, _} = Error ->
                            Error
                    end;
                {error, {Callback, CallbackError}} ->
                    append_pre_attempt_callback_failure(
                      RunId, State, StepId, Callback, CallbackError, Lease)
            end
    end.

execution_spec(RunId, Workflow, StepId, Input, Attempt) ->
    StepVersion = maps:get(step_version, Attempt),
    StepInput = maps:get(step_input, Attempt, Input),
    case safe_workflow_callback(timeout_ms, fun() -> Workflow:timeout_ms(StepId) end) of
        {ok, TimeoutMs} ->
            {ok, beamtrail_effect:call_step(Workflow,
                                            StepId,
                                            StepVersion,
                                            StepInput,
                                            TimeoutMs,
                                            execution_context(RunId, Attempt))};
        {error, CallbackError} ->
            {error, {bad_workflow_callback, timeout_ms, CallbackError}}
    end.

execution_context(RunId, Attempt) ->
    #{run_id => RunId,
      step_id => maps:get(step_id, Attempt),
      step_version => maps:get(step_version, Attempt),
      attempt => maps:get(attempt, Attempt),
      idempotency_key => maps:get(idempotency_key, Attempt)}.

execute_attempt(RunId, Workflow, Input, Attempt, Lease) ->
    StepId = maps:get(step_id, Attempt),
    case execution_spec(RunId, Workflow, StepId, Input, Attempt) of
        {ok, Effect} ->
            beamtrail_executor:execute_attempt(RunId, Lease, Effect);
        {error, {bad_workflow_callback, Callback, CallbackError}} ->
            {error, {bad_workflow_callback, Callback, CallbackError}}
    end.

handle_step_success(RunId, Attempt, Value, Lease, Options) ->
    case maps:get(runner_state, Options, undefined) of
        undefined ->
            handle_step_success_reload(RunId, Attempt, Value, Lease, Options);
        State ->
            handle_step_success_state(RunId, State, Attempt, Value, Lease, Options)
    end.

handle_step_success_reload(RunId, Attempt, Value, Lease, Options) ->
    case beamtrail_state:load(RunId, beamtrail_config:storage()) of
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
            EventSpecs =
                [event_spec('step.succeeded', StepId,
                            maps:get(step_version, Attempt),
                            maps:get(idempotency_key, Attempt),
                            #{result => Value}),
                 activity_event_spec('activity.succeeded', StepId, Attempt)],
            case append_events(RunId, ExpectedSeq, Lease, EventSpecs) of
                {ok, Events} when length(Events) =:= length(EventSpecs) ->
                    State1 = apply_runtime_events(State, Events),
                    _ = maybe_snapshot_state(RunId, State1, false),
                    case maps:get(runner_mode, Options, dispatch) of
                        finish ->
                            {ok, State1};
                        _ ->
                            dispatch_locked(RunId, State1, Lease,
                                            Options#{runner_state := State1})
                    end;
                {ok, Events} ->
                    {error, {unexpected_append_result, Events}};
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
    case beamtrail_state:load(RunId, beamtrail_config:storage()) of
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
            Workflow = maps:get(workflow, State),
            case safe_retry_policy(Workflow, StepId) of
                {ok, Policy} ->
                    case should_retry(Policy, Reason, maps:get(attempt, Attempt)) of
                        true ->
                            append_failed_retry_decision(
                              RunId, State, Attempt, Reason, Policy, Lease,
                              ExpectedSeq, Options, FailurePayload);
                        false ->
                            append_failed_terminal_decision(
                              RunId, State, Attempt, FailurePayload, Lease,
                              ExpectedSeq)
                    end;
                {error, PolicyError} ->
                    append_failed_terminal_decision(
                      RunId, State, Attempt,
                      FailurePayload#{retry_policy_error => PolicyError},
                      Lease, ExpectedSeq)
            end;
        {error, _} = Error ->
            Error
    end.

append_failed_retry_decision(RunId, State, Attempt, Reason, Policy, Lease,
                             ExpectedSeq, Options, FailurePayload) ->
    StepId = maps:get(step_id, Attempt),
    BackoffMs = maps:get(backoff_ms, Policy, 0),
    NextRetryAt = erlang:system_time(millisecond) + BackoffMs,
    RetryPayload =
        #{reason => Reason,
          class => error_key(Reason),
          attempt => maps:get(attempt, Attempt),
          next_retry_at => NextRetryAt},
    EventSpecs =
        [event_spec('step.failed', StepId, maps:get(step_version, Attempt),
                    maps:get(idempotency_key, Attempt), FailurePayload),
         activity_failed_event_spec(StepId, Attempt, FailurePayload),
         event_spec('retry.scheduled', StepId, maps:get(step_version, Attempt),
                    maps:get(idempotency_key, Attempt), RetryPayload)],
    case append_events(RunId, ExpectedSeq, Lease, EventSpecs) of
        {ok, [_FailedEvent, _ActivityFailedEvent, _RetryEvent] = Events} ->
            beamtrail_telemetry:execute([beamtrail, retry, scheduled], #{count => 1},
                                        #{run_id => RunId, step_id => StepId,
                                          next_retry_at => NextRetryAt}),
            State1 = apply_runtime_events(State, Events),
            _ = maybe_snapshot_state(RunId, State1, false),
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
        {ok, Events} ->
            {error, {unexpected_append_result, Events}};
        {error, _} = Error ->
            Error
    end.

append_failed_terminal_decision(RunId, State, Attempt, FailurePayload, Lease,
                                ExpectedSeq) ->
    StepId = maps:get(step_id, Attempt),
    EventSpecs =
        [event_spec('step.failed', StepId, maps:get(step_version, Attempt),
                    maps:get(idempotency_key, Attempt), FailurePayload),
         activity_failed_event_spec(StepId, Attempt, FailurePayload),
         event_spec('workflow.failed', StepId, maps:get(step_version, Attempt),
                    maps:get(idempotency_key, Attempt), FailurePayload)],
    case append_events(RunId, ExpectedSeq, Lease, EventSpecs) of
        {ok, [_StepFailedEvent, _ActivityFailedEvent, _WorkflowFailedEvent] = Events} ->
            State1 = apply_runtime_events(State, Events),
            _ = maybe_snapshot_state(RunId, State1, true),
            {ok, State1};
        {ok, Events} ->
            {error, {unexpected_append_result, Events}};
        {error, _} = Error ->
            Error
    end.

step_metadata(RunId, Workflow, StepId, Input) ->
    case safe_workflow_callback(step_version,
                                fun() -> Workflow:step_version(StepId) end) of
        {ok, StepVersion} ->
            case safe_workflow_callback(
                   idempotency_key,
                   fun() -> Workflow:idempotency_key(RunId, StepId, Input) end) of
                {ok, IdempotencyKey} ->
                    {ok, StepVersion, IdempotencyKey};
                {error, CallbackError} ->
                    {error, {idempotency_key, CallbackError}}
            end;
        {error, CallbackError} ->
            {error, {step_version, CallbackError}}
    end.

append_pre_attempt_callback_failure(RunId, State, StepId, Callback,
                                    CallbackError, Lease) ->
    Payload = callback_failure_payload(Callback, CallbackError),
    EventSpecs =
        [event_spec('workflow.failed', StepId, undefined, undefined, Payload)],
    case append_events(RunId, maps:get(last_event_seq, State, 0), Lease, EventSpecs) of
        {ok, [Event]} ->
            State1 = apply_runtime_event(State, Event),
            _ = maybe_snapshot_state(RunId, State1, true),
            {terminal, State1};
        {ok, Events} ->
            {error, {unexpected_append_result, Events}};
        {error, _} = Error ->
            Error
    end.

append_attempt_callback_failure(RunId, State, Attempt, Callback, CallbackError,
                                Lease) ->
    StepId = maps:get(step_id, Attempt),
    Payload =
        (callback_failure_payload(Callback, CallbackError))#{
          attempt => maps:get(attempt, Attempt)},
    EventSpecs =
        [event_spec('step.failed', StepId, maps:get(step_version, Attempt),
                    maps:get(idempotency_key, Attempt), Payload),
         activity_failed_event_spec(StepId, Attempt, Payload),
         event_spec('workflow.failed', StepId, maps:get(step_version, Attempt),
                    maps:get(idempotency_key, Attempt), Payload)],
    case append_events(RunId, maps:get(last_event_seq, State, 0), Lease, EventSpecs) of
        {ok, [_StepFailedEvent, _ActivityFailedEvent, _WorkflowFailedEvent] = Events} ->
            State1 = apply_runtime_events(State, Events),
            _ = maybe_snapshot_state(RunId, State1, true),
            {ok, State1};
        {ok, Events} ->
            {error, {unexpected_append_result, Events}};
        {error, _} = Error ->
            Error
    end.

append_control_event(RunId, State, Lease, EventType, Payload, ForceSnapshot) ->
    case append_event(RunId, maps:get(last_event_seq, State, 0), Lease,
                      EventType, undefined, undefined, undefined, Payload) of
        {ok, Event} ->
            State1 = apply_runtime_event(State, Event),
            _ = maybe_snapshot_state(RunId, State1, ForceSnapshot),
            {ok, State1};
        {error, _} = Error ->
            Error
    end.

callback_failure_payload(Callback, CallbackError) ->
    #{reason => bad_workflow_callback,
      class => bad_workflow_callback,
      callback => Callback,
      callback_error => CallbackError}.

safe_workflow_callback(Callback, Fun) ->
    try Fun() of
        Value ->
            {ok, Value}
    catch
        Class:Reason:_Stacktrace ->
            {error, #{callback => Callback, class => Class, reason => Reason}}
    end.

should_retry(Policy, Reason, AttemptNo) ->
    MaxAttempts = maps:get(max_attempts, Policy, 1),
    RetryableErrors = maps:get(retryable_errors, Policy, []),
    AttemptNo < MaxAttempts andalso lists:member(error_key(Reason), RetryableErrors).

safe_retry_policy(Workflow, StepId) ->
    try Workflow:retry_policy(StepId) of
        Policy when is_map(Policy) ->
            validate_retry_policy(Policy);
        Other ->
            {error, #{callback => retry_policy, reason => {bad_return, Other}}}
    catch
        Class:Reason:_Stacktrace ->
            {error, #{callback => retry_policy, class => Class, reason => Reason}}
    end.

validate_retry_policy(Policy) ->
    MaxAttempts = maps:get(max_attempts, Policy, 1),
    BackoffMs = maps:get(backoff_ms, Policy, 0),
    RetryableErrors = maps:get(retryable_errors, Policy, []),
    case valid_retry_policy(MaxAttempts, BackoffMs, RetryableErrors) of
        true ->
            {ok, Policy#{max_attempts => MaxAttempts,
                         backoff_ms => BackoffMs,
                         retryable_errors => RetryableErrors}};
        false ->
            {error, #{callback => retry_policy,
                      reason => {bad_policy, Policy}}}
    end.

valid_retry_policy(MaxAttempts, BackoffMs, RetryableErrors) ->
    is_integer(MaxAttempts) andalso MaxAttempts >= 1
        andalso is_integer(BackoffMs) andalso BackoffMs >= 0
        andalso is_list(RetryableErrors)
        andalso lists:all(fun is_atom/1, RetryableErrors).

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
    (beamtrail_config:storage()):append_event(
      RunId, ExpectedSeq, beamtrail_lease_manager:fencing_token(Lease),
      EventType, StepId, StepVersion, IdempotencyKey, Payload).

append_events(RunId, ExpectedSeq, Lease, EventSpecs) ->
    (beamtrail_config:storage()):append_events(
      RunId, ExpectedSeq, beamtrail_lease_manager:fencing_token(Lease),
      EventSpecs).

activity_event_spec(EventType, StepId, Attempt) ->
    activity_event_spec(EventType,
                        StepId,
                        maps:get(step_version, Attempt),
                        maps:get(idempotency_key, Attempt),
                        maps:get(attempt, Attempt)).

activity_event_spec(EventType, StepId, StepVersion, IdempotencyKey, AttemptNo) ->
    event_spec(EventType, StepId, StepVersion, IdempotencyKey,
               activity_payload(EventType, AttemptNo)).

activity_failed_event_spec(StepId, Attempt, FailurePayload) ->
    event_spec('activity.failed',
               StepId,
               maps:get(step_version, Attempt),
               maps:get(idempotency_key, Attempt),
               activity_payload('activity.failed',
                                maps:get(attempt, Attempt),
                                FailurePayload)).

activity_payload(EventType, AttemptNo) ->
    activity_payload(EventType, AttemptNo, #{}).

activity_payload(EventType, AttemptNo, Extra) ->
    Extra#{activity_type => step,
           activity_status => beamtrail_activity:status(EventType),
           attempt => AttemptNo}.

event_spec(EventType, StepId, StepVersion, IdempotencyKey, Payload) ->
    #{event_type => EventType,
      step_id => StepId,
      step_version => StepVersion,
      idempotency_key => IdempotencyKey,
      payload => Payload}.

maybe_snapshot_state(RunId, State, Force) ->
    beamtrail_state:maybe_snapshot(RunId, State, Force, beamtrail_config:storage()).

apply_runtime_event(State, Event) ->
    beamtrail_state:apply_event(State, Event).

apply_runtime_events(State, Events) ->
    lists:foldl(fun(Event, Acc) -> apply_runtime_event(Acc, Event) end,
                State,
                Events).

lease_current(RunId, Lease) ->
    FencingToken = beamtrail_lease_manager:fencing_token(Lease),
    Now = erlang:system_time(millisecond),
    case (beamtrail_config:storage()):read_lease(RunId) of
        {ok, #{fencing_token := FencingToken, lease_until := LeaseUntil}}
          when is_integer(FencingToken), LeaseUntil > Now ->
            true;
        _ ->
            false
    end.

owner() ->
    #{node => node(), pid => self()}.
