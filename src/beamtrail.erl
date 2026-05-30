-module(beamtrail).

-export([start/0, stop/0]).
-export([start_workflow/2, start_workflow/3, dispatch/1, recover_unfinished/0]).
-export([get_state/1, events/1]).

-define(STORAGE, beamtrail_memory_storage).
-define(SNAPSHOT_EVERY, 5).

start() ->
    application:ensure_all_started(beamtrail).

stop() ->
    application:stop(beamtrail).

start_workflow(Workflow, Input) ->
    start_workflow(Workflow, Input, #{}).

start_workflow(Workflow, Input, Options) ->
    ok = ensure_storage(),
    RunId = maps:get(run_id, Options, new_run_id()),
    Steps = Workflow:steps(Input),
    {ok, _Event} =
        ?STORAGE:append_event(
          RunId,
          'workflow.instance.created',
          undefined,
          undefined,
          undefined,
          #{workflow => Workflow, input => Input, steps => Steps}),
    maybe_snapshot(RunId, false),
    case maps:get(auto_dispatch, Options, true) of
        true ->
            {ok, _State} = dispatch(RunId),
            {ok, RunId};
        false ->
            {ok, RunId}
    end.

dispatch(RunId) ->
    ok = ensure_storage(),
    State = get_state(RunId),
    case maps:get(status, State) of
        completed ->
            {ok, State};
        failed ->
            case maps:get(terminal, State, false) of
                true -> {ok, State};
                false -> dispatch_ready(RunId, State)
            end;
        retrying ->
            dispatch_retrying(RunId, State);
        _ ->
            dispatch_ready(RunId, State)
    end.

recover_unfinished() ->
    ok = ensure_storage(),
    RunIds = ?STORAGE:list_run_ids(),
    Requeued =
        [RunId || RunId <- RunIds, recover_if_unfinished(RunId)],
    {ok, Requeued}.

get_state(RunId) ->
    ok = ensure_storage(),
    State =
        case ?STORAGE:read_snapshot(RunId) of
            {ok, Snapshot} ->
                SnapshotSeq = maps:get(snapshot_seq, Snapshot),
                {ok, TailEvents} = ?STORAGE:read_events(RunId, SnapshotSeq + 1, infinity),
                beamtrail_reducer:from_snapshot_and_events(maps:get(state, Snapshot), TailEvents);
            not_found ->
                {ok, Events} = ?STORAGE:events(RunId),
                beamtrail_reducer:from_events(Events)
        end,
    enrich_version_migration(State).

events(RunId) ->
    ok = ensure_storage(),
    ?STORAGE:events(RunId).

dispatch_retrying(RunId, State) ->
    Now = erlang:system_time(millisecond),
    case maps:get(next_retry_at, State, 0) =< Now of
        true -> dispatch_ready(RunId, State);
        false -> {ok, State}
    end.

dispatch_ready(RunId, State) ->
    case maps:get(current_step, State) of
        undefined ->
            complete_if_needed(RunId, State);
        StepId ->
            run_step(RunId, State, StepId)
    end.

complete_if_needed(RunId, State) ->
    case maps:get(status, State) of
        completed ->
            {ok, State};
        _ ->
            {ok, _Event} =
                ?STORAGE:append_event(
                  RunId,
                  'workflow.completed',
                  undefined,
                  undefined,
                  undefined,
                  #{completed_at => erlang:system_time(millisecond)}),
            maybe_snapshot(RunId, true),
            {ok, get_state(RunId)}
    end.

run_step(RunId, State, StepId) ->
    Workflow = maps:get(workflow, State),
    Input = maps:get(input, State),
    {Attempt, StartedNow} = ensure_attempt_started(RunId, Workflow, Input, State, StepId),
    case StartedNow of
        true ->
            beamtrail_telemetry:execute([beamtrail, attempt, started], #{count => 1},
                                        #{run_id => RunId, step_id => StepId});
        false ->
            ok
    end,
    Result = execute_attempt(RunId, Workflow, Input, Attempt),
    case Result of
        {ok, Value} ->
            handle_step_success(RunId, State, Attempt, Value);
        {error, Reason} ->
            handle_step_failure(RunId, State, Attempt, Reason)
    end.

ensure_attempt_started(RunId, Workflow, Input, State, StepId) ->
    case maps:get(pending_attempt, State, undefined) of
        #{step_id := StepId} = Attempt ->
            {Attempt, false};
        _ ->
            AttemptNo = maps:get(StepId, maps:get(attempt_counts, State), 0) + 1,
            StepVersion = Workflow:step_version(StepId),
            IdempotencyKey = Workflow:idempotency_key(RunId, StepId, Input),
            {ok, Event} =
                ?STORAGE:append_event(
                  RunId,
                  'attempt.started',
                  StepId,
                  StepVersion,
                  IdempotencyKey,
                  #{attempt => AttemptNo, owner_node => node()}),
            maybe_snapshot(RunId, false),
            {#{step_id => StepId,
               step_version => StepVersion,
               idempotency_key => IdempotencyKey,
               attempt => AttemptNo,
               started_event_seq => maps:get(event_seq, Event),
               status => unknown},
             true}
    end.

execute_attempt(RunId, Workflow, Input, Attempt) ->
    StepId = maps:get(step_id, Attempt),
    StepVersion = maps:get(step_version, Attempt),
    Context =
        #{run_id => RunId,
          step_id => StepId,
          step_version => StepVersion,
          attempt => maps:get(attempt, Attempt),
          idempotency_key => maps:get(idempotency_key, Attempt)},
    Timeout = Workflow:timeout_ms(StepId),
    run_with_timeout(fun() -> safe_execute(Workflow, StepId, StepVersion, Input, Context) end, Timeout).

safe_execute(Workflow, StepId, StepVersion, Input, Context) ->
    try Workflow:execute(StepId, StepVersion, Input, Context) of
        {ok, _Value} = Ok -> Ok;
        {error, _Reason} = Error -> Error;
        Other -> {error, {bad_return, Other}}
    catch
        Class:Reason:_Stacktrace ->
            {error, #{class => Class, reason => Reason}}
    end.

run_with_timeout(Fun, infinity) ->
    Fun();
run_with_timeout(Fun, TimeoutMs) when is_integer(TimeoutMs), TimeoutMs >= 0 ->
    Parent = self(),
    Ref = make_ref(),
    Pid = spawn(fun() -> Parent ! {Ref, Fun()} end),
    receive
        {Ref, Result} ->
            Result
    after TimeoutMs ->
        exit(Pid, kill),
        {error, timeout}
    end.

handle_step_success(RunId, _State, Attempt, Value) ->
    StepId = maps:get(step_id, Attempt),
    {ok, _Event} =
        ?STORAGE:append_event(
          RunId,
          'step.succeeded',
          StepId,
          maps:get(step_version, Attempt),
          maps:get(idempotency_key, Attempt),
          #{result => Value}),
    maybe_snapshot(RunId, false),
    dispatch(RunId).

handle_step_failure(RunId, State, Attempt, Reason) ->
    StepId = maps:get(step_id, Attempt),
    FailurePayload = #{reason => Reason, class => error_key(Reason), attempt => maps:get(attempt, Attempt)},
    {ok, _FailedEvent} =
        ?STORAGE:append_event(
          RunId,
          'step.failed',
          StepId,
          maps:get(step_version, Attempt),
          maps:get(idempotency_key, Attempt),
          FailurePayload),
    maybe_snapshot(RunId, false),
    Workflow = maps:get(workflow, State),
    Policy = Workflow:retry_policy(StepId),
    case should_retry(Policy, Reason, maps:get(attempt, Attempt)) of
        true ->
            schedule_retry(RunId, State, Attempt, Reason, Policy);
        false ->
            fail_workflow(RunId, Attempt, FailurePayload)
    end.

schedule_retry(RunId, _State, Attempt, Reason, Policy) ->
    StepId = maps:get(step_id, Attempt),
    BackoffMs = maps:get(backoff_ms, Policy, 0),
    NextRetryAt = erlang:system_time(millisecond) + BackoffMs,
    Payload =
        #{reason => Reason,
          class => error_key(Reason),
          attempt => maps:get(attempt, Attempt),
          next_retry_at => NextRetryAt},
    {ok, _RetryEvent} =
        ?STORAGE:append_event(
          RunId,
          'retry.scheduled',
          StepId,
          maps:get(step_version, Attempt),
          maps:get(idempotency_key, Attempt),
          Payload),
    beamtrail_telemetry:execute([beamtrail, retry, scheduled], #{count => 1},
                                #{run_id => RunId, step_id => StepId, next_retry_at => NextRetryAt}),
    maybe_snapshot(RunId, false),
    case BackoffMs of
        0 -> dispatch(RunId);
        _ -> {ok, get_state(RunId)}
    end.

fail_workflow(RunId, Attempt, FailurePayload) ->
    {ok, _FailedEvent} =
        ?STORAGE:append_event(
          RunId,
          'workflow.failed',
          maps:get(step_id, Attempt),
          maps:get(step_version, Attempt),
          maps:get(idempotency_key, Attempt),
          FailurePayload),
    maybe_snapshot(RunId, true),
    {ok, get_state(RunId)}.

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

recover_if_unfinished(RunId) ->
    State = get_state(RunId),
    case recoverable(State) of
        true ->
            {ok, _RecoveredState} = dispatch(RunId),
            beamtrail_telemetry:execute([beamtrail, recovery, requeued], #{count => 1}, #{run_id => RunId}),
            maybe_snapshot(RunId, true),
            true;
        false ->
            false
    end.

recoverable(State) ->
    case maps:get(status, State) of
        completed ->
            false;
        failed ->
            maps:get(terminal, State, false) =/= true;
        retrying ->
            maps:get(next_retry_at, State, 0) =< erlang:system_time(millisecond);
        _ ->
            true
    end.

maybe_snapshot(RunId, Force) ->
    State = get_state(RunId),
    Seq = maps:get(last_event_seq, State, 0),
    ShouldWrite = Force orelse (Seq > 0 andalso Seq rem ?SNAPSHOT_EVERY =:= 0),
    case ShouldWrite of
        true ->
            ok = ?STORAGE:write_snapshot(RunId, State, Seq, 1),
            beamtrail_telemetry:execute([beamtrail, snapshot, written], #{count => 1},
                                        #{run_id => RunId, snapshot_seq => Seq});
        false ->
            ok
    end.

enrich_version_migration(State = #{workflow := undefined}) ->
    State#{migration_required_for_version_change => false};
enrich_version_migration(State) ->
    Workflow = maps:get(workflow, State),
    Attempts = maps:get(attempts, State, []),
    MigrationRequired =
        lists:any(
          fun(Attempt) ->
                  StepId = maps:get(step_id, Attempt),
                  RecordedVersion = maps:get(step_version, Attempt),
                  current_step_version(Workflow, StepId) =/= RecordedVersion
          end,
          Attempts),
    State#{migration_required_for_version_change => MigrationRequired}.

current_step_version(Workflow, StepId) ->
    try Workflow:step_version(StepId) of
        Version -> Version
    catch
        _:_ -> undefined
    end.

ensure_storage() ->
    case whereis(?STORAGE) of
        undefined ->
            case ?STORAGE:start_link() of
                {ok, _Pid} -> ok;
                {error, {already_started, _Pid}} -> ok
            end;
        _Pid ->
            ok
    end.

new_run_id() ->
    iolist_to_binary(io_lib:format("run-~p-~p", [erlang:system_time(millisecond),
                                                  erlang:unique_integer([positive, monotonic])])).
