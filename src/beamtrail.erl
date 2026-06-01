-module(beamtrail).

-export([start/0, stop/0]).
-export([start_workflow/2, start_workflow/3, dispatch/1, dispatch/2,
         next_runner_action/2, execute_runner_attempt/1,
         finish_runner_attempt/4, recover_unfinished/0]).
-export([get_state/1, events/1, storage/0]).
-export([list_recoverable/0, list_recoverable/2,
         mark_recovery_requeued/1, mark_recovery_requeued_with_lease/1]).

-define(STORAGE_DEFAULT, beamtrail_memory_storage).
-define(SNAPSHOT_EVERY, 5).
-define(SNAPSHOT_REVISION, 1).

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
    case (storage()):append_event(
           RunId,
           0,
           undefined,
           'workflow.instance.created',
           undefined,
           undefined,
           undefined,
           #{workflow => Workflow, input => Input, steps => Steps}) of
        {ok, _Event} ->
            case maybe_snapshot(RunId, false) of
                ok -> start_workflow_dispatch(RunId, Options);
                {error, Reason} -> {error, {create_failed, RunId, Reason}}
            end;
        {error, Reason} ->
            {error, {create_failed, RunId, Reason}}
    end.

start_workflow_dispatch(RunId, Options) ->
    case maps:get(auto_dispatch, Options, true) of
        true ->
            case dispatch(RunId) of
                {ok, _State} -> {ok, RunId};
                {error, Reason} -> {error, {dispatch_failed, RunId, Reason}}
            end;
        false ->
            {ok, RunId}
    end.

dispatch(RunId) ->
    ok = ensure_storage(),
    case load_state(RunId) of
        {ok, State} ->
            case maps:get(status, State) of
                completed ->
                    {ok, State};
                failed ->
                    case maps:get(terminal, State, false) of
                        true -> {ok, State};
                        false -> dispatch_with_new_lease(RunId, State)
                    end;
                retrying ->
                    dispatch_retrying(RunId, State, none);
                _ ->
                    dispatch_with_new_lease(RunId, State)
            end;
        {error, _} = Error ->
            Error
    end.

dispatch(RunId, Lease) when is_map(Lease) ->
    dispatch_with_lease(RunId, Lease, #{lease_heartbeat => internal}).

%% Used by beamtrail_run. The active runner owns lease renewal, step process
%% lifetime, and timeout handling; this module still owns durable transitions.
next_runner_action(RunId, Lease) when is_map(Lease) ->
    dispatch_with_lease(RunId, Lease, #{runner_mode => prepare}).

execute_runner_attempt(ExecSpec) when is_map(ExecSpec) ->
    Workflow = maps:get(workflow, ExecSpec),
    safe_execute(Workflow,
                 maps:get(step_id, ExecSpec),
                 maps:get(step_version, ExecSpec),
                 maps:get(input, ExecSpec),
                 maps:get(context, ExecSpec)).

finish_runner_attempt(RunId, Lease, Attempt, Result) when is_map(Lease), is_map(Attempt) ->
    Options = #{runner_mode => finish},
    case Result of
        {ok, Value} ->
            handle_step_success(RunId, Attempt, Value, Lease, Options);
        {error, Reason} ->
            handle_step_failure(RunId, Attempt, Reason, Lease, Options)
    end.

dispatch_with_lease(RunId, Lease, Options) ->
    ok = ensure_storage(),
    case load_state(RunId) of
        {ok, State} -> dispatch_locked(RunId, State, Lease, Options);
        {error, _} = Error -> Error
    end.

recover_unfinished() ->
    ok = ensure_storage(),
    case (storage()):list_run_ids() of
        {ok, RunIds} ->
            Requeued =
                [RunId || RunId <- RunIds, recover_if_unfinished(RunId)],
            {ok, Requeued};
        {error, _} = Error ->
            Error
    end.

get_state(RunId) ->
    case load_state(RunId) of
        {ok, State} -> State;
        {error, _} = Error -> Error
    end.

load_state(RunId) ->
    ok = ensure_storage(),
    case (storage()):read_snapshot(RunId) of
        {ok, Snapshot} ->
            case snapshot_revision_compatible(Snapshot) of
                true -> load_state_from_snapshot(RunId, Snapshot);
                false -> load_state_from_events(RunId)
            end;
        not_found ->
            load_state_from_events(RunId);
        {error, _} = Error ->
            Error
    end.

load_state_from_snapshot(RunId, Snapshot) ->
    SnapshotSeq = maps:get(snapshot_seq, Snapshot),
    case (storage()):read_events(RunId, SnapshotSeq + 1, infinity) of
        {ok, TailEvents} ->
            State = beamtrail_reducer:from_snapshot_and_events(
                      maps:get(state, Snapshot), TailEvents),
            {ok, enrich_version_migration(State)};
        {error, _} = Error ->
            Error
    end.

load_state_from_events(RunId) ->
    case (storage()):events(RunId) of
        {ok, Events} ->
            {ok, enrich_version_migration(beamtrail_reducer:from_events(Events))};
        {error, _} = Error ->
            Error
    end.

snapshot_revision_compatible(Snapshot) ->
    maps:get(snapshot_revision, Snapshot, 0) =:= ?SNAPSHOT_REVISION.

events(RunId) ->
    ok = ensure_storage(),
    (storage()):events(RunId).

dispatch_with_new_lease(RunId, _State) ->
    case (storage()):acquire_lease(
           RunId, dispatch_owner(), beamtrail_lease_manager:default_ttl_ms()) of
        {ok, Lease} ->
            case load_state(RunId) of
                {ok, State} -> dispatch_locked(RunId, State, Lease);
                {error, _} = Error -> Error
            end;
        {error, leased} ->
            {error, leased};
        {error, _} = Error ->
            Error
    end.

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

dispatch_retrying(RunId, State, Lease) ->
    dispatch_retrying(RunId, State, Lease, #{lease_heartbeat => internal}).

dispatch_retrying(RunId, State, Lease, Options) ->
    Now = erlang:system_time(millisecond),
    case maps:get(next_retry_at, State, 0) =< Now of
        true ->
            case Lease of
                none -> dispatch_with_new_lease(RunId, State);
                _ -> dispatch_locked(RunId, State, Lease, Options)
            end;
        false -> {ok, State}
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
        {ok, _} ->
            with_snapshot(RunId, true, fun() -> {ok, get_state(RunId)} end);
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
                {ok, _Event} ->
                    with_snapshot(RunId, true, fun() -> {ok, get_state(RunId)} end);
                {error, _} = Error ->
                    Error
            end
    end.

run_step(RunId, State, StepId, Lease, Options) ->
    Workflow = maps:get(workflow, State),
    Input = maps:get(input, State),
    case ensure_attempt_started(RunId, Workflow, Input, State, StepId, Lease) of
        {ok, Attempt, StartedNow} ->
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
                          execution_spec(RunId, Workflow, StepId, Input, Attempt)}};
                _ ->
                    Result = execute_attempt(RunId, Workflow, Input, Attempt, Lease, Options),
                    case Result of
                        {ok, Value} ->
                            handle_step_success(RunId, Attempt, Value, Lease, Options);
                        {error, Reason} ->
                            handle_step_failure(RunId, Attempt, Reason, Lease, Options)
                    end
            end;
        {error, _} = Error ->
            Error
    end.

ensure_attempt_started(RunId, Workflow, Input, State, StepId, Lease) ->
    case maps:get(pending_attempt, State, undefined) of
        #{step_id := StepId} = Attempt ->
            {ok, Attempt, false};
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
                   #{attempt => AttemptNo, owner_node => dispatch_owner()}) of
                {ok, Event} ->
                    with_snapshot(
                      RunId,
                      false,
                      fun() ->
                              {ok,
                               #{step_id => StepId,
                                 step_version => StepVersion,
                                 idempotency_key => IdempotencyKey,
                                 attempt => AttemptNo,
                                 started_event_seq => maps:get(event_seq, Event),
                                 status => unknown},
                               true}
                      end);
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

execute_attempt(RunId, Workflow, Input, Attempt, Lease, _Options) ->
    StepId = maps:get(step_id, Attempt),
    ExecSpec = execution_spec(RunId, Workflow, StepId, Input, Attempt),
    Timeout = maps:get(timeout_ms, ExecSpec),
    Fun = fun() -> execute_runner_attempt(ExecSpec) end,
    run_with_timeout_and_lease_heartbeat(RunId, Lease, Fun, Timeout).

safe_execute(Workflow, StepId, StepVersion, Input, Context) ->
    try Workflow:execute(StepId, StepVersion, Input, Context) of
        {ok, _Value} = Ok -> Ok;
        {error, _Reason} = Error -> Error;
        Other -> {error, {bad_return, Other}}
    catch
        Class:Reason:_Stacktrace ->
            {error, #{class => Class, reason => Reason}}
    end.

run_with_timeout_and_lease_heartbeat(RunId, Lease, Fun, TimeoutMs) ->
    Parent = self(),
    ResultRef = make_ref(),
    StopRef = make_ref(),
    ExecPid = spawn(fun() -> Parent ! {ResultRef, execute, guarded_execute(Fun)} end),
    HeartbeatPid =
        spawn(fun() ->
                      ParentRef = erlang:monitor(process, Parent),
                      lease_heartbeat_loop(RunId, Lease, StopRef, ParentRef, Parent)
              end),
    TimeoutRef = arm_attempt_timeout(ResultRef, TimeoutMs),
    receive
        {ResultRef, execute, Result} ->
            cancel_attempt_timeout(TimeoutRef),
            HeartbeatPid ! {StopRef, stop},
            Result;
        {ResultRef, timeout} ->
            exit(ExecPid, kill),
            HeartbeatPid ! {StopRef, stop},
            {error, timeout};
        {StopRef, lease_lost, Reason} ->
            cancel_attempt_timeout(TimeoutRef),
            exit(ExecPid, kill),
            {error, #{class => lease_lost, reason => Reason}}
    end.

guarded_execute(Fun) ->
    try Fun()
    catch
        Class:Reason:_Stacktrace ->
            {error, #{class => Class, reason => Reason}}
    end.

arm_attempt_timeout(_ResultRef, infinity) ->
    undefined;
arm_attempt_timeout(ResultRef, TimeoutMs)
  when is_integer(TimeoutMs), TimeoutMs >= 0 ->
    erlang:send_after(TimeoutMs, self(), {ResultRef, timeout}).

cancel_attempt_timeout(undefined) ->
    ok;
cancel_attempt_timeout(TimeoutRef) ->
    erlang:cancel_timer(TimeoutRef),
    ok.

lease_heartbeat_loop(RunId, Lease, StopRef, ParentRef, Parent) ->
    Interval = lease_heartbeat_interval_ms(Lease),
    receive
        {StopRef, stop} ->
            erlang:demonitor(ParentRef, [flush]),
            ok;
        {'DOWN', ParentRef, process, _Pid, _Reason} ->
            ok
    after Interval ->
        case (storage()):renew_lease(RunId, lease_fencing_token(Lease),
                                     lease_ttl_ms(Lease)) of
            {ok, _Renewed} ->
                lease_heartbeat_loop(RunId, Lease, StopRef, ParentRef, Parent);
            {error, Reason} ->
                Parent ! {StopRef, lease_lost, Reason},
                ok
        end
    end.

handle_step_success(RunId, Attempt, Value, Lease, Options) ->
    StepId = maps:get(step_id, Attempt),
    case pending_attempt_expected_seq(RunId, Attempt) of
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
                {ok, _Event} ->
                    with_snapshot(
                      RunId,
                      false,
                      fun() ->
                              case maps:get(runner_mode, Options, dispatch) of
                                  finish -> {ok, get_state(RunId)};
                                  _ -> dispatch_locked(RunId, get_state(RunId), Lease, Options)
                              end
                      end);
                {error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

handle_step_failure(RunId, Attempt, Reason, Lease, Options) ->
    StepId = maps:get(step_id, Attempt),
    FailurePayload = #{reason => Reason, class => error_key(Reason), attempt => maps:get(attempt, Attempt)},
    case pending_attempt_expected_seq(RunId, Attempt) of
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
                    with_snapshot(
                      RunId,
                      false,
                      fun() ->
                              State = get_state(RunId),
                              Workflow = maps:get(workflow, State),
                              Policy = Workflow:retry_policy(StepId),
                              case should_retry(Policy, Reason, maps:get(attempt, Attempt)) of
                                  true ->
                                      schedule_retry(RunId, Attempt, Reason, Policy, Lease,
                                                     maps:get(event_seq, FailedEvent), Options);
                                  false ->
                                      fail_workflow(RunId, Attempt, FailurePayload, Lease,
                                                    maps:get(event_seq, FailedEvent))
                              end
                      end);
                {error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

schedule_retry(RunId, Attempt, Reason, Policy, Lease, ExpectedSeq, Options) ->
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
        {ok, _RetryEvent} ->
            beamtrail_telemetry:execute([beamtrail, retry, scheduled], #{count => 1},
                                        #{run_id => RunId, step_id => StepId, next_retry_at => NextRetryAt}),
            with_snapshot(
              RunId,
              false,
              fun() ->
                      case BackoffMs of
                          0 ->
                              case maps:get(runner_mode, Options, dispatch) of
                                  finish -> {ok, get_state(RunId)};
                                  _ -> dispatch_retrying(RunId, get_state(RunId), Lease, Options)
                              end;
                          _ -> {ok, get_state(RunId)}
                      end
              end);
        {error, _} = Error ->
            Error
    end.

fail_workflow(RunId, Attempt, FailurePayload, Lease, ExpectedSeq) ->
    case append_event(
           RunId,
           ExpectedSeq,
           Lease,
           'workflow.failed',
           maps:get(step_id, Attempt),
           maps:get(step_version, Attempt),
           maps:get(idempotency_key, Attempt),
           FailurePayload) of
        {ok, _FailedEvent} ->
            with_snapshot(RunId, true, fun() -> {ok, get_state(RunId)} end);
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

recover_if_unfinished(RunId) ->
    State = get_state(RunId),
    case recoverable(State) of
        true ->
            case mark_recovery_requeued_with_lease(RunId) of
                {ok, {requeued, Lease}} ->
                    case dispatch(RunId, Lease) of
                        {ok, _RecoveredState} ->
                            _ = maybe_snapshot(RunId, true),
                            true;
                        {error, {migration_required, _}} ->
                            false;
                        {error, _} ->
                            false
                    end;
                {ok, skipped} ->
                    false
            end;
        false ->
            false
    end.

list_recoverable() ->
    ok = ensure_storage(),
    case (storage()):list_run_ids() of
        {ok, RunIds} ->
            [RunId || RunId <- RunIds, recoverable(get_state(RunId))];
        {error, _} = Error ->
            Error
    end.

list_recoverable(Cursor, Limit) ->
    ok = ensure_storage(),
    case (storage()):list_run_ids(Cursor, Limit) of
        {ok, #{run_ids := RunIds} = Page} ->
            {ok, Page#{run_ids := [RunId || RunId <- RunIds,
                                            recoverable(get_state(RunId))]}};
        {error, _} = Error ->
            Error
    end.

%% Acquires a lease (best-effort) and appends a durable `recovery.requeued'
%% event to the log so the scanner's decision is observable in the inspector,
%% not only via telemetry counters. Idempotent on lease contention.
mark_recovery_requeued(RunId) ->
    case mark_recovery_requeued_with_lease(RunId) of
        {ok, {requeued, _Lease}} -> {ok, requeued};
        Other -> Other
    end.

mark_recovery_requeued_with_lease(RunId) ->
    ok = ensure_storage(),
    Mod = storage(),
    case Mod:acquire_lease(
           RunId, dispatch_owner(), beamtrail_lease_manager:default_ttl_ms()) of
        {ok, Lease} ->
            append_recovery_marker(Mod, RunId, 'recovery.requeued', Lease);
        {error, leased} ->
            ExistingLease = case Mod:read_lease(RunId) of
                                {ok, L} -> L;
                                _ -> undefined
                            end,
            append_recovery_marker(Mod, RunId, 'recovery.skipped', ExistingLease);
        {error, _} = Error ->
            Error
    end.

append_recovery_marker(Mod, RunId, EventType, LeaseInfo) ->
    Now = erlang:system_time(millisecond),
    ExpectedSeq = maps:get(last_event_seq, get_state(RunId), 0),
    RecoveredInMs = compute_recovered_in_ms(RunId, Now),
    Payload = #{requeued_at => Now,
                owner_node => dispatch_owner(),
                lease => LeaseInfo,
                recovered_in_ms => RecoveredInMs},
    FencingToken = case EventType of
                       'recovery.requeued' -> lease_fencing_token(LeaseInfo);
                       'recovery.skipped' -> undefined
                   end,
    case Mod:append_event(RunId, ExpectedSeq, FencingToken, EventType, undefined,
                          undefined, undefined, Payload) of
        {ok, _} ->
            TelemetryEvent = case EventType of
                                 'recovery.requeued' -> [beamtrail, recovery, requeued];
                                 'recovery.skipped' -> [beamtrail, recovery, skipped]
                             end,
            beamtrail_telemetry:execute(TelemetryEvent,
                                        #{count => 1},
                                        #{run_id => RunId, lease => LeaseInfo}),
            case EventType of
                'recovery.requeued' -> {ok, {requeued, LeaseInfo}};
                'recovery.skipped' -> {ok, skipped}
            end;
        {error, _} ->
            {ok, skipped}
    end.

%% recovered_in_ms = wall-clock gap between the earliest still-open
%% `attempt.started' (no closure observed) and this recovery event,
%% i.e. how long the longest-orphaned attempt has been stuck.
%% Returns `undefined' when there is no orphan attempt to recover.
compute_recovered_in_ms(RunId, Now) ->
    case (storage()):read_events(RunId, 1, infinity) of
        {ok, Events} ->
            case open_attempt_started_at(Events) of
                undefined -> undefined;
                Ts when is_integer(Ts) -> max(0, Now - Ts)
            end;
        _ -> undefined
    end.

%% Walk the log keeping at most one open attempt per step_id. A completion
%% event clears only the open attempt for its own step_id, so interleaved
%% step activity (today sequential, tomorrow possibly concurrent) does not
%% spuriously cancel another step's open attempt. Returns the earliest
%% still-open started_at, or undefined if nothing is open.
open_attempt_started_at(Events) ->
    Open = lists:foldl(fun open_step_fold/2, #{}, Events),
    case [T || T <- maps:values(Open)] of
        [] -> undefined;
        Ts -> lists:min(Ts)
    end.

open_step_fold(#{event_type := 'attempt.started',
                 step_id := StepId, occurred_at := T}, Acc) ->
    maps:put(StepId, T, Acc);
open_step_fold(#{event_type := Et, step_id := StepId}, Acc)
  when Et =:= 'step.succeeded'; Et =:= 'step.failed' ->
    maps:remove(StepId, Acc);
open_step_fold(#{event_type := Et}, _Acc)
  when Et =:= 'workflow.completed'; Et =:= 'workflow.failed';
       Et =:= 'recovery.requeued' ->
    #{};
open_step_fold(_, Acc) -> Acc.

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
    case load_state(RunId) of
        {ok, State} ->
            Seq = maps:get(last_event_seq, State, 0),
            ShouldWrite = Force orelse (Seq > 0 andalso Seq rem ?SNAPSHOT_EVERY =:= 0),
            case ShouldWrite of
                true ->
                    case (storage()):write_snapshot(RunId, State, Seq, ?SNAPSHOT_REVISION) of
                        ok ->
                            beamtrail_telemetry:execute([beamtrail, snapshot, written],
                                                        #{count => 1},
                                                        #{run_id => RunId,
                                                          snapshot_seq => Seq}),
                            ok;
                        {error, _} = Error ->
                            Error
                    end;
                false ->
                    ok
            end;
        {error, _} = Error ->
            Error
    end.

with_snapshot(RunId, Force, Continue) ->
    case maybe_snapshot(RunId, Force) of
        ok -> Continue();
        {error, _} = Error -> Error
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

append_event(RunId, ExpectedSeq, Lease, EventType, StepId, StepVersion,
             IdempotencyKey, Payload) ->
    (storage()):append_event(RunId, ExpectedSeq, lease_fencing_token(Lease),
                             EventType, StepId, StepVersion,
                             IdempotencyKey, Payload).

lease_fencing_token(Lease) when is_map(Lease) ->
    maps:get(fencing_token, Lease, undefined);
lease_fencing_token(_) ->
    undefined.

lease_ttl_ms(#{lease_until := Until, acquired_at := AcquiredAt})
  when is_integer(Until), is_integer(AcquiredAt), Until > AcquiredAt ->
    Until - AcquiredAt;
lease_ttl_ms(#{lease_until := Until}) when is_integer(Until) ->
    max(1, Until - erlang:system_time(millisecond));
lease_ttl_ms(_) ->
    beamtrail_lease_manager:default_ttl_ms().

lease_heartbeat_interval_ms(Lease) ->
    max(1, min(5000, lease_ttl_ms(Lease) div 3)).

lease_current(RunId, Lease) ->
    FencingToken = lease_fencing_token(Lease),
    Now = erlang:system_time(millisecond),
    case (storage()):read_lease(RunId) of
        {ok, #{fencing_token := FencingToken, lease_until := LeaseUntil}}
          when is_integer(FencingToken), LeaseUntil > Now ->
            true;
        _ ->
            false
    end.

dispatch_owner() ->
    #{node => node(), pid => self()}.

pending_attempt_expected_seq(RunId, Attempt) ->
    State = get_state(RunId),
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

ensure_storage() ->
    Mod = storage(),
    %% Only ad-hoc start for the in-memory adapter; durable adapters are
    %% expected to be started under the supervision tree with their own
    %% connection setup.
    case Mod =:= ?STORAGE_DEFAULT of
        true ->
            case whereis(Mod) of
                undefined ->
                    case Mod:start_link() of
                        {ok, _Pid} -> ok;
                        {error, {already_started, _Pid}} -> ok
                    end;
                _Pid -> ok
            end;
        false ->
            ok
    end.

storage() ->
    case application:get_env(beamtrail, storage_adapter) of
        {ok, M} when is_atom(M) -> M;
        _ -> ?STORAGE_DEFAULT
    end.

new_run_id() ->
    iolist_to_binary(io_lib:format("run-~p-~p", [erlang:system_time(millisecond),
                                                  erlang:unique_integer([positive, monotonic])])).
