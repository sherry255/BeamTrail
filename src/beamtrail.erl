-module(beamtrail).

-export([start/0, stop/0]).
-export([start_workflow/2, start_workflow/3, dispatch/1, dispatch/2,
         load_runner_state/1, next_runner_action/2, next_runner_action/3,
         finish_runner_attempt/4, finish_runner_attempt/5,
         recover_unfinished/0]).
-export([get_state/1, events/1, storage/0]).
-export([list_recoverable/0, list_recoverable/2,
         mark_recovery_requeued/1, mark_recovery_requeued_with_lease/1]).

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

%% Runner-facing compatibility API. The active runner owns lease renewal,
%% step process lifetime, and timeout handling.
load_runner_state(RunId) ->
    load_state(RunId).

next_runner_action(RunId, Lease) when is_map(Lease) ->
    case dispatch_with_lease(RunId, Lease, #{runner_mode => prepare}) of
        {ok, {execute, Attempt, ExecSpec, _State}} ->
            {ok, {execute, Attempt, ExecSpec}};
        Other ->
            Other
    end.

next_runner_action(RunId, Lease, State) when is_map(Lease), is_map(State) ->
    beamtrail_transition:dispatch_locked(RunId, State, Lease,
                                         #{runner_mode => prepare,
                                           runner_state => State}).

finish_runner_attempt(RunId, Lease, Attempt, Result) when is_map(Lease), is_map(Attempt) ->
    beamtrail_transition:finish_attempt(RunId, Lease, Attempt, Result).

finish_runner_attempt(RunId, Lease, Attempt, Result, State)
  when is_map(Lease), is_map(Attempt), is_map(State) ->
    beamtrail_transition:finish_attempt(RunId, Lease, Attempt, Result, State).

dispatch_with_lease(RunId, Lease, Options) ->
    ok = ensure_storage(),
    case load_state(RunId) of
        {ok, State} -> beamtrail_transition:dispatch_locked(RunId, State, Lease, Options);
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
    beamtrail_state:load(RunId, storage()).

events(RunId) ->
    ok = ensure_storage(),
    (storage()):events(RunId).

dispatch_with_new_lease(RunId, _State) ->
    case (storage()):acquire_lease(
           RunId, beamtrail_transition:owner(), beamtrail_lease_manager:default_ttl_ms()) of
        {ok, Lease} ->
            case load_state(RunId) of
                {ok, State} -> beamtrail_transition:dispatch_locked(RunId, State, Lease);
                {error, _} = Error -> Error
            end;
        {error, leased} ->
            {error, leased};
        {error, _} = Error ->
            Error
    end.

dispatch_retrying(RunId, State, Lease) ->
    dispatch_retrying(RunId, State, Lease, #{lease_heartbeat => internal}).

dispatch_retrying(RunId, State, Lease, Options) ->
    Now = erlang:system_time(millisecond),
    case maps:get(next_retry_at, State, 0) =< Now of
        true ->
            case Lease of
                none -> dispatch_with_new_lease(RunId, State);
                _ -> beamtrail_transition:dispatch_retrying(RunId, State, Lease, Options)
            end;
        false -> {ok, State}
    end.

recover_if_unfinished(RunId) ->
    State = get_state(RunId),
    case recoverable(RunId, State) of
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
            [RunId || RunId <- RunIds, recoverable(RunId, get_state(RunId))];
        {error, _} = Error ->
            Error
    end.

list_recoverable(Cursor, Limit) ->
    ok = ensure_storage(),
    case (storage()):list_run_ids(Cursor, Limit) of
        {ok, #{run_ids := RunIds} = Page} ->
            {ok, Page#{run_ids := [RunId || RunId <- RunIds,
                                            recoverable(RunId, get_state(RunId))]}};
        {error, _} = Error ->
            Error
    end.

%% Acquires a lease and appends a durable `recovery.requeued' event to the log
%% so successful recovery decisions are observable in the inspector. Lease
%% contention means another live owner is still responsible for the run and is
%% intentionally not written as durable log noise.
mark_recovery_requeued(RunId) ->
    case mark_recovery_requeued_with_lease(RunId) of
        {ok, {requeued, _Lease}} -> {ok, requeued};
        Other -> Other
    end.

mark_recovery_requeued_with_lease(RunId) ->
    ok = ensure_storage(),
    Mod = storage(),
    case Mod:acquire_lease(
           RunId, beamtrail_transition:owner(), beamtrail_lease_manager:default_ttl_ms()) of
        {ok, Lease} ->
            append_recovery_marker(Mod, RunId, 'recovery.requeued', Lease);
        {error, leased} ->
            {ok, skipped};
        {error, _} = Error ->
            Error
    end.

append_recovery_marker(Mod, RunId, 'recovery.requeued', Lease) ->
    Now = erlang:system_time(millisecond),
    ExpectedSeq = maps:get(last_event_seq, get_state(RunId), 0),
    RecoveredInMs = compute_recovered_in_ms(RunId, Now),
    Payload = #{requeued_at => Now,
                owner_node => beamtrail_transition:owner(),
                lease => Lease,
                recovered_in_ms => RecoveredInMs},
    FencingToken = beamtrail_lease_manager:fencing_token(Lease),
    case Mod:append_event(RunId, ExpectedSeq, FencingToken, 'recovery.requeued',
                          undefined, undefined, undefined, Payload) of
        {ok, _} ->
            beamtrail_telemetry:execute([beamtrail, recovery, requeued],
                                        #{count => 1},
                                        #{run_id => RunId, lease => Lease}),
            {ok, {requeued, Lease}};
        {error, _} = Error ->
            Error
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

recoverable(RunId, State) ->
    recoverable_state(State) andalso lease_recoverable(RunId).

recoverable_state(State) ->
    case maps:get(migration_required_for_version_change, State, false) of
        true ->
            false;
        false ->
            recoverable_by_status(State)
    end.

recoverable_by_status(State) ->
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

lease_recoverable(RunId) ->
    Now = erlang:system_time(millisecond),
    case (storage()):read_lease(RunId) of
        {ok, #{lease_until := LeaseUntil}} when is_integer(LeaseUntil) ->
            LeaseUntil =< Now;
        not_found ->
            true;
        {error, _} ->
            false
    end.

maybe_snapshot(RunId, Force) ->
    case load_state(RunId) of
        {ok, State} ->
            beamtrail_state:maybe_snapshot(RunId, State, Force, storage());
        {error, _} = Error ->
            Error
    end.

ensure_storage() ->
    beamtrail_config:ensure_storage().

storage() ->
    beamtrail_config:storage().

new_run_id() ->
    iolist_to_binary(io_lib:format("run-~p-~p", [erlang:system_time(millisecond),
                                                  erlang:unique_integer([positive, monotonic])])).
