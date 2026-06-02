-module(beamtrail).

-export([start/0, stop/0]).
-export([start_workflow/2, start_workflow/3, dispatch/1, dispatch/2,
         recover_unfinished/0]).
-export([get_state/1, events/1, storage/0]).
-export([list_recoverable/0, list_recoverable/2,
         mark_recovery_requeued/1, mark_recovery_requeued_with_lease/1]).

-define(RECOVER_UNFINISHED_BATCH_SIZE, 100).

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

dispatch_with_lease(RunId, Lease, Options) ->
    ok = ensure_storage(),
    case load_state(RunId) of
        {ok, State} -> beamtrail_transition:dispatch_locked(RunId, State, Lease, Options);
        {error, _} = Error -> Error
    end.

recover_unfinished() ->
    ok = ensure_storage(),
    recover_unfinished_pages(undefined, []).

recover_unfinished_pages(Cursor, Acc) ->
    case list_recoverable(Cursor, ?RECOVER_UNFINISHED_BATCH_SIZE) of
        {ok, #{run_ids := RunIds,
               has_more := HasMore,
               next_cursor := NextCursor}} ->
            Requeued = [RunId || RunId <- RunIds, recover_if_unfinished(RunId)],
            Acc1 = Acc ++ Requeued,
            case HasMore andalso NextCursor =/= undefined of
                true -> recover_unfinished_pages(NextCursor, Acc1);
                false -> {ok, Acc1}
            end;
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
    case load_state(RunId) of
        {ok, State} ->
            recover_loaded_if_unfinished(RunId, State);
        {error, _} ->
            false
    end.

recover_loaded_if_unfinished(RunId, State) ->
    case recoverable(RunId, State) of
        true ->
            recover_requeued(RunId);
        false ->
            false
    end.

recover_requeued(RunId) ->
    case mark_recovery_requeued_with_lease(RunId) of
        {ok, {failed, _State}} ->
            false;
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
            false;
        {error, _} ->
            false
    end.

list_recoverable() ->
    ok = ensure_storage(),
    case (storage()):list_run_ids() of
        {ok, RunIds} ->
            [RunId || RunId <- RunIds, run_recoverable(RunId)];
        {error, _} = Error ->
            Error
    end.

list_recoverable(Cursor, Limit) ->
    ok = ensure_storage(),
    fill_recoverable_page(storage(), Cursor, Limit,
                          erlang:system_time(millisecond), []).

%% Prefer the storage adapter's indexed recovery scan when it offers one, so the
%% scanner does not snapshot/replay every historical run. The page it returns is
%% a coarse candidate set; run_recoverable/1 still applies the precise check
%% (including the live-code migration gate) to each candidate. Adapters without
%% the indexed scan fall back to paging every run id, preserving prior behavior.
fill_recoverable_page(_Mod, _Cursor, Limit, _NowMs, Acc)
  when length(Acc) >= Limit ->
    {ok, #{run_ids => Acc, next_cursor => undefined, has_more => false}};
fill_recoverable_page(Mod, Cursor, Limit, NowMs, Acc) ->
    Need = Limit - length(Acc),
    case indexed_recoverable_page(Mod, Cursor, Need, NowMs) of
        {ok, #{run_ids := RunIds,
               has_more := HasMore,
               next_cursor := NextCursor}} ->
            Acc1 = Acc ++ [RunId || RunId <- RunIds, run_recoverable(RunId)],
            case length(Acc1) >= Limit of
                true ->
                    {ok, #{run_ids => Acc1,
                           next_cursor => NextCursor,
                           has_more => HasMore}};
                false ->
                    case HasMore andalso NextCursor =/= undefined of
                        true -> fill_recoverable_page(Mod, NextCursor, Limit,
                                                      NowMs, Acc1);
                        false -> {ok, #{run_ids => Acc1,
                                        next_cursor => NextCursor,
                                        has_more => false}}
                    end
            end;
        {error, _} = Error ->
            Error
    end.

indexed_recoverable_page(Mod, Cursor, Limit, NowMs) ->
    _ = code:ensure_loaded(Mod),
    case erlang:function_exported(Mod, list_recoverable_run_ids, 3) of
        true ->
            Mod:list_recoverable_run_ids(Cursor, Limit, NowMs);
        false ->
            Mod:list_run_ids(Cursor, Limit)
    end.

run_recoverable(RunId) ->
    case load_state(RunId) of
        {ok, State} -> recoverable(RunId, State);
        {error, _} -> false
    end.

%% Acquires a lease and appends a durable `recovery.requeued' event to the log
%% so successful recovery decisions are observable in the inspector. Lease
%% contention means another live owner is still responsible for the run and is
%% intentionally not written as durable log noise.
mark_recovery_requeued(RunId) ->
    case mark_recovery_requeued_with_lease(RunId) of
        {ok, {requeued, _Lease}} -> {ok, requeued};
        {ok, {failed, _State}} -> {ok, failed};
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
    case load_state(RunId) of
        {ok, State} ->
            append_recovery_decision(Mod, RunId, State, Lease, Now);
        {error, _} = Error ->
            Error
    end.

append_recovery_decision(Mod, RunId, State, Lease, Now) ->
    case recovery_requeue_count(RunId, maps:get(pending_attempt, State, undefined)) of
        {ok, Recoveries} ->
            MaxRecoveries = beamtrail_config:max_recoveries_per_attempt(),
            case recovery_budget_exceeded(Recoveries, MaxRecoveries) of
                true ->
                    append_recovery_budget_failure(Mod, RunId, State, Lease,
                                                   Recoveries, MaxRecoveries, Now);
                false ->
                    append_recovery_requeued(Mod, RunId, State, Lease,
                                             Recoveries, MaxRecoveries, Now)
            end;
        {error, _} = Error ->
            Error
    end.

append_recovery_requeued(Mod, RunId, State, Lease, Recoveries, MaxRecoveries, Now) ->
    ExpectedSeq = maps:get(last_event_seq, State, 0),
    RecoveredInMs = compute_recovered_in_ms(RunId, Now),
    Payload = #{requeued_at => Now,
                owner_node => beamtrail_transition:owner(),
                lease => Lease,
                recovered_in_ms => RecoveredInMs,
                recoveries => Recoveries + 1,
                max_recoveries => MaxRecoveries},
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

append_recovery_budget_failure(Mod, RunId, State, Lease, Recoveries,
                               MaxRecoveries, Now) ->
    case maps:get(pending_attempt, State, undefined) of
        #{step_id := StepId,
          step_version := StepVersion,
          idempotency_key := IdempotencyKey,
          attempt := AttemptNo} ->
            Payload = #{reason => recovery_budget_exceeded,
                        class => recovery_budget_exceeded,
                        attempt => AttemptNo,
                        recoveries => Recoveries,
                        max_recoveries => MaxRecoveries,
                        failed_at => Now},
            EventSpecs =
                [event_spec('step.failed', StepId, StepVersion,
                            IdempotencyKey, Payload),
                 event_spec('workflow.failed', StepId, StepVersion,
                            IdempotencyKey, Payload)],
            FencingToken = beamtrail_lease_manager:fencing_token(Lease),
            case Mod:append_events(RunId, maps:get(last_event_seq, State, 0),
                                   FencingToken, EventSpecs) of
                {ok, Events} ->
                    State1 = lists:foldl(
                               fun(Event, Acc) ->
                                       beamtrail_state:apply_event(Acc, Event)
                               end,
                               State,
                               Events),
                    _ = beamtrail_state:maybe_snapshot(RunId, State1, true, Mod),
                    beamtrail_telemetry:execute(
                      [beamtrail, recovery, budget_exceeded],
                      #{count => 1},
                      #{run_id => RunId,
                        recoveries => Recoveries,
                        max_recoveries => MaxRecoveries}),
                    {ok, {failed, State1}};
                {error, _} = Error ->
                    Error
            end;
        _ ->
            append_recovery_requeued(Mod, RunId, State, Lease, Recoveries,
                                     MaxRecoveries, Now)
    end.

event_spec(EventType, StepId, StepVersion, IdempotencyKey, Payload) ->
    #{event_type => EventType,
      step_id => StepId,
      step_version => StepVersion,
      idempotency_key => IdempotencyKey,
      payload => Payload}.

recovery_budget_exceeded(_Recoveries, infinity) ->
    false;
recovery_budget_exceeded(Recoveries, MaxRecoveries) ->
    Recoveries >= MaxRecoveries.

recovery_requeue_count(_RunId, undefined) ->
    {ok, 0};
recovery_requeue_count(RunId, #{started_event_seq := StartedSeq}) ->
    case (storage()):read_events(RunId, StartedSeq + 1, infinity) of
        {ok, Events} ->
            {ok, length([E || E <- Events,
                              maps:get(event_type, E) =:= 'recovery.requeued'])};
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
  when Et =:= 'workflow.completed'; Et =:= 'workflow.failed' ->
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
