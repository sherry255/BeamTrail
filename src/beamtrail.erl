-module(beamtrail).

-export([start/0, stop/0]).
-export([start_workflow/2, start_workflow/3, dispatch/1, dispatch/2,
         recover_unfinished/0]).
-export([get_state/1, await_terminal/2, events/1, storage/0]).
-export([cancel_run/2, park_run/2, resume_run/1, requeue_run/2]).
-export([signal_run/3, complete_effect/3, list_pending_effects/0]).
-export([list_recoverable/0, list_recoverable/2,
         mark_recovery_requeued/1, mark_recovery_requeued_with_lease/1]).

-export_type([run_id/0, state/0, event/0, lease/0,
              recoverable_page/0]).

-define(RECOVER_UNFINISHED_BATCH_SIZE, 100).

-type run_id() :: beamtrail_workflow:run_id().
-type state() :: map().
-type event() :: beamtrail_storage:event().
-type lease() :: beamtrail_storage:lease().
-type recoverable_page() :: beamtrail_storage:run_id_page().
-type workflow_module() :: module().
-type workflow_options() :: map().
-type workflow_start_result() ::
    {ok, run_id()} | {error, {create_failed | dispatch_failed, run_id(), term()}}.
-type state_result() :: {ok, state()} | {error, term()}.
-type effect_completion_result() :: {ok, state()} | {ok, ignored} | {error, term()}.
-type recovery_mark_result() :: {ok, requeued | failed | skipped} | {error, term()}.
-type recovery_with_lease_result() ::
    {ok, {requeued, lease()} | {failed, state()} | skipped} | {error, term()}.

-spec start() -> {ok, [atom()]} | {error, term()}.
start() ->
    application:ensure_all_started(beamtrail).

-spec stop() -> ok | {error, term()}.
stop() ->
    application:stop(beamtrail).

-spec start_workflow(workflow_module(), term()) -> workflow_start_result().
start_workflow(Workflow, Input) ->
    start_workflow(Workflow, Input, #{}).

-spec start_workflow(workflow_module(), term(), workflow_options()) ->
    workflow_start_result().
start_workflow(Workflow, Input, Options) ->
    ok = ensure_storage(),
    RunId = maps:get(run_id, Options, new_run_id()),
    case safe_workflow_callback(steps, fun() -> Workflow:steps(Input) end) of
        {ok, Steps} ->
            case validate_steps(Steps) of
                ok ->
                    case workflow_decider_metadata(Workflow) of
                        {ok, DeciderMetadata} ->
                            append_created_event(RunId, Workflow, Input, Steps,
                                                 DeciderMetadata, Options);
                        {error, Reason} ->
                            {error, {create_failed, RunId, Reason}}
                    end;
                {error, Reason} ->
                    {error, {create_failed, RunId, {bad_workflow_steps, Reason}}}
            end;
        {error, CallbackError} ->
            {error, {create_failed, RunId,
                     {bad_workflow_callback, steps, CallbackError}}}
    end.

validate_steps(Steps) when is_list(Steps) ->
    validate_step_list(Steps);
validate_steps(Steps) ->
    {error, #{class => bad_steps,
              reason => not_a_list,
              steps => Steps}}.

validate_step_list([]) ->
    ok;
validate_step_list([Step | Rest]) when is_atom(Step) ->
    validate_step_list(Rest);
validate_step_list([Step | _Rest]) ->
    {error, #{class => bad_step_id,
              reason => not_an_atom,
              step => Step}};
validate_step_list(Tail) ->
    {error, #{class => bad_steps,
              reason => improper_list,
              tail => Tail}}.

append_created_event(RunId, Workflow, Input, Steps, DeciderMetadata, Options) ->
    case (storage()):append_event(
           RunId,
           0,
           undefined,
           'workflow.instance.created',
           undefined,
           undefined,
           undefined,
           maps:merge(#{workflow => Workflow,
                        input => Input,
                        steps => Steps},
                      DeciderMetadata)) of
        {ok, _Event} ->
            case maybe_snapshot(RunId, false) of
                ok -> start_workflow_dispatch(RunId, Options);
                {error, Reason} -> {error, {create_failed, RunId, Reason}}
            end;
        {error, Reason} ->
            {error, {create_failed, RunId, Reason}}
    end.

workflow_decider_metadata(Workflow) ->
    _ = code:ensure_loaded(Workflow),
    case erlang:function_exported(Workflow, decide, 1) of
        true ->
            case workflow_decider_version(Workflow) of
                {ok, Version} ->
                    {ok, #{decider => module, decider_version => Version}};
                {error, _} = Error ->
                    Error
            end;
        false ->
            {ok, #{decider => legacy, decider_version => 1}}
    end.

workflow_decider_version(Workflow) ->
    case erlang:function_exported(Workflow, decider_version, 0) of
        true ->
            case safe_workflow_callback(decider_version,
                                        fun() -> Workflow:decider_version() end) of
                {ok, Version} when is_integer(Version), Version >= 0 ->
                    {ok, Version};
                {ok, Other} ->
                    {error, {bad_decider_version,
                             #{reason => not_a_non_negative_integer,
                               value => Other}}};
                {error, CallbackError} ->
                    {error, {bad_workflow_callback, decider_version,
                             CallbackError}}
            end;
        false ->
            {ok, 1}
    end.

start_workflow_dispatch(RunId, Options) ->
    case maps:get(auto_dispatch, Options, true) of
        true ->
            case dispatch_supervised(RunId) of
                {ok, _Pid} -> {ok, RunId};
                {error, Reason} -> {error, {dispatch_failed, RunId, Reason}}
            end;
        false ->
            {ok, RunId}
    end.

dispatch_supervised(RunId) ->
    case whereis(beamtrail_run_sup) of
        undefined -> {error, run_supervisor_not_started};
        _ -> beamtrail_run_sup:dispatch(RunId)
    end.

-spec dispatch(run_id()) -> state_result().
dispatch(RunId) ->
    ok = ensure_storage(),
    case load_state(RunId) of
        {ok, State} ->
            case maps:get(terminal, State, false) of
                true ->
                    {ok, State};
                false ->
                    dispatch_nonterminal(RunId, State)
            end;
        {error, _} = Error ->
            Error
    end.

dispatch_nonterminal(RunId, State) ->
    case maps:get(parked, State, false) of
        true ->
            {ok, State};
        false ->
            case maps:get(status, State) of
                completed ->
                    {ok, State};
                failed ->
                    dispatch_with_new_lease(RunId, State);
                retrying ->
                    dispatch_retrying(RunId, State);
                _ ->
                    dispatch_with_new_lease(RunId, State)
            end
    end.

-spec dispatch(run_id(), lease()) -> state_result().
dispatch(RunId, Lease) when is_map(Lease) ->
    dispatch_with_lease(RunId, Lease, #{lease_heartbeat => internal}).

dispatch_with_lease(RunId, Lease, Options) ->
    ok = ensure_storage(),
    case load_state(RunId) of
        {ok, State} -> beamtrail_transition:dispatch_locked(RunId, State, Lease, Options);
        {error, _} = Error -> Error
    end.

-spec recover_unfinished() -> {ok, [run_id()]} | {error, term()}.
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

-spec get_state(run_id()) -> state() | {error, term()}.
get_state(RunId) ->
    case load_state(RunId) of
        {ok, State} -> State;
        {error, _} = Error -> Error
    end.

-spec await_terminal(run_id(), non_neg_integer()) ->
    {ok, state()} | {error, timeout | term()}.
await_terminal(RunId, TimeoutMs)
  when is_integer(TimeoutMs), TimeoutMs >= 0 ->
    Deadline = erlang:monotonic_time(millisecond) + TimeoutMs,
    await_terminal_until(RunId, Deadline).

await_terminal_until(RunId, Deadline) ->
    case get_state(RunId) of
        #{terminal := true} = State ->
            {ok, State};
        {error, _} = Error ->
            Error;
        _State ->
            Now = erlang:monotonic_time(millisecond),
            Remaining = Deadline - Now,
            case Remaining =< 0 of
                true ->
                    {error, timeout};
                false ->
                    timer:sleep(min(20, Remaining)),
                    await_terminal_until(RunId, Deadline)
            end
    end.

-spec cancel_run(run_id(), term()) -> state_result().
cancel_run(RunId, Reason) ->
    case control_local_runner(RunId, cancel, Reason) of
        not_found -> cancel_run_with_new_lease(RunId, Reason);
        Result -> Result
    end.

-spec park_run(run_id(), term()) -> state_result().
park_run(RunId, Reason) ->
    case control_local_runner(RunId, park, Reason) of
        not_found -> park_run_with_new_lease(RunId, Reason);
        Result -> Result
    end.

-spec resume_run(run_id()) -> state_result().
resume_run(RunId) ->
    case load_state(RunId) of
        {ok, #{terminal := true}} ->
            {error, terminal};
        {ok, #{parked := false} = State} ->
            {ok, State};
        {ok, _State} ->
            case resume_run_with_new_lease(RunId) of
                {ok, State1} ->
                    _ = dispatch_supervised(RunId),
                    {ok, State1};
                {error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

-spec signal_run(run_id(), atom(), map()) -> state_result().
signal_run(RunId, SignalName, Payload)
  when is_atom(SignalName), is_map(Payload) ->
    ok = ensure_storage(),
    case load_state(RunId) of
        {ok, #{terminal := true}} ->
            {error, terminal};
        {ok, #{parked := true}} ->
            {error, parked};
        {ok, State} ->
            append_signal_event(RunId, State, SignalName, Payload);
        {error, _} = Error ->
            Error
    end.

append_signal_event(RunId, State, SignalName, Payload) ->
    Now = erlang:system_time(millisecond),
    case (storage()):append_event(
           RunId,
           maps:get(last_event_seq, State, 0),
           undefined,
           'signal.received',
           undefined,
           undefined,
           undefined,
           #{name => SignalName,
             payload => Payload,
             received_at => Now}) of
        {ok, Event} ->
            State1 = beamtrail_state:apply_event(State, Event),
            _ = beamtrail_state:maybe_snapshot(RunId, State1, false, storage()),
            _ = release_waiting_lease(RunId, State),
            _ = dispatch_supervised(RunId),
            {ok, State1};
        {error, _} = Error ->
            Error
    end.

release_waiting_lease(RunId, #{status := waiting}) ->
    case (storage()):read_lease(RunId) of
        {ok, Lease} ->
            beamtrail_lease_manager:release(
              RunId, beamtrail_lease_manager:fencing_token(Lease));
        _ ->
            ok
    end;
release_waiting_lease(_RunId, _State) ->
    ok.

-spec complete_effect(run_id(), term(), {ok, term()} | {error, term()}) ->
    effect_completion_result().
complete_effect(RunId, EffectId, Result) ->
    ok = ensure_storage(),
    case normalize_effect_result(Result) of
        {ok, Completion} ->
            complete_effect_loaded(RunId, EffectId, Completion);
        {error, _} = Error ->
            Error
    end.

normalize_effect_result({ok, _} = Result) ->
    {ok, Result};
normalize_effect_result({error, _} = Result) ->
    {ok, Result};
normalize_effect_result(Other) ->
    {error, {bad_effect_result, Other}}.

complete_effect_loaded(RunId, EffectId, Result) ->
    case load_state(RunId) of
        {ok, State} ->
            case complete_effect_candidate(State, EffectId) of
                complete ->
                    complete_effect_with_new_lease(RunId, EffectId, Result);
                ignored ->
                    {ok, ignored};
                {error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

complete_effect_candidate(#{terminal := true}, _EffectId) ->
    ignored;
complete_effect_candidate(#{parked := true}, _EffectId) ->
    {error, parked};
complete_effect_candidate(State, EffectId) ->
    case pending_effect_attempt(State, EffectId) of
        {ok, _Attempt} -> complete;
        ignored -> ignored;
        {error, _} = Error -> Error
    end.

complete_effect_with_new_lease(RunId, EffectId, Result) ->
    case (storage()):acquire_lease(
           RunId, beamtrail_transition:owner(),
           beamtrail_lease_manager:default_ttl_ms()) of
        {ok, Lease} ->
            complete_effect_with_lease(RunId, Lease, EffectId, Result);
        {error, _} = Error ->
            Error
    end.

complete_effect_with_lease(RunId, Lease, EffectId, Result) ->
    FencingToken = beamtrail_lease_manager:fencing_token(Lease),
    Completion =
        case load_state(RunId) of
            {ok, State} ->
                complete_effect_locked(RunId, State, Lease, EffectId, Result);
            {error, _} = Error ->
                Error
        end,
    _ = beamtrail_lease_manager:release(RunId, FencingToken),
    case Completion of
        {ok, ignored} ->
            {ok, ignored};
        {ok, State1} ->
            dispatch_after_external_effect(RunId, State1),
            {ok, State1};
        Other ->
            Other
    end.

complete_effect_locked(_RunId, #{terminal := true}, _Lease, _EffectId, _Result) ->
    {ok, ignored};
complete_effect_locked(_RunId, #{parked := true}, _Lease, _EffectId, _Result) ->
    {error, parked};
complete_effect_locked(RunId, State, Lease, EffectId, Result) ->
    case pending_effect_attempt(State, EffectId) of
        {ok, Attempt} ->
            beamtrail_transition:complete_effect(RunId, Lease, Attempt, Result, State);
        ignored ->
            {ok, ignored};
        {error, _} = Error ->
            Error
    end.

pending_effect_attempt(State, EffectId) ->
    PendingEffects = maps:get(pending_effects, State, #{}),
    case maps:get(EffectId, PendingEffects, undefined) of
        undefined ->
            ignored;
        #{effect_type := EffectType}
          when EffectType =:= call_step; EffectType =:= external_step ->
            pending_step_attempt(State, EffectId, EffectType);
        #{effect_type := EffectType} ->
            {error, {unsupported_effect_type, EffectType}};
        _ ->
            {error, {bad_pending_effect, EffectId}}
    end.

pending_step_attempt(State, EffectId, EffectType) ->
    case maps:get(pending_attempt, State, undefined) of
        #{effect_id := EffectId} = Attempt ->
            {ok, Attempt};
        #{step_id := StepId, attempt := AttemptNo} = Attempt ->
            ExpectedId = beamtrail_effect:step_effect_id(EffectType, StepId,
                                                         AttemptNo),
            case ExpectedId =:= EffectId of
                true ->
                    {ok, Attempt#{effect_id => EffectId,
                                   effect_type => EffectType}};
                false -> ignored
            end;
        _ ->
            ignored
    end.

dispatch_after_external_effect(_RunId, #{terminal := true}) ->
    ok;
dispatch_after_external_effect(_RunId, #{parked := true}) ->
    ok;
dispatch_after_external_effect(RunId, _State) ->
    _ = dispatch_supervised(RunId),
    ok.

-spec list_pending_effects() -> {ok, [map()]} | {error, term()}.
list_pending_effects() ->
    ok = ensure_storage(),
    case (storage()):list_run_ids() of
        {ok, RunIds} ->
            list_pending_effects_for_runs(lists:sort(RunIds), []);
        {error, _} = Error ->
            Error
    end.

list_pending_effects_for_runs([], Acc) ->
    {ok, lists:reverse(Acc)};
list_pending_effects_for_runs([RunId | Rest], Acc) ->
    case load_state(RunId) of
        {ok, State} ->
            list_pending_effects_for_runs(
              Rest, lists:reverse(pending_effects_for_state(State), Acc));
        {error, _} = Error ->
            Error
    end.

pending_effects_for_state(#{terminal := true}) ->
    [];
pending_effects_for_state(#{parked := true}) ->
    [];
pending_effects_for_state(State) ->
    RunId = maps:get(run_id, State),
    Effects = maps:values(maps:get(pending_effects, State, #{})),
    [Effect#{run_id => RunId} || Effect <- lists:sort(Effects)].

cancel_run_with_new_lease(RunId, Reason) ->
    append_control_with_new_lease(
      RunId,
      fun(State, Lease) ->
              beamtrail_transition:cancel_run(RunId, State, Lease, Reason)
      end).

park_run_with_new_lease(RunId, Reason) ->
    append_control_with_new_lease(
      RunId,
      fun(State, Lease) ->
              beamtrail_transition:park_run(RunId, State, Lease, Reason)
      end).

resume_run_with_new_lease(RunId) ->
    append_control_with_new_lease(
      RunId,
      fun(State, Lease) ->
              beamtrail_transition:resume_run(RunId, State, Lease)
      end).

-spec requeue_run(run_id(), term()) -> recovery_mark_result() | {error, terminal | parked | term()}.
requeue_run(RunId, _Reason) ->
    case load_state(RunId) of
        {ok, #{terminal := true}} ->
            {error, terminal};
        {ok, #{parked := true}} ->
            {error, parked};
        {ok, _State} ->
            mark_recovery_requeued(RunId);
        {error, _} = Error ->
            Error
    end.

control_local_runner(RunId, Operation, Reason) ->
    case whereis(beamtrail_run_registry) of
        undefined ->
            not_found;
        _ ->
            try beamtrail_run_registry:control(RunId, Operation, Reason) of
                Result -> Result
            catch
                exit:{noproc, _} -> not_found;
                exit:noproc -> not_found
            end
    end.

append_control_with_new_lease(RunId, ControlFun) ->
    ok = ensure_storage(),
    case (storage()):acquire_lease(
           RunId, beamtrail_transition:owner(),
           beamtrail_lease_manager:default_ttl_ms()) of
        {ok, Lease} ->
            append_control_with_lease(RunId, Lease, ControlFun);
        {error, _} = Error ->
            Error
    end.

append_control_with_lease(RunId, Lease, ControlFun) ->
    case load_state(RunId) of
        {ok, State} ->
            FencingToken = beamtrail_lease_manager:fencing_token(Lease),
            case ControlFun(State, Lease) of
                {ok, State1} ->
                    _ = beamtrail_lease_manager:release(RunId, FencingToken),
                    {ok, State1};
                {error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

load_state(RunId) ->
    ok = ensure_storage(),
    beamtrail_state:load(RunId, storage()).

-spec events(run_id()) -> {ok, [event()]} | {error, term()}.
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

dispatch_retrying(RunId, State) ->
    Now = erlang:system_time(millisecond),
    case maps:get(next_retry_at, State, 0) =< Now of
        true -> dispatch_with_new_lease(RunId, State);
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

-spec list_recoverable() -> [run_id()] | {error, term()}.
list_recoverable() ->
    ok = ensure_storage(),
    case (storage()):list_run_ids() of
        {ok, RunIds} ->
            [RunId || RunId <- RunIds, run_recoverable(RunId)];
        {error, _} = Error ->
            Error
    end.

-spec list_recoverable(run_id() | undefined, pos_integer()) ->
    {ok, recoverable_page()} | {error, term()}.
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
-spec mark_recovery_requeued(run_id()) -> recovery_mark_result().
mark_recovery_requeued(RunId) ->
    case mark_recovery_requeued_with_lease(RunId) of
        {ok, {requeued, _Lease}} -> {ok, requeued};
        {ok, {failed, _State}} -> {ok, failed};
        Other -> Other
    end.

-spec mark_recovery_requeued_with_lease(run_id()) -> recovery_with_lease_result().
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
  when Et =:= 'workflow.completed';
       Et =:= 'workflow.failed';
       Et =:= 'workflow.cancelled' ->
    #{};
open_step_fold(_, Acc) -> Acc.

recoverable(RunId, State) ->
    recoverable_state(State) andalso lease_recoverable(RunId).

recoverable_state(State) ->
    case maps:get(terminal, State, false) orelse maps:get(parked, State, false) of
        true ->
            false;
        false ->
            case maps:get(migration_required_for_version_change, State, false) of
                true ->
                    false;
                false ->
                    recoverable_by_status(State)
            end
    end.

recoverable_by_status(State) ->
    case maps:get(status, State) of
        completed ->
            false;
        failed ->
            maps:get(terminal, State, false) =/= true;
        retrying ->
            maps:get(next_retry_at, State, 0) =< erlang:system_time(millisecond);
        waiting ->
            wake_due(State);
        waiting_effect ->
            false;
        _ ->
            true
    end.

wake_due(State) ->
    case maps:get(next_wake_at, State, undefined) of
        WakeAt when is_integer(WakeAt) ->
            WakeAt =< erlang:system_time(millisecond);
        _ ->
            false
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

safe_workflow_callback(Callback, Fun) ->
    try Fun() of
        Value ->
            {ok, Value}
    catch
        Class:Reason:_Stacktrace ->
            {error, #{callback => Callback, class => Class, reason => Reason}}
    end.

-spec storage() -> module().
storage() ->
    beamtrail_config:storage().

new_run_id() ->
    iolist_to_binary(io_lib:format("run-~p-~p", [erlang:system_time(millisecond),
                                                  erlang:unique_integer([positive, monotonic])])).
