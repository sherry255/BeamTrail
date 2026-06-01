-module(beamtrail_query).

%% Query/read-model API. Returns a single map for runtime inspection:
%% event ledger, attempts, snapshot/replay info, recovery
%% status, lease/fencing, telemetry counters, version mismatch, and a
%% copyable query body.

-export([describe/1, list/0, telemetry/0]).

list() ->
    Mod = beamtrail_config:storage(),
    case Mod:list_run_ids() of
        {ok, RunIds} -> [describe(R) || R <- RunIds];
        {error, _} = Error -> Error
    end.

telemetry() ->
    Mod = beamtrail_config:storage(),
    case erlang:function_exported(Mod, telemetry_counters, 0) of
        true -> Mod:telemetry_counters();
        false -> #{adapter => Mod, note => <<"telemetry counters not implemented by adapter">>}
    end.

describe(RunId) ->
    Mod = beamtrail_config:storage(),
    case beamtrail:get_state(RunId) of
        {error, _} = Error ->
            Error;
        State ->
            describe_loaded(RunId, Mod, State)
    end.

describe_loaded(RunId, Mod, State) ->
    case beamtrail:events(RunId) of
        {ok, Events} ->
            describe_loaded_events(RunId, Mod, State, Events);
        {error, _} = Error ->
            Error
    end.

describe_loaded_events(RunId, Mod, State, Events) ->
    Snapshot =
        case Mod:read_snapshot(RunId) of
            {ok, S} -> S;
            not_found -> undefined;
            {error, _} -> undefined
        end,
    Lease =
        case Mod:read_lease(RunId) of
            {ok, L} -> L;
            not_found -> undefined
        end,
    Attempts = maps:get(attempts, State, []),
    Instance = instance_from_state(State),
    LastSeq = maps:get(last_event_seq, State, 0),
    TailLen = replay_tail(Snapshot, LastSeq),
    Recovered = recovered_in_ms_from_events(Events),
    Workflow = maps:get(workflow, State),
    #{run_id => RunId,
      instance => Instance,
      status => maps:get(status, State),
      current_step => maps:get(current_step, State),
      workflow => Workflow,
      module => Workflow,
      last_event_seq => LastSeq,
      next_retry_at => maps:get(next_retry_at, State, undefined),
      failure => maps:get(failure, State, undefined),
      terminal => maps:get(terminal, State, false),
      migration_required_for_version_change =>
          maps:get(migration_required_for_version_change, State, false),
      attempts => Attempts,
      snapshot => Snapshot,
      snapshots => list_snapshots(Snapshot),
      replay_tail_length => TailLen,
      lease => Lease,
      active_runner => active_runner(RunId),
      recovered_in_ms => Recovered,
      events => Events,
      source_of_truth =>
          #{authoritative => <<"workflow_events append-only stream">>,
            event_seq => LastSeq,
            snapshot =>
                #{snapshot_seq => snapshot_field(Snapshot, snapshot_seq, 0),
                  snapshot_revision => snapshot_field(Snapshot, snapshot_revision, 0),
                  policy => beamtrail_state:snapshot_policy(),
                  replay_tail_events => TailLen},
            read_models =>
                [reducer_state,
                 telemetry_counters]},
      replay_policy =>
          #{step_version_source => <<"attempt.started.step_version">>,
            rule => <<"recover and retry in-flight work with recorded step_version, "
                      "not latest deployed code">>,
            migration_required_for_version_change =>
                maps:get(migration_required_for_version_change, State, false)},
      ownership =>
          #{owner_node => lease_field(Lease, owner_node, undefined),
            lease_until => lease_field(Lease, lease_until, undefined),
            fencing_token => lease_field(Lease, fencing_token, undefined),
            next_retry_at => maps:get(next_retry_at, State, undefined)},
      recovery =>
          #{target_ms => 30000,
            recovered_in_ms => Recovered,
            scanner_event => recovery_scanner_event_marker(Events),
            scanner_event_detail => recovery_scanner_event(Events),
            status => recovery_status(Recovered)},
      storage_adapter =>
          #{module => Mod,
            primary_writes =>
                [<<"append_event(run_id, expected_seq, fencing_token, type, step_id, step_version, key, payload)">>,
                 <<"write_snapshot(run_id, state, snapshot_seq, snapshot_revision)">>,
                 <<"acquire_lease(run_id, owner_node, ttl_ms)">>,
                 <<"renew_lease(run_id, fencing_token, ttl_ms)">>],
            query_path =>
                <<"read_snapshot + read_events(from_snapshot_seq + 1) + reduce tail">>},
      query => #{api => <<"beamtrail_query:describe/1">>,
                 run_id => RunId}}.

snapshot_field(undefined, _, Default) -> Default;
snapshot_field(Snapshot, Key, Default) when is_map(Snapshot) ->
    maps:get(Key, Snapshot, Default).

lease_field(undefined, _, Default) -> Default;
lease_field(Lease, Key, Default) when is_map(Lease) ->
    maps:get(Key, Lease, Default).

active_runner(RunId) ->
    case whereis(beamtrail_run_registry) of
        undefined ->
            #{status => not_found};
        _ ->
            case beamtrail_run_registry:lookup(RunId) of
                {ok, Info} -> Info;
                not_found -> #{status => not_found};
                {error, Reason} -> #{status => unknown, error => Reason}
            end
    end.

%% Latest-only: the memory adapter exposes a single current snapshot per
%% run. Real adapters with snapshot history can extend the read model to
%% return a chronological list here.
list_snapshots(undefined) -> [];
list_snapshots(S) -> [S].

recovery_scanner_event(Events) ->
    case [E || #{event_type := 'recovery.requeued'} = E <- Events] of
        [] -> undefined;
        L -> lists:last(L)
    end.

recovery_scanner_event_marker(Events) ->
    case recovery_scanner_event(Events) of
        undefined -> undefined;
        _ -> 'recovery.requeued'
    end.

recovery_status(undefined) -> not_measured;
recovery_status(Ms) when is_integer(Ms), Ms =< 30000 -> pass;
recovery_status(_) -> over_budget.

replay_tail(undefined, LastSeq) -> LastSeq;
replay_tail(#{snapshot_seq := S}, LastSeq) when LastSeq >= S -> LastSeq - S;
replay_tail(_, _) -> 0.

%% Prefer the value recorded on the latest recovery.requeued payload (the
%% event log is the displayed fact). Fall back to a derived value only when
%% the payload is missing — e.g. logs written by older runtimes.
recovered_in_ms_from_events(Events) ->
    case payload_recovered_in_ms(Events) of
        Ms when is_integer(Ms) -> Ms;
        undefined -> derived_recovered_in_ms(Events)
    end.

payload_recovered_in_ms(Events) ->
    lists:foldl(
      fun(#{event_type := 'recovery.requeued', payload := P}, _Acc) ->
              maps:get(recovered_in_ms, P, undefined);
         (_, Acc) -> Acc
      end, undefined, Events).

derived_recovered_in_ms(Events) ->
    case last_recovery_marker(Events) of
        undefined -> undefined;
        {StartedAt, RecoveredAt} -> RecoveredAt - StartedAt
    end.

last_recovery_marker(Events) ->
    %% Pair the latest attempt.started that lacked an immediate result with
    %% the next observable closure (succeeded/failed/recovery.requeued).
    pair_starts(Events, undefined, undefined).

pair_starts([], _PendingStart, Pair) -> Pair;
pair_starts([#{event_type := 'attempt.started', occurred_at := T} | Rest], _Pending, Pair) ->
    pair_starts(Rest, T, Pair);
pair_starts([#{event_type := Et, occurred_at := T} | Rest], Pending, _Pair)
  when (Et =:= 'step.succeeded' orelse Et =:= 'step.failed'
        orelse Et =:= 'workflow.completed' orelse Et =:= 'workflow.failed'
        orelse Et =:= 'recovery.requeued')
       andalso Pending =/= undefined ->
    pair_starts(Rest, undefined, {Pending, T});
pair_starts([_ | Rest], Pending, Pair) ->
    pair_starts(Rest, Pending, Pair).

instance_from_state(#{run_id := undefined}) ->
    undefined;
instance_from_state(State) ->
    #{run_id => maps:get(run_id, State),
      workflow => maps:get(workflow, State),
      status => maps:get(status, State),
      current_step => maps:get(current_step, State),
      last_event_seq => maps:get(last_event_seq, State, 0),
      next_retry_at => maps:get(next_retry_at, State, undefined),
      failure => maps:get(failure, State, undefined),
      terminal => maps:get(terminal, State, false)}.
