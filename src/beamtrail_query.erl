-module(beamtrail_query).

%% Query/read-model API. Returns a single map that matches the prototype's
%% inspector shape: event ledger, attempts, snapshot/replay info, recovery
%% status, lease/fencing, telemetry counters, version mismatch, and a
%% copyable query body.

-export([describe/1, list/0, telemetry/0]).

list() ->
    Mod = beamtrail:storage(),
    [describe(R) || R <- Mod:list_run_ids()].

telemetry() ->
    Mod = beamtrail:storage(),
    case erlang:function_exported(Mod, telemetry_counters, 0) of
        true -> Mod:telemetry_counters();
        false -> #{adapter => Mod, note => <<"telemetry counters not implemented by adapter">>}
    end.

describe(RunId) ->
    Mod = beamtrail:storage(),
    State = beamtrail:get_state(RunId),
    {ok, Events} = beamtrail:events(RunId),
    Snapshot =
        case Mod:read_snapshot(RunId) of
            {ok, S} -> S;
            not_found -> undefined
        end,
    Lease =
        case Mod:read_lease(RunId) of
            {ok, L} -> L;
            not_found -> undefined
        end,
    Attempts =
        case optional_read(Mod, read_attempts, [RunId], not_found) of
            {ok, A} -> A;
            not_found -> derive_attempts(Events)
        end,
    Instance =
        case optional_read(Mod, read_instance, [RunId], not_found) of
            {ok, I} -> I;
            not_found -> derive_instance(Events)
        end,
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
      recovered_in_ms => Recovered,
      events => Events,
      source_of_truth =>
          #{authoritative => <<"workflow_events append-only stream">>,
            event_seq => LastSeq,
            snapshot =>
                #{snapshot_seq => snapshot_field(Snapshot, snapshot_seq, 0),
                  snapshot_revision => snapshot_field(Snapshot, snapshot_revision, 0),
                  policy => <<"every_5_events_plus_terminal_and_recovery">>,
                  replay_tail_events => TailLen},
            read_models =>
                [workflow_instances_projection,
                 step_attempts_projection,
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
                [<<"append_event(run_id, type, step_id, step_version, key, payload)">>,
                 <<"write_snapshot(run_id, state, snapshot_seq, snapshot_revision)">>,
                 <<"acquire_lease(run_id, owner_node, ttl_ms)">>],
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

optional_read(Mod, Fun, Args, Default) ->
    case erlang:function_exported(Mod, Fun, length(Args)) of
        true -> apply(Mod, Fun, Args);
        false -> Default
    end.

%% Fallback when an adapter does not provide a read-model accessor: derive
%% the instance descriptor from the workflow.instance.created event.
derive_instance(Events) ->
    case [P || #{event_type := 'workflow.instance.created', payload := P} <- Events] of
        [P | _] -> P;
        [] -> undefined
    end.

%% Fallback when an adapter lacks read_attempts/1: derive a flat list of
%% attempt records by scanning the event log.
derive_attempts(Events) ->
    lists:reverse(lists:foldl(fun derive_attempt/2, [], Events)).

derive_attempt(#{event_type := 'attempt.started'} = E, Acc) ->
    [#{step_id => maps:get(step_id, E),
       step_version => maps:get(step_version, E),
       idempotency_key => maps:get(idempotency_key, E),
       started_at => maps:get(occurred_at, E),
       status => started} | Acc];
derive_attempt(#{event_type := Et} = E, [Last | Rest])
  when Et =:= 'step.succeeded'; Et =:= 'step.failed' ->
    Status = case Et of 'step.succeeded' -> succeeded; _ -> failed end,
    [Last#{status => Status, closed_at => maps:get(occurred_at, E)} | Rest];
derive_attempt(_, Acc) -> Acc.
