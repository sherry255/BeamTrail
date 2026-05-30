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
    #{run_id => RunId,
      instance => Instance,
      status => maps:get(status, State),
      current_step => maps:get(current_step, State),
      workflow => maps:get(workflow, State),
      last_event_seq => maps:get(last_event_seq, State, 0),
      next_retry_at => maps:get(next_retry_at, State, undefined),
      failure => maps:get(failure, State, undefined),
      terminal => maps:get(terminal, State, false),
      migration_required_for_version_change =>
          maps:get(migration_required_for_version_change, State, false),
      attempts => Attempts,
      snapshot => Snapshot,
      replay_tail_length => replay_tail(Snapshot, maps:get(last_event_seq, State, 0)),
      lease => Lease,
      recovered_in_ms => recovered_in_ms(Events),
      events => Events,
      query => #{api => <<"beamtrail_query:describe/1">>,
                 run_id => RunId}}.

replay_tail(undefined, LastSeq) -> LastSeq;
replay_tail(#{snapshot_seq := S}, LastSeq) when LastSeq >= S -> LastSeq - S;
replay_tail(_, _) -> 0.

%% recovered_in_ms = time between the earliest unfinished attempt.started
%% and the most recent recovery.requeued or successor completion event.
recovered_in_ms(Events) ->
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
        orelse Et =:= 'workflow.completed' orelse Et =:= 'workflow.failed')
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
