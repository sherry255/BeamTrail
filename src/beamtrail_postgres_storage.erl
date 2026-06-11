-module(beamtrail_postgres_storage).
-behaviour(beamtrail_storage).

-include_lib("epgsql/include/epgsql.hrl").

%% PostgreSQL storage adapter. It stores Erlang payload/state/idempotency
%% terms as external-term-format bytea values so replay semantics come before
%% SQL-level inspection.

-export([init_schema/0, backfill_run_projections/0, backfill_effect_index/0]).
-export([append_event/8, append_events/4, read_events/3, events/1, write_snapshot/4, read_snapshot/1,
         acquire_lease/3, renew_lease/3, release_lease/2, read_lease/1, list_run_ids/0, list_run_ids/2,
         list_recoverable_run_ids/3, list_pending_effects/1]).

append_event(RunId, ExpectedSeq, FencingToken,
             EventType, StepId, StepVersion, IdempotencyKey, Payload) ->
    Spec = #{event_type => EventType,
             step_id => StepId,
             step_version => StepVersion,
             idempotency_key => IdempotencyKey,
             payload => Payload},
    case append_events(RunId, ExpectedSeq, FencingToken, [Spec]) of
        {ok, [Event]} -> {ok, Event};
        {error, _} = Error -> Error
    end.

append_events(RunId, ExpectedSeq, FencingToken, EventSpecs) ->
    transaction(
      fun(C) ->
              append_events_with_run_lock(C, RunId, ExpectedSeq, FencingToken,
                                          EventSpecs)
      end).

read_events(RunId, FromSeq, infinity) ->
    with_connection(
      fun(C) ->
              decode_events(
                epgsql:equery(
                  C,
                  "SELECT event_seq, event_type, step_id, step_version, "
                  "idempotency_key, payload, fencing_token, occurred_at_ms "
                  "FROM workflow_events "
                  "WHERE run_id = $1 AND event_seq >= $2 "
                  "ORDER BY event_seq",
                  [RunId, FromSeq]),
                RunId)
      end);
read_events(RunId, FromSeq, Limit) when is_integer(Limit), Limit >= 0 ->
    with_connection(
      fun(C) ->
              decode_events(
                epgsql:equery(
                  C,
                  "SELECT event_seq, event_type, step_id, step_version, "
                  "idempotency_key, payload, fencing_token, occurred_at_ms "
                  "FROM workflow_events "
                  "WHERE run_id = $1 AND event_seq >= $2 "
                  "ORDER BY event_seq LIMIT $3",
                  [RunId, FromSeq, Limit]),
                RunId)
      end).

events(RunId) ->
    read_events(RunId, 1, infinity).

write_snapshot(RunId, State, SnapshotSeq, SnapshotRevision) ->
    with_connection(
      fun(C) ->
              case epgsql:equery(
                     C,
                     "INSERT INTO workflow_snapshots "
                     "(run_id, snapshot_seq, snapshot_revision, state, written_at_ms) "
                     "VALUES ($1,$2,$3,$4,$5) "
                     "ON CONFLICT (run_id) DO UPDATE SET "
                     "snapshot_seq = EXCLUDED.snapshot_seq, "
                     "snapshot_revision = EXCLUDED.snapshot_revision, "
                     "state = EXCLUDED.state, "
                     "written_at_ms = EXCLUDED.written_at_ms "
                     "WHERE workflow_snapshots.snapshot_seq < EXCLUDED.snapshot_seq",
                     [RunId, SnapshotSeq, SnapshotRevision,
                      term_to_binary(State), now_ms()]) of
                  {ok, 1} -> ok;
                  {ok, 0} -> ok;
                  {error, Reason} -> {error, Reason}
              end
      end).

read_snapshot(RunId) ->
    with_connection(
      fun(C) ->
              case epgsql:equery(
                     C,
                     "SELECT snapshot_seq, snapshot_revision, state, written_at_ms "
                     "FROM workflow_snapshots WHERE run_id = $1",
                     [RunId]) of
                  {ok, _Cols, []} ->
                      not_found;
                  {ok, _Cols, [{Seq, Revision, StateBin, WrittenAt}]} ->
                      case decode_term(StateBin) of
                          {ok, SnapshotState} ->
                              {ok, #{run_id => RunId,
                                     snapshot_seq => Seq,
                                     snapshot_revision => Revision,
                                     state => SnapshotState,
                                     written_at => WrittenAt}};
                          {error, _} = Error ->
                              Error
                      end;
                  {error, Reason} ->
                      {error, Reason}
              end
      end).

acquire_lease(RunId, Owner, TtlMs) ->
    transaction(
      fun(C) ->
              Now = now_ms(),
              Until = Now + TtlMs,
              case select_lease_for_update(C, RunId) of
                  not_found ->
                      Lease = lease_map(RunId, Owner, Until, 1, Now, Now),
                      insert_lease(C, Lease);
                  {ok, #{lease_until := LeaseUntil} = Current} when LeaseUntil =< Now ->
                      Fence = maps:get(fencing_token, Current) + 1,
                      Lease = lease_map(RunId, Owner, Until, Fence, Now, Now),
                      update_lease(C, Lease);
                  {ok, _Current} ->
                      {error, leased};
                  {error, _} = Error ->
                      Error
              end
      end).

renew_lease(RunId, FencingToken, TtlMs) ->
    transaction(
      fun(C) ->
              Now = now_ms(),
              case select_lease_for_update(C, RunId) of
                  not_found ->
                      {error, no_lease};
                  {ok, #{lease_until := LeaseUntil}} when LeaseUntil =< Now ->
                      {error, lease_expired};
                  {ok, #{fencing_token := FencingToken} = Current} ->
                      Lease = Current#{lease_until := Now + TtlMs,
                                       updated_at := Now,
                                       renewed_at => Now},
                      update_lease(C, Lease);
                  {ok, #{fencing_token := CurrentFence}} when FencingToken < CurrentFence ->
                      {error, stale_fence};
                  {ok, #{fencing_token := CurrentFence}} ->
                      {error, {invalid_fence, #{provided => FencingToken,
                                                current => CurrentFence}}};
                  {error, _} = Error ->
                      Error
              end
      end).

release_lease(RunId, FencingToken) ->
    transaction(
      fun(C) ->
              Now = now_ms(),
              case select_lease_for_update(C, RunId) of
                  not_found ->
                      {error, no_lease};
                  {ok, #{fencing_token := FencingToken} = Current} ->
                      case update_lease(C, Current#{lease_until := Now,
                                                    updated_at := Now}) of
                          {ok, _Lease} -> ok;
                          {error, _} = Error -> Error
                      end;
                  {ok, #{fencing_token := CurrentFence}} when FencingToken < CurrentFence ->
                      {error, stale_fence};
                  {ok, #{fencing_token := CurrentFence}} ->
                      {error, {invalid_fence, #{provided => FencingToken,
                                                current => CurrentFence}}};
                  {error, _} = Error ->
                      Error
              end
      end).

read_lease(RunId) ->
    with_connection(fun(C) -> select_lease(C, RunId, "") end).

list_run_ids() ->
    with_connection(
      fun(C) ->
              case epgsql:equery(
                     C,
                     "SELECT DISTINCT run_id FROM workflow_events ORDER BY run_id",
                     []) of
                  {ok, _Cols, Rows} -> {ok, [RunId || {RunId} <- Rows]};
                  {error, Reason} -> {error, Reason}
              end
      end).

list_run_ids(Cursor, Limit) ->
    with_connection(
      fun(C) ->
              {Sql, Params} =
                  case Cursor of
                      undefined ->
                          {"SELECT DISTINCT run_id FROM workflow_events "
                           "ORDER BY run_id LIMIT $1",
                           [Limit + 1]};
                      _ ->
                          {"SELECT DISTINCT run_id FROM workflow_events "
                           "WHERE run_id > $1 ORDER BY run_id LIMIT $2",
                           [Cursor, Limit + 1]}
                  end,
              case epgsql:equery(C, Sql, Params) of
                  {ok, _Cols, Rows} ->
                      All = [RunId || {RunId} <- Rows],
                      Page = lists:sublist(All, Limit),
                      {ok, #{run_ids => Page,
                             next_cursor => next_cursor(Page),
                             has_more => length(All) > Limit}};
                  {error, Reason} ->
                      {error, Reason}
              end
      end).

list_recoverable_run_ids(Cursor, Limit, NowMs) ->
    with_connection(
      fun(C) ->
              {Sql, Params} = recoverable_query(Cursor, Limit, NowMs),
              case epgsql:equery(C, Sql, Params) of
                  {ok, _Cols, Rows} ->
                      All = [RunId || {RunId} <- Rows],
                      Page = lists:sublist(All, Limit),
                      {ok, #{run_ids => Page,
                             next_cursor => next_cursor(Page),
                             has_more => length(All) > Limit}};
                  {error, Reason} ->
                      {error, Reason}
              end
      end).

list_pending_effects(NowMs) ->
    with_connection(
      fun(C) ->
              case epgsql:equery(
                     C,
                     "SELECT e.run_id, e.effect_id, e.effect_type, e.step_id, "
                     "e.step_version, e.idempotency_key, e.attempt, e.status, "
                     "e.visible_at_ms, e.visibility_timeout_ms, e.deadline_at_ms, "
                     "e.claim_owner, e.claim_token, e.claim_until_ms, e.claimed_at_ms "
                     "FROM workflow_effects e "
                     "JOIN workflow_runs r ON r.run_id = e.run_id "
                     "WHERE r.terminal = false AND r.parked = false "
                     "AND e.status IN ('scheduled', 'claimed') "
                     "AND (e.visible_at_ms IS NULL OR e.visible_at_ms <= $1) "
                     "AND (e.claim_token IS NULL OR e.claim_until_ms <= $1) "
                     "ORDER BY e.run_id, e.effect_id",
                     [NowMs]) of
                  {ok, _Cols, Rows} ->
                      decode_pending_effect_rows(Rows);
                  {error, Reason} ->
                      {error, Reason}
              end
      end).

%% Coarse recovery candidates at NowMs: not terminal, not parked, not waiting
%% on a future retry/timer/effect wake, and not currently leased. The
%% lease is read live via LEFT JOIN (never denormalized into workflow_runs,
%% which is updated in a different transaction).
%% Mirrors beamtrail:recoverable_by_status/1 + lease_recoverable/1, minus the
%% migration gate, which beamtrail re-checks per candidate from live code.
recoverable_query(undefined, Limit, NowMs) ->
    {"SELECT r.run_id FROM workflow_runs r "
     "LEFT JOIN workflow_leases l ON l.run_id = r.run_id "
     "WHERE r.terminal = false "
     "AND r.parked = false "
     "AND (r.status <> 'waiting_effect' OR r.next_wake_at_ms <= $1) "
     "AND (r.status <> 'waiting' OR r.next_wake_at_ms <= $1) "
     "AND (r.status <> 'retrying' OR r.next_retry_at_ms <= $1) "
     "AND (l.lease_until_ms IS NULL OR l.lease_until_ms <= $1) "
     "ORDER BY r.run_id LIMIT $2",
     [NowMs, Limit + 1]};
recoverable_query(Cursor, Limit, NowMs) ->
    {"SELECT r.run_id FROM workflow_runs r "
     "LEFT JOIN workflow_leases l ON l.run_id = r.run_id "
     "WHERE r.terminal = false "
     "AND r.parked = false "
     "AND (r.status <> 'waiting_effect' OR r.next_wake_at_ms <= $1) "
     "AND (r.status <> 'waiting' OR r.next_wake_at_ms <= $1) "
     "AND (r.status <> 'retrying' OR r.next_retry_at_ms <= $1) "
     "AND (l.lease_until_ms IS NULL OR l.lease_until_ms <= $1) "
     "AND r.run_id > $2 "
     "ORDER BY r.run_id LIMIT $3",
     [NowMs, Cursor, Limit + 1]}.

init_schema() ->
    with_connection(
      fun(C) ->
              Path = filename:join([code:priv_dir(beamtrail), "sql", "postgres.sql"]),
              case file:read_file(Path) of
                  {ok, Sql} ->
                      case epgsql:squery(C, Sql) of
                          {error, Reason} -> {error, Reason};
                          _ -> ok
                      end;
                  {error, Reason} ->
                      {error, Reason}
              end
      end).

with_connection(Fun) ->
    case whereis(beamtrail_postgres_pool) of
        undefined ->
            with_direct_connection(Fun);
        _Pid ->
            with_pooled_connection(Fun)
    end.

with_pooled_connection(Fun) ->
    case beamtrail_postgres_pool:checkout() of
        {ok, C} ->
            try Fun(C)
            after
                beamtrail_postgres_pool:checkin(C)
            end;
        {error, _} = Error ->
            Error
    end.

with_direct_connection(Fun) ->
    case connect() of
        {ok, C} ->
            try Fun(C)
            after
                catch epgsql:close(C)
            end;
        {error, _} = Error ->
            Error
    end.

transaction(Fun) ->
    with_connection(
      fun(C) ->
              case epgsql:squery(C, "BEGIN") of
                  {ok, _, _} ->
                      run_transaction_body(C, Fun);
                  {error, Reason} ->
                      {error, Reason}
              end
      end).

run_transaction_body(C, Fun) ->
    try Fun(C) of
        {error, _} = Error ->
            _ = epgsql:squery(C, "ROLLBACK"),
            Error;
        Other ->
            case epgsql:squery(C, "COMMIT") of
                {ok, _, _} -> Other;
                {error, Reason} -> {error, Reason}
            end
    catch
        Class:Reason:_Stacktrace ->
            _ = epgsql:squery(C, "ROLLBACK"),
            {error, {transaction_failed, Class, Reason}}
    end.

connect() ->
    case beamtrail_postgres_config:connection() of
        {ok, Config} ->
            epgsql:connect(Config);
        {error, _} = Error ->
            Error
    end.

append_events_with_run_lock(_C, _RunId, _ExpectedSeq, _FencingToken, []) ->
    {ok, []};
append_events_with_run_lock(C, RunId, ExpectedSeq, FencingToken, EventSpecs) ->
    case lock_run(C, RunId) of
        ok ->
            append_events_after_lock(C, RunId, ExpectedSeq, FencingToken,
                                     EventSpecs);
        {error, _} = Error ->
            Error
    end.

append_events_after_lock(C, RunId, ExpectedSeq, FencingToken, EventSpecs) ->
    case current_event_seq(C, RunId) of
        {ok, ExpectedSeq} ->
            append_events_at_expected_seq(C, RunId, ExpectedSeq, FencingToken,
                                          EventSpecs);
        {ok, ActualSeq} ->
            {error, {conflict, #{expected_seq => ExpectedSeq,
                                 actual_seq => ActualSeq}}};
        {error, _} = Error ->
            Error
    end.

append_events_at_expected_seq(C, RunId, ExpectedSeq, FencingToken, EventSpecs) ->
    case validate_fencing_for_specs(C, RunId, FencingToken, EventSpecs) of
        ok ->
            append_event_specs(C, RunId, ExpectedSeq, FencingToken,
                               EventSpecs, []);
        {error, _} = Error ->
            Error
    end.

validate_fencing_for_specs(_C, _RunId, _FencingToken, []) ->
    ok;
validate_fencing_for_specs(C, RunId, FencingToken, [Spec | Rest]) ->
    EventType = maps:get(event_type, Spec),
    case validate_fencing(C, RunId, EventType, FencingToken) of
        ok -> validate_fencing_for_specs(C, RunId, FencingToken, Rest);
        {error, _} = Error -> Error
    end.

append_event_specs(C, RunId, _Seq, _FencingToken, [], Acc) ->
    Events = lists:reverse(Acc),
    Last = lists:last(Events),
    case update_run_after_append(C, RunId, Events, maps:get(occurred_at, Last)) of
        ok -> {ok, Events};
        {error, _} = Error -> Error
    end;
append_event_specs(C, RunId, Seq, FencingToken, [Spec | Rest], Acc) ->
    EventSeq = Seq + 1,
    EventType = maps:get(event_type, Spec),
    StepId = maps:get(step_id, Spec, undefined),
    StepVersion = maps:get(step_version, Spec, undefined),
    IdempotencyKey = maps:get(idempotency_key, Spec, undefined),
    Payload = maps:get(payload, Spec),
    case insert_event(C, RunId, EventSeq, FencingToken, EventType, StepId,
                      StepVersion, IdempotencyKey, Payload) of
        {ok, Event} ->
            append_event_specs(C, RunId, EventSeq, FencingToken, Rest,
                               [Event | Acc]);
        {error, _} = Error ->
            Error
    end.

insert_event(C, RunId, EventSeq, FencingToken,
             EventType, StepId, StepVersion, IdempotencyKey, Payload) ->
    OccurredAt = now_ms(),
    Params =
        [RunId, EventSeq, atom_to_binary(EventType, utf8),
         nullable_atom(StepId), nullable_int(StepVersion),
         nullable_term(IdempotencyKey), term_to_binary(Payload),
         nullable_int(FencingToken), OccurredAt],
    case epgsql:equery(
           C,
           "INSERT INTO workflow_events "
           "(run_id, event_seq, event_type, step_id, step_version, "
           " idempotency_key, payload, fencing_token, occurred_at_ms) "
           "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
           Params) of
        {ok, 1} ->
            {ok, #{run_id => RunId,
                   event_seq => EventSeq,
                   event_type => EventType,
                   step_id => StepId,
                   step_version => StepVersion,
                   idempotency_key => IdempotencyKey,
                   fencing_token => FencingToken,
                   payload => Payload,
                   occurred_at => OccurredAt}};
        {error, Reason} ->
            {error, Reason}
    end.

lock_run(C, RunId) ->
    Now = now_ms(),
    case epgsql:equery(
           C,
           "INSERT INTO workflow_runs (run_id, created_at_ms, updated_at_ms) "
           "VALUES ($1,$2,$2) ON CONFLICT (run_id) DO NOTHING",
           [RunId, Now]) of
        {ok, _Count} ->
            select_run_for_update(C, RunId);
        {error, Reason} ->
            {error, Reason}
    end.

select_run_for_update(C, RunId) ->
    case epgsql:equery(
           C,
           "SELECT run_id FROM workflow_runs WHERE run_id = $1 FOR UPDATE",
           [RunId]) of
        {ok, _Cols, [{RunId}]} -> ok;
        {ok, _Cols, []} -> {error, run_lock_missing};
        {error, Reason} -> {error, Reason}
    end.

touch_run(C, RunId, UpdatedAt) ->
    case epgsql:equery(
           C,
           "UPDATE workflow_runs SET updated_at_ms = $2 WHERE run_id = $1",
           [RunId, UpdatedAt]) of
        {ok, 1} -> ok;
        {ok, 0} -> {error, run_lock_missing};
        {error, Reason} -> {error, Reason}
    end.

%% Maintain the recovery-scan projection in the same transaction as the append,
%% so the index can never disagree with a committed decision. Marker-only
%% batches (recovery.requeued) leave the projection unchanged, matching the
%% reducer's no-op for unrecognized events; they still bump updated_at_ms.
update_run_after_append(C, RunId, Events, UpdatedAt) ->
    case update_run_projection_after_append(C, RunId, Events, UpdatedAt) of
        ok ->
            update_effect_index_after_append(C, RunId, Events, UpdatedAt);
        {error, _} = Error ->
            Error
    end.

update_run_projection_after_append(C, RunId, Events, UpdatedAt) ->
    case run_projection(Events) of
        no_change ->
            touch_run(C, RunId, UpdatedAt);
        #{status := Status,
          terminal := Terminal,
          next_retry_at := NextRetry,
          next_wake_at := NextWake,
          parked := Parked} ->
            update_run_projection(C, RunId, UpdatedAt, Status, Terminal,
                                  NextRetry, NextWake, Parked);
        #{status := Status,
          terminal := Terminal,
          next_retry_at := NextRetry,
          parked := Parked} ->
            update_run_projection_without_wake(C, RunId, UpdatedAt, Status,
                                               Terminal, NextRetry, Parked);
        #{next_wake_at := NextWake} ->
            update_run_next_wake(C, RunId, UpdatedAt, NextWake);
        #{parked := Parked} ->
            update_run_parked(C, RunId, UpdatedAt, Parked)
    end.

update_run_projection(C, RunId, UpdatedAt, Status, Terminal, NextRetry,
                      NextWake, Parked) ->
    case epgsql:equery(
           C,
           "UPDATE workflow_runs SET updated_at_ms = $2, status = $3, "
           "terminal = $4, next_retry_at_ms = $5, "
           "next_wake_at_ms = $6, parked = $7 WHERE run_id = $1",
           [RunId, UpdatedAt, Status, Terminal, NextRetry, NextWake, Parked]) of
        {ok, 1} -> ok;
        {ok, 0} -> {error, run_lock_missing};
        {error, Reason} -> {error, Reason}
    end.

update_run_projection_without_wake(C, RunId, UpdatedAt, Status, Terminal,
                                   NextRetry, Parked) ->
    case epgsql:equery(
           C,
           "UPDATE workflow_runs SET updated_at_ms = $2, status = $3, "
           "terminal = $4, next_retry_at_ms = $5, parked = $6 "
           "WHERE run_id = $1",
           [RunId, UpdatedAt, Status, Terminal, NextRetry, Parked]) of
        {ok, 1} -> ok;
        {ok, 0} -> {error, run_lock_missing};
        {error, Reason} -> {error, Reason}
    end.

update_run_next_wake(C, RunId, UpdatedAt, NextWake) ->
    case epgsql:equery(
           C,
           "UPDATE workflow_runs SET updated_at_ms = $2, "
           "next_wake_at_ms = $3 WHERE run_id = $1",
           [RunId, UpdatedAt, NextWake]) of
        {ok, 1} -> ok;
        {ok, 0} -> {error, run_lock_missing};
        {error, Reason} -> {error, Reason}
    end.

update_run_parked(C, RunId, UpdatedAt, Parked) ->
    case epgsql:equery(
           C,
           "UPDATE workflow_runs SET updated_at_ms = $2, parked = $3 "
           "WHERE run_id = $1",
           [RunId, UpdatedAt, Parked]) of
        {ok, 1} -> ok;
        {ok, 0} -> {error, run_lock_missing};
        {error, Reason} -> {error, Reason}
    end.

%% Derive the recovery-scan projection from a committed batch. The reducer is
%% the authority for run state (see beamtrail_reducer); this mirrors only the
%% columns the scan filters on. The parity assertion in the PostgreSQL
%% integration test guards against drift from the reducer.
run_projection(Events) ->
    lists:foldl(fun project_event/2, no_change, Events).

project_event(#{event_type := 'workflow.instance.created'}, _Acc) ->
    #{status => <<"running">>, terminal => false,
      next_retry_at => null, next_wake_at => null, parked => false};
project_event(#{event_type := 'attempt.started'}, Acc) ->
    project_status(<<"running">>, false, null, false, Acc);
project_event(#{event_type := 'step.succeeded'}, Acc) ->
    project_status(<<"running">>, false, null, false, Acc);
project_event(#{event_type := 'timer.scheduled', payload := Payload}, Acc) ->
    project_next_wake(maps:get(next_wake_at, Payload,
                               maps:get(fire_at_ms, Payload, null)),
                      Acc);
project_event(#{event_type := 'timer.fired', payload := Payload}, Acc) ->
    project_next_wake(maps:get(next_wake_at, Payload, null), Acc);
project_event(#{event_type := 'step.failed'}, Acc) ->
    project_status(<<"failed">>, false, null, false, Acc);
project_event(#{event_type := 'retry.scheduled', payload := Payload}, Acc) ->
    project_status(<<"retrying">>, false,
                   maps:get(next_retry_at, Payload, null), false, Acc);
project_event(#{event_type := 'workflow.waiting'}, Acc) ->
    project_status(<<"waiting">>, false, null, false, Acc);
project_event(#{event_type := 'activity.scheduled',
                payload := #{effect_type := external_step} = Payload}, Acc) ->
    project_status(<<"waiting_effect">>, false, null,
                   maps:get(deadline_at_ms, Payload, null), false, Acc);
project_event(#{event_type := 'signal.received'}, Acc) ->
    project_status(<<"running">>, false, null, false, Acc);
project_event(#{event_type := 'workflow.completed'}, _Acc) ->
    #{status => <<"completed">>, terminal => true,
      next_retry_at => null, next_wake_at => null, parked => false};
project_event(#{event_type := 'workflow.failed'}, _Acc) ->
    #{status => <<"failed">>, terminal => true,
      next_retry_at => null, next_wake_at => null, parked => false};
project_event(#{event_type := 'workflow.cancelled'}, _Acc) ->
    #{status => <<"cancelled">>, terminal => true,
      next_retry_at => null, next_wake_at => null, parked => false};
project_event(#{event_type := 'workflow.parked'}, _Acc) ->
    #{parked => true};
project_event(#{event_type := 'workflow.resumed'}, _Acc) ->
    #{parked => false};
project_event(_Event, Acc) ->
    Acc.

project_status(Status, Terminal, NextRetry, Parked, Acc) ->
    Base = #{status => Status, terminal => Terminal,
             next_retry_at => NextRetry, parked => Parked},
    case Acc of
        #{next_wake_at := NextWake} ->
            Base#{next_wake_at => NextWake};
        _ ->
            Base
    end.

project_status(Status, Terminal, NextRetry, NextWake, Parked, _Acc) ->
    #{status => Status, terminal => Terminal,
      next_retry_at => NextRetry, next_wake_at => NextWake, parked => Parked}.

project_next_wake(NextWake, no_change) ->
    #{next_wake_at => NextWake};
project_next_wake(NextWake, Acc) ->
    Acc#{next_wake_at => NextWake}.

update_effect_index_after_append(C, RunId, Events, UpdatedAt) ->
    lists:foldl(
      fun(_Event, {error, _} = Error) ->
              Error;
         (Event, ok) ->
              project_effect_event(C, RunId, Event, UpdatedAt)
      end,
      ok,
      Events).

project_effect_event(C, RunId,
                     #{event_type := 'activity.scheduled',
                       step_id := StepId,
                       step_version := StepVersion,
                       idempotency_key := IdempotencyKey,
                       payload := #{effect_id := EffectId,
                                    effect_type := external_step,
                                    attempt := Attempt} = Payload},
                     UpdatedAt) ->
    upsert_effect_index(C, RunId, EffectId, external_step, StepId, StepVersion,
                        IdempotencyKey, Attempt, <<"scheduled">>, Payload,
                        UpdatedAt);
project_effect_event(C, RunId,
                     #{event_type := 'effect.claimed',
                       payload := #{effect_id := EffectId} = Payload},
                     UpdatedAt) ->
    update_effect_claim(C, RunId, EffectId, Payload, UpdatedAt);
project_effect_event(C, RunId,
                     #{event_type := EventType,
                       payload := #{effect_id := EffectId}},
                     _UpdatedAt)
  when EventType =:= 'activity.succeeded';
       EventType =:= 'activity.failed' ->
    delete_effect_index(C, RunId, EffectId);
project_effect_event(C, RunId, #{event_type := EventType}, _UpdatedAt)
  when EventType =:= 'workflow.completed';
       EventType =:= 'workflow.failed';
       EventType =:= 'workflow.cancelled' ->
    delete_all_effects_for_run(C, RunId);
project_effect_event(_C, _RunId, _Event, _UpdatedAt) ->
    ok.

upsert_effect_index(C, RunId, EffectId, EffectType, StepId, StepVersion,
                    IdempotencyKey, Attempt, Status, Payload, UpdatedAt) ->
    case epgsql:equery(
           C,
           "INSERT INTO workflow_effects "
           "(run_id, effect_id, effect_type, step_id, step_version, "
           "idempotency_key, attempt, status, visible_at_ms, "
           "visibility_timeout_ms, deadline_at_ms, updated_at_ms) "
           "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) "
           "ON CONFLICT (run_id, effect_id) DO UPDATE SET "
           "effect_type = EXCLUDED.effect_type, "
           "step_id = EXCLUDED.step_id, "
           "step_version = EXCLUDED.step_version, "
           "idempotency_key = EXCLUDED.idempotency_key, "
           "attempt = EXCLUDED.attempt, "
           "status = EXCLUDED.status, "
           "visible_at_ms = EXCLUDED.visible_at_ms, "
           "visibility_timeout_ms = EXCLUDED.visibility_timeout_ms, "
           "deadline_at_ms = EXCLUDED.deadline_at_ms, "
           "claim_owner = NULL, claim_token = NULL, claim_until_ms = NULL, "
           "claimed_at_ms = NULL, updated_at_ms = EXCLUDED.updated_at_ms",
           [RunId, term_to_binary(EffectId), atom_to_binary(EffectType, utf8),
            nullable_atom(StepId), nullable_int(StepVersion),
            nullable_term(IdempotencyKey), nullable_int(Attempt), Status,
            nullable_int(maps:get(visible_at_ms, Payload, undefined)),
            nullable_int(maps:get(visibility_timeout_ms, Payload, undefined)),
            nullable_int(maps:get(deadline_at_ms, Payload, undefined)),
            UpdatedAt]) of
        {ok, 1} -> ok;
        {error, Reason} -> {error, Reason}
    end.

update_effect_claim(C, RunId, EffectId, Payload, UpdatedAt) ->
    case epgsql:equery(
           C,
           "UPDATE workflow_effects SET status = 'claimed', claim_owner = $3, "
           "claim_token = $4, claim_until_ms = $5, claimed_at_ms = $6, "
           "updated_at_ms = $7 WHERE run_id = $1 AND effect_id = $2",
           [RunId, term_to_binary(EffectId),
            nullable_term(maps:get(claim_owner, Payload, undefined)),
            nullable_term(maps:get(claim_token, Payload, undefined)),
            nullable_int(maps:get(claim_until_ms, Payload, undefined)),
            nullable_int(maps:get(claimed_at_ms, Payload, undefined)),
            UpdatedAt]) of
        {ok, 1} -> ok;
        {ok, 0} -> ok;
        {error, Reason} -> {error, Reason}
    end.

delete_effect_index(C, RunId, EffectId) ->
    case epgsql:equery(
           C,
           "DELETE FROM workflow_effects WHERE run_id = $1 AND effect_id = $2",
           [RunId, term_to_binary(EffectId)]) of
        {ok, _Count} -> ok;
        {error, Reason} -> {error, Reason}
    end.

delete_all_effects_for_run(C, RunId) ->
    case epgsql:equery(C, "DELETE FROM workflow_effects WHERE run_id = $1",
                       [RunId]) of
        {ok, _Count} -> ok;
        {error, Reason} -> {error, Reason}
    end.

%% One-time normalization for runs that predate the projection columns: replays
%% each run through the reducer and writes its current projection. Safe to run
%% repeatedly; it does not touch updated_at_ms.
backfill_run_projections() ->
    backfill_run_projections(undefined, []).

backfill_run_projections(Cursor, Failures) ->
    case list_run_ids(Cursor, 500) of
        {ok, #{run_ids := []}} ->
            backfill_result(Failures);
        {ok, #{run_ids := RunIds, has_more := HasMore, next_cursor := Next}} ->
            Failures1 = lists:foldl(fun collect_backfill_result/2,
                                     Failures,
                                     RunIds),
            case HasMore of
                true -> backfill_run_projections(Next, Failures1);
                false -> backfill_result(Failures1)
            end;
        {error, _} = Error ->
            Error
    end.

collect_backfill_result(RunId, Failures) ->
    case backfill_one(RunId) of
        ok ->
            Failures;
        {error, Reason} ->
            [#{run_id => RunId, reason => Reason} | Failures]
    end.

backfill_result([]) ->
    ok;
backfill_result(Failures) ->
    {error, {backfill_failed, lists:reverse(Failures)}}.

backfill_one(RunId) ->
    case beamtrail_state:load(RunId, ?MODULE) of
        {ok, State} ->
            write_backfill_projection(RunId, State);
        {error, Reason} ->
            {error, Reason}
    end.

write_backfill_projection(RunId, State) ->
    Status = atom_to_binary(maps:get(status, State, running), utf8),
    Terminal = maps:get(terminal, State, false),
    Parked = maps:get(parked, State, false),
    NextRetry = case maps:get(next_retry_at, State, undefined) of
                    undefined -> null;
                    N -> N
                end,
    NextWake = case maps:get(next_wake_at, State, undefined) of
                   undefined -> null;
                   W -> W
               end,
    case with_connection(
           fun(C) ->
                   epgsql:equery(
                     C,
                     "UPDATE workflow_runs SET status = $2, "
                     "terminal = $3, next_retry_at_ms = $4, "
                     "next_wake_at_ms = $5, parked = $6 "
                     "WHERE run_id = $1",
                     [RunId, Status, Terminal, NextRetry, NextWake, Parked])
           end) of
        {ok, 1} ->
            ok;
        {ok, 0} ->
            {error, run_not_found};
        {ok, Count} ->
            {error, {unexpected_update_count, Count}};
        {error, Reason} ->
            {error, Reason}
    end.

backfill_effect_index() ->
    backfill_effect_index(undefined, []).

backfill_effect_index(Cursor, Failures) ->
    case list_run_ids(Cursor, 500) of
        {ok, #{run_ids := []}} ->
            backfill_result(Failures);
        {ok, #{run_ids := RunIds, has_more := HasMore, next_cursor := Next}} ->
            Failures1 = lists:foldl(fun collect_effect_backfill_result/2,
                                     Failures,
                                     RunIds),
            case HasMore of
                true -> backfill_effect_index(Next, Failures1);
                false -> backfill_result(Failures1)
            end;
        {error, _} = Error ->
            Error
    end.

collect_effect_backfill_result(RunId, Failures) ->
    case backfill_effects_one(RunId) of
        ok ->
            Failures;
        {error, Reason} ->
            [#{run_id => RunId, reason => Reason} | Failures]
    end.

backfill_effects_one(RunId) ->
    case beamtrail_state:load(RunId, ?MODULE) of
        {ok, State} ->
            write_backfill_effects(RunId, State);
        {error, Reason} ->
            {error, Reason}
    end.

write_backfill_effects(RunId, State) ->
    Effects =
        [Effect || Effect <- maps:values(maps:get(pending_effects, State, #{})),
                   maps:get(effect_type, Effect, undefined) =:= external_step],
    transaction(
      fun(C) ->
              case lock_run(C, RunId) of
                  ok ->
                      case delete_all_effects_for_run(C, RunId) of
                          ok -> backfill_effect_rows(C, RunId, Effects, now_ms());
                          {error, _} = Error -> Error
                      end;
                  {error, _} = Error ->
                      Error
              end
      end).

backfill_effect_rows(_C, _RunId, [], _UpdatedAt) ->
    ok;
backfill_effect_rows(C, RunId, [Effect | Rest], UpdatedAt) ->
    case upsert_effect_index_from_effect(C, RunId, Effect, UpdatedAt) of
        ok -> backfill_effect_rows(C, RunId, Rest, UpdatedAt);
        {error, _} = Error -> Error
    end.

upsert_effect_index_from_effect(C, RunId, Effect, UpdatedAt) ->
    Payload =
        maps:with([visible_at_ms, visibility_timeout_ms, deadline_at_ms],
                  Effect),
    Status = atom_to_binary(maps:get(status, Effect, scheduled), utf8),
    case upsert_effect_index(
           C, RunId, maps:get(effect_id, Effect),
           maps:get(effect_type, Effect), maps:get(step_id, Effect, undefined),
           maps:get(step_version, Effect, undefined),
           maps:get(idempotency_key, Effect, undefined),
           maps:get(attempt, Effect, undefined), Status, Payload, UpdatedAt) of
        ok ->
            maybe_backfill_effect_claim(C, RunId, Effect, UpdatedAt);
        {error, _} = Error ->
            Error
    end.

maybe_backfill_effect_claim(C, RunId, #{claim_token := _} = Effect,
                            UpdatedAt) ->
    update_effect_claim(C, RunId, maps:get(effect_id, Effect), Effect,
                        UpdatedAt);
maybe_backfill_effect_claim(_C, _RunId, _Effect, _UpdatedAt) ->
    ok.

current_event_seq(C, RunId) ->
    case epgsql:equery(C,
                       "SELECT COALESCE(MAX(event_seq), 0) "
                       "FROM workflow_events WHERE run_id = $1",
                       [RunId]) of
        {ok, _Cols, [{Seq}]} -> {ok, Seq};
        {error, Reason} -> {error, Reason}
    end.

validate_fencing(_C, _RunId, 'workflow.instance.created', undefined) ->
    ok;
validate_fencing(_C, _RunId, 'signal.received', undefined) ->
    ok;
validate_fencing(C, RunId, _EventType, FencingToken) when is_integer(FencingToken) ->
    Now = now_ms(),
    case select_lease(C, RunId, "FOR UPDATE") of
        not_found ->
            {error, no_lease};
        {ok, #{lease_until := LeaseUntil}} when LeaseUntil =< Now ->
            {error, lease_expired};
        {ok, #{fencing_token := FencingToken}} ->
            ok;
        {ok, #{fencing_token := Current}} when FencingToken < Current ->
            {error, stale_fence};
        {ok, #{fencing_token := Current}} ->
            {error, {invalid_fence, #{provided => FencingToken,
                                      current => Current}}};
        {error, _} = Error ->
            Error
    end;
validate_fencing(_C, _RunId, _EventType, undefined) ->
    {error, missing_fence}.

select_lease_for_update(C, RunId) ->
    select_lease(C, RunId, "FOR UPDATE").

select_lease(C, RunId, Suffix) ->
    Sql = "SELECT owner_node, lease_until_ms, fencing_token, "
          "acquired_at_ms, updated_at_ms FROM workflow_leases "
          "WHERE run_id = $1 " ++ Suffix,
    case epgsql:equery(C, Sql, [RunId]) of
        {ok, _Cols, []} ->
            not_found;
        {ok, _Cols, [{OwnerBin, Until, Fence, AcquiredAt, UpdatedAt}]} ->
            case decode_term(OwnerBin) of
                {ok, Owner} ->
                    {ok, lease_map(RunId, Owner, Until, Fence,
                                   AcquiredAt, UpdatedAt)};
                {error, _} = Error ->
                    Error
            end;
        {error, Reason} ->
            {error, Reason}
    end.

insert_lease(C, Lease) ->
    Params = lease_params(Lease),
    case epgsql:equery(
           C,
           "INSERT INTO workflow_leases "
           "(run_id, owner_node, lease_until_ms, fencing_token, acquired_at_ms, updated_at_ms) "
           "VALUES ($1,$2,$3,$4,$5,$6)",
           Params) of
        {ok, 1} -> {ok, Lease};
        {error, #error{codename = unique_violation}} -> {error, leased};
        {error, Reason} -> {error, Reason}
    end.

update_lease(C, Lease) ->
    Params = lease_params(Lease),
    case epgsql:equery(
           C,
           "UPDATE workflow_leases SET owner_node = $2, lease_until_ms = $3, "
           "fencing_token = $4, acquired_at_ms = $5, updated_at_ms = $6 "
           "WHERE run_id = $1",
           Params) of
        {ok, 1} -> {ok, Lease};
        {error, Reason} -> {error, Reason}
    end.

lease_params(#{run_id := RunId,
               owner_node := Owner,
               lease_until := Until,
               fencing_token := Fence,
               acquired_at := AcquiredAt,
               updated_at := UpdatedAt}) ->
    [RunId, term_to_binary(Owner), Until, Fence, AcquiredAt, UpdatedAt].

lease_map(RunId, Owner, Until, Fence, AcquiredAt, UpdatedAt) ->
    #{run_id => RunId,
      owner_node => Owner,
      lease_until => Until,
      fencing_token => Fence,
      acquired_at => AcquiredAt,
      updated_at => UpdatedAt}.

decode_events({ok, _Cols, Rows}, RunId) ->
    decode_event_rows(Rows, RunId, []);
decode_events({error, Reason}, _RunId) ->
    {error, Reason}.

decode_event_rows([], _RunId, Acc) ->
    {ok, lists:reverse(Acc)};
decode_event_rows([Row | Rest], RunId, Acc) ->
    case decode_event(RunId, Row) of
        {ok, Event} -> decode_event_rows(Rest, RunId, [Event | Acc]);
        {error, _} = Error -> Error
    end.

decode_event(RunId, {Seq, TypeBin, StepBin, StepVersion,
                     IdempotencyBin, PayloadBin, Fence, OccurredAt}) ->
    case {decode_event_type(TypeBin),
          decode_step_id(StepBin),
          decode_term(IdempotencyBin),
          decode_term(PayloadBin)} of
        {{ok, EventType}, {ok, StepId}, {ok, IdempotencyKey}, {ok, Payload}} ->
            {ok, #{run_id => RunId,
                   event_seq => Seq,
                   event_type => EventType,
                   step_id => StepId,
                   step_version => decode_null(StepVersion),
                   idempotency_key => IdempotencyKey,
                   payload => Payload,
                   fencing_token => decode_null(Fence),
                   occurred_at => OccurredAt}};
        {{error, _} = Error, _, _, _} ->
            Error;
        {_, {error, _} = Error, _, _} ->
            Error;
        {_, _, {error, _} = Error, _} ->
            Error;
        {_, _, _, {error, _} = Error} ->
            Error
    end.

decode_pending_effect_rows(Rows) ->
    decode_pending_effect_rows(Rows, []).

decode_pending_effect_rows([], Acc) ->
    {ok, lists:reverse(Acc)};
decode_pending_effect_rows([Row | Rest], Acc) ->
    case decode_pending_effect_row(Row) of
        {ok, Effect} -> decode_pending_effect_rows(Rest, [Effect | Acc]);
        {error, _} = Error -> Error
    end.

decode_pending_effect_row({RunId, EffectIdBin, EffectTypeBin, StepBin,
                           StepVersion, IdempotencyBin, Attempt, StatusBin,
                           VisibleAt, VisibilityTimeout, DeadlineAt,
                           ClaimOwnerBin, ClaimTokenBin, ClaimUntil,
                           ClaimedAt}) ->
    case {decode_term(EffectIdBin),
          decode_effect_type(EffectTypeBin),
          decode_step_id(StepBin),
          decode_term(IdempotencyBin),
          decode_term(ClaimOwnerBin),
          decode_term(ClaimTokenBin)} of
        {{ok, EffectId}, {ok, EffectType}, {ok, StepId},
         {ok, IdempotencyKey}, {ok, ClaimOwner}, {ok, ClaimToken}} ->
            Base =
                #{run_id => RunId,
                  effect_id => EffectId,
                  effect_type => EffectType,
                  step_id => StepId,
                  step_version => decode_null(StepVersion),
                  idempotency_key => IdempotencyKey,
                  attempt => Attempt,
                  status => decode_effect_status(StatusBin),
                  visible_at_ms => decode_null(VisibleAt),
                  visibility_timeout_ms => decode_null(VisibilityTimeout)},
            {ok, maybe_put_defined(
                   claimed_at_ms, decode_null(ClaimedAt),
                   maybe_put_defined(
                     claim_until_ms, decode_null(ClaimUntil),
                     maybe_put_defined(
                       claim_token, ClaimToken,
                       maybe_put_defined(
                         claim_owner, ClaimOwner,
                         maybe_put_defined(deadline_at_ms,
                                           decode_null(DeadlineAt),
                                           Base)))))};
        {{error, _} = Error, _, _, _, _, _} ->
            Error;
        {_, {error, _} = Error, _, _, _, _} ->
            Error;
        {_, _, {error, _} = Error, _, _, _} ->
            Error;
        {_, _, _, {error, _} = Error, _, _} ->
            Error;
        {_, _, _, _, {error, _} = Error, _} ->
            Error;
        {_, _, _, _, _, {error, _} = Error} ->
            Error
    end.

nullable_atom(undefined) -> null;
nullable_atom(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8).

nullable_int(undefined) -> null;
nullable_int(Int) when is_integer(Int) -> Int.

nullable_term(undefined) -> null;
nullable_term(Term) -> term_to_binary(Term).

decode_effect_type(<<"call_step">>) -> {ok, call_step};
decode_effect_type(<<"external_step">>) -> {ok, external_step};
decode_effect_type(Bin) when is_binary(Bin) -> {error, {unknown_effect_type, Bin}}.

decode_effect_status(<<"scheduled">>) -> scheduled;
decode_effect_status(<<"claimed">>) -> claimed.

maybe_put_defined(_Key, undefined, Map) ->
    Map;
maybe_put_defined(Key, Value, Map) ->
    Map#{Key => Value}.

decode_event_type(<<"workflow.instance.created">>) ->
    {ok, 'workflow.instance.created'};
decode_event_type(<<"attempt.started">>) ->
    {ok, 'attempt.started'};
decode_event_type(<<"activity.scheduled">>) ->
    {ok, 'activity.scheduled'};
decode_event_type(<<"activity.started">>) ->
    {ok, 'activity.started'};
decode_event_type(<<"activity.succeeded">>) ->
    {ok, 'activity.succeeded'};
decode_event_type(<<"activity.failed">>) ->
    {ok, 'activity.failed'};
decode_event_type(<<"effect.claimed">>) ->
    {ok, 'effect.claimed'};
decode_event_type(<<"step.succeeded">>) ->
    {ok, 'step.succeeded'};
decode_event_type(<<"step.failed">>) ->
    {ok, 'step.failed'};
decode_event_type(<<"retry.scheduled">>) ->
    {ok, 'retry.scheduled'};
decode_event_type(<<"timer.scheduled">>) ->
    {ok, 'timer.scheduled'};
decode_event_type(<<"timer.fired">>) ->
    {ok, 'timer.fired'};
decode_event_type(<<"workflow.waiting">>) ->
    {ok, 'workflow.waiting'};
decode_event_type(<<"signal.received">>) ->
    {ok, 'signal.received'};
decode_event_type(<<"workflow.completed">>) ->
    {ok, 'workflow.completed'};
decode_event_type(<<"workflow.failed">>) ->
    {ok, 'workflow.failed'};
decode_event_type(<<"workflow.cancelled">>) ->
    {ok, 'workflow.cancelled'};
decode_event_type(<<"workflow.parked">>) ->
    {ok, 'workflow.parked'};
decode_event_type(<<"workflow.resumed">>) ->
    {ok, 'workflow.resumed'};
decode_event_type(<<"recovery.requeued">>) ->
    {ok, 'recovery.requeued'};
decode_event_type(<<"recovery.skipped">>) ->
    {ok, 'recovery.skipped'};
decode_event_type(Bin) when is_binary(Bin) ->
    {error, {unknown_event_type, Bin}}.

decode_step_id(null) ->
    {ok, undefined};
decode_step_id(Bin) when is_binary(Bin) ->
    try binary_to_existing_atom(Bin, utf8) of
        StepId -> {ok, StepId}
    catch
        error:badarg -> {error, {unknown_step_id, Bin}}
    end.

decode_null(null) -> undefined;
decode_null(Value) -> Value.

decode_term(null) ->
    {ok, undefined};
decode_term(Bin) when is_binary(Bin) ->
    decode_term(Bin, false).

decode_term(Bin, Retried) ->
    try binary_to_term(Bin, [safe]) of
        Term -> {ok, Term}
    catch
        error:badarg ->
            maybe_preload_and_decode_term(Bin, Retried)
    end.

maybe_preload_and_decode_term(_Bin, true) ->
    {error, bad_external_term};
maybe_preload_and_decode_term(Bin, false) ->
    case beamtrail_config:preload_workflows() of
        ok ->
            decode_term(Bin, true);
        {error, Reason} ->
            {error, {bad_external_term, Reason}}
    end.

next_cursor([]) -> undefined;
next_cursor(Page) -> lists:last(Page).

now_ms() ->
    erlang:system_time(millisecond).
