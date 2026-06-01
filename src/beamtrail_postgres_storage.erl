-module(beamtrail_postgres_storage).
-behaviour(beamtrail_storage).

-include_lib("epgsql/include/epgsql.hrl").

%% PostgreSQL storage adapter. It stores Erlang payload/state/idempotency
%% terms as external-term-format bytea values so replay semantics come before
%% SQL-level inspection.

-export([init_schema/0]).
-export([append_event/8, read_events/3, events/1, write_snapshot/4, read_snapshot/1,
         acquire_lease/3, renew_lease/3, read_lease/1, list_run_ids/0, list_run_ids/2]).

append_event(RunId, ExpectedSeq, FencingToken,
             EventType, StepId, StepVersion, IdempotencyKey, Payload) ->
    transaction(
      fun(C) ->
              append_event_with_run_lock(C, RunId, ExpectedSeq, FencingToken,
                                         EventType, StepId, StepVersion,
                                         IdempotencyKey, Payload)
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
                      case Fun(C) of
                          {error, _} = Error ->
                              _ = epgsql:squery(C, "ROLLBACK"),
                              Error;
                          Other ->
                              case epgsql:squery(C, "COMMIT") of
                                  {ok, _, _} -> Other;
                                  {error, Reason} -> {error, Reason}
                              end
                      end;
                  {error, Reason} ->
                      {error, Reason}
              end
      end).

connect() ->
    case beamtrail_postgres_config:connection() of
        {ok, Config} ->
            epgsql:connect(Config);
        {error, _} = Error ->
            Error
    end.

append_event_with_run_lock(C, RunId, ExpectedSeq, FencingToken,
                           EventType, StepId, StepVersion, IdempotencyKey, Payload) ->
    case lock_run(C, RunId) of
        ok ->
            append_event_after_lock(C, RunId, ExpectedSeq, FencingToken,
                                    EventType, StepId, StepVersion,
                                    IdempotencyKey, Payload);
        {error, _} = Error ->
            Error
    end.

append_event_after_lock(C, RunId, ExpectedSeq, FencingToken,
                        EventType, StepId, StepVersion, IdempotencyKey, Payload) ->
    case current_event_seq(C, RunId) of
        {ok, ExpectedSeq} ->
            append_event_at_expected_seq(C, RunId, ExpectedSeq, FencingToken,
                                         EventType, StepId, StepVersion,
                                         IdempotencyKey, Payload);
        {ok, ActualSeq} ->
            {error, {conflict, #{expected_seq => ExpectedSeq,
                                 actual_seq => ActualSeq}}};
        {error, _} = Error ->
            Error
    end.

append_event_at_expected_seq(C, RunId, ExpectedSeq, FencingToken,
                             EventType, StepId, StepVersion, IdempotencyKey, Payload) ->
    case validate_fencing(C, RunId, EventType, FencingToken) of
        ok ->
            EventSeq = ExpectedSeq + 1,
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
                    case touch_run(C, RunId, OccurredAt) of
                        ok ->
                            {ok, #{run_id => RunId,
                                   event_seq => EventSeq,
                                   event_type => EventType,
                                   step_id => StepId,
                                   step_version => StepVersion,
                                   idempotency_key => IdempotencyKey,
                                   fencing_token => FencingToken,
                                   payload => Payload,
                                   occurred_at => OccurredAt}};
                        {error, _} = Error ->
                            Error
                    end;
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, _} = Error ->
            Error
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
    case {decode_term(IdempotencyBin), decode_term(PayloadBin)} of
        {{ok, IdempotencyKey}, {ok, Payload}} ->
            {ok, #{run_id => RunId,
                   event_seq => Seq,
                   event_type => binary_to_atom(TypeBin, utf8),
                   step_id => decode_atom(StepBin),
                   step_version => decode_null(StepVersion),
                   idempotency_key => IdempotencyKey,
                   payload => Payload,
                   fencing_token => decode_null(Fence),
                   occurred_at => OccurredAt}};
        {{error, _} = Error, _} ->
            Error;
        {_, {error, _} = Error} ->
            Error
    end.

nullable_atom(undefined) -> null;
nullable_atom(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8).

nullable_int(undefined) -> null;
nullable_int(Int) when is_integer(Int) -> Int.

nullable_term(undefined) -> null;
nullable_term(Term) -> term_to_binary(Term).

decode_atom(null) -> undefined;
decode_atom(Bin) when is_binary(Bin) -> binary_to_atom(Bin, utf8).

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
