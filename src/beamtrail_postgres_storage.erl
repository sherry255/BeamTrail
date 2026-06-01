-module(beamtrail_postgres_storage).
-behaviour(beamtrail_storage).

%%% PostgreSQL adapter stub.
%%%
%%% This pass intentionally ships only the schema (priv/sql/postgres.sql)
%%% and this adapter scaffold. Wiring a real epgsql/postgrex connection,
%%% migration runner, JSON encoding, and txn boundaries is non-trivial and
%%% out of scope for this iteration. Calls return {error, not_implemented}
%%% so callers get a loud, honest failure instead of silently falling back
%%% to memory storage.
%%%
%%% Required to finish this adapter:
%%%   * epgsql driver + connection pool wiring (sup tree)
%%%   * SQL implementations of each callback against priv/sql/postgres.sql
%%%   * JSON codec for payload/state (jsone or jiffy)
%%%   * Tests against a real Postgres instance (docker-compose + ct)
%%%
%%% Until then, the in-memory adapter is the only supported backend.

-export([append_event/8, read_events/3, events/1, write_snapshot/4, read_snapshot/1,
         acquire_lease/3, renew_lease/3, read_lease/1, list_run_ids/0, list_run_ids/2]).

append_event(_, _, _, _, _, _, _, _) -> {error, not_implemented}.
read_events(_, _, _) -> {error, not_implemented}.
events(_) -> {error, not_implemented}.
write_snapshot(_, _, _, _) -> {error, not_implemented}.
read_snapshot(_) -> {error, not_implemented}.
acquire_lease(_, _, _) -> {error, not_implemented}.
renew_lease(_, _, _) -> {error, not_implemented}.
read_lease(_) -> not_found.
list_run_ids() -> {error, not_implemented}.
list_run_ids(_, _) -> {error, not_implemented}.
