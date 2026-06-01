-module(beamtrail_lease_manager).

%% Thin facade over the storage lease primitives used by dispatch,
%% recovery handoff, and heartbeat renewal.

-export([acquire/2, acquire/3, renew/2, renew/3, read/1, default_ttl_ms/0]).

-define(DEFAULT_TTL_MS, 30000).

acquire(RunId, Owner) ->
    acquire(RunId, Owner, ?DEFAULT_TTL_MS).

acquire(RunId, Owner, TtlMs) ->
    (beamtrail:storage()):acquire_lease(RunId, Owner, TtlMs).

renew(RunId, FencingToken) ->
    renew(RunId, FencingToken, ?DEFAULT_TTL_MS).

renew(RunId, FencingToken, TtlMs) ->
    (beamtrail:storage()):renew_lease(RunId, FencingToken, TtlMs).

read(RunId) ->
    (beamtrail:storage()):read_lease(RunId).

default_ttl_ms() -> ?DEFAULT_TTL_MS.
