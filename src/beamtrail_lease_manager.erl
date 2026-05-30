-module(beamtrail_lease_manager).

%% Thin facade over the storage lease primitives. MVP only guards the
%% recovery handoff boundary; multi-node takeover is out of MVP scope.

-export([acquire/2, acquire/3, read/1, default_ttl_ms/0]).

-define(DEFAULT_TTL_MS, 30000).

acquire(RunId, Owner) ->
    acquire(RunId, Owner, ?DEFAULT_TTL_MS).

acquire(RunId, Owner, TtlMs) ->
    (beamtrail:storage()):acquire_lease(RunId, Owner, TtlMs).

read(RunId) ->
    (beamtrail:storage()):read_lease(RunId).

default_ttl_ms() -> ?DEFAULT_TTL_MS.
