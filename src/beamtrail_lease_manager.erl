-module(beamtrail_lease_manager).

%% Thin facade over the storage lease primitives used by dispatch,
%% recovery handoff, and heartbeat renewal.

-export([acquire/2, acquire/3, renew/2, renew/3, read/1, default_ttl_ms/0]).

-define(DEFAULT_TTL_MS, 30000).

acquire(RunId, Owner) ->
    acquire(RunId, Owner, default_ttl_ms()).

acquire(RunId, Owner, TtlMs) ->
    (beamtrail:storage()):acquire_lease(RunId, Owner, TtlMs).

renew(RunId, FencingToken) ->
    renew(RunId, FencingToken, default_ttl_ms()).

renew(RunId, FencingToken, TtlMs) ->
    (beamtrail:storage()):renew_lease(RunId, FencingToken, TtlMs).

read(RunId) ->
    (beamtrail:storage()):read_lease(RunId).

default_ttl_ms() ->
    case application:get_env(beamtrail, lease_ttl_ms) of
        {ok, N} when is_integer(N), N > 0 -> N;
        _ -> ?DEFAULT_TTL_MS
    end.
