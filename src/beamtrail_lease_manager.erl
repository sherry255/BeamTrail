-module(beamtrail_lease_manager).

%% Thin facade over the storage lease primitives used by dispatch,
%% recovery handoff, and heartbeat renewal.

-export([acquire/2, acquire/3, renew/2, renew/3, read/1,
         fencing_token/1, ttl_ms/1, heartbeat_interval_ms/0,
         heartbeat_interval_ms/1, default_ttl_ms/0]).

-define(DEFAULT_TTL_MS, 30000).

acquire(RunId, Owner) ->
    acquire(RunId, Owner, default_ttl_ms()).

acquire(RunId, Owner, TtlMs) ->
    (beamtrail_config:storage()):acquire_lease(RunId, Owner, TtlMs).

renew(RunId, FencingToken) ->
    renew(RunId, FencingToken, default_ttl_ms()).

renew(RunId, FencingToken, TtlMs) ->
    (beamtrail_config:storage()):renew_lease(RunId, FencingToken, TtlMs).

read(RunId) ->
    (beamtrail_config:storage()):read_lease(RunId).

fencing_token(Lease) when is_map(Lease) ->
    maps:get(fencing_token, Lease, undefined);
fencing_token(_) ->
    undefined.

ttl_ms(#{lease_until := Until, renewed_at := RenewedAt})
  when is_integer(Until), is_integer(RenewedAt), Until > RenewedAt ->
    Until - RenewedAt;
ttl_ms(#{lease_until := Until, acquired_at := AcquiredAt})
  when is_integer(Until), is_integer(AcquiredAt), Until > AcquiredAt ->
    Until - AcquiredAt;
ttl_ms(#{lease_until := Until}) when is_integer(Until) ->
    max(1, Until - erlang:system_time(millisecond));
ttl_ms(_) ->
    default_ttl_ms().

heartbeat_interval_ms() ->
    heartbeat_interval_ms(default_ttl_ms()).

heartbeat_interval_ms(Lease) when is_map(Lease) ->
    heartbeat_interval_ms(ttl_ms(Lease));
heartbeat_interval_ms(TtlMs) when is_integer(TtlMs) ->
    max(1, min(5000, TtlMs div 3));
heartbeat_interval_ms(_) ->
    heartbeat_interval_ms().

default_ttl_ms() ->
    case application:get_env(beamtrail, lease_ttl_ms) of
        {ok, N} when is_integer(N), N > 0 -> N;
        _ -> ?DEFAULT_TTL_MS
    end.
