-module(beamtrail_postgres_config).

-export([connection/0, pool_size/0, checkout_timeout_ms/0,
         reconnect_interval_ms/0]).

-define(DEFAULT_POOL_SIZE, 5).
-define(DEFAULT_CHECKOUT_TIMEOUT_MS, 5000).
-define(DEFAULT_RECONNECT_INTERVAL_MS, 1000).

connection() ->
    case application:get_env(beamtrail, postgres) of
        {ok, Config0} when is_map(Config0) ->
            Config = maps:merge(#{host => "localhost",
                                  port => 5432,
                                  ssl => false,
                                  timeout => 5000},
                                Config0),
            {ok, maps:without([pool_size], Config)};
        _ ->
            {error, postgres_not_configured}
    end.

pool_size() ->
    case application:get_env(beamtrail, postgres_pool_size) of
        {ok, N} when is_integer(N), N > 0 ->
            N;
        _ ->
            pool_size_from_postgres_config()
    end.

pool_size_from_postgres_config() ->
    case application:get_env(beamtrail, postgres) of
        {ok, #{pool_size := N}} when is_integer(N), N > 0 -> N;
        _ -> ?DEFAULT_POOL_SIZE
    end.

checkout_timeout_ms() ->
    positive_env_ms(postgres_pool_checkout_timeout_ms,
                    ?DEFAULT_CHECKOUT_TIMEOUT_MS).

reconnect_interval_ms() ->
    positive_env_ms(postgres_pool_reconnect_interval_ms,
                    ?DEFAULT_RECONNECT_INTERVAL_MS).

positive_env_ms(Name, Default) ->
    case application:get_env(beamtrail, Name) of
        {ok, N} when is_integer(N), N > 0 -> N;
        _ -> Default
    end.
