-module(beamtrail_postgres_config).

-export([connection/0, pool_size/0]).

-define(DEFAULT_POOL_SIZE, 5).

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
