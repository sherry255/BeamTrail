-module(beamtrail_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    ScanInterval =
        case application:get_env(beamtrail, scanner_interval_ms) of
            {ok, V} -> V;
            undefined -> 5000
        end,
    StorageChild =
        case beamtrail_config:storage() of
            beamtrail_memory_storage ->
                [#{id => beamtrail_memory_storage,
                   start => {beamtrail_memory_storage, start_link, []},
                   restart => permanent,
                   shutdown => 5000,
                   type => worker,
                   modules => [beamtrail_memory_storage]}];
            _Other ->
                %% Durable adapters wire their own connection pool /
                %% supervision; we don't start them ad-hoc here.
                []
        end,
    Children = StorageChild ++
        [#{id => beamtrail_run_registry,
           start => {beamtrail_run_registry, start_link, []},
           restart => permanent,
           shutdown => 5000,
           type => worker,
           modules => [beamtrail_run_registry]},
         #{id => beamtrail_run_sup,
           start => {beamtrail_run_sup, start_link, []},
           restart => permanent,
           shutdown => 5000,
           type => supervisor,
           modules => [beamtrail_run_sup]},
         #{id => beamtrail_worker_sup,
           start => {beamtrail_worker_sup, start_link, []},
           restart => permanent,
           shutdown => 5000,
           type => supervisor,
           modules => [beamtrail_worker_sup]},
         #{id => beamtrail_scanner,
           start => {beamtrail_scanner, start_link,
                     [#{interval_ms => ScanInterval, auto_start => true}]},
           restart => permanent,
           shutdown => 5000,
           type => worker,
           modules => [beamtrail_scanner]}],
    {ok, {{one_for_one, 5, 10}, Children}}.
