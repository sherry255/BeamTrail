-module(beamtrail_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Children =
        [#{id => beamtrail_memory_storage,
           start => {beamtrail_memory_storage, start_link, []},
           restart => permanent,
           shutdown => 5000,
           type => worker,
           modules => [beamtrail_memory_storage]}],
    {ok, {{one_for_one, 3, 10}, Children}}.
