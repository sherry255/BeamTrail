-module(beamtrail_worker_sup).
-behaviour(supervisor).

%% Bounded worker pool for dispatching workflow runs off the scanner's
%% process. simple_one_for_one: each dispatch spawns a transient
%% worker that calls `beamtrail:dispatch' and exits. The scanner stays
%% responsive even if a single run blocks.

-export([start_link/0, init/1, dispatch_async/1, dispatch_async/2]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Child = #{id => beamtrail_worker,
              start => {beamtrail_worker, start_link, []},
              restart => temporary,
              shutdown => 5000,
              type => worker,
              modules => [beamtrail_worker]},
    {ok, {{simple_one_for_one, 5, 10}, [Child]}}.

dispatch_async(RunId) ->
    supervisor:start_child(?MODULE, [RunId]).

dispatch_async(RunId, Lease) ->
    supervisor:start_child(?MODULE, [RunId, Lease]).
