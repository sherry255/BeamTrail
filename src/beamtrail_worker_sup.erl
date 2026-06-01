-module(beamtrail_worker_sup).
-behaviour(supervisor).

%% Bounded worker supervisor for dispatching workflow runs off the scanner's
%% process. Each dispatch spawns a temporary worker that calls
%% `beamtrail:dispatch' and exits. The scanner stays responsive even if a
%% single run blocks.

-export([start_link/0, init/1, dispatch_async/1, dispatch_async/2]).

-define(DEFAULT_MAX_WORKERS, 64).

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
    start_child_bounded([RunId]).

dispatch_async(RunId, Lease) ->
    start_child_bounded([RunId, Lease]).

start_child_bounded(Args) ->
    Active = proplists:get_value(active, supervisor:count_children(?MODULE), 0),
    case Active >= max_workers() of
        true -> {error, worker_pool_full};
        false -> supervisor:start_child(?MODULE, Args)
    end.

max_workers() ->
    case application:get_env(beamtrail, worker_max_children) of
        {ok, N} when is_integer(N), N > 0 -> N;
        _ -> ?DEFAULT_MAX_WORKERS
    end.
