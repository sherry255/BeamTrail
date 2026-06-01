-module(beamtrail_run_sup).
-behaviour(supervisor).

%% Dynamic supervisor for active workflow-run processes.

-export([start_link/0, init/1, dispatch/1, dispatch/2, start_run/1]).

-define(DEFAULT_MAX_RUNS, 64).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

dispatch(RunId) ->
    dispatch(RunId, undefined).

dispatch(RunId, Lease) ->
    case whereis(beamtrail_run_registry) of
        undefined -> dispatch_unregistered(RunId, Lease);
        _ -> beamtrail_run_registry:dispatch(RunId, Lease)
    end.

init([]) ->
    Child = #{id => beamtrail_run,
              start => {beamtrail_run, start_link, []},
              restart => temporary,
              shutdown => 5000,
              type => worker,
              modules => [beamtrail_run]},
    {ok, {{simple_one_for_one, 5, 10}, [Child]}}.

dispatch_unregistered(RunId, Lease) ->
    case start_run(RunId) of
        {ok, Pid} ->
            beamtrail_run:dispatch(Pid, Lease),
            {ok, Pid};
        {ok, Pid, _Info} ->
            beamtrail_run:dispatch(Pid, Lease),
            {ok, Pid};
        {error, _} = Error ->
            Error
    end.

start_run(RunId) ->
    Active = proplists:get_value(active, supervisor:count_children(?MODULE), 0),
    case Active >= max_runs() of
        true -> {error, run_pool_full};
        false -> supervisor:start_child(?MODULE, [RunId])
    end.

max_runs() ->
    case application:get_env(beamtrail, run_max_children) of
        {ok, N} when is_integer(N), N > 0 -> N;
        _ -> ?DEFAULT_MAX_RUNS
    end.
