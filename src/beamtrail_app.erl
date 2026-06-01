-module(beamtrail_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    case beamtrail_config:preload_workflows() of
        ok -> beamtrail_sup:start_link();
        {error, Reason} -> {error, Reason}
    end.

stop(_State) ->
    ok.
