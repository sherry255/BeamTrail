-module(beamtrail_telemetry).

-export([execute/3]).

execute(EventName, Measurements, Metadata) ->
    case code:ensure_loaded(telemetry) of
        {module, telemetry} ->
            case erlang:function_exported(telemetry, execute, 3) of
                true -> telemetry:execute(EventName, Measurements, Metadata);
                false -> ok
            end;
        _ ->
            ok
    end.
