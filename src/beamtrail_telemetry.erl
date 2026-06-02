-module(beamtrail_telemetry).

-export([execute/3]).

execute(EventName, Measurements, Metadata) ->
    bump_storage_counter(EventName, Measurements),
    Mod = telemetry_module(),
    case code:ensure_loaded(Mod) of
        {module, Mod} ->
            case erlang:function_exported(Mod, execute, 3) of
                true -> erlang:apply(Mod, execute,
                                      [EventName, Measurements, Metadata]);
                false -> ok
            end;
        _ ->
            ok
    end.

telemetry_module() ->
    list_to_atom("telemetry").

%% Counters are an optional capability of the storage adapter. Adapters that
%% want in-process counters (e.g. beamtrail_memory_storage) export
%% bump_counter/2; adapters that don't simply skip. The :telemetry application,
%% if present, is always notified.
bump_storage_counter(EventName, Measurements) ->
    Count = maps:get(count, Measurements, 1),
    Mod = try beamtrail_config:storage()
          catch _:_ -> beamtrail_memory_storage end,
    case erlang:function_exported(Mod, bump_counter, 2) of
        true ->
            case whereis(Mod) of
                undefined -> ok;
                _ -> catch Mod:bump_counter(EventName, Count), ok
            end;
        false -> ok
    end.
