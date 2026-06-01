-module(beamtrail_config).

-export([storage/0, ensure_storage/0]).

-define(STORAGE_DEFAULT, beamtrail_memory_storage).

storage() ->
    case application:get_env(beamtrail, storage_adapter) of
        {ok, M} when is_atom(M) -> M;
        _ -> ?STORAGE_DEFAULT
    end.

ensure_storage() ->
    Mod = storage(),
    %% Only ad-hoc start for the in-memory adapter; durable adapters are
    %% expected to be started under the supervision tree with their own
    %% connection setup.
    case Mod =:= ?STORAGE_DEFAULT of
        true ->
            case whereis(Mod) of
                undefined ->
                    case Mod:start_link() of
                        {ok, _Pid} -> ok;
                        {error, {already_started, _Pid}} -> ok
                    end;
                _Pid -> ok
            end;
        false ->
            ok
    end.
