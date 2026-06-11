-module(beamtrail_config).

-export([storage/0, ensure_storage/0, preload_workflows/0,
         max_recoveries_per_attempt/0, external_effect_visibility_timeout_ms/0,
         external_effect_timeout_ms/0]).

-define(STORAGE_DEFAULT, beamtrail_memory_storage).
-define(DEFAULT_MAX_RECOVERIES_PER_ATTEMPT, 3).
-define(DEFAULT_EXTERNAL_EFFECT_VISIBILITY_TIMEOUT_MS, 30000).
-define(DEFAULT_EXTERNAL_EFFECT_TIMEOUT_MS, infinity).

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

max_recoveries_per_attempt() ->
    case application:get_env(beamtrail, max_recoveries_per_attempt) of
        {ok, infinity} ->
            infinity;
        {ok, N} when is_integer(N), N >= 0 ->
            N;
        _ ->
            ?DEFAULT_MAX_RECOVERIES_PER_ATTEMPT
    end.

external_effect_visibility_timeout_ms() ->
    positive_or_infinity_env_ms(external_effect_visibility_timeout_ms,
                                ?DEFAULT_EXTERNAL_EFFECT_VISIBILITY_TIMEOUT_MS).

external_effect_timeout_ms() ->
    positive_or_infinity_env_ms(external_effect_timeout_ms,
                                ?DEFAULT_EXTERNAL_EFFECT_TIMEOUT_MS).

preload_workflows() ->
    case workflow_modules() of
        {ok, Modules} ->
            preload_workflow_modules(Modules);
        {error, _} = Error ->
            Error
    end.

workflow_modules() ->
    case application:get_env(beamtrail, workflow_modules) of
        undefined ->
            {ok, []};
        {ok, Modules} ->
            normalize_workflow_modules(Modules)
    end.

normalize_workflow_modules([]) ->
    {ok, []};
normalize_workflow_modules(Module) when is_atom(Module); is_binary(Module) ->
    {ok, [Module]};
normalize_workflow_modules(Modules) when is_list(Modules) ->
    case charlist(Modules) of
        true -> {ok, [Modules]};
        false -> {ok, Modules}
    end;
normalize_workflow_modules(Other) ->
    {error, {bad_workflow_modules, Other}}.

preload_workflow_modules([]) ->
    ok;
preload_workflow_modules([Module0 | Rest]) ->
    case module_atom(Module0) of
        {ok, Module} ->
            case code:ensure_loaded(Module) of
                {module, Module} ->
                    preload_workflow_modules(Rest);
                {error, Reason} ->
                    {error, {workflow_module_load_failed, Module, Reason}}
            end;
        {error, _} = Error ->
            Error
    end.

module_atom(Module) when is_atom(Module) ->
    {ok, Module};
module_atom(Module) when is_binary(Module) ->
    try binary_to_atom(Module, utf8) of
        Atom -> {ok, Atom}
    catch
        error:badarg -> {error, {bad_workflow_module, Module}}
    end;
module_atom(Module) when is_list(Module) ->
    case charlist(Module) of
        true ->
            try list_to_atom(Module) of
                Atom -> {ok, Atom}
            catch
                error:badarg -> {error, {bad_workflow_module, Module}}
            end;
        false ->
            {error, {bad_workflow_module, Module}}
    end;
module_atom(Module) ->
    {error, {bad_workflow_module, Module}}.

positive_or_infinity_env_ms(Key, Default) ->
    case application:get_env(beamtrail, Key) of
        {ok, infinity} ->
            infinity;
        {ok, Ms} when is_integer(Ms), Ms >= 0 ->
            Ms;
        _ ->
            Default
    end.

charlist(List) ->
    lists:all(fun is_integer/1, List).
