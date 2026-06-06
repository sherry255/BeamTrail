-module(bt_pg_stress_blocking_workflow).
-behaviour(beamtrail_workflow).

-export([steps/1, step_version/1, retry_policy/1, timeout_ms/1,
         idempotency_key/3, execute/4]).

steps(_Input) ->
    [block].

step_version(_StepId) ->
    1.

retry_policy(_StepId) ->
    #{max_attempts => 1, backoff_ms => 0, retryable_errors => []}.

timeout_ms(_StepId) ->
    infinity.

idempotency_key(RunId, StepId, _Input) ->
    {StepId, RunId}.

execute(StepId, StepVersion, Input, Ctx) ->
    ModeFile = maps:get(mode_file, Input),
    MarkerFile = maps:get(marker_file, Input),
    Attempt = maps:get(attempt, Ctx),
    IdempotencyKey = maps:get(idempotency_key, Ctx),
    append_marker(MarkerFile,
                  io_lib:format("started step=~p version=~p attempt=~p idempotency_key=~p~n",
                                [StepId, StepVersion, Attempt, IdempotencyKey])),
    case read_mode(ModeFile) of
        complete ->
            append_marker(MarkerFile,
                          io_lib:format("completed step=~p attempt=~p~n",
                                        [StepId, Attempt])),
            {ok, #{step => StepId,
                   attempt => Attempt,
                   recovered => true}};
        block ->
            receive
                stop ->
                    {error, stopped}
            end
    end.

read_mode(ModeFile) ->
    case file:read_file(ModeFile) of
        {ok, Bin} ->
            case string:trim(binary_to_list(Bin)) of
                "complete" -> complete;
                _ -> block
            end;
        {error, _} ->
            block
    end.

append_marker(Path, Iolist) ->
    ok = file:write_file(Path, Iolist, [append]).
