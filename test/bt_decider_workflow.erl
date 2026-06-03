-module(bt_decider_workflow).
-behaviour(beamtrail_workflow).

-export([steps/1, step_version/1, retry_policy/1, timeout_ms/1,
         idempotency_key/3, execute/4, decide/1]).

steps(_Input) ->
    [charge, ship].

step_version(_StepId) ->
    1.

retry_policy(_StepId) ->
    #{max_attempts => 1,
      backoff_ms => 0,
      retryable_errors => []}.

timeout_ms(_StepId) ->
    infinity.

idempotency_key(_RunId, StepId, Input) ->
    {StepId, maps:get(order_id, Input, undefined)}.

execute(StepId, StepVersion, Input, Ctx) ->
    case maps:get(test_pid, Input, undefined) of
        undefined -> ok;
        TestPid ->
            TestPid ! {executed, StepId, StepVersion,
                       maps:get(idempotency_key, Ctx), Input}
    end,
    {ok, #{step => StepId, input => Input}}.

decide(View) ->
    Input = maps:get(input, View),
    case maps:get(command, Input) of
        {run_step, _StepId, _StepInput} = Command ->
            case maps:get(results, View, []) of
                [] -> Command;
                _ -> complete
            end;
        crash ->
            error(decider_crash);
        Command ->
            Command
    end.
