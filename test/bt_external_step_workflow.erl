-module(bt_external_step_workflow).
-behaviour(beamtrail_workflow).

-export([steps/1, step_version/1, retry_policy/1, timeout_ms/1,
         idempotency_key/3, execute/4, effect_mode/1]).

steps(Input) ->
    maps:get(steps, Input, [external_charge]).

step_version(_StepId) ->
    1.

retry_policy(_StepId) ->
    #{max_attempts => 1, backoff_ms => 0, retryable_errors => []}.

timeout_ms(_StepId) ->
    infinity.

idempotency_key(_RunId, StepId, Input) ->
    {StepId, maps:get(order_id, Input)}.

effect_mode(external_charge) ->
    external;
effect_mode(external_refund) ->
    external.

execute(StepId, StepVersion, Input, Ctx) ->
    maps:get(test_pid, Input) !
        {unexpected_external_execute, StepId, StepVersion, Ctx},
    {error, unexpected_local_execution}.
