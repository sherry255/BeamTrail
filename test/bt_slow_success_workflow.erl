-module(bt_slow_success_workflow).
-behaviour(beamtrail_workflow).

-export([steps/1, step_version/1, retry_policy/1, timeout_ms/1, idempotency_key/3, execute/4]).

steps(_Input) ->
    [slow].

step_version(_StepId) ->
    1.

retry_policy(_StepId) ->
    #{max_attempts => 1, backoff_ms => 0, retryable_errors => []}.

timeout_ms(_StepId) ->
    infinity.

idempotency_key(_RunId, StepId, Input) ->
    {StepId, maps:get(order_id, Input)}.

execute(StepId, _StepVersion, Input, _Ctx) ->
    timer:sleep(maps:get(sleep_ms, Input, 100)),
    maps:get(test_pid, Input) ! {slow_executed, StepId},
    {ok, #{step => StepId}}.
