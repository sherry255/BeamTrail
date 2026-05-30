-module(bt_versioned_workflow).
-behaviour(beamtrail_workflow).

-export([steps/1, step_version/1, retry_policy/1, timeout_ms/1, idempotency_key/3, execute/4]).

steps(_Input) ->
    [charge].

step_version(_StepId) ->
    2.

retry_policy(_StepId) ->
    #{max_attempts => 1, backoff_ms => 0, retryable_errors => []}.

timeout_ms(_StepId) ->
    infinity.

idempotency_key(_RunId, StepId, Input) ->
    {StepId, maps:get(order_id, Input)}.

execute(StepId, StepVersion, Input, Ctx) ->
    maps:get(test_pid, Input) ! {versioned_execute, StepVersion, maps:get(idempotency_key, Ctx)},
    {ok, #{step => StepId}}.
