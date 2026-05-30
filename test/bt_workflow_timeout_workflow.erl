-module(bt_workflow_timeout_workflow).
-behaviour(beamtrail_workflow).

-export([steps/1, step_version/1, retry_policy/1, timeout_ms/1,
         idempotency_key/3, execute/4, workflow_timeout_ms/0]).

steps(_Input) -> [first, second].

step_version(_) -> 1.

retry_policy(_) -> #{max_attempts => 1, backoff_ms => 0, retryable_errors => []}.

timeout_ms(_) -> infinity.

workflow_timeout_ms() -> 1.

idempotency_key(_RunId, StepId, Input) ->
    {StepId, maps:get(order_id, Input)}.

execute(first, _V, _Input, _Ctx) ->
    timer:sleep(20),
    {ok, done};
execute(second, _V, _Input, _Ctx) ->
    {ok, done}.
