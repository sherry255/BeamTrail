-module(bt_malformed_retry_policy_workflow).
-behaviour(beamtrail_workflow).

-export([steps/1, step_version/1, retry_policy/1, timeout_ms/1,
         idempotency_key/3, execute/4]).

steps(_Input) ->
    [charge].

step_version(_StepId) ->
    1.

retry_policy(_StepId) ->
    #{max_attempts => 2,
      backoff_ms => <<"bad-backoff">>,
      retryable_errors => [transient]}.

timeout_ms(_StepId) ->
    infinity.

idempotency_key(_RunId, StepId, Input) ->
    {StepId, maps:get(order_id, Input)}.

execute(_StepId, _StepVersion, Input, Ctx) ->
    Attempt = maps:get(attempt, Ctx),
    maps:get(test_pid, Input) ! {malformed_retry_execute, Attempt},
    {error, transient}.
