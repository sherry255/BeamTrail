-module(bt_pg_stress_workflow).
-behaviour(beamtrail_workflow).

-export([steps/1, step_version/1, retry_policy/1, timeout_ms/1,
         idempotency_key/3, execute/4]).

steps(_Input) ->
    [work].

step_version(_StepId) ->
    1.

retry_policy(_StepId) ->
    #{max_attempts => 1, backoff_ms => 0, retryable_errors => []}.

timeout_ms(_StepId) ->
    infinity.

idempotency_key(RunId, StepId, _Input) ->
    {StepId, RunId}.

execute(_StepId, _StepVersion, Input, _Ctx) ->
    SleepMs = maps:get(sleep_ms, Input, 25),
    timer:sleep(SleepMs),
    {ok, #{slept_ms => SleepMs}}.
