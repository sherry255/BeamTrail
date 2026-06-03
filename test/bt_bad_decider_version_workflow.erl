-module(bt_bad_decider_version_workflow).
-behaviour(beamtrail_workflow).

-export([steps/1, step_version/1, retry_policy/1, timeout_ms/1,
         idempotency_key/3, execute/4, decide/1, decider_version/0]).

steps(_Input) ->
    [charge].

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

execute(StepId, _StepVersion, _Input, _Ctx) ->
    {ok, #{step => StepId}}.

decide(View) ->
    {run_step, charge, maps:get(input, View)}.

decider_version() ->
    error(bad_decider_version_crash).
