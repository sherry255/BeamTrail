-module(bt_external_worker_workflow).
-behaviour(beamtrail_workflow).

-export([steps/1, step_version/1, retry_policy/1, timeout_ms/1,
         idempotency_key/3, effect_mode/1, execute/4]).

steps(_Input) ->
    [charge, ship].

step_version(_StepId) ->
    1.

retry_policy(_StepId) ->
    #{max_attempts => 2,
      backoff_ms => 0,
      retryable_errors => [external_effect_timeout, transient]}.

timeout_ms(_StepId) ->
    2000.

idempotency_key(_RunId, StepId, Input) ->
    {StepId, maps:get(order_id, Input)}.

effect_mode(charge) ->
    external;
effect_mode(_StepId) ->
    local.

execute(charge, _StepVersion, _Input, _Ctx) ->
    {error, unexpected_local_charge_execution};
execute(ship, _StepVersion, Input, _Ctx) ->
    OrderId = maps:get(order_id, Input),
    {ok, #{shipment_id => <<"ship-", OrderId/binary>>}}.

