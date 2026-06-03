-module(bt_result_branching_workflow).
-behaviour(beamtrail_workflow).

-export([steps/1, step_version/1, retry_policy/1, timeout_ms/1,
         idempotency_key/3, execute/4, decide/1, decider_version/0]).

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

execute(charge, _StepVersion, Input, _Ctx) ->
    notify(Input, charge),
    Shipment =
        #{order_id => maps:get(order_id, Input),
          address => maps:get(ship_to, Input),
          carrier => <<"beam-post">>,
          test_pid => maps:get(test_pid, Input, undefined)},
    {ok, #{paid => true, shipment => Shipment}};
execute(ship, _StepVersion, Input, _Ctx) ->
    notify(Input, ship),
    {ok, #{shipped => true, shipment => Input}}.

decide(View) ->
    case maps:get(results, View, []) of
        [] ->
            {run_step, charge, maps:get(input, View)};
        [#{step_id := charge, result := #{paid := true,
                                          shipment := Shipment}}] ->
            {run_step, ship, Shipment};
        [#{step_id := charge, result := #{paid := false}}] ->
            {fail, payment_failed};
        [#{step_id := charge},
         #{step_id := ship, result := #{shipment := Shipment}}] ->
            {complete, #{fulfilled => true, shipment => Shipment}}
    end.

decider_version() ->
    1.

notify(Input, StepId) ->
    case maps:get(test_pid, Input, undefined) of
        undefined -> ok;
        TestPid -> TestPid ! {result_branch_executed, StepId, Input}
    end.
