-module(bt_signal_workflow).
-behaviour(beamtrail_workflow).

-export([steps/1, step_version/1, retry_policy/1, timeout_ms/1,
         idempotency_key/3, execute/4, decide/1, decider_version/0]).

steps(_Input) ->
    [fulfill].

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

execute(fulfill, _StepVersion, Input, _Ctx) ->
    notify(Input, fulfill),
    {ok, #{fulfilled => true,
           approved_by => maps:get(approved_by, Input, undefined)}}.

decide(View) ->
    case maps:get(results, View, []) of
        [] ->
            case latest_signal(approved, maps:get(signals, View, [])) of
                undefined ->
                    {wait, waiting_for_approval};
                #{payload := Payload} ->
                    Input = maps:get(input, View),
                    {run_step, fulfill,
                     #{order_id => maps:get(order_id, Input),
                       approved_by => maps:get(approved_by, Payload),
                       test_pid => maps:get(test_pid, Input, undefined)}}
            end;
        [#{step_id := fulfill, result := Result}] ->
            {complete, Result}
    end.

decider_version() ->
    1.

latest_signal(Name, Signals) ->
    Matching = [S || #{name := SignalName} = S <- Signals, SignalName =:= Name],
    case Matching of
        [] -> undefined;
        _ -> lists:last(Matching)
    end.

notify(Input, StepId) ->
    case maps:get(test_pid, Input, undefined) of
        undefined -> ok;
        TestPid -> TestPid ! {signal_workflow_executed, StepId, Input}
    end.
