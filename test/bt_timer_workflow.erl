-module(bt_timer_workflow).
-behaviour(beamtrail_workflow).

-export([steps/1, step_version/1, retry_policy/1, timeout_ms/1,
         idempotency_key/3, execute/4, decide/1, decider_version/0]).

steps(_Input) ->
    [].

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

execute(_StepId, _StepVersion, _Input, _Ctx) ->
    {error, unexpected_timer_step}.

decide(View) ->
    Input = maps:get(input, View),
    TimerId = maps:get(timer_id, Input, approval_deadline),
    Timers = maps:get(timers, View, #{}),
    case maps:get(TimerId, Timers, undefined) of
        undefined ->
            sleep_command(TimerId, Input);
        #{status := scheduled, fire_at_ms := FireAtMs} ->
            case maps:get(conflict_after_schedule, Input, false) of
                true -> {sleep_until, TimerId, FireAtMs + 1};
                false -> {wait, waiting_for_timer}
            end;
        #{status := fired, fire_at_ms := FireAtMs} ->
            case maps:get(reuse_after_fired, Input, false) of
                true -> {sleep_until, TimerId, FireAtMs};
                false -> {complete, #{timer_id => TimerId, fired => true}}
            end
    end.

decider_version() ->
    1.

sleep_command(TimerId, Input) ->
    case maps:get(delay_ms, Input, undefined) of
        undefined ->
            {sleep_until, TimerId, maps:get(fire_at_ms, Input)};
        DelayMs ->
            {sleep, TimerId, DelayMs}
    end.
