-module(bt_crash_approval_workflow).
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

idempotency_key(RunId, StepId, _Input) ->
    {StepId, RunId}.

execute(fulfill, _StepVersion, Input, _Ctx) ->
    MarkerFile = maps:get(marker_file, Input),
    append_marker(MarkerFile,
                  io_lib:format("fulfilled run=~s approved_by=~s~n",
                                [maps:get(order_id, Input),
                                 maps:get(approved_by, Input)])),
    {ok, #{fulfilled => true,
           approved_by => maps:get(approved_by, Input)}}.

decide(View) ->
    case maps:get(results, View, []) of
        [] ->
            decide_pending(View);
        [#{step_id := fulfill, result := Result}] ->
            {complete, Result}
    end.

decider_version() ->
    1.

decide_pending(View) ->
    Input = maps:get(input, View),
    TimerId = maps:get(timer_id, Input, approval_deadline),
    DeadlineAt = maps:get(deadline_at_ms, Input),
    case first_decisive_input(View, TimerId) of
        {approved, #{payload := Payload}} ->
            {run_step, fulfill,
             #{order_id => maps:get(order_id, Input),
               marker_file => maps:get(marker_file, Input),
               approved_by => maps:get(approved_by, Payload)}};
        {rejected, #{payload := Payload}} ->
            {fail, #{reason => approval_rejected,
                     rejected_by => maps:get(rejected_by, Payload, undefined),
                     payload => Payload}};
        {timeout, #{timer_id := TimerId, fire_at_ms := FireAtMs}} ->
            {fail, #{reason => approval_timeout,
                     timer_id => TimerId,
                     fire_at_ms => FireAtMs}};
        none ->
            case maps:get(TimerId, maps:get(timers, View, #{}), undefined) of
                undefined ->
                    {sleep_until, TimerId, DeadlineAt};
                #{status := scheduled} ->
                    {wait, waiting_for_approval}
            end
    end.

first_decisive_input(View, TimerId) ->
    Signals = maps:get(signals, View, []),
    SignalDecisions =
        [{Seq, approved, Signal}
         || #{name := approved, event_seq := Seq} = Signal <- Signals] ++
        [{Seq, rejected, Signal}
         || #{name := rejected, event_seq := Seq} = Signal <- Signals],
    TimerDecisions =
        case maps:get(TimerId, maps:get(timers, View, #{}), undefined) of
            #{status := fired, fired_event_seq := Seq} = Timer
              when is_integer(Seq) ->
                [{Seq, timeout, Timer}];
            _ ->
                []
        end,
    case lists:sort(SignalDecisions ++ TimerDecisions) of
        [] ->
            none;
        [{_Seq, Decision, Data} | _] ->
            {Decision, Data}
    end.

append_marker(Path, Iolist) ->
    ok = file:write_file(Path, Iolist, [append]).
