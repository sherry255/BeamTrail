-module(bt_external_worker_demo).

-export([run/2]).

run(RunId0, Port0) ->
    RunId = normalize_run_id(RunId0),
    Port = normalize_int(Port0),
    configure(Port),
    ok = init_runtime(),
    StartedAt = erlang:monotonic_time(millisecond),
    Input = #{order_id => RunId},
    {ok, RunId} =
        beamtrail:start_workflow(bt_external_worker_workflow, Input,
                                 #{run_id => RunId}),
    Effect0 = wait_pending_effect(RunId, 5000),
    EffectId = maps:get(effect_id, Effect0),
    io:format("scheduled effect run=~s effect_id=~p~n", [RunId, EffectId]),

    WorkerA = <<"worker-a">>,
    WorkerB = <<"worker-b">>,
    {ok, ClaimA} = claim_with_retry(RunId, EffectId, WorkerA, 5000),
    ClaimTokenA = maps:get(claim_token, ClaimA),
    io:format("worker A claimed effect token=~p and then stopped before completion~n",
              [ClaimTokenA]),
    ok = assert_hidden_while_claimed(RunId, EffectId),

    timer:sleep(1100),
    Effect1 = wait_pending_effect(RunId, 5000),
    io:format("claim expired; effect visible again status=~p claim_owner=~p~n",
              [maps:get(status, Effect1),
               maps:get(claim_owner, Effect1, undefined)]),

    {ok, ClaimB} = claim_with_retry(RunId, EffectId, WorkerB, 5000),
    ClaimTokenB = maps:get(claim_token, ClaimB),
    ok = assert_stale_claim_rejected(RunId, EffectId, ClaimTokenA),
    {ok, _StateAfterCompletion} =
        complete_with_retry(RunId, EffectId, WorkerB, ClaimTokenB,
                            {ok, #{<<"charged">> => true,
                                   <<"worker">> => WorkerB,
                                   <<"authorization_id">> => <<"auth-123">>}},
                            5000),
    State = wait_terminal(RunId, 10000),
    {ok, Events} = beamtrail:events(RunId),
    FinishedAt = erlang:monotonic_time(millisecond),

    assert_demo_result(State, Events),
    io:format("final status=~p elapsed_ms=~p~n",
              [maps:get(status, State), FinishedAt - StartedAt]),
    io:format("workflow_result=~p~n",
              [maps:get(workflow_result, State, undefined)]),
    io:format("event log:~n"),
    print_events(Events),
    halt(0).

configure(Port) ->
    ok = application:set_env(beamtrail, storage_adapter,
                             beamtrail_postgres_storage),
    ok = application:set_env(beamtrail, postgres,
                             #{host => "localhost",
                               port => Port,
                               username => "beamtrail",
                               password => "beamtrail",
                               database => "beamtrail"}),
    ok = application:set_env(beamtrail, postgres_pool_size, 2),
    ok = application:set_env(beamtrail, postgres_pool_checkout_timeout_ms, 1000),
    ok = application:set_env(beamtrail, workflow_modules,
                             [bt_external_worker_workflow]),
    ok = application:set_env(beamtrail, external_effect_visibility_timeout_ms, 1000),
    ok = application:set_env(beamtrail, external_effect_timeout_ms, 5000),
    ok = application:set_env(beamtrail, scanner_interval_ms, 200),
    ok = application:set_env(beamtrail, lease_ttl_ms, 1000),
    ok.

init_runtime() ->
    {ok, _} = application:ensure_all_started(epgsql),
    ok = beamtrail_postgres_storage:init_schema(),
    {ok, _} = application:ensure_all_started(beamtrail),
    ok.

wait_pending_effect(RunId, RemainingMs) when RemainingMs =< 0 ->
    io:format("Timed out waiting for pending effect for ~s.~n", [RunId]),
    halt(2);
wait_pending_effect(RunId, RemainingMs) ->
    case beamtrail:list_pending_effects() of
        {ok, Effects} ->
            case [Effect || Effect <- Effects,
                            maps:get(run_id, Effect) =:= RunId] of
                [Effect | _] ->
                    Effect;
                [] ->
                    timer:sleep(50),
                    wait_pending_effect(RunId, RemainingMs - 50)
            end;
        {error, Reason} ->
            io:format("list_pending_effects failed: ~p~n", [Reason]),
            halt(2)
    end.

assert_hidden_while_claimed(RunId, EffectId) ->
    case beamtrail:list_pending_effects() of
        {ok, Effects} ->
            Visible =
                [Effect || Effect <- Effects,
                           maps:get(run_id, Effect) =:= RunId,
                           maps:get(effect_id, Effect) =:= EffectId],
            case Visible of
                [] ->
                    ok;
                _ ->
                    io:format("claimed effect was still visible: ~p~n",
                              [Visible]),
                    halt(2)
            end;
        {error, Reason} ->
            io:format("list_pending_effects failed after claim: ~p~n", [Reason]),
            halt(2)
    end.

claim_with_retry(RunId, EffectId, _Owner, RemainingMs) when RemainingMs =< 0 ->
    io:format("Timed out claiming effect ~p for ~s.~n", [EffectId, RunId]),
    halt(2);
claim_with_retry(RunId, EffectId, Owner, RemainingMs) ->
    case beamtrail:claim_effect(RunId, EffectId, Owner) of
        {ok, _Claim} = Ok ->
            Ok;
        {error, leased} ->
            timer:sleep(50),
            claim_with_retry(RunId, EffectId, Owner, RemainingMs - 50);
        {error, not_visible} ->
            timer:sleep(50),
            claim_with_retry(RunId, EffectId, Owner, RemainingMs - 50);
        {error, claimed} ->
            timer:sleep(50),
            claim_with_retry(RunId, EffectId, Owner, RemainingMs - 50);
        {error, Reason} ->
            io:format("claim_effect failed: ~p~n", [Reason]),
            halt(2)
    end.

assert_stale_claim_rejected(RunId, EffectId, ClaimToken) ->
    StaleResult =
        {ok, #{<<"charged">> => true,
               <<"worker">> => <<"worker-a">>,
               <<"authorization_id">> => <<"stale-auth">>}},
    case beamtrail:complete_effect(RunId, EffectId, ClaimToken, StaleResult) of
        {error, stale_claim} ->
            io:format("worker A stale claim token was rejected~n"),
            ok;
        Other ->
            io:format("stale claim completion was not rejected: ~p~n", [Other]),
            halt(2)
    end.

complete_with_retry(RunId, EffectId, _Owner, _ClaimToken, _Result, RemainingMs)
  when RemainingMs =< 0 ->
    io:format("Timed out completing effect ~p for ~s.~n", [EffectId, RunId]),
    halt(2);
complete_with_retry(RunId, EffectId, Owner, ClaimToken, Result, RemainingMs) ->
    case beamtrail:complete_effect(RunId, EffectId, ClaimToken, Result) of
        {ok, _State} = Ok ->
            Ok;
        {error, leased} ->
            timer:sleep(50),
            complete_with_retry(RunId, EffectId, Owner, ClaimToken, Result,
                                RemainingMs - 50);
        {error, claim_expired} ->
            {ok, Claim} = claim_with_retry(RunId, EffectId, Owner, RemainingMs),
            complete_with_retry(RunId, EffectId, Owner,
                                maps:get(claim_token, Claim), Result,
                                RemainingMs);
        {error, Reason} ->
            io:format("complete_effect failed: ~p~n", [Reason]),
            halt(2)
    end.

wait_terminal(RunId, RemainingMs) when RemainingMs =< 0 ->
    io:format("Timed out waiting for terminal state for ~s.~n", [RunId]),
    halt(2);
wait_terminal(RunId, RemainingMs) ->
    case beamtrail:get_state(RunId) of
        #{terminal := true} = State ->
            State;
        {error, Reason} ->
            io:format("get_state failed: ~p~n", [Reason]),
            halt(2);
        _State ->
            timer:sleep(50),
            wait_terminal(RunId, RemainingMs - 50)
    end.

assert_demo_result(State, Events) ->
    Status = maps:get(status, State),
    Completed = event_count('workflow.completed', Events),
    Claims = event_count('effect.claimed', Events),
    ActivitySucceeded = event_count('activity.succeeded', Events),
    StepSucceeded = event_count('step.succeeded', Events),
    case Status =:= completed
        andalso Completed =:= 1
        andalso Claims =:= 2
        andalso ActivitySucceeded =:= 2
        andalso StepSucceeded =:= 2 of
        true ->
            ok;
        false ->
            io:format("unexpected demo result status=~p claims=~p activity_succeeded=~p step_succeeded=~p workflow_completed=~p~n",
                      [Status, Claims, ActivitySucceeded, StepSucceeded,
                       Completed]),
            halt(2)
    end.

event_count(EventType, Events) ->
    length([Event || Event <- Events,
                     maps:get(event_type, Event) =:= EventType]).

print_events(Events) ->
    lists:foreach(fun print_event/1, Events).

print_event(Event) ->
    io:format("  seq=~p type=~p step=~p payload=~p~n",
              [maps:get(event_seq, Event),
               maps:get(event_type, Event),
               maps:get(step_id, Event, undefined),
               maps:get(payload, Event, #{})]).

normalize_run_id(RunId) when is_binary(RunId) ->
    RunId;
normalize_run_id(RunId) when is_list(RunId) ->
    list_to_binary(RunId).

normalize_int(Value) when is_integer(Value) ->
    Value;
normalize_int(Value) when is_list(Value) ->
    list_to_integer(Value).
