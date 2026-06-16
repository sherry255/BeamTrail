-module(beamtrail_runtime_tests).

-include_lib("eunit/include/eunit.hrl").

durable_runtime_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      fun successful_workflow_writes_append_only_events_and_snapshot/0,
      fun call_step_effect_executes_workflow_callback/0,
      fun runner_transition_returns_tagged_effect/0,
      fun complete_effect_finishes_pending_call_step_once/0,
      fun complete_effect_ignores_cancelled_pending_call_step/0,
      fun complete_effect_failure_uses_retry_decision/0,
      fun external_step_waits_for_worker_completion/0,
      fun external_step_deadline_fails_abandoned_effect/0,
      fun list_pending_effects_filters_future_visible_at/0,
      fun list_pending_effects_excludes_local_call_step_effects/0,
      fun list_pending_effects_replay_fallback_returns_public_shape_in_order/0,
      fun claim_effect_hides_pending_effect_until_claim_expires/0,
      fun complete_effect_requires_active_claim_token/0,
      fun complete_effect_matches_legacy_external_attempt_by_effect_type/0,
      fun external_worker_run_once_reclaims_expired_effect/0,
      fun external_worker_run_once_leaves_claimed_effect_when_handler_crashes/0,
      fun external_worker_run_once_filters_by_step_id/0,
      fun external_worker_run_once_renews_claim_while_handler_runs/0,
      fun external_worker_run_once_rejects_unsafe_renew_interval/0,
      fun external_worker_run_once_drains_renewer_stop_message/0,
      fun retry_uses_stable_idempotency_key_across_attempts/0,
      fun recovery_replays_unknown_attempt_with_recorded_step_version/0,
      fun step_timeout_records_observable_failure/0
     ]}.

setup() ->
    case whereis(beamtrail_memory_storage) of
        undefined ->
            {ok, _Pid} = beamtrail_memory_storage:start_link();
        _ ->
            ok
    end,
    ok = beamtrail_memory_storage:reset(),
    ensure_runner_infra(),
    ok.

cleanup(_) ->
    stop_runner_infra(),
    ok = beamtrail_memory_storage:reset(),
    ok = application:unset_env(beamtrail, storage_adapter),
    ok = application:unset_env(beamtrail, external_effect_visibility_timeout_ms),
    ok = application:unset_env(beamtrail, external_effect_timeout_ms).

successful_workflow_writes_append_only_events_and_snapshot() ->
    Input = #{order_id => <<"order-1">>, test_pid => self()},
    {ok, RunId} = beamtrail:start_workflow(bt_success_workflow, Input),

    ?assertMatch({executed, charge, 1, {charge, <<"order-1">>}}, receive_exec()),
    ?assertMatch({executed, ship, 1, {ship, <<"order-1">>}}, receive_exec()),
    ok = wait_for_status(RunId, completed, 1000),

    State = beamtrail:get_state(RunId),
    ?assertEqual(completed, maps:get(status, State)),
    ?assertEqual(2, maps:get(completed_steps, State)),

    {ok, Events} = beamtrail:events(RunId),
    ?assertEqual(
       ['workflow.instance.created',
        'attempt.started',
        'activity.scheduled',
        'activity.started',
        'step.succeeded',
        'activity.succeeded',
        'attempt.started',
        'activity.scheduled',
        'activity.started',
        'step.succeeded',
        'activity.succeeded',
        'workflow.completed'],
       [maps:get(event_type, Event) || Event <- Events]),
    ?assertEqual(lists:seq(1, 12), [maps:get(event_seq, Event) || Event <- Events]),

    {ok, Snapshot} = beamtrail_memory_storage:read_snapshot(RunId),
    ?assertEqual(12, maps:get(snapshot_seq, Snapshot)),
    ?assertEqual(completed, maps:get(status, maps:get(state, Snapshot))),

    Q = beamtrail_query:describe(RunId),
    ?assertEqual(#{}, maps:get(pending_effects, Q)),
    ?assertMatch(
       [#{effect_id := {call_step, charge, 1},
          events := [#{effect_id := {call_step, charge, 1}},
                     #{effect_id := {call_step, charge, 1}},
                     #{effect_id := {call_step, charge, 1}}]},
        #{effect_id := {call_step, ship, 1},
          events := [#{effect_id := {call_step, ship, 1}},
                     #{effect_id := {call_step, ship, 1}},
                     #{effect_id := {call_step, ship, 1}}]}],
       maps:get(activities, Q)).

call_step_effect_executes_workflow_callback() ->
    Input = #{order_id => <<"effect-1">>, test_pid => self()},
    Ctx = #{run_id => <<"effect-run">>,
            step_id => charge,
            step_version => 1,
            attempt => 1,
            idempotency_key => {charge, <<"effect-1">>}},
    Effect =
        beamtrail_effect:call_step(bt_success_workflow,
                                   charge,
                                   1,
                                   Input,
                                   infinity,
                                   Ctx),

    ?assertEqual(call_step, beamtrail_effect:type(Effect)),
    ?assertEqual(charge, beamtrail_effect:step_id(Effect)),
    ?assertEqual(infinity, beamtrail_effect:timeout_ms(Effect)),
    ?assertEqual({call_step, charge, 1}, beamtrail_effect:id(Effect)),
    ?assertEqual({ok, #{step => charge}},
                 beamtrail_executor:execute_attempt(Effect)),
    ?assertMatch({executed, charge, 1, {charge, <<"effect-1">>}},
                 receive_exec()).

runner_transition_returns_tagged_effect() ->
    Input = #{order_id => <<"effect-2">>, test_pid => self()},
    {ok, RunId} =
        beamtrail:start_workflow(bt_success_workflow, Input,
                                 #{auto_dispatch => false}),
    {ok, Lease} =
        beamtrail_memory_storage:acquire_lease(
          RunId, #{owner => effect_test}, 30000),
    {ok, State} = beamtrail_runner_transition:load_state(RunId),

    {ok, {execute, Attempt, Effect, State1}} =
        beamtrail_runner_transition:next_action(RunId, Lease, State),
    EffectId = beamtrail_effect:id(Effect),

    ?assertEqual(charge, maps:get(step_id, Attempt)),
    ?assertEqual(call_step, beamtrail_effect:type(Effect)),
    ?assertEqual(charge, beamtrail_effect:step_id(Effect)),
    ?assertEqual({charge, <<"effect-2">>},
                 beamtrail_effect:idempotency_key(Effect)),
    ?assertMatch(
       #{EffectId := #{effect_id := EffectId,
                       effect_type := call_step,
                       step_id := charge,
                       attempt := 1,
                       status := started}},
       maps:get(pending_effects, State1)).

complete_effect_finishes_pending_call_step_once() ->
    RunId = <<"complete-effect-success-run">>,
    Input = #{order_id => <<"complete-effect-1">>,
              test_pid => self(),
              gate => complete_effect_gate},
    {ok, RunId} =
        beamtrail:start_workflow(bt_blocking_success_workflow, Input,
                                 #{run_id => RunId, auto_dispatch => false}),
    {_Attempt, Effect, _PreparedState} = prepare_pending_call_step(RunId),
    EffectId = beamtrail_effect:id(Effect),

    ?assertEqual(timeout, receive_exec_short()),
    {ok, State1} =
        beamtrail:complete_effect(RunId, EffectId, {ok, #{external => done}}),
    ?assertEqual(#{}, maps:get(pending_effects, State1)),
    ok = wait_for_status(RunId, completed, 1000),
    ?assertEqual(timeout, receive_exec_short()),

    {ok, Events1} = beamtrail:events(RunId),
    ?assertEqual(1, count_events('step.succeeded', Events1)),
    ?assertEqual(1, count_events('activity.succeeded', Events1)),
    Succeeded = only_event('step.succeeded', Events1),
    ?assertEqual(EffectId, maps:get(effect_id, maps:get(payload, Succeeded))),
    ?assertEqual(#{external => done}, maps:get(result, maps:get(payload, Succeeded))),

    ?assertEqual({ok, ignored},
                 beamtrail:complete_effect(RunId, EffectId,
                                           {ok, #{external => duplicate}})),
    {ok, Events2} = beamtrail:events(RunId),
    ?assertEqual(length(Events1), length(Events2)).

complete_effect_ignores_cancelled_pending_call_step() ->
    RunId = <<"complete-effect-cancelled-run">>,
    Input = #{order_id => <<"complete-effect-2">>,
              test_pid => self(),
              gate => complete_effect_cancelled_gate},
    {ok, RunId} =
        beamtrail:start_workflow(bt_blocking_success_workflow, Input,
                                 #{run_id => RunId, auto_dispatch => false}),
    {_Attempt, Effect, _PreparedState} = prepare_pending_call_step(RunId),
    EffectId = beamtrail_effect:id(Effect),

    {ok, Cancelled} = beamtrail:cancel_run(RunId, operator_cancel),
    ?assertMatch(#{status := cancelled, terminal := true}, Cancelled),
    ?assertEqual({ok, ignored},
                 beamtrail:complete_effect(RunId, EffectId,
                                           {ok, #{external => too_late}})),
    ?assertEqual(timeout, receive_exec_short()),
    {ok, Events} = beamtrail:events(RunId),
    ?assertEqual(0, count_events('step.succeeded', Events)),
    ?assertEqual(0, count_events('activity.succeeded', Events)).

complete_effect_failure_uses_retry_decision() ->
    RunId = <<"complete-effect-failure-run">>,
    Input = #{order_id => <<"complete-effect-3">>, test_pid => self()},
    {ok, RunId} =
        beamtrail:start_workflow(bt_single_fail_workflow, Input,
                                 #{run_id => RunId, auto_dispatch => false}),
    {_Attempt, Effect, _PreparedState} = prepare_pending_call_step(RunId),
    EffectId = beamtrail_effect:id(Effect),

    {ok, State1} = beamtrail:complete_effect(RunId, EffectId, {error, transient}),
    ?assertMatch(#{status := failed, terminal := true}, State1),
    ?assertEqual(#{}, maps:get(pending_effects, State1)),
    ?assertEqual(timeout, receive_exec_short()),
    {ok, Events1} = beamtrail:events(RunId),
    ?assertEqual(1, count_events('step.failed', Events1)),
    ?assertEqual(1, count_events('activity.failed', Events1)),
    ?assertEqual(1, count_events('workflow.failed', Events1)),

    ?assertEqual({ok, ignored},
                 beamtrail:complete_effect(RunId, EffectId, {error, transient})),
    {ok, Events2} = beamtrail:events(RunId),
    ?assertEqual(length(Events1), length(Events2)).

external_step_waits_for_worker_completion() ->
    RunId = <<"external-step-run">>,
    Input = #{order_id => <<"external-1">>, test_pid => self()},
    {ok, RunId} =
        beamtrail:start_workflow(bt_external_step_workflow, Input,
                                 #{run_id => RunId}),
    ok = wait_for_status(RunId, waiting_effect, 1000),
    ?assertEqual(timeout, receive_exec_short()),

    {ok, Pending} = beamtrail:list_pending_effects(),
    ?assertMatch(
       [#{run_id := RunId,
          effect_id := {external_step, external_charge, 1},
          effect_type := external_step,
          step_id := external_charge,
          status := scheduled}],
       Pending),

    [Effect] = Pending,
    ?assert(is_integer(maps:get(visible_at_ms, Effect))),
    ?assertEqual(30000, maps:get(visibility_timeout_ms, Effect)),
    EffectId = maps:get(effect_id, Effect),
    {ok, State1} =
        beamtrail:complete_effect(RunId, EffectId,
                                  {ok, #{external_result => authorized}}),
    ?assertEqual(#{}, maps:get(pending_effects, State1)),
    {ok, Completed} = beamtrail:await_terminal(RunId, 1000),
    ?assertMatch(#{status := completed,
                   terminal := true,
                   completed_steps := 1},
                 Completed),
    {ok, []} = beamtrail:list_pending_effects(),

    {ok, Events} = beamtrail:events(RunId),
    ?assertEqual(
       ['workflow.instance.created',
        'attempt.started',
        'activity.scheduled',
        'step.succeeded',
        'activity.succeeded',
        'workflow.completed'],
       [maps:get(event_type, E) || E <- Events]),
    ActivityScheduled = only_event('activity.scheduled', Events),
    ?assertEqual(external_step,
                 maps:get(effect_type, maps:get(payload, ActivityScheduled))),
    ?assertEqual(0, count_events('activity.started', Events)).

external_step_deadline_fails_abandoned_effect() ->
    ok = application:set_env(beamtrail, external_effect_timeout_ms, 20),
    RunId = <<"external-step-deadline-run">>,
    Input = #{order_id => <<"external-deadline">>, test_pid => self()},
    {ok, RunId} =
        beamtrail:start_workflow(bt_external_step_workflow, Input,
                                 #{run_id => RunId}),
    ok = wait_for_status(RunId, waiting_effect, 1000),
    ?assertEqual(timeout, receive_exec_short()),

    timer:sleep(30),
    {ok, _Recovered} = beamtrail:recover_unfinished(),
    {ok, Failed} = beamtrail:await_terminal(RunId, 1000),
    ?assertMatch(#{status := failed, terminal := true}, Failed),
    ?assertEqual(#{}, maps:get(pending_effects, Failed)),
    ?assertMatch(#{reason := external_effect_timeout,
                   class := external_effect_timeout},
                 maps:get(failure, Failed)),

    {ok, []} = beamtrail:list_pending_effects(),
    {ok, Events} = beamtrail:events(RunId),
    ?assertEqual(1, count_events('step.failed', Events)),
    ?assertEqual(1, count_events('activity.failed', Events)),
    ?assertEqual(1, count_events('workflow.failed', Events)).

list_pending_effects_filters_future_visible_at() ->
    HiddenRunId = <<"future-visible-effect-run">>,
    VisibleRunId = <<"past-visible-effect-run">>,
    EffectId = beamtrail_effect:external_step_id(external_charge, 1),
    Now = erlang:system_time(millisecond),
    ok = append_external_pending_run(HiddenRunId, EffectId, Now + 60000),
    ok = append_external_pending_run(VisibleRunId, EffectId, Now - 1),

    {ok, Pending} = beamtrail:list_pending_effects(),
    PendingRunIds = [maps:get(run_id, Effect) || Effect <- Pending],
    ?assertNot(lists:member(HiddenRunId, PendingRunIds)),
    ?assert(lists:member(VisibleRunId, PendingRunIds)),
    [Visible] =
        [Effect || Effect <- Pending, maps:get(run_id, Effect) =:= VisibleRunId],
    ?assertEqual(Now - 1, maps:get(visible_at_ms, Visible)).

list_pending_effects_excludes_local_call_step_effects() ->
    RunId = <<"local-call-step-effect-not-worker-work">>,
    Input = #{order_id => <<"local-call-step">>, test_pid => self()},
    {ok, RunId} =
        beamtrail:start_workflow(bt_success_workflow, Input,
                                 #{run_id => RunId, auto_dispatch => false}),
    {_Attempt, _Effect, _PreparedState} = prepare_pending_call_step(RunId),

    ?assertEqual({ok, []}, beamtrail:list_pending_effects()),
    ?assertEqual(timeout, receive_exec_short()).

list_pending_effects_replay_fallback_returns_public_shape_in_order() ->
    ok = application:set_env(beamtrail, storage_adapter, bt_counting_storage),
    try
        RunIdB = <<"fallback-effect-b">>,
        RunIdA = <<"fallback-effect-a">>,
        EffectId = beamtrail_effect:external_step_id(external_charge, 1),
        Now = erlang:system_time(millisecond),
        ok = append_external_pending_run(RunIdB, EffectId, Now - 1),
        ok = append_external_pending_run(RunIdA, EffectId, Now - 1),

        {ok, Pending} = beamtrail:list_pending_effects(),
        ?assertMatch(
           [#{run_id := RunIdA,
              effect_id := EffectId,
              effect_type := external_step,
              step_id := external_charge,
              step_version := 1,
              idempotency_key := {external_charge, RunIdA},
              attempt := 1,
              status := scheduled,
              visible_at_ms := _,
              visibility_timeout_ms := _},
            #{run_id := RunIdB,
              effect_id := EffectId}],
           Pending),
        [First | _] = Pending,
        ?assertNot(maps:is_key(scheduled_event_seq, First)),
        ?assertNot(maps:is_key(scheduled_at, First))
    after
        ok = application:unset_env(beamtrail, storage_adapter)
    end.

claim_effect_hides_pending_effect_until_claim_expires() ->
    ok = application:set_env(beamtrail, external_effect_visibility_timeout_ms, 20),
    RunId = <<"claim-visible-effect-run">>,
    Input = #{order_id => <<"claim-visible">>, test_pid => self()},
    {ok, RunId} =
        beamtrail:start_workflow(bt_external_step_workflow, Input,
                                 #{run_id => RunId}),
    ok = wait_for_status(RunId, waiting_effect, 1000),
    {ok, [Effect]} = beamtrail:list_pending_effects(),
    EffectId = maps:get(effect_id, Effect),

    {ok, Claim} = beamtrail:claim_effect(RunId, EffectId, worker_a),
    ?assertEqual(EffectId, maps:get(effect_id, Claim)),
    ?assertEqual(worker_a, maps:get(claim_owner, Claim)),
    ?assert(maps:is_key(claim_token, Claim)),
    ?assert(is_integer(maps:get(claim_until_ms, Claim))),
    {ok, []} = beamtrail:list_pending_effects(),

    timer:sleep(30),
    {ok, [VisibleAgain]} = beamtrail:list_pending_effects(),
    ?assertEqual(RunId, maps:get(run_id, VisibleAgain)),
    ?assertEqual(EffectId, maps:get(effect_id, VisibleAgain)),
    ?assertEqual(worker_a, maps:get(claim_owner, VisibleAgain)),
    ?assertEqual(maps:get(claim_token, Claim),
                 maps:get(claim_token, VisibleAgain)).

complete_effect_requires_active_claim_token() ->
    ok = application:set_env(beamtrail, external_effect_visibility_timeout_ms, 5000),
    RunId = <<"claim-complete-effect-run">>,
    Input = #{order_id => <<"claim-complete">>, test_pid => self()},
    {ok, RunId} =
        beamtrail:start_workflow(bt_external_step_workflow, Input,
                                 #{run_id => RunId}),
    ok = wait_for_status(RunId, waiting_effect, 1000),
    {ok, [Effect]} = beamtrail:list_pending_effects(),
    EffectId = maps:get(effect_id, Effect),
    {ok, Claim} = beamtrail:claim_effect(RunId, EffectId, worker_a),
    ClaimToken = maps:get(claim_token, Claim),

    ?assertEqual({error, claimed},
                 beamtrail:complete_effect(RunId, EffectId,
                                           {ok, #{external_result => stale}})),
    ?assertEqual({error, stale_claim},
                 beamtrail:complete_effect(RunId, EffectId, wrong_token,
                                           {ok, #{external_result => stale}})),
    {ok, State1} =
        beamtrail:complete_effect(RunId, EffectId, ClaimToken,
                                  {ok, #{external_result => claimed_done}}),
    ?assertEqual(#{}, maps:get(pending_effects, State1)),
    {ok, Completed} = beamtrail:await_terminal(RunId, 1000),
    ?assertMatch(#{status := completed, terminal := true}, Completed),
    ?assertEqual({ok, ignored},
                 beamtrail:complete_effect(RunId, EffectId, ClaimToken,
                                           {ok, #{external_result => duplicate}})).

complete_effect_matches_legacy_external_attempt_by_effect_type() ->
    RunId = <<"legacy-external-effect-run">>,
    Input = #{order_id => <<"legacy-external">>, test_pid => self()},
    EffectId = beamtrail_effect:external_step_id(external_charge, 1),
    {ok, _Created} =
        beamtrail_memory_storage:append_event(
          RunId,
          0,
          undefined,
          'workflow.instance.created',
          undefined,
          undefined,
          undefined,
          #{workflow => bt_external_step_workflow,
            input => Input,
            steps => [external_charge]}),
    {ok, Lease} =
        beamtrail_memory_storage:acquire_lease(
          RunId, #{owner => legacy_external_test}, 30000),
    Fence = maps:get(fencing_token, Lease),
    {ok, [_Started, _Scheduled]} =
        beamtrail_memory_storage:append_events(
          RunId,
          1,
          Fence,
          [#{event_type => 'attempt.started',
             step_id => external_charge,
             step_version => 1,
             idempotency_key => {external_charge, <<"legacy-external">>},
             payload => #{attempt => 1}},
           #{event_type => 'activity.scheduled',
             step_id => external_charge,
             step_version => 1,
             idempotency_key => {external_charge, <<"legacy-external">>},
             payload => #{activity_type => step,
                          activity_status => scheduled,
                          effect_id => EffectId,
                          effect_type => external_step,
                          attempt => 1}}]),
    ok = beamtrail_memory_storage:release_lease(RunId, Fence),

    {ok, State1} =
        beamtrail:complete_effect(RunId, EffectId,
                                  {ok, #{external_result => legacy_done}}),
    ?assertEqual(#{}, maps:get(pending_effects, State1)),
    {ok, Completed} = beamtrail:await_terminal(RunId, 1000),
    ?assertMatch(#{status := completed, terminal := true}, Completed).

external_worker_run_once_reclaims_expired_effect() ->
    ok = application:set_env(beamtrail, external_effect_visibility_timeout_ms, 20),
    RunId = <<"external-worker-loop-run">>,
    Input = #{order_id => <<"external-worker-loop">>, test_pid => self()},
    {ok, RunId} =
        beamtrail:start_workflow(bt_external_step_workflow, Input,
                                 #{run_id => RunId}),
    ok = wait_for_status(RunId, waiting_effect, 1000),
    {ok, [Effect]} = beamtrail:list_pending_effects(),
    EffectId = maps:get(effect_id, Effect),

    {ok, ClaimA} = beamtrail:claim_effect(RunId, EffectId, <<"worker-a">>),
    ?assertEqual({ok, idle},
                 beamtrail_external_worker:run_once(
                   <<"worker-b">>,
                   fun(_ClaimedEffect) ->
                           {ok, #{<<"external_result">> => <<"too_early">>}}
                   end)),

    timer:sleep(30),
    TestPid = self(),
    {ok, #{run_id := RunId,
           effect_id := EffectId,
           state := State1}} =
        beamtrail_external_worker:run_once(
          <<"worker-b">>,
          fun(ClaimedEffect) ->
                  TestPid ! {worker_claimed, ClaimedEffect},
                  {ok, #{<<"external_result">> => <<"authorized">>}}
          end),
    ?assertEqual(#{}, maps:get(pending_effects, State1)),
    ?assertMatch({worker_claimed,
                  #{run_id := RunId,
                    effect_id := EffectId,
                    claim_owner := <<"worker-b">>,
                    claim_token := _}},
                 receive_exec()),
    ?assertEqual({ok, ignored},
                 beamtrail:complete_effect(
                   RunId, EffectId, maps:get(claim_token, ClaimA),
                   {ok, #{<<"external_result">> => <<"stale">>}})),
    {ok, Completed} = beamtrail:await_terminal(RunId, 1000),
    ?assertMatch(#{status := completed, terminal := true}, Completed),
    ?assertEqual(timeout, receive_exec_short()),

    {ok, Events} = beamtrail:events(RunId),
    ?assertEqual(2, count_events('effect.claimed', Events)),
    ?assertEqual(1, count_events('step.succeeded', Events)),
    ?assertEqual(1, count_events('activity.succeeded', Events)).

external_worker_run_once_leaves_claimed_effect_when_handler_crashes() ->
    ok = application:set_env(beamtrail, external_effect_visibility_timeout_ms, 20),
    RunId = <<"external-worker-crash-run">>,
    Input = #{order_id => <<"external-worker-crash">>, test_pid => self()},
    {ok, RunId} =
        beamtrail:start_workflow(bt_external_step_workflow, Input,
                                 #{run_id => RunId}),
    ok = wait_for_status(RunId, waiting_effect, 1000),
    {ok, [Effect]} = beamtrail:list_pending_effects(),
    EffectId = maps:get(effect_id, Effect),

    ?assertError(worker_crashed,
                 beamtrail_external_worker:run_once(
                   <<"worker-a">>,
                   fun(_ClaimedEffect) ->
                           error(worker_crashed)
                   end)),
    {ok, []} = beamtrail:list_pending_effects(),
    {ok, EventsBeforeVisible} = beamtrail:events(RunId),
    ?assertEqual(1, count_events('effect.claimed', EventsBeforeVisible)),
    ?assertEqual(0, count_events('step.failed', EventsBeforeVisible)),
    ?assertEqual(0, count_events('activity.failed', EventsBeforeVisible)),

    timer:sleep(30),
    {ok, [VisibleAgain]} = beamtrail:list_pending_effects(),
    ?assertEqual(EffectId, maps:get(effect_id, VisibleAgain)),
    ?assertEqual(<<"worker-a">>, maps:get(claim_owner, VisibleAgain)).

external_worker_run_once_filters_by_step_id() ->
    ChargeRunId = <<"external-worker-filter-charge">>,
    RefundRunId = <<"external-worker-filter-refund">>,
    {ok, ChargeRunId} =
        beamtrail:start_workflow(
          bt_external_step_workflow,
          #{order_id => <<"filter-charge">>,
            steps => [external_charge],
            test_pid => self()},
          #{run_id => ChargeRunId}),
    {ok, RefundRunId} =
        beamtrail:start_workflow(
          bt_external_step_workflow,
          #{order_id => <<"filter-refund">>,
            steps => [external_refund],
            test_pid => self()},
          #{run_id => RefundRunId}),
    ok = wait_for_status(ChargeRunId, waiting_effect, 1000),
    ok = wait_for_status(RefundRunId, waiting_effect, 1000),

    TestPid = self(),
    {ok, #{run_id := RefundRunId, effect_id := RefundEffectId}} =
        beamtrail_external_worker:run_once(
          <<"refund-worker">>,
          fun(ClaimedEffect) ->
                  TestPid ! {filtered_worker_claimed, ClaimedEffect},
                  {ok, #{<<"external_result">> => <<"refunded">>}}
          end,
          #{step_ids => [external_refund]}),

    ?assertMatch({filtered_worker_claimed,
                  #{run_id := RefundRunId,
                    effect_id := RefundEffectId,
                    step_id := external_refund}},
                 receive_exec()),
    ok = wait_for_status(RefundRunId, completed, 1000),
    ?assertEqual(waiting_effect,
                 maps:get(status, beamtrail:get_state(ChargeRunId))),
    {ok, Pending} = beamtrail:list_pending_effects(),
    ?assertEqual([external_charge],
                 [maps:get(step_id, Effect) || Effect <- Pending]).

external_worker_run_once_renews_claim_while_handler_runs() ->
    ok = application:set_env(beamtrail, external_effect_visibility_timeout_ms, 40),
    RunId = <<"external-worker-renew-run">>,
    Input = #{order_id => <<"external-worker-renew">>, test_pid => self()},
    {ok, RunId} =
        beamtrail:start_workflow(bt_external_step_workflow, Input,
                                 #{run_id => RunId}),
    ok = wait_for_status(RunId, waiting_effect, 1000),
    TestPid = self(),
    Worker =
        spawn_link(
          fun() ->
                  Result =
                      beamtrail_external_worker:run_once(
                        <<"slow-worker">>,
                        fun(ClaimedEffect) ->
                                TestPid ! {slow_worker_started, ClaimedEffect},
                                receive continue_slow_worker -> ok end,
                                {ok, #{<<"external_result">> => <<"slow-ok">>}}
                        end,
                        #{renew_claim => true,
                          claim_renew_interval_ms => 10}),
                  TestPid ! {slow_worker_result, Result}
          end),

    ?assertMatch({slow_worker_started,
                  #{run_id := RunId,
                    effect_id := _,
                    claim_token := _}},
                 receive_exec()),
    timer:sleep(90),
    ?assertEqual({ok, []}, beamtrail:list_pending_effects()),
    ?assertEqual({ok, idle},
                 beamtrail_external_worker:run_once(
                   <<"competing-worker">>,
                   fun(_ClaimedEffect) ->
                           {ok, #{<<"external_result">> => <<"stolen">>}}
                   end)),

    Worker ! continue_slow_worker,
    ?assertMatch({slow_worker_result,
                  {ok, #{run_id := RunId,
                         effect_id := _,
                         state := #{pending_effects := #{}}}}},
                 receive_exec()),
    ok = wait_for_status(RunId, completed, 1000),
    {ok, Events} = beamtrail:events(RunId),
    ?assert(count_events('effect.claimed', Events) >= 2).

external_worker_run_once_rejects_unsafe_renew_interval() ->
    ok = application:set_env(beamtrail, external_effect_visibility_timeout_ms, 40),
    RunId = <<"external-worker-bad-renew-interval">>,
    Input = #{order_id => <<"external-worker-bad-renew">>, test_pid => self()},
    {ok, RunId} =
        beamtrail:start_workflow(bt_external_step_workflow, Input,
                                 #{run_id => RunId}),
    ok = wait_for_status(RunId, waiting_effect, 1000),

    ?assertEqual({error, {bad_worker_option, claim_renew_interval_ms}},
                 beamtrail_external_worker:run_once(
                   <<"bad-renew-worker">>,
                   fun(_ClaimedEffect) ->
                           {ok, #{<<"external_result">> => <<"bad">>}}
                   end,
                   #{renew_claim => true,
                     claim_renew_interval_ms => 40})),
    ?assertEqual(timeout, receive_exec_short()),
    {ok, Events} = beamtrail:events(RunId),
    ?assertEqual(0, count_events('effect.claimed', Events)),
    {ok, [_StillVisible]} = beamtrail:list_pending_effects().

external_worker_run_once_drains_renewer_stop_message() ->
    ok = application:set_env(beamtrail, external_effect_visibility_timeout_ms, 1000),
    RunId = <<"external-worker-renewer-drain">>,
    Input = #{order_id => <<"external-worker-renewer-drain">>, test_pid => self()},
    {ok, RunId} =
        beamtrail:start_workflow(bt_external_step_workflow, Input,
                                 #{run_id => RunId}),
    ok = wait_for_status(RunId, waiting_effect, 1000),
    TestPid = self(),
    {ok, idle} =
        beamtrail_external_worker:run_once(
          <<"drain-worker">>,
          fun(ClaimedEffect) ->
                  EffectId = maps:get(effect_id, ClaimedEffect),
                  ClaimToken = maps:get(claim_token, ClaimedEffect),
                  spawn_link(
                    fun() ->
                            timer:sleep(30),
                            _ = beamtrail:complete_effect(
                                  RunId, EffectId, ClaimToken,
                                  {ok, #{<<"external_result">> => <<"done">>}})
                    end),
                  timer:sleep(90),
                  TestPid ! handler_returning_after_external_completion,
                  {ok, #{<<"external_result">> => <<"late">>}}
          end,
          #{renew_claim => true,
            claim_renew_interval_ms => 10}),
    ?assertEqual(handler_returning_after_external_completion, receive_exec()),
    ?assertEqual(timeout, receive_renewer_message_short()).

retry_uses_stable_idempotency_key_across_attempts() ->
    Input = #{order_id => <<"order-2">>, test_pid => self()},
    {ok, RunId} = beamtrail:start_workflow(bt_retry_workflow, Input),

    ?assertMatch({retry_execute, 1, {charge, <<"order-2">>}}, receive_exec()),
    ?assertMatch({retry_execute, 2, {charge, <<"order-2">>}}, receive_exec()),
    ok = wait_for_status(RunId, completed, 1000),

    State = beamtrail:get_state(RunId),
    ?assertEqual(completed, maps:get(status, State)),
    ?assertEqual(2, maps:get(charge, maps:get(attempt_counts, State))),

    {ok, Events} = beamtrail:events(RunId),
    Started = [Event || Event <- Events, maps:get(event_type, Event) =:= 'attempt.started'],
    ?assertEqual(2, length(Started)),
    ?assertEqual(
       [{charge, <<"order-2">>}, {charge, <<"order-2">>}],
       [maps:get(idempotency_key, Event) || Event <- Started]),
    Types = [maps:get(event_type, Event) || Event <- Events],
    ?assertMatch(
       [_,
        'attempt.started',
        'activity.scheduled',
        'activity.started',
        'step.failed',
        'activity.failed',
        'retry.scheduled',
        'attempt.started',
        'activity.scheduled',
        'activity.started',
        'step.succeeded',
        'activity.succeeded',
        'workflow.completed'],
       Types),
    ?assert(lists:member('retry.scheduled', Types)),
    Q = beamtrail_query:describe(RunId),
    ?assertMatch(
       [#{step_id := charge,
          attempt := 1,
          status := failed,
          events := [#{event_type := 'activity.scheduled'},
                     #{event_type := 'activity.started'},
                     #{event_type := 'activity.failed'}]},
        #{step_id := charge,
          attempt := 2,
          status := succeeded,
          events := [#{event_type := 'activity.scheduled'},
                     #{event_type := 'activity.started'},
                     #{event_type := 'activity.succeeded'}]}],
       maps:get(activities, Q)).

recovery_replays_unknown_attempt_with_recorded_step_version() ->
    RunId = <<"manual-versioned-run">>,
    Input = #{order_id => <<"order-3">>, test_pid => self()},
    {ok, _Created} =
        beamtrail_memory_storage:append_event(
          RunId,
          0,
          undefined,
          'workflow.instance.created',
          undefined,
          undefined,
          undefined,
          #{workflow => bt_success_workflow, input => Input, steps => [charge]}),
    {ok, Lease} = beamtrail_memory_storage:acquire_lease(RunId, stale_worker, 100),
    {ok, _Attempt} =
        beamtrail_memory_storage:append_event(
          RunId,
          1,
          maps:get(fencing_token, Lease),
          'attempt.started',
          charge,
          1,
          {charge, <<"order-3">>},
          #{attempt => 1}),
    timer:sleep(120),

    {ok, [RunId]} = beamtrail:recover_unfinished(),

    ?assertMatch({executed, charge, 1, {charge, <<"order-3">>}}, receive_exec()),
    State = beamtrail:get_state(RunId),
    ?assertEqual(completed, maps:get(status, State)),
    ?assertEqual(false, maps:get(migration_required_for_version_change, State)),

    {ok, Events} = beamtrail:events(RunId),
    ?assertEqual(1, length([Event || Event <- Events, maps:get(event_type, Event) =:= 'attempt.started'])).

step_timeout_records_observable_failure() ->
    Input = #{order_id => <<"order-4">>},
    {ok, RunId} = beamtrail:start_workflow(bt_timeout_workflow, Input),
    ok = wait_for_status(RunId, failed, 1000),

    State = beamtrail:get_state(RunId),
    ?assertEqual(failed, maps:get(status, State)),
    ?assertMatch(#{reason := timeout}, maps:get(failure, State)),
    ?assertEqual(1, maps:get(slow, maps:get(attempt_counts, State))),

    {ok, Events} = beamtrail:events(RunId),
    ?assert(lists:member('activity.failed', [maps:get(event_type, Event) || Event <- Events])),
    ?assert(lists:member('step.failed', [maps:get(event_type, Event) || Event <- Events])),
    ?assert(lists:member('workflow.failed', [maps:get(event_type, Event) || Event <- Events])).

receive_exec() ->
    receive
        Message -> Message
    after 1000 ->
        timeout
    end.

receive_exec_short() ->
    receive
        Message -> Message
    after 50 ->
        timeout
    end.

receive_renewer_message_short() ->
    receive
        {beamtrail_external_worker_claim_renew_stopped, _EffectId, _Reason} =
                Message ->
            Message;
        {beamtrail_external_worker_claim_renew_failed, _EffectId, _Reason} =
                Message ->
            Message
    after 50 ->
        timeout
    end.

prepare_pending_call_step(RunId) ->
    {ok, Lease} =
        beamtrail_memory_storage:acquire_lease(
          RunId, #{owner => complete_effect_test}, 30000),
    {ok, State0} = beamtrail_runner_transition:load_state(RunId),
    {ok, {execute, Attempt, Effect, State1}} =
        beamtrail_runner_transition:next_action(RunId, Lease, State0),
    ok = beamtrail_memory_storage:release_lease(
           RunId, maps:get(fencing_token, Lease)),
    {Attempt, Effect, State1}.

append_external_pending_run(RunId, EffectId, VisibleAtMs) ->
    Input = #{order_id => RunId, test_pid => self()},
    {ok, _Created} =
        beamtrail_memory_storage:append_event(
          RunId,
          0,
          undefined,
          'workflow.instance.created',
          undefined,
          undefined,
          undefined,
          #{workflow => bt_external_step_workflow,
            input => Input,
            steps => [external_charge]}),
    {ok, Lease} =
        beamtrail_memory_storage:acquire_lease(
          RunId, #{owner => visible_effect_test}, 30000),
    Fence = maps:get(fencing_token, Lease),
    {ok, [_Started, _Scheduled]} =
        beamtrail_memory_storage:append_events(
          RunId,
          1,
          Fence,
          [#{event_type => 'attempt.started',
             step_id => external_charge,
             step_version => 1,
             idempotency_key => {external_charge, RunId},
             payload => #{attempt => 1,
                          effect_id => EffectId,
                          effect_type => external_step,
                          step_input => Input}},
           #{event_type => 'activity.scheduled',
             step_id => external_charge,
             step_version => 1,
             idempotency_key => {external_charge, RunId},
             payload => #{activity_type => step,
                          activity_status => scheduled,
                          effect_id => EffectId,
                          effect_type => external_step,
                          attempt => 1,
                          visible_at_ms => VisibleAtMs}}]),
    ok = beamtrail_memory_storage:release_lease(RunId, Fence).

count_events(EventType, Events) ->
    length([Event || Event <- Events,
                     maps:get(event_type, Event) =:= EventType]).

only_event(EventType, Events) ->
    [Event] = [Event || Event <- Events,
                        maps:get(event_type, Event) =:= EventType],
    Event.

wait_for_status(_RunId, _Target, Remaining) when Remaining =< 0 ->
    {error, timeout};
wait_for_status(RunId, Target, Remaining) ->
    case maps:get(status, beamtrail:get_state(RunId)) of
        Target ->
            ok;
        _ ->
            timer:sleep(20),
            wait_for_status(RunId, Target, Remaining - 20)
    end.

ensure_runner_infra() ->
    case whereis(beamtrail_run_registry) of
        undefined ->
            {ok, Registry} = beamtrail_run_registry:start_link(),
            unlink(Registry);
        _ ->
            ok
    end,
    case whereis(beamtrail_run_sup) of
        undefined ->
            {ok, RunSup} = beamtrail_run_sup:start_link(),
            unlink(RunSup);
        _ ->
            ok
    end.

stop_runner_infra() ->
    stop_registered(beamtrail_run_sup),
    stop_registered(beamtrail_run_registry).

stop_registered(Name) ->
    case whereis(Name) of
        undefined ->
            ok;
        Pid ->
            exit(Pid, shutdown),
            wait_until_stopped(Pid, 1000)
    end.

wait_until_stopped(_Pid, Remaining) when Remaining =< 0 ->
    ok;
wait_until_stopped(Pid, Remaining) ->
    case is_process_alive(Pid) of
        false ->
            ok;
        true ->
            timer:sleep(20),
            wait_until_stopped(Pid, Remaining - 20)
    end.
