-module(beamtrail_runtime_tests).

-include_lib("eunit/include/eunit.hrl").

durable_runtime_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      fun successful_workflow_writes_append_only_events_and_snapshot/0,
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
    ok.

cleanup(_) ->
    ok = beamtrail_memory_storage:reset().

successful_workflow_writes_append_only_events_and_snapshot() ->
    Input = #{order_id => <<"order-1">>, test_pid => self()},
    {ok, RunId} = beamtrail:start_workflow(bt_success_workflow, Input),

    ?assertMatch({executed, charge, 1, {charge, <<"order-1">>}}, receive_exec()),
    ?assertMatch({executed, ship, 1, {ship, <<"order-1">>}}, receive_exec()),

    State = beamtrail:get_state(RunId),
    ?assertEqual(completed, maps:get(status, State)),
    ?assertEqual(2, maps:get(completed_steps, State)),

    {ok, Events} = beamtrail:events(RunId),
    ?assertEqual(
       ['workflow.instance.created',
        'attempt.started',
        'step.succeeded',
        'attempt.started',
        'step.succeeded',
        'workflow.completed'],
       [maps:get(event_type, Event) || Event <- Events]),
    ?assertEqual([1, 2, 3, 4, 5, 6], [maps:get(event_seq, Event) || Event <- Events]),

    {ok, Snapshot} = beamtrail_memory_storage:read_snapshot(RunId),
    ?assertEqual(6, maps:get(snapshot_seq, Snapshot)),
    ?assertEqual(completed, maps:get(status, maps:get(state, Snapshot))).

retry_uses_stable_idempotency_key_across_attempts() ->
    Input = #{order_id => <<"order-2">>, test_pid => self()},
    {ok, RunId} = beamtrail:start_workflow(bt_retry_workflow, Input),

    ?assertMatch({retry_execute, 1, {charge, <<"order-2">>}}, receive_exec()),
    ?assertMatch({retry_execute, 2, {charge, <<"order-2">>}}, receive_exec()),

    State = beamtrail:get_state(RunId),
    ?assertEqual(completed, maps:get(status, State)),
    ?assertEqual(2, maps:get(charge, maps:get(attempt_counts, State))),

    {ok, Events} = beamtrail:events(RunId),
    Started = [Event || Event <- Events, maps:get(event_type, Event) =:= 'attempt.started'],
    ?assertEqual(2, length(Started)),
    ?assertEqual(
       [{charge, <<"order-2">>}, {charge, <<"order-2">>}],
       [maps:get(idempotency_key, Event) || Event <- Started]),
    ?assert(lists:member('retry.scheduled', [maps:get(event_type, Event) || Event <- Events])).

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
    {ok, Lease} = beamtrail_memory_storage:acquire_lease(RunId, stale_worker, 1),
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
    timer:sleep(2),

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

    State = beamtrail:get_state(RunId),
    ?assertEqual(failed, maps:get(status, State)),
    ?assertMatch(#{reason := timeout}, maps:get(failure, State)),
    ?assertEqual(1, maps:get(slow, maps:get(attempt_counts, State))),

    {ok, Events} = beamtrail:events(RunId),
    ?assert(lists:member('step.failed', [maps:get(event_type, Event) || Event <- Events])),
    ?assert(lists:member('workflow.failed', [maps:get(event_type, Event) || Event <- Events])).

receive_exec() ->
    receive
        Message -> Message
    after 1000 ->
        timeout
    end.
