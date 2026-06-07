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
    ensure_runner_infra(),
    ok.

cleanup(_) ->
    stop_runner_infra(),
    ok = beamtrail_memory_storage:reset().

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
    ?assertEqual(completed, maps:get(status, maps:get(state, Snapshot))).

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
