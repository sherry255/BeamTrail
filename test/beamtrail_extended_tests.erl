-module(beamtrail_extended_tests).

-include_lib("eunit/include/eunit.hrl").

extended_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      fun workflow_timeout_emits_workflow_failed/0,
      fun scanner_requeues_unknown_attempts/0,
      fun query_describe_exposes_read_model/0,
      fun telemetry_counters_track_attempts/0,
      fun postgres_stub_is_loud_about_not_implemented/0,
      fun storage_rejects_expected_seq_conflict/0,
      fun dispatch_refuses_when_run_is_leased/0,
      fun dispatch_refuses_stale_lease_before_replay/0,
      fun dispatch_refuses_version_mismatch_without_migration/0,
      fun retry_attempts_preserved_in_chronological_order/0,
      fun storage_adapter_is_application_configurable/0,
      fun query_describe_exposes_inspector_blocks/0,
      fun recovery_requeued_records_recovered_in_ms/0
     ]}.

setup() ->
    case whereis(beamtrail_memory_storage) of
        undefined -> {ok, _} = beamtrail_memory_storage:start_link();
        _ -> ok
    end,
    ok = beamtrail_memory_storage:reset(),
    ok.

cleanup(_) ->
    case whereis(beamtrail_scanner) of
        undefined -> ok;
        _ -> beamtrail_scanner:stop()
    end,
    ok = beamtrail_memory_storage:reset().

workflow_timeout_emits_workflow_failed() ->
    Input = #{order_id => <<"o-wt-1">>},
    {ok, RunId} = beamtrail:start_workflow(bt_workflow_timeout_workflow, Input),
    State = beamtrail:get_state(RunId),
    ?assertEqual(failed, maps:get(status, State)),
    {ok, Events} = beamtrail:events(RunId),
    Failed = [E || E <- Events, maps:get(event_type, E) =:= 'workflow.failed'],
    ?assertMatch([_], Failed),
    [#{payload := P}] = Failed,
    ?assertEqual(workflow_timeout, maps:get(reason, P)).

scanner_requeues_unknown_attempts() ->
    RunId = <<"scanner-run-1">>,
    Input = #{order_id => <<"o-sc-1">>, test_pid => self()},
    {ok, _} = beamtrail_memory_storage:append_event(
                RunId, 0, undefined,
                'workflow.instance.created', undefined, undefined,
                undefined,
                #{workflow => bt_success_workflow, input => Input,
                  steps => [charge]}),
    {ok, Lease} = beamtrail_memory_storage:acquire_lease(RunId, scanner_seed, 1),
    {ok, _} = beamtrail_memory_storage:append_event(
                RunId, 1, maps:get(fencing_token, Lease),
                'attempt.started', charge, 1,
                {charge, <<"o-sc-1">>}, #{attempt => 1}),
    timer:sleep(2),
    {ok, _Pid} = beamtrail_scanner:start_link(#{interval_ms => infinity,
                                                auto_start => false}),
    {ok, Requeued} = beamtrail_scanner:scan_now(),
    ?assertEqual([RunId], Requeued),
    ?assertMatch({executed, charge, 1, {charge, <<"o-sc-1">>}}, receive_exec()),
    ok = wait_for_status(RunId, completed, 1000),
    State = beamtrail:get_state(RunId),
    ?assertEqual(completed, maps:get(status, State)),
    {ok, Events} = beamtrail:events(RunId),
    Types = [maps:get(event_type, E) || E <- Events],
    ?assert(lists:member('recovery.requeued', Types)).

query_describe_exposes_read_model() ->
    Input = #{order_id => <<"o-q-1">>, test_pid => self()},
    {ok, RunId} = beamtrail:start_workflow(bt_success_workflow, Input),
    _ = receive_exec(), _ = receive_exec(),
    Q = beamtrail_query:describe(RunId),
    ?assertEqual(completed, maps:get(status, Q)),
    ?assertEqual(RunId, maps:get(run_id, Q)),
    ?assertMatch(#{run_id := RunId}, maps:get(instance, Q)),
    ?assertMatch([#{status := succeeded}, #{status := succeeded}],
                 maps:get(attempts, Q)),
    Snapshot = maps:get(snapshot, Q),
    ?assert(is_map(Snapshot)),
    ?assert(is_list(maps:get(events, Q))),
    ?assertMatch(#{api := <<"beamtrail_query:describe/1">>,
                   run_id := RunId}, maps:get(query, Q)),
    ?assertEqual(false, maps:get(migration_required_for_version_change, Q)).

telemetry_counters_track_attempts() ->
    Input = #{order_id => <<"o-t-1">>, test_pid => self()},
    {ok, _RunId} = beamtrail:start_workflow(bt_success_workflow, Input),
    _ = receive_exec(), _ = receive_exec(),
    Counters = beamtrail_query:telemetry(),
    Started = maps:get([beamtrail, attempt, started], Counters, 0),
    Snap = maps:get([beamtrail, snapshot, written], Counters, 0),
    ?assertEqual(2, Started),
    ?assert(Snap >= 1).

postgres_stub_is_loud_about_not_implemented() ->
    ?assertEqual({error, not_implemented},
                 beamtrail_postgres_storage:append_event(
                   <<"r">>, 0, undefined, 'workflow.instance.created', undefined,
                   undefined, undefined, #{})),
    ?assertEqual({error, not_implemented},
                 beamtrail_postgres_storage:read_events(<<"r">>, 1, infinity)),
    ?assertEqual(not_found, beamtrail_postgres_storage:read_lease(<<"r">>)).

storage_rejects_expected_seq_conflict() ->
    RunId = <<"cas-run-1">>,
    {ok, _} = beamtrail_memory_storage:append_event(
                RunId, 0, undefined,
                'workflow.instance.created', undefined, undefined,
                undefined,
                #{workflow => bt_success_workflow, input => #{}, steps => []}),
    ?assertMatch({error, {conflict, #{expected_seq := 0, actual_seq := 1}}},
                 beamtrail_memory_storage:append_event(
                   RunId, 0, undefined,
                   'workflow.completed', undefined, undefined,
                   undefined, #{})).

dispatch_refuses_when_run_is_leased() ->
    RunId = <<"leased-run-1">>,
    Input = #{order_id => <<"o-lease-1">>, test_pid => self()},
    {ok, _} = beamtrail_memory_storage:append_event(
                RunId, 0, undefined,
                'workflow.instance.created', undefined, undefined,
                undefined,
                #{workflow => bt_success_workflow, input => Input,
                  steps => [charge]}),
    {ok, _Lease} = beamtrail_memory_storage:acquire_lease(RunId, other_worker, 30000),
    ?assertEqual({error, leased}, beamtrail:dispatch(RunId)),
    ?assertEqual(timeout, receive_exec_short()),
    {ok, Events} = beamtrail:events(RunId),
    ?assertEqual(['workflow.instance.created'],
                 [maps:get(event_type, E) || E <- Events]).

dispatch_refuses_stale_lease_before_replay() ->
    RunId = <<"stale-lease-run-1">>,
    Input = #{order_id => <<"o-stale-lease-1">>, test_pid => self()},
    {ok, _} = beamtrail_memory_storage:append_event(
                RunId, 0, undefined,
                'workflow.instance.created', undefined, undefined,
                undefined,
                #{workflow => bt_success_workflow, input => Input,
                  steps => [charge]}),
    {ok, Lease} = beamtrail_memory_storage:acquire_lease(RunId, stale_worker, 1),
    {ok, _} = beamtrail_memory_storage:append_event(
                RunId, 1, maps:get(fencing_token, Lease),
                'attempt.started', charge, 1,
                {charge, <<"o-stale-lease-1">>}, #{attempt => 1}),
    timer:sleep(2),
    ?assertEqual({error, stale_lease}, beamtrail:dispatch(RunId, Lease)),
    ?assertEqual(timeout, receive_exec_short()),
    {ok, Events} = beamtrail:events(RunId),
    ?assertEqual(['workflow.instance.created', 'attempt.started'],
                 [maps:get(event_type, E) || E <- Events]).

dispatch_refuses_version_mismatch_without_migration() ->
    RunId = <<"version-gate-run-1">>,
    Input = #{order_id => <<"o-version-1">>, test_pid => self()},
    {ok, _} = beamtrail_memory_storage:append_event(
                RunId, 0, undefined,
                'workflow.instance.created', undefined, undefined,
                undefined,
                #{workflow => bt_versioned_workflow, input => Input,
                  steps => [charge]}),
    {ok, Lease} = beamtrail_memory_storage:acquire_lease(RunId, old_worker, 1),
    {ok, _} = beamtrail_memory_storage:append_event(
                RunId, 1, maps:get(fencing_token, Lease),
                'attempt.started', charge, 1,
                {charge, <<"o-version-1">>}, #{attempt => 1}),
    timer:sleep(2),
    ?assertMatch({error, {migration_required, _}}, beamtrail:dispatch(RunId)),
    ?assertEqual(timeout, receive_exec_short()),
    State = beamtrail:get_state(RunId),
    ?assertEqual(true, maps:get(migration_required_for_version_change, State)),
    {ok, Events} = beamtrail:events(RunId),
    ?assertEqual(1, length([E || E <- Events,
                            maps:get(event_type, E) =:= 'attempt.started'])),
    ?assertEqual(0, length([E || E <- Events,
                            maps:get(event_type, E) =:= 'step.succeeded'])).

retry_attempts_preserved_in_chronological_order() ->
    Input = #{order_id => <<"o-ord-1">>, test_pid => self()},
    {ok, RunId} = beamtrail:start_workflow(bt_retry_workflow, Input),
    _ = receive_exec(), _ = receive_exec(),
    Q = beamtrail_query:describe(RunId),
    Attempts = maps:get(attempts, Q),
    Nums = [maps:get(attempt, A) || A <- Attempts],
    ?assertEqual([1, 2], Nums),
    [A1, A2] = Attempts,
    ?assert(maps:get(started_event_seq, A1)
            < maps:get(started_event_seq, A2)),
    ?assertEqual(failed, maps:get(status, A1)),
    ?assertEqual(succeeded, maps:get(status, A2)).

storage_adapter_is_application_configurable() ->
    ?assertEqual(beamtrail_memory_storage, beamtrail:storage()),
    ok = application:set_env(beamtrail, storage_adapter,
                             beamtrail_postgres_storage),
    ?assertEqual(beamtrail_postgres_storage, beamtrail:storage()),
    ok = application:unset_env(beamtrail, storage_adapter),
    ?assertEqual(beamtrail_memory_storage, beamtrail:storage()).

query_describe_exposes_inspector_blocks() ->
    Input = #{order_id => <<"o-proto-1">>, test_pid => self()},
    {ok, RunId} = beamtrail:start_workflow(bt_success_workflow, Input),
    _ = receive_exec(), _ = receive_exec(),
    Q = beamtrail_query:describe(RunId),
    ?assertMatch(#{authoritative := <<"workflow_events", _/binary>>,
                   snapshot := #{snapshot_seq := _, replay_tail_events := _},
                   read_models := [_ | _]}, maps:get(source_of_truth, Q)),
    ?assertMatch(#{step_version_source := <<"attempt.started.step_version">>,
                   migration_required_for_version_change := false},
                 maps:get(replay_policy, Q)),
    ?assertMatch(#{owner_node := _}, maps:get(ownership, Q)),
    ?assertMatch(#{target_ms := 30000, status := _},
                 maps:get(recovery, Q)),
    ?assertMatch(#{module := beamtrail_memory_storage,
                   primary_writes := [_ | _]},
                 maps:get(storage_adapter, Q)).

recovery_requeued_records_recovered_in_ms() ->
    RunId = <<"recov-in-ms-1">>,
    Input = #{order_id => <<"o-rec-1">>},
    {ok, _} = beamtrail_memory_storage:append_event(
                RunId, 0, undefined,
                'workflow.instance.created', undefined, undefined,
                undefined,
                #{workflow => bt_versioned_workflow, input => Input,
                  steps => [charge]}),
    {ok, Lease} = beamtrail_memory_storage:acquire_lease(RunId, recovery_seed, 1),
    {ok, _} = beamtrail_memory_storage:append_event(
                RunId, 1, maps:get(fencing_token, Lease),
                'attempt.started', charge, 1,
                {charge, <<"o-rec-1">>}, #{attempt => 1}),
    timer:sleep(15),
    {ok, requeued} = beamtrail:mark_recovery_requeued(RunId),
    {ok, Events} = beamtrail:events(RunId),
    [Req] = [E || E <- Events, maps:get(event_type, E) =:= 'recovery.requeued'],
    Payload = maps:get(payload, Req),
    Recovered = maps:get(recovered_in_ms, Payload),
    ?assert(is_integer(Recovered)),
    ?assert(Recovered >= 10),
    Q = beamtrail_query:describe(RunId),
    ?assertEqual(Recovered, maps:get(recovered_in_ms, Q)),
    ?assertMatch(#{status := pass, recovered_in_ms := Recovered},
                 maps:get(recovery, Q)).

receive_exec() ->
    receive Message -> Message
    after 1000 -> timeout
    end.

receive_exec_short() ->
    receive Message -> Message
    after 50 -> timeout
    end.

wait_for_status(_RunId, _Target, Remaining) when Remaining =< 0 ->
    {error, timeout};
wait_for_status(RunId, Target, Remaining) ->
    case maps:get(status, beamtrail:get_state(RunId)) of
        Target -> ok;
        _ ->
            timer:sleep(20),
            wait_for_status(RunId, Target, Remaining - 20)
    end.
