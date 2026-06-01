-module(beamtrail_extended_tests).

-include_lib("eunit/include/eunit.hrl").

extended_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      fun workflow_timeout_emits_workflow_failed/0,
      fun scanner_requeues_unknown_attempts/0,
      fun scanner_scans_one_batch_at_a_time/0,
      fun worker_supervisor_rejects_when_pool_full/0,
      fun start_workflow_returns_error_for_duplicate_run_id/0,
      fun query_describe_exposes_read_model/0,
      fun query_instance_current_step_matches_reducer/0,
      fun telemetry_counters_track_attempts/0,
      fun postgres_stub_is_loud_about_not_implemented/0,
      fun recover_unfinished_returns_storage_error/0,
      fun storage_lists_run_ids_with_cursor/0,
      fun storage_rejects_expected_seq_conflict/0,
      fun storage_renews_current_lease_without_changing_fence/0,
      fun storage_refuses_to_renew_expired_lease/0,
      fun dispatch_refuses_when_run_is_leased/0,
      fun dispatch_refuses_stale_lease_before_replay/0,
      fun dispatch_renews_lease_while_step_runs/0,
      fun dispatch_refuses_version_mismatch_without_migration/0,
      fun retry_attempts_preserved_in_chronological_order/0,
      fun storage_adapter_is_application_configurable/0,
      fun query_describe_exposes_inspector_blocks/0,
      fun recovery_requeued_records_recovered_in_ms/0
     ]}.

setup() ->
    ok = application:unset_env(beamtrail, storage_adapter),
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
    case whereis(beamtrail_worker_sup) of
        undefined -> ok;
        Pid ->
            unlink(Pid),
            exit(Pid, shutdown)
    end,
    ok = application:unset_env(beamtrail, worker_max_children),
    ok = application:unset_env(beamtrail, storage_adapter),
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
    {ok, Lease} = beamtrail_memory_storage:acquire_lease(RunId, scanner_seed, 100),
    {ok, _} = beamtrail_memory_storage:append_event(
                RunId, 1, maps:get(fencing_token, Lease),
                'attempt.started', charge, 1,
                {charge, <<"o-sc-1">>}, #{attempt => 1}),
    timer:sleep(120),
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

scanner_scans_one_batch_at_a_time() ->
    {ok, Run1} = beamtrail:start_workflow(
                   bt_timeout_workflow,
                   #{order_id => <<"o-batch-1">>},
                   #{run_id => <<"batch-run-1">>, auto_dispatch => false}),
    {ok, Run2} = beamtrail:start_workflow(
                   bt_timeout_workflow,
                   #{order_id => <<"o-batch-2">>},
                   #{run_id => <<"batch-run-2">>, auto_dispatch => false}),
    {ok, _Pid} = beamtrail_scanner:start_link(#{interval_ms => infinity,
                                                auto_start => false,
                                                batch_size => 1}),
    ?assertEqual({ok, [Run1]}, beamtrail_scanner:scan_now()),
    ?assertEqual({ok, [Run2]}, beamtrail_scanner:scan_now()).

worker_supervisor_rejects_when_pool_full() ->
    ok = application:set_env(beamtrail, worker_max_children, 1),
    {ok, Sup} = beamtrail_worker_sup:start_link(),
    unlink(Sup),
    Input1 = #{order_id => <<"o-pool-1">>, test_pid => self(),
               sleep_ms => 200},
    Input2 = #{order_id => <<"o-pool-2">>, test_pid => self(),
               sleep_ms => 200},
    {ok, Run1} = beamtrail:start_workflow(bt_slow_success_workflow, Input1,
                                          #{run_id => <<"pool-run-1">>,
                                            auto_dispatch => false}),
    {ok, Run2} = beamtrail:start_workflow(bt_slow_success_workflow, Input2,
                                          #{run_id => <<"pool-run-2">>,
                                            auto_dispatch => false}),
    ?assertMatch({ok, _}, beamtrail_worker_sup:dispatch_async(Run1)),
    ?assertEqual({error, worker_pool_full}, beamtrail_worker_sup:dispatch_async(Run2)),
    ?assertMatch({slow_executed, slow}, receive_exec()),
    ?assertEqual(timeout, receive_exec_short()).

start_workflow_returns_error_for_duplicate_run_id() ->
    RunId = <<"duplicate-run-1">>,
    Input = #{order_id => <<"o-dup-1">>},
    {ok, RunId} = beamtrail:start_workflow(bt_timeout_workflow, Input,
                                           #{run_id => RunId,
                                             auto_dispatch => false}),
    ?assertMatch({error, {create_failed, RunId, {conflict, _}}},
                 beamtrail:start_workflow(bt_timeout_workflow, Input,
                                          #{run_id => RunId,
                                            auto_dispatch => false})).

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

query_instance_current_step_matches_reducer() ->
    RunId = <<"query-step-run-1">>,
    Input = #{order_id => <<"o-q-step-1">>},
    {ok, _} = beamtrail_memory_storage:append_event(
                RunId, 0, undefined,
                'workflow.instance.created', undefined, undefined,
                undefined,
                #{workflow => bt_success_workflow, input => Input,
                  steps => [charge, ship]}),
    {ok, Lease} = beamtrail_memory_storage:acquire_lease(RunId, query_seed, 1000),
    Fence = maps:get(fencing_token, Lease),
    {ok, _} = beamtrail_memory_storage:append_event(
                RunId, 1, Fence,
                'attempt.started', charge, 1,
                {charge, <<"o-q-step-1">>}, #{attempt => 1}),
    {ok, _} = beamtrail_memory_storage:append_event(
                RunId, 2, Fence,
                'step.succeeded', charge, 1,
                {charge, <<"o-q-step-1">>}, #{result => #{ok => true}}),
    Q = beamtrail_query:describe(RunId),
    ?assertEqual(ship, maps:get(current_step, Q)),
    ?assertEqual(ship, maps:get(current_step, maps:get(instance, Q))).

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
    ?assertEqual({error, not_implemented},
                 beamtrail_postgres_storage:events(<<"r">>)),
    ?assertEqual({error, not_implemented},
                 beamtrail_postgres_storage:list_run_ids()),
    ?assertEqual({error, not_implemented},
                 beamtrail_postgres_storage:list_run_ids(undefined, 10)),
    ?assertEqual(not_found, beamtrail_postgres_storage:read_lease(<<"r">>)).

recover_unfinished_returns_storage_error() ->
    ok = application:set_env(beamtrail, storage_adapter,
                             beamtrail_postgres_storage),
    ?assertEqual({error, not_implemented}, beamtrail:recover_unfinished()).

storage_lists_run_ids_with_cursor() ->
    {ok, _} = beamtrail:start_workflow(bt_timeout_workflow, #{order_id => <<"o-page-2">>},
                                       #{run_id => <<"page-run-2">>, auto_dispatch => false}),
    {ok, _} = beamtrail:start_workflow(bt_timeout_workflow, #{order_id => <<"o-page-1">>},
                                       #{run_id => <<"page-run-1">>, auto_dispatch => false}),
    {ok, _} = beamtrail:start_workflow(bt_timeout_workflow, #{order_id => <<"o-page-3">>},
                                       #{run_id => <<"page-run-3">>, auto_dispatch => false}),
    ?assertEqual(
       {ok, #{run_ids => [<<"page-run-1">>, <<"page-run-2">>],
              next_cursor => <<"page-run-2">>,
              has_more => true}},
       beamtrail_memory_storage:list_run_ids(undefined, 2)),
    ?assertEqual(
       {ok, #{run_ids => [<<"page-run-3">>],
              next_cursor => <<"page-run-3">>,
              has_more => false}},
       beamtrail_memory_storage:list_run_ids(<<"page-run-2">>, 2)).

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

storage_renews_current_lease_without_changing_fence() ->
    RunId = <<"renew-run-1">>,
    {ok, Lease} = beamtrail_memory_storage:acquire_lease(RunId, worker_a, 10),
    Fence = maps:get(fencing_token, Lease),
    timer:sleep(2),
    {ok, Renewed} = beamtrail_memory_storage:renew_lease(RunId, Fence, 100),
    ?assertEqual(Fence, maps:get(fencing_token, Renewed)),
    ?assert(maps:get(lease_until, Renewed) > maps:get(lease_until, Lease)),
    ?assertEqual({error, stale_fence},
                 beamtrail_memory_storage:renew_lease(RunId, Fence - 1, 100)).

storage_refuses_to_renew_expired_lease() ->
    RunId = <<"expired-renew-run-1">>,
    {ok, Lease} = beamtrail_memory_storage:acquire_lease(RunId, worker_a, 1),
    timer:sleep(2),
    ?assertEqual({error, lease_expired},
                 beamtrail_memory_storage:renew_lease(
                   RunId, maps:get(fencing_token, Lease), 100)).

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

dispatch_renews_lease_while_step_runs() ->
    Input = #{order_id => <<"o-slow-lease-1">>, test_pid => self(),
              sleep_ms => 160},
    {ok, RunId} = beamtrail:start_workflow(bt_slow_success_workflow, Input,
                                           #{auto_dispatch => false}),
    {ok, Lease} = beamtrail_memory_storage:acquire_lease(RunId, slow_worker, 50),
    ?assertMatch({ok, #{status := completed}}, beamtrail:dispatch(RunId, Lease)),
    ?assertMatch({slow_executed, slow}, receive_exec()),
    {ok, Events} = beamtrail:events(RunId),
    ?assertEqual(1, length([E || E <- Events,
                            maps:get(event_type, E) =:= 'step.succeeded'])),
    {ok, CurrentLease} = beamtrail_memory_storage:read_lease(RunId),
    ?assertEqual(maps:get(fencing_token, Lease),
                 maps:get(fencing_token, CurrentLease)),
    ?assert(maps:get(lease_until, CurrentLease) > maps:get(lease_until, Lease)).

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
    {ok, Lease} = beamtrail_memory_storage:acquire_lease(RunId, recovery_seed, 100),
    {ok, _} = beamtrail_memory_storage:append_event(
                RunId, 1, maps:get(fencing_token, Lease),
                'attempt.started', charge, 1,
                {charge, <<"o-rec-1">>}, #{attempt => 1}),
    timer:sleep(120),
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
