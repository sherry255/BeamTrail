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
      fun postgres_adapter_requires_config/0,
      fun get_state_returns_storage_error/0,
      fun dispatch_returns_storage_error/0,
      fun query_describe_returns_storage_error/0,
      fun recover_unfinished_returns_storage_error/0,
      fun recovery_marker_returns_storage_error/0,
      fun recovery_marker_surfaces_append_error/0,
      fun snapshot_write_failure_is_nonfatal/0,
      fun load_state_ignores_obsolete_snapshot_revision/0,
      fun snapshot_schema_contract_pins_revision_to_state_shape/0,
      fun storage_lists_run_ids_with_cursor/0,
      fun storage_rejects_expected_seq_conflict/0,
      fun storage_rejects_zombie_append_after_fence_takeover/0,
      fun storage_renews_current_lease_without_changing_fence/0,
      fun storage_refuses_to_renew_expired_lease/0,
      fun dispatch_refuses_when_run_is_leased/0,
      fun dispatch_refuses_stale_lease_before_replay/0,
      fun dispatch_renews_lease_while_step_runs/0,
      fun scanner_does_not_write_skipped_marker_for_live_lease/0,
      fun scanner_rejects_invalid_interval/0,
      fun active_runner_wakes_retry_without_scanner_tick/0,
      fun active_runner_retry_timer_survives_heartbeat_and_lookup/0,
      fun active_runner_supervisor_rejects_when_pool_full/0,
      fun active_runner_crash_allows_scanner_takeover_after_lease_expiry/0,
      fun active_runner_stops_step_when_lease_heartbeat_fails/0,
      fun active_runner_records_step_timeout/0,
      fun active_runner_registry_exposes_live_state/0,
      fun query_describe_exposes_active_runner/0,
      fun active_runner_stays_inspectable_while_step_executes/0,
      fun active_runner_reuses_loaded_state_across_steps/0,
      fun dispatch_refuses_version_mismatch_without_migration/0,
      fun completed_step_version_change_does_not_block_next_step/0,
      fun scanner_skips_migration_blocked_runs/0,
      fun retry_policy_failure_fails_terminally_without_retry_loop/0,
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
            stop_registered(beamtrail_worker_sup)
    end,
    case whereis(beamtrail_run_sup) of
        undefined -> ok;
        RunSup ->
            unlink(RunSup),
            stop_registered(beamtrail_run_sup)
    end,
    case whereis(beamtrail_run_registry) of
        undefined -> ok;
        Registry ->
            unlink(Registry),
            stop_registered(beamtrail_run_registry)
    end,
    ok = application:unset_env(beamtrail, worker_max_children),
    ok = application:unset_env(beamtrail, run_max_children),
    ok = application:unset_env(beamtrail, lease_ttl_ms),
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

postgres_adapter_requires_config() ->
    ?assertEqual({error, postgres_not_configured},
                 beamtrail_postgres_storage:append_event(
                   <<"r">>, 0, undefined, 'workflow.instance.created', undefined,
                   undefined, undefined, #{})),
    ?assertEqual({error, postgres_not_configured},
                 beamtrail_postgres_storage:read_events(<<"r">>, 1, infinity)),
    ?assertEqual({error, postgres_not_configured},
                 beamtrail_postgres_storage:events(<<"r">>)),
    ?assertEqual({error, postgres_not_configured},
                 beamtrail_postgres_storage:list_run_ids()),
    ?assertEqual({error, postgres_not_configured},
                 beamtrail_postgres_storage:list_run_ids(undefined, 10)),
    ?assertEqual({error, postgres_not_configured},
                 beamtrail_postgres_storage:read_lease(<<"r">>)).

get_state_returns_storage_error() ->
    ok = application:set_env(beamtrail, storage_adapter,
                             beamtrail_postgres_storage),
    ?assertEqual({error, postgres_not_configured},
                 beamtrail:get_state(<<"state-error-run">>)).

dispatch_returns_storage_error() ->
    ok = application:set_env(beamtrail, storage_adapter,
                             beamtrail_postgres_storage),
    ?assertEqual({error, postgres_not_configured},
                 beamtrail:dispatch(<<"dispatch-error-run">>)).

query_describe_returns_storage_error() ->
    ok = application:set_env(beamtrail, storage_adapter,
                             beamtrail_postgres_storage),
    ?assertEqual({error, postgres_not_configured},
                 beamtrail_query:describe(<<"query-error-run">>)).

recover_unfinished_returns_storage_error() ->
    ok = application:set_env(beamtrail, storage_adapter,
                             beamtrail_postgres_storage),
    ?assertEqual({error, postgres_not_configured}, beamtrail:recover_unfinished()).

recovery_marker_returns_storage_error() ->
    ok = application:set_env(beamtrail, storage_adapter,
                             beamtrail_postgres_storage),
    ?assertEqual({error, postgres_not_configured},
                 beamtrail:mark_recovery_requeued(<<"missing-run">>)).

recovery_marker_surfaces_append_error() ->
    ok = application:set_env(beamtrail, storage_adapter,
                             bt_recovery_marker_fail_storage),
    RunId = <<"recovery-marker-error-run-1">>,
    {ok, _} = beamtrail_memory_storage:append_event(
                RunId, 0, undefined,
                'workflow.instance.created', undefined, undefined,
                undefined,
                #{workflow => bt_success_workflow, input => #{}, steps => [charge]}),
    {ok, Lease} = beamtrail_memory_storage:acquire_lease(RunId, recovery_seed, 20),
    {ok, _} = beamtrail_memory_storage:append_event(
                RunId, 1, maps:get(fencing_token, Lease),
                'attempt.started', charge, 1,
                {charge, <<"recovery-marker-error">>}, #{attempt => 1}),
    ok = wait_until_lease_expired(RunId, 1000),
    ?assertEqual({error, recovery_marker_failed},
                 beamtrail:mark_recovery_requeued(RunId)).

snapshot_write_failure_is_nonfatal() ->
    ok = application:set_env(beamtrail, storage_adapter,
                             bt_snapshot_fail_storage),
    Input = #{order_id => <<"o-snapshot-fail-1">>, test_pid => self()},
    {ok, RunId} = beamtrail:start_workflow(bt_success_workflow, Input),
    ?assertMatch({executed, charge, 1, {charge, <<"o-snapshot-fail-1">>}},
                 receive_exec()),
    ?assertMatch({executed, ship, 1, {ship, <<"o-snapshot-fail-1">>}},
                 receive_exec()),
    ?assertEqual(completed, maps:get(status, beamtrail:get_state(RunId))).

load_state_ignores_obsolete_snapshot_revision() ->
    RunId = <<"obsolete-snapshot-run-1">>,
    {ok, _} = beamtrail_memory_storage:append_event(
                RunId, 0, undefined,
                'workflow.instance.created', undefined, undefined,
                undefined,
                #{workflow => bt_success_workflow, input => #{}, steps => []}),
    {ok, Lease} = beamtrail_memory_storage:acquire_lease(RunId, snapshot_seed, 1000),
    {ok, _} = beamtrail_memory_storage:append_event(
                RunId, 1, maps:get(fencing_token, Lease),
                'workflow.completed', undefined, undefined,
                undefined, #{}),
    ok = beamtrail_memory_storage:write_snapshot(
           RunId, beamtrail_reducer:new(), 2, 0),
    State = beamtrail:get_state(RunId),
    ?assertEqual(completed, maps:get(status, State)),
    ?assertEqual(2, maps:get(last_event_seq, State)).

snapshot_schema_contract_pins_revision_to_state_shape() ->
    Schema = beamtrail_state:snapshot_schema(),
    ?assertEqual(beamtrail_state:snapshot_revision(), maps:get(revision, Schema)),
    BaseKeys = maps:keys(beamtrail_reducer:new()),
    RuntimeKeys = [created_at, migration_required_for_version_change],
    ?assertEqual(lists:sort(BaseKeys ++ RuntimeKeys),
                 maps:get(state_keys, Schema)),
    ?assertEqual(beamtrail_reducer:attempt_keys(),
                 maps:get(attempt_keys, Schema)),
    ?assert(beamtrail_state:snapshot_revision() > 0).

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

storage_rejects_zombie_append_after_fence_takeover() ->
    RunId = <<"zombie-fence-run-1">>,
    {ok, _} = beamtrail_memory_storage:append_event(
                RunId, 0, undefined,
                'workflow.instance.created', undefined, undefined,
                undefined,
                #{workflow => bt_success_workflow, input => #{},
                  steps => [charge]}),
    {ok, Lease1} = beamtrail_memory_storage:acquire_lease(RunId, worker_a, 30),
    Fence1 = maps:get(fencing_token, Lease1),
    {ok, _} = beamtrail_memory_storage:append_event(
                RunId, 1, Fence1,
                'attempt.started', charge, 1,
                {charge, <<"zombie">>}, #{attempt => 1}),
    timer:sleep(40),
    {ok, Lease2} = beamtrail_memory_storage:acquire_lease(RunId, worker_b, 1000),
    Fence2 = maps:get(fencing_token, Lease2),
    ?assert(Fence2 > Fence1),
    {ok, _} = beamtrail_memory_storage:append_event(
                RunId, 2, Fence2,
                'recovery.requeued', undefined, undefined,
                undefined, #{requeued_at => erlang:system_time(millisecond)}),
    ?assertEqual({error, stale_fence},
                 beamtrail_memory_storage:append_event(
                   RunId, 3, Fence1,
                   'step.succeeded', charge, 1,
                   {charge, <<"zombie">>}, #{result => zombie_late_success})),
    {ok, Events} = beamtrail_memory_storage:events(RunId),
    ?assertEqual(3, length(Events)),
    ?assertEqual(0, length([E || E <- Events,
                            maps:get(event_type, E) =:= 'step.succeeded'])).

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
    {ok, Lease} = beamtrail_memory_storage:acquire_lease(RunId, stale_worker, 200),
    {ok, _} = beamtrail_memory_storage:append_event(
                RunId, 1, maps:get(fencing_token, Lease),
                'attempt.started', charge, 1,
                {charge, <<"o-stale-lease-1">>}, #{attempt => 1}),
    ok = wait_until_lease_expired(RunId, 1000),
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

scanner_does_not_write_skipped_marker_for_live_lease() ->
    RunId = <<"scanner-live-lease-run-1">>,
    {ok, _} = beamtrail_memory_storage:append_event(
                RunId, 0, undefined,
                'workflow.instance.created', undefined, undefined,
                undefined,
                #{workflow => bt_success_workflow, input => #{},
                  steps => [charge]}),
    {ok, _Lease} = beamtrail_memory_storage:acquire_lease(
                     RunId, live_runner, 30000),
    {ok, _Pid} = beamtrail_scanner:start_link(#{interval_ms => infinity,
                                                auto_start => false}),
    ?assertEqual({ok, []}, beamtrail_scanner:scan_now()),
    {ok, Events} = beamtrail:events(RunId),
    ?assertEqual(0, length([E || E <- Events,
                            maps:get(event_type, E) =:= 'recovery.skipped'])),
    ?assertEqual(['workflow.instance.created'],
                 [maps:get(event_type, E) || E <- Events]).

scanner_rejects_invalid_interval() ->
    {ok, _Pid} = beamtrail_scanner:start_link(#{interval_ms => infinity,
                                                auto_start => false}),
    ?assertEqual({error, invalid_interval}, beamtrail_scanner:set_interval(0)),
    ?assertEqual({error, invalid_interval}, beamtrail_scanner:set_interval(-1)),
    ?assertEqual(ok, beamtrail_scanner:set_interval(infinity)).

active_runner_wakes_retry_without_scanner_tick() ->
    {ok, Registry} = beamtrail_run_registry:start_link(),
    unlink(Registry),
    {ok, RunSup} = beamtrail_run_sup:start_link(),
    unlink(RunSup),
    Input = #{order_id => <<"o-active-retry-1">>, test_pid => self()},
    {ok, RunId} = beamtrail:start_workflow(bt_retry_backoff_workflow, Input,
                                           #{run_id => <<"active-retry-run-1">>,
                                             auto_dispatch => false}),
    ?assertMatch({ok, _Pid}, beamtrail_run_sup:dispatch(RunId)),
    ?assertMatch({retry_backoff_execute, 1, {charge, <<"o-active-retry-1">>}},
                 receive_exec()),
    ?assertMatch({retry_backoff_execute, 2, {charge, <<"o-active-retry-1">>}},
                 receive_exec()),
    ok = wait_for_status(RunId, completed, 1000).

active_runner_retry_timer_survives_heartbeat_and_lookup() ->
    ok = application:set_env(beamtrail, lease_ttl_ms, 60),
    {ok, Registry} = beamtrail_run_registry:start_link(),
    unlink(Registry),
    {ok, RunSup} = beamtrail_run_sup:start_link(),
    unlink(RunSup),
    Input = #{order_id => <<"o-active-retry-heartbeat-1">>, test_pid => self()},
    {ok, RunId} = beamtrail:start_workflow(bt_retry_heartbeat_backoff_workflow, Input,
                                           #{run_id => <<"active-retry-heartbeat-run-1">>,
                                             auto_dispatch => false}),
    ?assertMatch({ok, _Pid}, beamtrail_run_sup:dispatch(RunId)),
    ?assertMatch({retry_backoff_execute, 1, {charge, <<"o-active-retry-heartbeat-1">>}},
                 receive_exec()),
    ok = wait_for_status(RunId, retrying, 1000),
    ?assertMatch(
       {ok, #{status := waiting_retry,
              retry_due_in_ms := RetryDueInMs,
              heartbeat_due_in_ms := _}}
       when is_integer(RetryDueInMs),
       beamtrail_run_registry:lookup(RunId)),
    timer:sleep(80),
    ?assertMatch(
       {ok, #{status := waiting_retry,
              retry_due_in_ms := RetryDueInMs,
              heartbeat_due_in_ms := _}}
       when is_integer(RetryDueInMs),
       beamtrail_run_registry:lookup(RunId)),
    ?assertMatch({retry_backoff_execute, 2, {charge, <<"o-active-retry-heartbeat-1">>}},
                 receive_exec()),
    ok = wait_for_status(RunId, completed, 1000).

active_runner_supervisor_rejects_when_pool_full() ->
    ok = application:set_env(beamtrail, run_max_children, 1),
    {ok, Registry} = beamtrail_run_registry:start_link(),
    unlink(Registry),
    {ok, RunSup} = beamtrail_run_sup:start_link(),
    unlink(RunSup),
    Input1 = #{order_id => <<"o-active-pool-1">>, test_pid => self(),
               sleep_ms => 200},
    Input2 = #{order_id => <<"o-active-pool-2">>, test_pid => self(),
               sleep_ms => 200},
    {ok, Run1} = beamtrail:start_workflow(bt_slow_success_workflow, Input1,
                                          #{run_id => <<"active-pool-run-1">>,
                                            auto_dispatch => false}),
    {ok, Run2} = beamtrail:start_workflow(bt_slow_success_workflow, Input2,
                                          #{run_id => <<"active-pool-run-2">>,
                                            auto_dispatch => false}),
    ?assertMatch({ok, _Pid}, beamtrail_run_sup:dispatch(Run1)),
    ?assertEqual({error, run_pool_full}, beamtrail_run_sup:dispatch(Run2)),
    ?assertMatch({slow_executed, slow}, receive_exec()).

active_runner_crash_allows_scanner_takeover_after_lease_expiry() ->
    ok = application:set_env(beamtrail, lease_ttl_ms, 80),
    {ok, Registry} = beamtrail_run_registry:start_link(),
    unlink(Registry),
    {ok, RunSup} = beamtrail_run_sup:start_link(),
    unlink(RunSup),
    Input = #{order_id => <<"o-active-crash-1">>, test_pid => self()},
    {ok, RunId} = beamtrail:start_workflow(bt_retry_backoff_workflow, Input,
                                           #{run_id => <<"active-crash-run-1">>,
                                             auto_dispatch => false}),
    {ok, Pid} = beamtrail_run_sup:dispatch(RunId),
    ?assertMatch({retry_backoff_execute, 1, {charge, <<"o-active-crash-1">>}},
                 receive_exec()),
    ok = wait_for_status(RunId, retrying, 1000),
    exit(Pid, kill),
    timer:sleep(140),
    {ok, _Scanner} = beamtrail_scanner:start_link(#{interval_ms => infinity,
                                                    auto_start => false}),
    ?assertEqual({ok, [RunId]}, beamtrail_scanner:scan_now()),
    ?assertMatch({retry_backoff_execute, 2, {charge, <<"o-active-crash-1">>}},
                 receive_exec()),
    ok = wait_for_status(RunId, completed, 1000).

active_runner_stops_step_when_lease_heartbeat_fails() ->
    ok = application:set_env(beamtrail, storage_adapter, bt_no_renew_storage),
    ok = application:set_env(beamtrail, lease_ttl_ms, 60),
    {ok, Registry} = beamtrail_run_registry:start_link(),
    unlink(Registry),
    {ok, RunSup} = beamtrail_run_sup:start_link(),
    unlink(RunSup),
    Input = #{order_id => <<"o-lease-lost-1">>, test_pid => self(),
              gate => lease_lost_gate},
    {ok, RunId} = beamtrail:start_workflow(bt_blocking_success_workflow, Input,
                                           #{run_id => <<"lease-lost-run-1">>,
                                             auto_dispatch => false}),
    {ok, Pid} = beamtrail_run_sup:dispatch(RunId),
    ?assertMatch({blocking_started, ExecPid, 1} when is_pid(ExecPid), receive_exec()),
    ok = wait_until_dead(Pid, 1000),
    ?assertEqual(timeout, receive_exec_short()),
    {ok, Events} = beamtrail:events(RunId),
    ?assertEqual(0, length([E || E <- Events,
                            maps:get(event_type, E) =:= 'step.succeeded'])).

active_runner_records_step_timeout() ->
    {ok, Registry} = beamtrail_run_registry:start_link(),
    unlink(Registry),
    {ok, RunSup} = beamtrail_run_sup:start_link(),
    unlink(RunSup),
    Input = #{order_id => <<"o-active-timeout-1">>},
    {ok, RunId} = beamtrail:start_workflow(bt_timeout_workflow, Input,
                                           #{run_id => <<"active-timeout-run-1">>,
                                             auto_dispatch => false}),
    {ok, Pid} = beamtrail_run_sup:dispatch(RunId),
    ok = wait_until_dead(Pid, 1000),
    State = beamtrail:get_state(RunId),
    ?assertEqual(failed, maps:get(status, State)),
    ?assertMatch(#{reason := timeout}, maps:get(failure, State)),
    {ok, Events} = beamtrail:events(RunId),
    ?assertEqual(1, length([E || E <- Events,
                            maps:get(event_type, E) =:= 'step.failed'])),
    ?assertEqual(0, length([E || E <- Events,
                            maps:get(event_type, E) =:= 'step.succeeded'])).

active_runner_registry_exposes_live_state() ->
    {ok, Registry} = beamtrail_run_registry:start_link(),
    unlink(Registry),
    {ok, RunSup} = beamtrail_run_sup:start_link(),
    unlink(RunSup),
    Input = #{order_id => <<"o-active-inspect-1">>, test_pid => self()},
    {ok, RunId} = beamtrail:start_workflow(bt_retry_long_backoff_workflow, Input,
                                           #{run_id => <<"active-inspect-run-1">>,
                                             auto_dispatch => false}),
    {ok, Pid} = beamtrail_run_sup:dispatch(RunId),
    ?assertMatch({retry_backoff_execute, 1, {charge, <<"o-active-inspect-1">>}},
                 receive_exec()),
    ok = wait_for_status(RunId, retrying, 1000),
    ?assertMatch(
       {ok, #{run_id := RunId,
              pid := Pid,
              status := waiting_retry,
              fencing_token := 1,
              retry_due_in_ms := _,
              heartbeat_due_in_ms := _}},
       beamtrail_run_registry:lookup(RunId)),
    ActiveRuns = beamtrail_run_registry:list(),
    ?assert(lists:any(fun(#{run_id := R}) -> R =:= RunId end, ActiveRuns)).

query_describe_exposes_active_runner() ->
    {ok, Registry} = beamtrail_run_registry:start_link(),
    unlink(Registry),
    {ok, RunSup} = beamtrail_run_sup:start_link(),
    unlink(RunSup),
    Input = #{order_id => <<"o-query-active-1">>, test_pid => self()},
    {ok, RunId} = beamtrail:start_workflow(bt_retry_long_backoff_workflow, Input,
                                           #{run_id => <<"query-active-run-1">>,
                                             auto_dispatch => false}),
    {ok, Pid} = beamtrail_run_sup:dispatch(RunId),
    ?assertMatch({retry_backoff_execute, 1, {charge, <<"o-query-active-1">>}},
                 receive_exec()),
    ok = wait_for_status(RunId, retrying, 1000),
    Q = beamtrail_query:describe(RunId),
    ?assertMatch(#{status := waiting_retry,
                   pid := Pid,
                   fencing_token := 1,
                   retry_due_in_ms := _},
                 maps:get(active_runner, Q)),
    exit(Pid, kill),
    ok = wait_until_dead(Pid, 1000),
    ?assertEqual(#{status => not_found},
                 maps:get(active_runner, beamtrail_query:describe(RunId))).

active_runner_stays_inspectable_while_step_executes() ->
    {ok, Registry} = beamtrail_run_registry:start_link(),
    unlink(Registry),
    {ok, RunSup} = beamtrail_run_sup:start_link(),
    unlink(RunSup),
    Gate = active_inspect_gate,
    Input = #{order_id => <<"o-active-executing-1">>, test_pid => self(),
              gate => Gate},
    {ok, RunId} = beamtrail:start_workflow(bt_blocking_success_workflow, Input,
                                           #{run_id => <<"active-executing-run-1">>,
                                             auto_dispatch => false}),
    {ok, Pid} = beamtrail_run_sup:dispatch(RunId),
    {blocking_started, ExecPid, 1} = receive_exec(),
    StartedAt = erlang:monotonic_time(millisecond),
    {ok, Active} = beamtrail_run_registry:lookup(RunId),
    ?assertMatch(#{run_id := RunId,
                   pid := Pid,
                   status := executing,
                   fencing_token := 1,
                   dispatch_pid := ExecPid},
                 Active),
    ?assert(is_integer(maps:get(heartbeat_due_in_ms, Active))),
    ?assert(erlang:monotonic_time(millisecond) - StartedAt < 200),
    Q = beamtrail_query:describe(RunId),
    ?assertMatch(#{status := executing, pid := Pid},
                 maps:get(active_runner, Q)),
    ExecPid ! {Gate, continue},
    ?assertMatch({ok, #{status := completed}}, wait_for_state(RunId, completed, 1000)).

active_runner_reuses_loaded_state_across_steps() ->
    ok = application:set_env(beamtrail, storage_adapter, bt_counting_storage),
    bt_counting_storage:reset_counts(),
    {ok, Registry} = beamtrail_run_registry:start_link(),
    unlink(Registry),
    {ok, RunSup} = beamtrail_run_sup:start_link(),
    unlink(RunSup),
    Input = #{order_id => <<"o-active-state-cache-1">>, test_pid => self()},
    {ok, RunId} = beamtrail:start_workflow(bt_success_workflow, Input,
                                           #{run_id => <<"active-state-cache-run-1">>,
                                             auto_dispatch => false}),
    bt_counting_storage:reset_counts(),
    {ok, Pid} = beamtrail_run_sup:dispatch(RunId),
    ?assertMatch({executed, charge, 1, {charge, <<"o-active-state-cache-1">>}},
                 receive_exec()),
    ?assertMatch({executed, ship, 1, {ship, <<"o-active-state-cache-1">>}},
                 receive_exec()),
    ok = wait_until_dead(Pid, 1000),
    Counts = bt_counting_storage:counts(),
    ?assertEqual(1, maps:get(read_snapshot, Counts, 0)),
    ?assertEqual(1, maps:get(events, Counts, 0)),
    ?assertEqual(0, maps:get(read_events, Counts, 0)).

dispatch_refuses_version_mismatch_without_migration() ->
    RunId = <<"version-gate-run-1">>,
    Input = #{order_id => <<"o-version-1">>, test_pid => self()},
    {ok, _} = beamtrail_memory_storage:append_event(
                RunId, 0, undefined,
                'workflow.instance.created', undefined, undefined,
                undefined,
                #{workflow => bt_versioned_workflow, input => Input,
                  steps => [charge]}),
    {ok, Lease} = beamtrail_memory_storage:acquire_lease(RunId, old_worker, 200),
    {ok, _} = beamtrail_memory_storage:append_event(
                RunId, 1, maps:get(fencing_token, Lease),
                'attempt.started', charge, 1,
                {charge, <<"o-version-1">>}, #{attempt => 1}),
    ok = wait_until_lease_expired(RunId, 1000),
    ?assertMatch({error, {migration_required, _}}, beamtrail:dispatch(RunId)),
    ?assertEqual(timeout, receive_exec_short()),
    State = beamtrail:get_state(RunId),
    ?assertEqual(true, maps:get(migration_required_for_version_change, State)),
    {ok, Events} = beamtrail:events(RunId),
    ?assertEqual(1, length([E || E <- Events,
                            maps:get(event_type, E) =:= 'attempt.started'])),
    ?assertEqual(0, length([E || E <- Events,
                            maps:get(event_type, E) =:= 'step.succeeded'])).

completed_step_version_change_does_not_block_next_step() ->
    RunId = <<"completed-version-change-run-1">>,
    Input = #{order_id => <<"o-completed-version-1">>, test_pid => self()},
    {ok, _} = beamtrail_memory_storage:append_event(
                RunId, 0, undefined,
                'workflow.instance.created', undefined, undefined,
                undefined,
                #{workflow => bt_completed_version_change_workflow,
                  input => Input, steps => [charge, ship]}),
    {ok, Lease} = beamtrail_memory_storage:acquire_lease(RunId, old_worker, 1000),
    {ok, _} = beamtrail_memory_storage:append_event(
                RunId, 1, maps:get(fencing_token, Lease),
                'attempt.started', charge, 1,
                {charge, <<"o-completed-version-1">>}, #{attempt => 1}),
    {ok, _} = beamtrail_memory_storage:append_event(
                RunId, 2, maps:get(fencing_token, Lease),
                'step.succeeded', charge, undefined,
                undefined, #{result => #{step => charge}}),
    State = beamtrail:get_state(RunId),
    ?assertEqual(false, maps:get(migration_required_for_version_change, State)),
    ?assertEqual(ship, maps:get(current_step, State)),
    ?assertMatch({ok, #{status := completed}}, beamtrail:dispatch(RunId, Lease)),
    ?assertEqual({completed_version_execute, ship, 1,
                  {ship, <<"o-completed-version-1">>}},
                 receive_exec()).

scanner_skips_migration_blocked_runs() ->
    RunId = <<"migration-blocked-scanner-run-1">>,
    Input = #{order_id => <<"o-migration-blocked-1">>, test_pid => self()},
    {ok, _} = beamtrail_memory_storage:append_event(
                RunId, 0, undefined,
                'workflow.instance.created', undefined, undefined,
                undefined,
                #{workflow => bt_versioned_workflow, input => Input,
                  steps => [charge]}),
    {ok, Lease} = beamtrail_memory_storage:acquire_lease(RunId, old_worker, 20),
    {ok, _} = beamtrail_memory_storage:append_event(
                RunId, 1, maps:get(fencing_token, Lease),
                'attempt.started', charge, 1,
                {charge, <<"o-migration-blocked-1">>}, #{attempt => 1}),
    ok = wait_until_lease_expired(RunId, 1000),
    State = beamtrail:get_state(RunId),
    ?assertEqual(true, maps:get(migration_required_for_version_change, State)),
    {ok, _Pid} = beamtrail_scanner:start_link(#{interval_ms => infinity,
                                                auto_start => false}),
    ?assertEqual({ok, []}, beamtrail_scanner:scan_now()),
    {ok, Events} = beamtrail:events(RunId),
    ?assertEqual(0, length([E || E <- Events,
                            maps:get(event_type, E) =:= 'recovery.requeued'])),
    ?assertEqual(timeout, receive_exec_short()).

retry_policy_failure_fails_terminally_without_retry_loop() ->
    RunId = <<"bad-retry-policy-run-1">>,
    Input = #{order_id => <<"o-bad-retry-policy-1">>, test_pid => self()},
    ?assertEqual({ok, RunId},
                 beamtrail:start_workflow(bt_bad_retry_policy_workflow, Input,
                                          #{run_id => RunId})),
    ?assertEqual({bad_retry_execute, 1}, receive_exec()),
    State = beamtrail:get_state(RunId),
    ?assertEqual(failed, maps:get(status, State)),
    ?assertEqual(true, maps:get(terminal, State)),
    Failure = maps:get(failure, State),
    ?assertMatch(#{retry_policy_error := #{callback := retry_policy}}, Failure),
    {ok, Events} = beamtrail:events(RunId),
    ?assertEqual(1, length([E || E <- Events,
                            maps:get(event_type, E) =:= 'attempt.started'])),
    ?assertEqual(0, length([E || E <- Events,
                            maps:get(event_type, E) =:= 'retry.scheduled'])),
    ?assertEqual({ok, []}, beamtrail:recover_unfinished()),
    {ok, EventsAfterRecovery} = beamtrail:events(RunId),
    ?assertEqual(length(Events), length(EventsAfterRecovery)).

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
    SourceOfTruth = maps:get(source_of_truth, Q),
    SnapshotInfo = maps:get(snapshot, SourceOfTruth),
    ?assertMatch(#{authoritative := <<"workflow_events", _/binary>>,
                   snapshot := #{snapshot_seq := _, replay_tail_events := _},
                   read_models := [_ | _]}, SourceOfTruth),
    ?assertEqual(beamtrail_state:snapshot_policy(),
                 maps:get(policy, SnapshotInfo)),
    ?assertEqual(beamtrail_state:snapshot_revision(),
                 maps:get(snapshot_revision, SnapshotInfo)),
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

wait_for_state(_RunId, _Target, Remaining) when Remaining =< 0 ->
    {error, timeout};
wait_for_state(RunId, Target, Remaining) ->
    State = beamtrail:get_state(RunId),
    case maps:get(status, State) of
        Target -> {ok, State};
        _ ->
            timer:sleep(20),
            wait_for_state(RunId, Target, Remaining - 20)
    end.

wait_until_lease_expired(_RunId, Remaining) when Remaining =< 0 ->
    {error, timeout};
wait_until_lease_expired(RunId, Remaining) ->
    {ok, Lease} = beamtrail_memory_storage:read_lease(RunId),
    case maps:get(lease_until, Lease) =< erlang:system_time(millisecond) of
        true -> ok;
        false ->
            timer:sleep(20),
            wait_until_lease_expired(RunId, Remaining - 20)
    end.

wait_until_dead(_Pid, Remaining) when Remaining =< 0 ->
    {error, timeout};
wait_until_dead(Pid, Remaining) ->
    case is_process_alive(Pid) of
        false -> ok;
        true ->
            timer:sleep(20),
            wait_until_dead(Pid, Remaining - 20)
    end.

stop_registered(Name) ->
    _ = catch gen_server:stop(Name, shutdown, 1000),
    ok.
