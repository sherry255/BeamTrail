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
      fun scanner_requeues_via_active_runner_when_worker_pool_full/0,
      fun worker_supervisor_rejects_when_pool_full/0,
      fun start_workflow_returns_error_for_duplicate_run_id/0,
      fun start_workflow_returns_error_when_runner_supervisor_missing/0,
      fun start_workflow_default_dispatches_supervised_runner_async/0,
      fun await_terminal_returns_terminal_state/0,
      fun await_terminal_times_out_for_nonterminal_run/0,
      fun query_describe_exposes_read_model/0,
      fun query_describe_exposes_decider_metadata/0,
      fun query_describe_exposes_step_results/0,
      fun query_describe_exposes_pending_step_input/0,
      fun query_instance_current_step_matches_reducer/0,
      fun telemetry_counters_track_attempts/0,
      fun postgres_adapter_requires_config/0,
      fun get_state_returns_storage_error/0,
      fun dispatch_returns_storage_error/0,
      fun query_describe_returns_storage_error/0,
      fun query_handles_storage_bootstrap_and_lease_errors/0,
      fun recover_unfinished_returns_storage_error/0,
      fun recovery_skips_runs_with_state_load_errors/0,
      fun recovery_marker_returns_storage_error/0,
      fun recovery_marker_surfaces_append_error/0,
      fun snapshot_write_failure_is_nonfatal/0,
      fun load_state_ignores_obsolete_snapshot_revision/0,
      fun snapshot_schema_contract_pins_revision_to_state_shape/0,
      fun reducer_records_step_results_and_workflow_result/0,
      fun reducer_tracks_decider_metadata/0,
      fun reducer_applies_cancelled_parked_and_resumed/0,
      fun legacy_decider_returns_linear_commands/0,
      fun start_workflow_records_module_decider_metadata/0,
      fun start_workflow_records_custom_decider_version/0,
      fun legacy_created_runs_ignore_later_decider_callback/0,
      fun workflow_decider_validates_commands/0,
      fun workflow_decider_catches_callback_errors/0,
      fun workflow_decider_run_step_uses_command_step_input/0,
      fun workflow_decider_branches_on_prior_step_result/0,
      fun workflow_decider_waits_for_durable_signal/0,
      fun workflow_decider_schedules_timer_then_waits/0,
      fun waiting_timer_becomes_recoverable_when_due/0,
      fun due_timer_fires_before_decider_continues/0,
      fun approval_signal_before_deadline_completes/0,
      fun approval_rejection_before_deadline_fails/0,
      fun approval_deadline_timeout_fails/0,
      fun approval_signal_wins_when_recorded_before_due_timer_fire/0,
      fun terminal_approval_run_ignores_stale_deadline_timer/0,
      fun conflicting_timer_deadline_fails_terminally/0,
      fun fired_timer_id_reuse_fails_terminally/0,
      fun workflow_decider_invalid_command_fails_terminally/0,
      fun workflow_decider_callback_error_fails_terminally/0,
      fun storage_lists_run_ids_with_cursor/0,
      fun storage_append_events_validates_each_event_fence/0,
      fun storage_rejects_expected_seq_conflict/0,
      fun storage_rejects_zombie_append_after_fence_takeover/0,
      fun storage_renews_current_lease_without_changing_fence/0,
      fun storage_refuses_to_renew_expired_lease/0,
      fun storage_release_lease_preserves_fencing/0,
      fun memory_recoverable_index_excludes_parked_runs/0,
      fun dispatch_does_not_complete_cancelled_run/0,
      fun dispatch_does_not_progress_parked_run/0,
      fun cancel_run_appends_terminal_event/0,
      fun cancel_run_stops_local_active_runner/0,
      fun park_run_stops_local_active_runner/0,
      fun park_resume_round_trip/0,
      fun requeue_run_rejects_parked_run/0,
      fun dispatch_refuses_when_run_is_leased/0,
      fun dispatch_refuses_stale_lease_before_replay/0,
      fun dispatch_renews_lease_while_step_runs/0,
      fun scanner_does_not_write_skipped_marker_for_live_lease/0,
      fun scanner_rejects_invalid_interval/0,
      fun active_runner_wakes_retry_without_scanner_tick/0,
      fun active_runner_retry_timer_survives_heartbeat_and_lookup/0,
      fun active_runner_heartbeat_uses_supplied_lease_ttl/0,
      fun active_runner_supervisor_rejects_when_pool_full/0,
      fun active_runner_crash_allows_scanner_takeover_after_lease_expiry/0,
      fun active_runner_stops_step_when_lease_heartbeat_fails/0,
      fun active_runner_records_step_timeout/0,
      fun active_runner_registry_exposes_live_state/0,
      fun query_describe_exposes_active_runner/0,
      fun active_runner_stays_inspectable_while_step_executes/0,
      fun active_runner_stays_inspectable_while_loading_state/0,
      fun active_runner_stays_inspectable_while_finishing_attempt/0,
      fun active_runner_reuses_loaded_state_across_steps/0,
      fun query_describe_exposes_run_control_state/0,
      fun dispatch_refuses_version_mismatch_without_migration/0,
      fun dispatch_refuses_decider_version_mismatch_without_migration/0,
      fun completed_step_version_change_does_not_block_next_step/0,
      fun scanner_skips_migration_blocked_runs/0,
      fun start_workflow_returns_structured_steps_callback_error/0,
      fun start_workflow_returns_structured_decider_version_callback_error/0,
      fun start_workflow_rejects_non_atom_step_ids/0,
      fun step_version_callback_error_fails_terminally/0,
      fun idempotency_key_callback_error_fails_terminally/0,
      fun timeout_callback_error_closes_attempt_terminally/0,
      fun retry_policy_failure_fails_terminally_without_retry_loop/0,
      fun malformed_retry_policy_map_fails_terminally_without_retry_loop/0,
      fun failed_step_decision_is_crash_atomic/0,
      fun open_attempt_recovery_reuses_attempt_budget/0,
      fun legacy_attempt_started_falls_back_to_workflow_input/0,
      fun retry_attempts_preserved_in_chronological_order/0,
      fun workflow_module_preload_accepts_configured_modules/0,
      fun storage_adapter_is_application_configurable/0,
      fun query_describe_exposes_inspector_blocks/0,
      fun recovery_requeued_records_recovered_in_ms/0,
      fun recovery_budget_exceeded_fails_open_attempt/0,
      fun scanner_handles_recovery_budget_failure/0,
      fun repeated_recovery_requeued_keeps_open_attempt_metric/0,
      fun scanner_uses_indexed_recoverable_without_per_run_replay/0,
      fun recover_unfinished_uses_index_without_full_replay/0,
      fun list_recoverable_fills_page_after_filtered_candidates/0,
      fun list_recoverable_matches_recoverable_states/0
     ]}.

setup() ->
    ok = application:unset_env(beamtrail, storage_adapter),
    case whereis(beamtrail_memory_storage) of
        undefined ->
            {ok, Pid} = beamtrail_memory_storage:start_link(),
            unlink(Pid);
        _ -> ok
    end,
    ok = beamtrail_memory_storage:reset(),
    ensure_runner_infra(),
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
    ok = application:unset_env(beamtrail, max_recoveries_per_attempt),
    ok = application:unset_env(beamtrail, workflow_modules),
    ok = application:unset_env(beamtrail, bt_decider_versioned_workflow_version),
    ok = application:unset_env(beamtrail, storage_adapter),
    ok = beamtrail_memory_storage:reset().

workflow_timeout_emits_workflow_failed() ->
    Input = #{order_id => <<"o-wt-1">>},
    {ok, RunId} = beamtrail:start_workflow(bt_workflow_timeout_workflow, Input),
    ok = wait_for_status(RunId, failed, 1000),
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

scanner_requeues_via_active_runner_when_worker_pool_full() ->
    stop_registered(beamtrail_run_registry),
    ok = application:set_env(beamtrail, worker_max_children, 1),
    {ok, WorkerSup} = beamtrail_worker_sup:start_link(),
    unlink(WorkerSup),
    SlowInput = #{order_id => <<"o-worker-busy-1">>, test_pid => self(),
                  sleep_ms => 500},
    {ok, SlowRun} = beamtrail:start_workflow(
                      bt_slow_success_workflow,
                      SlowInput,
                      #{run_id => <<"worker-busy-run-1">>,
                        auto_dispatch => false}),
    ?assertMatch({ok, _}, beamtrail_worker_sup:dispatch_async(SlowRun)),

    RunId = <<"scanner-active-recover-run-1">>,
    Input = #{order_id => <<"o-scanner-active-recover-1">>, test_pid => self()},
    {ok, _} = beamtrail_memory_storage:append_event(
                RunId, 0, undefined,
                'workflow.instance.created', undefined, undefined,
                undefined,
                #{workflow => bt_success_workflow, input => Input,
                  steps => [charge]}),
    {ok, Lease} = beamtrail_memory_storage:acquire_lease(RunId, scanner_seed, 80),
    {ok, _} = beamtrail_memory_storage:append_event(
                RunId, 1, maps:get(fencing_token, Lease),
                'attempt.started', charge, 1,
                {charge, <<"o-scanner-active-recover-1">>}, #{attempt => 1}),
    timer:sleep(100),

    {ok, _Pid} = beamtrail_scanner:start_link(#{interval_ms => infinity,
                                                auto_start => false}),
    ?assertEqual({ok, [RunId]}, beamtrail_scanner:scan_now()),
    ok = wait_for_status(RunId, completed, 1000).

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

start_workflow_returns_error_when_runner_supervisor_missing() ->
    stop_registered(beamtrail_run_registry),
    stop_registered(beamtrail_run_sup),
    RunId = <<"missing-run-sup-run-1">>,
    Input = #{order_id => <<"o-missing-run-sup-1">>},
    ?assertEqual({error, {dispatch_failed, RunId, run_supervisor_not_started}},
                 beamtrail:start_workflow(bt_success_workflow, Input,
                                          #{run_id => RunId})).

start_workflow_default_dispatches_supervised_runner_async() ->
    ensure_runner_infra(),
    RunId = <<"default-async-run-1">>,
    Gate = default_async_gate,
    Parent = self(),
    Input = #{order_id => <<"o-default-async-1">>, test_pid => Parent,
              gate => Gate},
    Caller =
        spawn(fun() ->
                      Parent ! {start_result,
                                beamtrail:start_workflow(
                                  bt_blocking_success_workflow, Input,
                                  #{run_id => RunId})}
              end),
    StartResultBeforeCompletion =
        receive
            {start_result, Result} -> Result
        after 50 ->
            timeout
        end,
    {blocking_started, ExecPid, 1} = receive_exec(),
    {ok, Active} = beamtrail_run_registry:lookup(RunId),
    ?assertEqual(RunId, maps:get(run_id, Active)),
    ?assertEqual(executing, maps:get(status, Active)),
    ExecPid ! {Gate, continue},
    ok = wait_until_dead(Caller, 1000),
    ?assertEqual({ok, RunId}, StartResultBeforeCompletion),
    ok = wait_for_status(RunId, completed, 1000).

await_terminal_returns_terminal_state() ->
    Input = #{order_id => <<"o-await-terminal-1">>, test_pid => self()},
    {ok, RunId} = beamtrail:start_workflow(bt_success_workflow, Input),
    ?assertMatch({ok, #{status := completed, terminal := true}},
                 beamtrail:await_terminal(RunId, 1000)),
    _ = receive_exec(),
    _ = receive_exec().

await_terminal_times_out_for_nonterminal_run() ->
    {ok, RunId} = beamtrail:start_workflow(
                    bt_timeout_workflow,
                    #{order_id => <<"o-await-timeout-1">>},
                    #{run_id => <<"await-timeout-run-1">>,
                      auto_dispatch => false}),
    ?assertEqual({error, timeout}, beamtrail:await_terminal(RunId, 40)).

query_describe_exposes_read_model() ->
    Input = #{order_id => <<"o-q-1">>, test_pid => self()},
    {ok, RunId} = beamtrail:start_workflow(bt_success_workflow, Input),
    _ = receive_exec(), _ = receive_exec(),
    ok = wait_for_status(RunId, completed, 1000),
    Q = beamtrail_query:describe(RunId),
    ?assertEqual(completed, maps:get(status, Q)),
    ?assertEqual(RunId, maps:get(run_id, Q)),
    ?assertMatch(#{run_id := RunId}, maps:get(instance, Q)),
    ?assertMatch([#{status := succeeded}, #{status := succeeded}],
                 maps:get(attempts, Q)),
    ?assertMatch(
       [#{step_id := charge,
          attempt := 1,
          status := succeeded,
          events := [#{event_type := 'activity.scheduled'},
                     #{event_type := 'activity.started'},
                     #{event_type := 'activity.succeeded'}]},
        #{step_id := ship,
          attempt := 1,
          status := succeeded,
          events := [#{event_type := 'activity.scheduled'},
                     #{event_type := 'activity.started'},
                     #{event_type := 'activity.succeeded'}]}],
       maps:get(activities, Q)),
    Snapshot = maps:get(snapshot, Q),
    ?assert(is_map(Snapshot)),
    ?assert(is_list(maps:get(events, Q))),
    ?assertMatch(#{api := <<"beamtrail_query:describe/1">>,
                   run_id := RunId}, maps:get(query, Q)),
    ?assertEqual(false, maps:get(migration_required_for_version_change, Q)).

query_describe_exposes_decider_metadata() ->
    RunId = <<"query-decider-metadata-run-1">>,
    ok = application:set_env(beamtrail,
                             bt_decider_versioned_workflow_version,
                             7),
    {ok, RunId} = beamtrail:start_workflow(
                    bt_decider_versioned_workflow,
                    #{order_id => <<"o-query-decider-metadata-1">>},
                    #{run_id => RunId, auto_dispatch => false}),
    Q = beamtrail_query:describe(RunId),
    ?assertEqual(module, maps:get(decider, Q)),
    ?assertEqual(7, maps:get(decider_version, Q)).

query_describe_exposes_step_results() ->
    Input = #{order_id => <<"o-q-results-1">>, test_pid => self()},
    {ok, RunId} = beamtrail:start_workflow(bt_success_workflow, Input),
    _ = receive_exec(), _ = receive_exec(),
    ok = wait_for_status(RunId, completed, 1000),
    Q = beamtrail_query:describe(RunId),
    ?assertEqual(undefined, maps:get(workflow_result, Q)),
    ?assertMatch(
       [#{step_id := charge,
          attempt := 1,
          event_seq := _,
          result := #{step := charge}},
        #{step_id := ship,
          attempt := 1,
          event_seq := _,
          result := #{step := ship}}],
       maps:get(results, Q)).

query_describe_exposes_pending_step_input() ->
    RunId = <<"query-step-input-run-1">>,
    Gate = query_step_input_gate,
    Input = #{order_id => <<"o-q-step-input-1">>, test_pid => self(),
              gate => Gate},
    {ok, RunId} = beamtrail:start_workflow(bt_blocking_success_workflow, Input,
                                           #{run_id => RunId}),
    {blocking_started, ExecPid, 1} = receive_exec(),
    Q = beamtrail_query:describe(RunId),
    Pending = maps:get(pending_attempt, Q),
    ?assertEqual(Input, maps:get(step_input, Pending)),
    {ok, Events} = beamtrail:events(RunId),
    [Started] = [E || E <- Events, maps:get(event_type, E) =:= 'attempt.started'],
    ?assertEqual(Input, maps:get(step_input, maps:get(payload, Started))),
    ExecPid ! {Gate, continue},
    ok = wait_for_status(RunId, completed, 1000).

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
    {ok, RunId} = beamtrail:start_workflow(bt_success_workflow, Input),
    _ = receive_exec(), _ = receive_exec(),
    ok = wait_for_status(RunId, completed, 1000),
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
                 beamtrail_postgres_storage:list_recoverable_run_ids(undefined, 10, 0)),
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

query_handles_storage_bootstrap_and_lease_errors() ->
    OldTrapExit = process_flag(trap_exit, true),
    ok = stop_registered(beamtrail_memory_storage),
    receive {'EXIT', _Pid, shutdown} -> ok after 0 -> ok end,
    process_flag(trap_exit, OldTrapExit),
    ?assertEqual([], beamtrail_query:list()),
    ?assertEqual(#{}, beamtrail_query:telemetry()),
    ok = application:set_env(beamtrail, storage_adapter,
                             bt_read_error_storage),
    RunId = <<"query-lease-error-run">>,
    {ok, _} = beamtrail_memory_storage:append_event(
                RunId, 0, undefined,
                'workflow.instance.created', undefined, undefined,
                undefined,
                #{workflow => bt_success_workflow, input => #{},
                  steps => []}),
    Q = beamtrail_query:describe(RunId),
    ?assertMatch(#{error := lease_read_failed}, maps:get(lease, Q)).

recover_unfinished_returns_storage_error() ->
    ok = application:set_env(beamtrail, storage_adapter,
                             beamtrail_postgres_storage),
    ?assertEqual({error, postgres_not_configured}, beamtrail:recover_unfinished()).

recovery_skips_runs_with_state_load_errors() ->
    ok = application:set_env(beamtrail, storage_adapter,
                             bt_read_error_storage),
    BadRun = <<"state-read-error-run">>,
    GoodRun = <<"state-read-good-run">>,
    {ok, _} = beamtrail_memory_storage:append_event(
                BadRun, 0, undefined,
                'workflow.instance.created', undefined, undefined,
                undefined,
                #{workflow => bt_success_workflow, input => #{},
                  steps => []}),
    {ok, _} = beamtrail_memory_storage:append_event(
                GoodRun, 0, undefined,
                'workflow.instance.created', undefined, undefined,
                undefined,
                #{workflow => bt_success_workflow, input => #{},
                  steps => []}),
    ?assertEqual([GoodRun], beamtrail:list_recoverable()),
    ?assertMatch({ok, #{run_ids := [GoodRun]}},
                 beamtrail:list_recoverable(undefined, 100)),
    ?assertEqual({ok, [GoodRun]}, beamtrail:recover_unfinished()),
    ?assertEqual({error, state_read_failed},
                 beamtrail:mark_recovery_requeued(BadRun)).

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
    ok = wait_for_status(RunId, completed, 1000),
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
    ?assertEqual(1, maps:get(schema_version, Schema)),
    BaseKeys = maps:keys(beamtrail_reducer:new()),
    RuntimeKeys = [created_at, migration_required_for_version_change],
    ?assertEqual(lists:sort(BaseKeys ++ RuntimeKeys),
                 maps:get(state_keys, Schema)),
    ?assertEqual(beamtrail_reducer:attempt_keys(),
                 maps:get(attempt_keys, Schema)),
    ?assert(beamtrail_state:snapshot_revision() > 0).

reducer_records_step_results_and_workflow_result() ->
    RunId = <<"reducer-results-run-1">>,
    Events =
        [#{run_id => RunId,
           event_seq => 1,
           event_type => 'workflow.instance.created',
           payload => #{workflow => bt_success_workflow,
                        input => #{},
                        steps => [charge]},
           occurred_at => 1},
         #{run_id => RunId,
           event_seq => 2,
           event_type => 'attempt.started',
           step_id => charge,
           step_version => 1,
           idempotency_key => {charge, RunId},
           payload => #{attempt => 1},
           occurred_at => 2},
         #{run_id => RunId,
           event_seq => 3,
           event_type => 'step.succeeded',
           step_id => charge,
           payload => #{result => #{charged => true}},
           occurred_at => 3},
         #{run_id => RunId,
           event_seq => 4,
           event_type => 'workflow.completed',
           payload => #{result => #{ok => true}},
           occurred_at => 4}],
    State = beamtrail_reducer:from_events(Events),
    ?assertEqual([#{step_id => charge,
                    attempt => 1,
                    event_seq => 3,
                    result => #{charged => true}}],
                 maps:get(results, State)),
    ?assertEqual(#{ok => true}, maps:get(workflow_result, State)).

reducer_tracks_decider_metadata() ->
    RunId = <<"reducer-decider-run-1">>,
    State = beamtrail_reducer:from_events(
              [#{run_id => RunId,
                 event_seq => 1,
                 event_type => 'workflow.instance.created',
                 payload => #{workflow => bt_decider_workflow,
                              input => #{},
                              steps => [charge],
                              decider => module,
                              decider_version => 7},
                 occurred_at => 1}]),
    ?assertEqual(module, maps:get(decider, State)),
    ?assertEqual(7, maps:get(decider_version, State)).

reducer_applies_cancelled_parked_and_resumed() ->
    Created =
        #{run_id => <<"control-run">>,
          event_seq => 1,
          event_type => 'workflow.instance.created',
          payload => #{workflow => bt_success_workflow,
                       input => #{},
                       steps => [charge]},
          occurred_at => 1},
    Parked =
        #{run_id => <<"control-run">>,
          event_seq => 2,
          event_type => 'workflow.parked',
          payload => #{reason => maintenance, parked_at => 2},
          occurred_at => 2},
    Resumed =
        #{run_id => <<"control-run">>,
          event_seq => 3,
          event_type => 'workflow.resumed',
          payload => #{resumed_at => 3},
          occurred_at => 3},
    Cancelled =
        #{run_id => <<"control-run">>,
          event_seq => 4,
          event_type => 'workflow.cancelled',
          payload => #{reason => operator_cancel,
                       class => cancelled,
                       cancelled_at => 4},
          occurred_at => 4},

    ParkedState = beamtrail_reducer:from_events([Created, Parked]),
    ?assertMatch(#{status := running,
                   parked := true,
                   parked_reason := maintenance,
                   parked_at := 2,
                   terminal := false},
                 ParkedState),

    ResumedState = beamtrail_reducer:from_events([Created, Parked, Resumed]),
    ?assertMatch(#{status := running,
                   parked := false,
                   parked_reason := undefined,
                   parked_at := undefined,
                   terminal := false},
                 ResumedState),

    CancelledState = beamtrail_reducer:from_events([Created, Cancelled]),
    ?assertMatch(#{status := cancelled,
                   terminal := true,
                   current_step := undefined,
                   pending_attempt := undefined,
                   failure := #{class := cancelled}},
                 CancelledState).

legacy_decider_returns_linear_commands() ->
    RunId = <<"legacy-decider-run-1">>,
    Input = #{order_id => <<"o-legacy-decider-1">>},
    Created =
        #{run_id => RunId,
          event_seq => 1,
          event_type => 'workflow.instance.created',
          payload => #{workflow => bt_success_workflow,
                       input => Input,
                       steps => [charge, ship]},
          occurred_at => 1},
    State0 = beamtrail_reducer:from_events([Created]),
    ?assertEqual({run_step, charge, Input},
                 beamtrail_decider:legacy_decide(State0)),
    ChargeStarted =
        #{run_id => RunId,
          event_seq => 2,
          event_type => 'attempt.started',
          step_id => charge,
          step_version => 1,
          idempotency_key => {charge, <<"o-legacy-decider-1">>},
          payload => #{attempt => 1, step_input => Input},
          occurred_at => 2},
    ChargeSucceeded =
        #{run_id => RunId,
          event_seq => 3,
          event_type => 'step.succeeded',
          step_id => charge,
          step_version => 1,
          idempotency_key => {charge, <<"o-legacy-decider-1">>},
          payload => #{result => charged},
          occurred_at => 3},
    State1 = beamtrail_reducer:from_events([Created, ChargeStarted,
                                            ChargeSucceeded]),
    ?assertEqual({run_step, ship, Input},
                 beamtrail_decider:legacy_decide(State1)),
    ShipStarted =
        #{run_id => RunId,
          event_seq => 4,
          event_type => 'attempt.started',
          step_id => ship,
          step_version => 1,
          idempotency_key => {ship, <<"o-legacy-decider-1">>},
          payload => #{attempt => 1, step_input => Input},
          occurred_at => 4},
    ShipSucceeded =
        #{run_id => RunId,
          event_seq => 5,
          event_type => 'step.succeeded',
          step_id => ship,
          step_version => 1,
          idempotency_key => {ship, <<"o-legacy-decider-1">>},
          payload => #{result => shipped},
          occurred_at => 5},
    State2 = beamtrail_reducer:from_events([Created, ChargeStarted,
                                            ChargeSucceeded, ShipStarted,
                                            ShipSucceeded]),
    ?assertEqual(complete, beamtrail_decider:legacy_decide(State2)).

start_workflow_records_module_decider_metadata() ->
    RunId = <<"decider-metadata-run-1">>,
    Input = #{order_id => <<"o-decider-metadata-1">>,
              command => {run_step, charge}},
    {ok, RunId} = beamtrail:start_workflow(bt_decider_workflow, Input,
                                           #{run_id => RunId,
                                             auto_dispatch => false}),
    {ok, [#{event_type := 'workflow.instance.created',
            payload := Payload}]} = beamtrail:events(RunId),
    ?assertEqual(module, maps:get(decider, Payload)),
    ?assertEqual(1, maps:get(decider_version, Payload)).

start_workflow_records_custom_decider_version() ->
    RunId = <<"decider-versioned-run-1">>,
    ok = application:set_env(beamtrail,
                             bt_decider_versioned_workflow_version,
                             7),
    {ok, RunId} = beamtrail:start_workflow(
                    bt_decider_versioned_workflow,
                    #{order_id => <<"o-decider-versioned-1">>},
                    #{run_id => RunId, auto_dispatch => false}),
    {ok, [#{event_type := 'workflow.instance.created',
            payload := Payload}]} = beamtrail:events(RunId),
    ?assertEqual(module, maps:get(decider, Payload)),
    ?assertEqual(7, maps:get(decider_version, Payload)).

legacy_created_runs_ignore_later_decider_callback() ->
    RunId = <<"legacy-created-decider-run-1">>,
    Input = #{order_id => <<"o-legacy-created-decider-1">>,
              command => {run_step, ship, #{order_id => <<"wrong">>}}},
    State = beamtrail_reducer:from_events(
              [#{run_id => RunId,
                 event_seq => 1,
                 event_type => 'workflow.instance.created',
                 payload => #{workflow => bt_decider_workflow,
                              input => Input,
                              steps => [charge, ship]},
                 occurred_at => 1}]),
    ?assertEqual({ok, {run_step, charge, Input}},
                 beamtrail_decider:decide(State)).

workflow_decider_validates_commands() ->
    State = decider_state({run_step, charge, #{order_id => <<"o-step-input">>}}),
    ?assertEqual({ok, {run_step, charge, #{order_id => <<"o-step-input">>}}},
                 beamtrail_decider:decide(State)),
    ?assertMatch(
       {error, #{reason := invalid_decider_command,
                 command := {run_step, refund, #{}}}},
       beamtrail_decider:decide(decider_state({run_step, refund, #{}}))),
    ?assertEqual({ok, {sleep_until, retry_later, 1000}},
                 beamtrail_decider:decide(
                   decider_state({sleep_until, retry_later, 1000}))),
    ?assertEqual({ok, {sleep, retry_later, 1000}},
                 beamtrail_decider:decide(
                   decider_state({sleep, retry_later, 1000}))),
    ?assertMatch(
       {error, #{reason := invalid_decider_command,
                 command := {sleep, {bad, id}, 1000}}},
       beamtrail_decider:decide(
         decider_state({sleep, {bad, id}, 1000}))),
    ?assertMatch(
       {error, #{reason := invalid_decider_command,
                 command := {sleep_until, retry_later, -1}}},
       beamtrail_decider:decide(
         decider_state({sleep_until, retry_later, -1}))),
    ?assertMatch(
       {error, #{reason := invalid_decider_command,
                 command := <<"bad">>}},
       beamtrail_decider:decide(decider_state(<<"bad">>))).

workflow_decider_catches_callback_errors() ->
    ?assertMatch(
       {error, #{reason := bad_workflow_callback,
                 callback := decide,
                 callback_error := #{class := error, reason := decider_crash}}},
       beamtrail_decider:decide(decider_state(crash))).

workflow_decider_run_step_uses_command_step_input() ->
    RunId = <<"decider-run-step-run-1">>,
    StepInput = #{order_id => <<"o-decider-step-input-1">>,
                  test_pid => self()},
    Input = #{order_id => <<"o-decider-root-1">>,
              command => {run_step, ship, StepInput}},
    {ok, RunId} = beamtrail:start_workflow(bt_decider_workflow, Input,
                                           #{run_id => RunId}),
    ?assertMatch({executed, ship, 1,
                  {ship, <<"o-decider-step-input-1">>},
                  StepInput},
                 receive_exec()),
    {ok, State} = beamtrail:await_terminal(RunId, 1000),
    ?assertEqual(completed, maps:get(status, State)),
    {ok, Events} = beamtrail:events(RunId),
    [Started] = [E || E <- Events, maps:get(event_type, E) =:= 'attempt.started'],
    ?assertEqual(ship, maps:get(step_id, Started)),
    ?assertEqual(StepInput, maps:get(step_input, maps:get(payload, Started))),
    ?assertEqual(
       ['workflow.instance.created',
        'attempt.started',
        'activity.scheduled',
        'activity.started',
        'step.succeeded',
        'activity.succeeded',
        'workflow.completed'],
       [maps:get(event_type, E) || E <- Events]).

workflow_decider_branches_on_prior_step_result() ->
    RunId = <<"decider-result-branch-run-1">>,
    ShipInput = #{order_id => <<"o-decider-result-branch-1">>,
                  address => <<"1 OTP Way">>,
                  carrier => <<"beam-post">>,
                  test_pid => self()},
    Input = #{order_id => <<"o-decider-result-branch-1">>,
              amount => 5000,
              ship_to => <<"1 OTP Way">>,
              test_pid => self()},
    {ok, RunId} = beamtrail:start_workflow(
                    bt_result_branching_workflow,
                    Input,
                    #{run_id => RunId}),
    ?assertEqual({result_branch_executed, charge, Input}, receive_exec()),
    ?assertEqual({result_branch_executed, ship, ShipInput}, receive_exec()),
    {ok, State} = beamtrail:await_terminal(RunId, 1000),
    ?assertMatch(#{status := completed,
                   workflow_result := #{fulfilled := true,
                                        shipment := ShipInput}},
                 State),
    {ok, Events} = beamtrail:events(RunId),
    Started = [E || E <- Events, maps:get(event_type, E) =:= 'attempt.started'],
    ?assertMatch([_, _], Started),
    [ChargeStarted, ShipStarted] = Started,
    ?assertEqual(Input, maps:get(step_input, maps:get(payload, ChargeStarted))),
    ?assertEqual(ShipInput, maps:get(step_input, maps:get(payload, ShipStarted))),
    Q = beamtrail_query:describe(RunId),
    ?assertMatch([#{step_id := charge,
                    result := #{paid := true, shipment := ShipInput}},
                  #{step_id := ship,
                    result := #{shipped := true}}],
                 maps:get(results, Q)).

workflow_decider_waits_for_durable_signal() ->
    RunId = <<"decider-signal-run-1">>,
    Input = #{order_id => <<"o-signal-1">>, test_pid => self()},
    {ok, RunId} = beamtrail:start_workflow(
                    bt_signal_workflow,
                    Input,
                    #{run_id => RunId}),
    {ok, Waiting} = wait_for_state(RunId, waiting, 1000),
    ?assertMatch(#{terminal := false, wait_reason := waiting_for_approval},
                 Waiting),
    {ok, Signalled} = beamtrail:signal_run(
                        RunId,
                        approved,
                        #{approved_by => <<"ops">>}),
    ?assertMatch(#{status := running,
                   signals := [#{name := approved,
                                 payload := #{approved_by := <<"ops">>}}]},
                 Signalled),
    ?assertEqual({signal_workflow_executed,
                  fulfill,
                  #{order_id => <<"o-signal-1">>, approved_by => <<"ops">>,
                    test_pid => self()}},
                 receive_exec()),
    {ok, State} = beamtrail:await_terminal(RunId, 1000),
    ?assertMatch(#{status := completed,
                   workflow_result := #{approved_by := <<"ops">>}},
                 State),
    {ok, Events} = beamtrail:events(RunId),
    ?assertEqual(
       ['workflow.instance.created',
        'workflow.waiting',
        'signal.received',
        'attempt.started',
        'activity.scheduled',
        'activity.started',
        'step.succeeded',
        'activity.succeeded',
        'workflow.completed'],
       [maps:get(event_type, E) || E <- Events]),
    Q = beamtrail_query:describe(RunId),
    ?assertMatch([#{name := approved,
                    payload := #{approved_by := <<"ops">>}}],
                 maps:get(signals, Q)),
    ?assertEqual(waiting_for_approval, maps:get(wait_reason, Q)).

workflow_decider_schedules_timer_then_waits() ->
    RunId = <<"decider-timer-wait-run-1">>,
    Before = erlang:system_time(millisecond),
    Input = #{order_id => <<"o-timer-wait-1">>,
              timer_id => approval_deadline,
              delay_ms => 60000},
    {ok, RunId} = beamtrail:start_workflow(bt_timer_workflow, Input,
                                           #{run_id => RunId}),
    {ok, Waiting} = wait_for_state(RunId, waiting, 1000),
    ?assertMatch(#{terminal := false,
                   wait_reason := waiting_for_timer,
                   next_wake_at := WakeAt,
                   timers := #{approval_deadline :=
                                   #{status := scheduled,
                                     fire_at_ms := WakeAt}}}
                   when is_integer(WakeAt) andalso WakeAt >= Before + 60000,
                 Waiting),
    {ok, Events} = beamtrail:events(RunId),
    ?assertEqual(
       ['workflow.instance.created',
        'timer.scheduled',
        'workflow.waiting'],
       [maps:get(event_type, E) || E <- Events]),
    Q = beamtrail_query:describe(RunId),
    ?assertMatch(#{approval_deadline := #{status := scheduled}},
                 maps:get(timers, Q)),
    ?assertEqual(maps:get(next_wake_at, Waiting), maps:get(next_wake_at, Q)).

waiting_timer_becomes_recoverable_when_due() ->
    Now = erlang:system_time(millisecond),
    seed_waiting_timer(<<"timer-recoverable-future">>, Now + 60000),
    seed_waiting_timer(<<"timer-recoverable-due">>, Now - 1000),
    {ok, #{run_ids := Recoverable}} = beamtrail:list_recoverable(undefined, 100),
    ?assertNot(lists:member(<<"timer-recoverable-future">>, Recoverable)),
    ?assert(lists:member(<<"timer-recoverable-due">>, Recoverable)).

due_timer_fires_before_decider_continues() ->
    {ok, _Pid} = beamtrail_worker_sup:start_link(),
    RunId = <<"decider-timer-fired-run-1">>,
    Input = #{order_id => <<"o-timer-fired-1">>,
              timer_id => approval_deadline,
              fire_at_ms => erlang:system_time(millisecond) - 1},
    {ok, RunId} = beamtrail:start_workflow(bt_timer_workflow, Input,
                                           #{run_id => RunId}),
    {ok, Waiting} = wait_for_state(RunId, waiting, 1000),
    ?assertMatch(#{next_wake_at := WakeAt} when is_integer(WakeAt), Waiting),
    {ok, _Scanner} = beamtrail_scanner:start_link(#{interval_ms => infinity,
                                                    batch_size => 100}),
    {ok, [RunId]} = beamtrail_scanner:scan_now(),
    {ok, State} = beamtrail:await_terminal(RunId, 1000),
    ?assertMatch(#{status := completed,
                   workflow_result := #{timer_id := approval_deadline,
                                        fired := true}},
                 State),
    {ok, Events} = beamtrail:events(RunId),
    ?assertEqual(
       ['workflow.instance.created',
        'timer.scheduled',
        'workflow.waiting',
        'recovery.requeued',
        'timer.fired',
       'workflow.completed'],
       [maps:get(event_type, E) || E <- Events]).

conflicting_timer_deadline_fails_terminally() ->
    RunId = <<"decider-timer-conflict-run-1">>,
    Input = #{order_id => <<"o-timer-conflict-1">>,
              timer_id => approval_deadline,
              fire_at_ms => erlang:system_time(millisecond) + 60000,
              conflict_after_schedule => true},
    {ok, RunId} = beamtrail:start_workflow(bt_timer_workflow, Input,
                                           #{run_id => RunId}),
    {ok, State} = beamtrail:await_terminal(RunId, 1000),
    ?assertMatch(#{status := failed,
                   terminal := true,
                   failure := #{reason := invalid_decider_command,
                                timer_error := conflicting_timer_deadline}},
                 State).

fired_timer_id_reuse_fails_terminally() ->
    {ok, _Pid} = beamtrail_worker_sup:start_link(),
    RunId = <<"decider-timer-reuse-run-1">>,
    Input = #{order_id => <<"o-timer-reuse-1">>,
              timer_id => approval_deadline,
              fire_at_ms => erlang:system_time(millisecond) - 1,
              reuse_after_fired => true},
    {ok, RunId} = beamtrail:start_workflow(bt_timer_workflow, Input,
                                           #{run_id => RunId}),
    {ok, _Waiting} = wait_for_state(RunId, waiting, 1000),
    {ok, _Scanner} = beamtrail_scanner:start_link(#{interval_ms => infinity,
                                                    batch_size => 100}),
    {ok, [RunId]} = beamtrail_scanner:scan_now(),
    {ok, State} = beamtrail:await_terminal(RunId, 1000),
    ?assertMatch(#{status := failed,
                   terminal := true,
                   failure := #{reason := invalid_decider_command,
                                timer_error := timer_already_fired}},
                 State).

approval_signal_before_deadline_completes() ->
    RunId = <<"approval-signal-run-1">>,
    Input = #{order_id => <<"o-approval-signal-1">>,
              deadline_at_ms => erlang:system_time(millisecond) + 60000,
              test_pid => self()},
    {ok, RunId} = beamtrail:start_workflow(bt_approval_deadline_workflow, Input,
                                           #{run_id => RunId}),
    {ok, Waiting} = wait_for_state(RunId, waiting, 1000),
    ?assertMatch(#{wait_reason := waiting_for_approval,
                   timers := #{approval_deadline := #{status := scheduled}}},
                 Waiting),
    {ok, _Signalled} = beamtrail:signal_run(
                         RunId, approved, #{approved_by => <<"ops">>}),
    ?assertEqual({approval_workflow_executed,
                  fulfill,
                  #{order_id => <<"o-approval-signal-1">>,
                    approved_by => <<"ops">>,
                    test_pid => self()}},
                 receive_exec()),
    {ok, State} = beamtrail:await_terminal(RunId, 1000),
    ?assertMatch(#{status := completed,
                   workflow_result := #{fulfilled := true,
                                        approved_by := <<"ops">>}},
                 State),
    {ok, Events} = beamtrail:events(RunId),
    ?assertEqual(
       ['workflow.instance.created',
        'timer.scheduled',
        'workflow.waiting',
        'signal.received',
        'attempt.started',
        'activity.scheduled',
        'activity.started',
        'step.succeeded',
        'activity.succeeded',
        'workflow.completed'],
       [maps:get(event_type, E) || E <- Events]).

approval_rejection_before_deadline_fails() ->
    RunId = <<"approval-rejected-run-1">>,
    Input = #{order_id => <<"o-approval-rejected-1">>,
              deadline_at_ms => erlang:system_time(millisecond) + 60000},
    {ok, RunId} = beamtrail:start_workflow(bt_approval_deadline_workflow, Input,
                                           #{run_id => RunId}),
    {ok, _Waiting} = wait_for_state(RunId, waiting, 1000),
    {ok, _Signalled} = beamtrail:signal_run(
                         RunId, rejected,
                         #{rejected_by => <<"risk">>, note => <<"hold">>}),
    {ok, State} = beamtrail:await_terminal(RunId, 1000),
    ?assertMatch(#{status := failed,
                   terminal := true,
                   failure := #{reason := #{reason := approval_rejected,
                                            rejected_by := <<"risk">>},
                                class := approval_rejected}},
                 State),
    {ok, Events} = beamtrail:events(RunId),
    ?assertEqual(0, length([E || E <- Events,
                            maps:get(event_type, E) =:= 'attempt.started'])),
    ?assertEqual(
       ['workflow.instance.created',
        'timer.scheduled',
        'workflow.waiting',
        'signal.received',
        'workflow.failed'],
       [maps:get(event_type, E) || E <- Events]).

approval_deadline_timeout_fails() ->
    {ok, _Pid} = beamtrail_worker_sup:start_link(),
    RunId = <<"approval-timeout-run-1">>,
    Input = #{order_id => <<"o-approval-timeout-1">>,
              deadline_at_ms => erlang:system_time(millisecond) - 1},
    {ok, RunId} = beamtrail:start_workflow(bt_approval_deadline_workflow, Input,
                                           #{run_id => RunId}),
    {ok, _Waiting} = wait_for_state(RunId, waiting, 1000),
    {ok, _Scanner} = beamtrail_scanner:start_link(#{interval_ms => infinity,
                                                    batch_size => 100}),
    {ok, [RunId]} = beamtrail_scanner:scan_now(),
    {ok, State} = beamtrail:await_terminal(RunId, 1000),
    ?assertMatch(#{status := failed,
                   terminal := true,
                   failure := #{reason := #{reason := approval_timeout,
                                            timer_id := approval_deadline},
                                class := approval_timeout}},
                 State),
    ?assertEqual(timeout, receive_exec_short()),
    {ok, Events} = beamtrail:events(RunId),
    ?assertEqual(
       ['workflow.instance.created',
        'timer.scheduled',
        'workflow.waiting',
        'recovery.requeued',
        'timer.fired',
        'workflow.failed'],
       [maps:get(event_type, E) || E <- Events]).

approval_signal_wins_when_recorded_before_due_timer_fire() ->
    RunId = <<"approval-signal-before-due-fire-run-1">>,
    Input = #{order_id => <<"o-approval-race-1">>,
              deadline_at_ms => erlang:system_time(millisecond) - 1,
              test_pid => self()},
    {ok, RunId} = beamtrail:start_workflow(bt_approval_deadline_workflow, Input,
                                           #{run_id => RunId}),
    {ok, _Waiting} = wait_for_state(RunId, waiting, 1000),
    {ok, _Signalled} = beamtrail:signal_run(
                         RunId, approved, #{approved_by => <<"ops">>}),
    ?assertEqual({approval_workflow_executed,
                  fulfill,
                  #{order_id => <<"o-approval-race-1">>,
                    approved_by => <<"ops">>,
                    test_pid => self()}},
                 receive_exec()),
    {ok, State} = beamtrail:await_terminal(RunId, 1000),
    ?assertMatch(#{status := completed,
                   workflow_result := #{approved_by := <<"ops">>}},
                 State),
    {ok, Events} = beamtrail:events(RunId),
    ?assertEqual(
       ['workflow.instance.created',
        'timer.scheduled',
        'workflow.waiting',
        'signal.received',
        'timer.fired',
        'attempt.started',
        'activity.scheduled',
        'activity.started',
        'step.succeeded',
        'activity.succeeded',
        'workflow.completed'],
       [maps:get(event_type, E) || E <- Events]).

terminal_approval_run_ignores_stale_deadline_timer() ->
    {ok, _Pid} = beamtrail_worker_sup:start_link(),
    RunId = <<"approval-stale-timer-run-1">>,
    Input = #{order_id => <<"o-approval-stale-timer-1">>,
              deadline_at_ms => erlang:system_time(millisecond) + 50,
              test_pid => self()},
    {ok, RunId} = beamtrail:start_workflow(bt_approval_deadline_workflow, Input,
                                           #{run_id => RunId}),
    {ok, _Waiting} = wait_for_state(RunId, waiting, 1000),
    {ok, _Signalled} = beamtrail:signal_run(
                         RunId, approved, #{approved_by => <<"ops">>}),
    ?assertMatch({approval_workflow_executed, fulfill, _}, receive_exec()),
    {ok, _Completed} = beamtrail:await_terminal(RunId, 1000),
    timer:sleep(80),
    {ok, _Scanner} = beamtrail_scanner:start_link(#{interval_ms => infinity,
                                                    batch_size => 100}),
    {ok, []} = beamtrail_scanner:scan_now(),
    ?assertEqual({error, terminal},
                 beamtrail:signal_run(RunId, rejected,
                                      #{rejected_by => <<"late">>})),
    {ok, Events} = beamtrail:events(RunId),
    ?assertEqual(0, length([E || E <- Events,
                            maps:get(event_type, E) =:= 'timer.fired'])).

workflow_decider_invalid_command_fails_terminally() ->
    RunId = <<"decider-invalid-run-1">>,
    Input = #{order_id => <<"o-decider-invalid-1">>,
              command => {run_step, refund, #{}}},
    {ok, RunId} = beamtrail:start_workflow(bt_decider_workflow, Input,
                                           #{run_id => RunId}),
    {ok, State} = beamtrail:await_terminal(RunId, 1000),
    ?assertEqual(failed, maps:get(status, State)),
    ?assertEqual(true, maps:get(terminal, State)),
    ?assertMatch(#{reason := invalid_decider_command,
                   command := {run_step, refund, #{}}},
                 maps:get(failure, State)),
    {ok, Events} = beamtrail:events(RunId),
    ?assertEqual(0, length([E || E <- Events,
                            maps:get(event_type, E) =:= 'attempt.started'])).

workflow_decider_callback_error_fails_terminally() ->
    RunId = <<"decider-crash-run-1">>,
    Input = #{order_id => <<"o-decider-crash-1">>, command => crash},
    {ok, RunId} = beamtrail:start_workflow(bt_decider_workflow, Input,
                                           #{run_id => RunId}),
    {ok, State} = beamtrail:await_terminal(RunId, 1000),
    ?assertEqual(failed, maps:get(status, State)),
    ?assertEqual(true, maps:get(terminal, State)),
    ?assertMatch(#{reason := bad_workflow_callback,
                   callback := decide,
                   callback_error := #{class := error,
                                       reason := decider_crash}},
                 maps:get(failure, State)),
    {ok, Events} = beamtrail:events(RunId),
    ?assertEqual(0, length([E || E <- Events,
                            maps:get(event_type, E) =:= 'attempt.started'])).

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

storage_append_events_validates_each_event_fence() ->
    RunId = <<"batch-fence-run-1">>,
    EventSpecs =
        [#{event_type => 'workflow.instance.created',
           payload => #{workflow => bt_success_workflow, input => #{},
                        steps => [charge]}},
         #{event_type => 'step.failed',
           step_id => charge,
           step_version => 1,
           idempotency_key => {charge, RunId},
           payload => #{reason => transient, class => transient, attempt => 1}}],
    ?assertEqual({error, missing_fence},
                 beamtrail_memory_storage:append_events(RunId, 0, undefined,
                                                        EventSpecs)),
    ?assertEqual({ok, []}, beamtrail_memory_storage:events(RunId)).

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
    {ok, Lease} = beamtrail_memory_storage:acquire_lease(RunId, worker_a, 100),
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

storage_release_lease_preserves_fencing() ->
    RunId = <<"release-lease-run">>,
    {ok, _} = beamtrail:start_workflow(bt_success_workflow, #{},
                                       #{run_id => RunId,
                                         auto_dispatch => false}),
    {ok, Lease1} = beamtrail_memory_storage:acquire_lease(RunId, owner1, 30000),
    Fence1 = maps:get(fencing_token, Lease1),
    ok = beamtrail_memory_storage:release_lease(RunId, Fence1),
    {ok, Lease2} = beamtrail_memory_storage:acquire_lease(RunId, owner2, 30000),
    ?assertEqual(Fence1 + 1, maps:get(fencing_token, Lease2)),
    ?assertEqual({error, stale_fence},
                 beamtrail_memory_storage:release_lease(RunId, Fence1)).

memory_recoverable_index_excludes_parked_runs() ->
    RunId = <<"parked-index-run">>,
    {ok, _} = beamtrail:start_workflow(bt_success_workflow, #{},
                                       #{run_id => RunId,
                                         auto_dispatch => false}),
    {ok, Lease} = beamtrail_memory_storage:acquire_lease(RunId, owner1, 5),
    {ok, State0} = beamtrail_state:load(RunId, beamtrail_memory_storage),
    Parked =
        #{event_type => 'workflow.parked',
          step_id => undefined,
          step_version => undefined,
          idempotency_key => undefined,
          payload => #{reason => maintenance,
                       parked_at => erlang:system_time(millisecond)}},
    {ok, [_]} =
        beamtrail_memory_storage:append_events(
          RunId, maps:get(last_event_seq, State0),
          maps:get(fencing_token, Lease), [Parked]),
    timer:sleep(10),
    {ok, #{run_ids := RunIds}} =
        beamtrail_memory_storage:list_recoverable_run_ids(
          undefined, 100, erlang:system_time(millisecond)),
    ?assertNot(lists:member(RunId, RunIds)).

dispatch_does_not_complete_cancelled_run() ->
    RunId = <<"cancel-dispatch-run">>,
    {ok, _} = beamtrail:start_workflow(bt_success_workflow,
                                       #{order_id => RunId,
                                         test_pid => self()},
                                       #{run_id => RunId,
                                         auto_dispatch => false}),
    {ok, Lease} = beamtrail_memory_storage:acquire_lease(RunId, owner1, 30000),
    {ok, State0} = beamtrail_state:load(RunId, beamtrail_memory_storage),
    Cancelled =
        #{event_type => 'workflow.cancelled',
          step_id => undefined,
          step_version => undefined,
          idempotency_key => undefined,
          payload => #{reason => operator_cancel,
                       class => cancelled,
                       cancelled_at => erlang:system_time(millisecond)}},
    {ok, [_]} =
        beamtrail_memory_storage:append_events(
          RunId, maps:get(last_event_seq, State0),
          maps:get(fencing_token, Lease), [Cancelled]),
    ok = beamtrail_memory_storage:release_lease(
           RunId, maps:get(fencing_token, Lease)),
    {ok, State1} = beamtrail:dispatch(RunId),
    ?assertMatch(#{status := cancelled, terminal := true}, State1),
    ?assertEqual(timeout, receive_exec_short()),
    {ok, Events} = beamtrail:events(RunId),
    ?assertNot(lists:member('workflow.completed',
                            [maps:get(event_type, E) || E <- Events])).

dispatch_does_not_progress_parked_run() ->
    RunId = <<"park-dispatch-run">>,
    {ok, _} = beamtrail:start_workflow(bt_success_workflow,
                                       #{order_id => RunId,
                                         test_pid => self()},
                                       #{run_id => RunId,
                                         auto_dispatch => false}),
    {ok, Lease} = beamtrail_memory_storage:acquire_lease(RunId, owner1, 30000),
    {ok, State0} = beamtrail_state:load(RunId, beamtrail_memory_storage),
    Parked =
        #{event_type => 'workflow.parked',
          step_id => undefined,
          step_version => undefined,
          idempotency_key => undefined,
          payload => #{reason => maintenance,
                       parked_at => erlang:system_time(millisecond)}},
    {ok, [_]} =
        beamtrail_memory_storage:append_events(
          RunId, maps:get(last_event_seq, State0),
          maps:get(fencing_token, Lease), [Parked]),
    ok = beamtrail_memory_storage:release_lease(
           RunId, maps:get(fencing_token, Lease)),
    {ok, State1} = beamtrail:dispatch(RunId),
    ?assertMatch(#{status := running, parked := true, terminal := false},
                 State1),
    ?assertEqual(timeout, receive_exec_short()),
    {ok, Events} = beamtrail:events(RunId),
    ?assertNot(lists:member('attempt.started',
                            [maps:get(event_type, E) || E <- Events])).

cancel_run_appends_terminal_event() ->
    RunId = <<"cancel-api-run">>,
    {ok, _} = beamtrail:start_workflow(bt_success_workflow, #{},
                                       #{run_id => RunId,
                                         auto_dispatch => false}),
    {ok, State} = beamtrail:cancel_run(RunId, operator_cancel),
    ?assertMatch(#{status := cancelled, terminal := true}, State),
    {ok, Events} = beamtrail:events(RunId),
    ?assert(lists:member('workflow.cancelled',
                         [maps:get(event_type, E) || E <- Events])).

cancel_run_stops_local_active_runner() ->
    RunId = <<"cancel-active-run">>,
    Gate = cancel_active_gate,
    Input = #{order_id => <<"o-cancel-active">>, test_pid => self(),
              gate => Gate},
    {ok, RunId} = beamtrail:start_workflow(bt_blocking_success_workflow, Input,
                                           #{run_id => RunId}),
    ?assertMatch({blocking_started, ExecPid, 1} when is_pid(ExecPid),
                 receive_exec()),
    {ok, State} = beamtrail:cancel_run(RunId, operator_cancel),
    ?assertMatch(#{status := cancelled, terminal := true}, State),
    ok = wait_for_status(RunId, cancelled, 1000),
    ?assertEqual(not_found, beamtrail_run_registry:lookup(RunId)),
    ?assertEqual(timeout, receive_exec_short()),
    {ok, Events} = beamtrail:events(RunId),
    Types = [maps:get(event_type, E) || E <- Events],
    ?assert(lists:member('workflow.cancelled', Types)),
    ?assertNot(lists:member('step.succeeded', Types)).

park_run_stops_local_active_runner() ->
    RunId = <<"park-active-run">>,
    Gate = park_active_gate,
    Input = #{order_id => <<"o-park-active">>, test_pid => self(),
              gate => Gate},
    {ok, RunId} = beamtrail:start_workflow(bt_blocking_success_workflow, Input,
                                           #{run_id => RunId}),
    ?assertMatch({blocking_started, ExecPid, 1} when is_pid(ExecPid),
                 receive_exec()),
    {ok, State} = beamtrail:park_run(RunId, maintenance),
    ?assertMatch(#{status := running,
                   parked := true,
                   terminal := false},
                 State),
    timer:sleep(50),
    ?assertEqual(not_found, beamtrail_run_registry:lookup(RunId)),
    ?assertEqual(timeout, receive_exec_short()),
    Parked = beamtrail:get_state(RunId),
    ?assertMatch(#{status := running,
                   parked := true,
                   terminal := false},
                 Parked),
    {ok, Events} = beamtrail:events(RunId),
    Types = [maps:get(event_type, E) || E <- Events],
    ?assert(lists:member('workflow.parked', Types)),
    ?assertNot(lists:member('step.succeeded', Types)).

park_resume_round_trip() ->
    RunId = <<"park-resume-api-run">>,
    {ok, _} = beamtrail:start_workflow(bt_success_workflow, #{},
                                       #{run_id => RunId,
                                         auto_dispatch => false}),
    {ok, Parked} = beamtrail:park_run(RunId, maintenance),
    ?assertMatch(#{parked := true,
                   parked_reason := maintenance,
                   terminal := false},
                 Parked),
    {ok, Resumed} = beamtrail:resume_run(RunId),
    ?assertMatch(#{parked := false, terminal := false}, Resumed).

requeue_run_rejects_parked_run() ->
    RunId = <<"parked-requeue-run">>,
    {ok, _} = beamtrail:start_workflow(bt_success_workflow, #{},
                                       #{run_id => RunId,
                                         auto_dispatch => false}),
    {ok, _} = beamtrail:park_run(RunId, maintenance),
    ?assertEqual({error, parked}, beamtrail:requeue_run(RunId, manual)).

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
              sleep_ms => 220},
    {ok, RunId} = beamtrail:start_workflow(bt_slow_success_workflow, Input,
                                           #{auto_dispatch => false}),
    {ok, Lease} = beamtrail_memory_storage:acquire_lease(RunId, slow_worker, 100),
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
    ensure_runner_infra(),
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
    ensure_runner_infra(),
    Input = #{order_id => <<"o-active-retry-heartbeat-1">>, test_pid => self()},
    {ok, RunId} = beamtrail:start_workflow(bt_retry_heartbeat_backoff_workflow, Input,
                                           #{run_id => <<"active-retry-heartbeat-run-1">>,
                                             auto_dispatch => false}),
    ?assertMatch({ok, _Pid}, beamtrail_run_sup:dispatch(RunId)),
    ?assertMatch({retry_backoff_execute, 1, {charge, <<"o-active-retry-heartbeat-1">>}},
                 receive_exec()),
    ok = wait_for_status(RunId, retrying, 1000),
    ok = wait_for_runner_status(RunId, waiting_retry, 1000),
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

active_runner_heartbeat_uses_supplied_lease_ttl() ->
    ensure_runner_infra(),
    Input = #{order_id => <<"o-active-short-lease-1">>, test_pid => self(),
              sleep_ms => 160},
    {ok, RunId} = beamtrail:start_workflow(bt_slow_success_workflow, Input,
                                           #{run_id => <<"active-short-lease-run-1">>,
                                             auto_dispatch => false}),
    {ok, Lease} = beamtrail_memory_storage:acquire_lease(RunId, short_owner, 60),
    ?assertMatch({ok, _Pid}, beamtrail_run_sup:dispatch(RunId, Lease)),
    ?assertMatch({slow_executed, slow}, receive_exec()),
    ok = wait_for_status(RunId, completed, 1000),
    {ok, CurrentLease} = beamtrail_memory_storage:read_lease(RunId),
    ?assert(maps:get(lease_until, CurrentLease) > maps:get(lease_until, Lease)),
    ?assert(beamtrail_lease_manager:ttl_ms(CurrentLease) =< 200).

active_runner_supervisor_rejects_when_pool_full() ->
    ok = application:set_env(beamtrail, run_max_children, 1),
    ensure_runner_infra(),
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
    ensure_runner_infra(),
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
    ensure_runner_infra(),
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
    ensure_runner_infra(),
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
    ensure_runner_infra(),
    Input = #{order_id => <<"o-active-inspect-1">>, test_pid => self()},
    {ok, RunId} = beamtrail:start_workflow(bt_retry_long_backoff_workflow, Input,
                                           #{run_id => <<"active-inspect-run-1">>,
                                             auto_dispatch => false}),
    {ok, Pid} = beamtrail_run_sup:dispatch(RunId),
    ?assertMatch({retry_backoff_execute, 1, {charge, <<"o-active-inspect-1">>}},
                 receive_exec()),
    ok = wait_for_status(RunId, retrying, 1000),
    ok = wait_for_runner_status(RunId, waiting_retry, 1000),
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
    ensure_runner_infra(),
    Input = #{order_id => <<"o-query-active-1">>, test_pid => self()},
    {ok, RunId} = beamtrail:start_workflow(bt_retry_long_backoff_workflow, Input,
                                           #{run_id => <<"query-active-run-1">>,
                                             auto_dispatch => false}),
    {ok, Pid} = beamtrail_run_sup:dispatch(RunId),
    ?assertMatch({retry_backoff_execute, 1, {charge, <<"o-query-active-1">>}},
                 receive_exec()),
    ok = wait_for_status(RunId, retrying, 1000),
    ok = wait_for_runner_status(RunId, waiting_retry, 1000),
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
    ensure_runner_infra(),
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

active_runner_stays_inspectable_while_loading_state() ->
    ok = application:set_env(beamtrail, storage_adapter, bt_blocking_storage),
    ok = bt_blocking_storage:reset(),
    ensure_runner_infra(),
    Input = #{order_id => <<"o-active-loading-1">>, test_pid => self()},
    {ok, RunId} = beamtrail:start_workflow(bt_success_workflow, Input,
                                           #{run_id => <<"active-loading-run-1">>,
                                             auto_dispatch => false}),
    ok = bt_blocking_storage:block_read_snapshot(self()),
    {ok, Pid} = beamtrail_run_sup:dispatch(RunId),
    BlockedPid =
        receive
            {bt_blocking_storage, read_snapshot_blocked, P} -> P
        after 1000 ->
            error(read_snapshot_not_blocked)
        end,
    StartedAt = erlang:monotonic_time(millisecond),
    {ok, Active} = beamtrail_run_registry:lookup(RunId),
    ?assert(erlang:monotonic_time(millisecond) - StartedAt < 200),
    ?assertMatch(#{run_id := RunId,
                   pid := Pid,
                   status := loading,
                   storage_op := load_state},
                 Active),
    BlockedPid ! {bt_blocking_storage, continue},
    ?assertMatch({executed, charge, 1, {charge, <<"o-active-loading-1">>}},
                 receive_exec()),
    ?assertMatch({executed, ship, 1, {ship, <<"o-active-loading-1">>}},
                 receive_exec()),
    ?assertMatch({ok, #{status := completed}}, wait_for_state(RunId, completed, 1000)).

active_runner_stays_inspectable_while_finishing_attempt() ->
    ok = application:set_env(beamtrail, storage_adapter, bt_blocking_storage),
    ok = bt_blocking_storage:reset(),
    ensure_runner_infra(),
    Gate = active_finishing_gate,
    Input = #{order_id => <<"o-active-finishing-1">>, test_pid => self(),
              gate => Gate},
    {ok, RunId} = beamtrail:start_workflow(bt_blocking_success_workflow, Input,
                                           #{run_id => <<"active-finishing-run-1">>,
                                             auto_dispatch => false}),
    {ok, Pid} = beamtrail_run_sup:dispatch(RunId),
    {blocking_started, ExecPid, 1} = receive_exec(),
    ok = bt_blocking_storage:block_append_event(self()),
    ExecPid ! {Gate, continue},
    BlockedPid =
        receive
            {bt_blocking_storage, append_event_blocked, P} -> P
        after 1000 ->
            error(append_event_not_blocked)
        end,
    StartedAt = erlang:monotonic_time(millisecond),
    {ok, Active} = beamtrail_run_registry:lookup(RunId),
    ?assert(erlang:monotonic_time(millisecond) - StartedAt < 200),
    ?assertMatch(#{run_id := RunId,
                   pid := Pid,
                   status := finishing,
                   storage_op := finish_attempt},
                 Active),
    BlockedPid ! {bt_blocking_storage, continue},
    ?assertMatch({ok, #{status := completed}}, wait_for_state(RunId, completed, 1000)).

active_runner_reuses_loaded_state_across_steps() ->
    ok = application:set_env(beamtrail, storage_adapter, bt_counting_storage),
    bt_counting_storage:reset_counts(),
    ensure_runner_infra(),
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

query_describe_exposes_run_control_state() ->
    ParkedRun = <<"query-control-parked-run">>,
    {ok, ParkedRun} = beamtrail:start_workflow(bt_success_workflow, #{},
                                               #{run_id => ParkedRun,
                                                 auto_dispatch => false}),
    {ok, _Parked} = beamtrail:park_run(ParkedRun, maintenance),
    Q1 = beamtrail_query:describe(ParkedRun),
    ?assertMatch(#{parked := true,
                   parked_reason := maintenance,
                   terminal := false},
                 Q1),
    ?assertMatch(#{parked := true,
                   parked_reason := maintenance,
                   latest_event := 'workflow.parked',
                   dispatch_allowed := false},
                 maps:get(control, Q1)),
    {ok, _Resumed} = beamtrail:resume_run(ParkedRun),
    Q2 = beamtrail_query:describe(ParkedRun),
    ?assertMatch(#{parked := false,
                   terminal := false},
                 Q2),
    ?assertMatch(#{parked := false,
                   latest_event := 'workflow.resumed',
                   dispatch_allowed := true},
                 maps:get(control, Q2)),

    CancelledRun = <<"query-control-cancelled-run">>,
    {ok, CancelledRun} = beamtrail:start_workflow(bt_success_workflow, #{},
                                                  #{run_id => CancelledRun,
                                                    auto_dispatch => false}),
    {ok, _Cancelled} = beamtrail:cancel_run(CancelledRun, operator_cancel),
    Q3 = beamtrail_query:describe(CancelledRun),
    ?assertMatch(#{status := cancelled,
                   terminal := true},
                 Q3),
    ?assertMatch(#{cancelled := true,
                   latest_event := 'workflow.cancelled',
                   dispatch_allowed := false},
                 maps:get(control, Q3)).

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

dispatch_refuses_decider_version_mismatch_without_migration() ->
    RunId = <<"decider-version-gate-run-1">>,
    Input = #{order_id => <<"o-decider-version-gate-1">>,
              test_pid => self()},
    {ok, _} = beamtrail_memory_storage:append_event(
                RunId, 0, undefined,
                'workflow.instance.created', undefined, undefined,
                undefined,
                #{workflow => bt_decider_versioned_workflow,
                  input => Input,
                  steps => [charge],
                  decider => module,
                  decider_version => 1}),
    ok = application:set_env(beamtrail,
                             bt_decider_versioned_workflow_version,
                             2),
    ?assertMatch({error, {migration_required, _}}, beamtrail:dispatch(RunId)),
    ?assertEqual(timeout, receive_exec_short()),
    State = beamtrail:get_state(RunId),
    ?assertEqual(true, maps:get(migration_required_for_version_change, State)),
    {ok, Events} = beamtrail:events(RunId),
    ?assertEqual(0, length([E || E <- Events,
                            maps:get(event_type, E) =:= 'attempt.started'])).

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

start_workflow_returns_structured_steps_callback_error() ->
    RunId = <<"bad-steps-run-1">>,
    ?assertMatch({error,
                  {create_failed, RunId,
                   {bad_workflow_callback, steps, #{class := error}}}},
                 beamtrail:start_workflow(bt_bad_steps_workflow,
                                          #{order_id => <<"o-bad-steps-1">>},
                                          #{run_id => RunId})),
    ?assertEqual({ok, []}, beamtrail:events(RunId)).

start_workflow_returns_structured_decider_version_callback_error() ->
    RunId = <<"bad-decider-version-run-1">>,
    ?assertMatch({error,
                  {create_failed, RunId,
                   {bad_workflow_callback, decider_version,
                    #{callback := decider_version,
                      class := error,
                      reason := bad_decider_version_crash}}}},
                 beamtrail:start_workflow(
                   bt_bad_decider_version_workflow,
                   #{order_id => <<"o-bad-decider-version-1">>},
                   #{run_id => RunId})),
    ?assertEqual({ok, []}, beamtrail:events(RunId)).

start_workflow_rejects_non_atom_step_ids() ->
    RunId = <<"bad-step-id-run-1">>,
    ?assertMatch({error,
                  {create_failed, RunId,
                   {bad_workflow_steps, #{class := bad_step_id,
                                          step := <<"charge">>}}}},
                 beamtrail:start_workflow(bt_bad_step_id_workflow,
                                          #{order_id => <<"o-bad-step-id-1">>},
                                          #{run_id => RunId})),
    ?assertEqual({ok, []}, beamtrail:events(RunId)).

step_version_callback_error_fails_terminally() ->
    RunId = <<"bad-step-version-run-1">>,
    {ok, RunId} = beamtrail:start_workflow(
                    bt_bad_step_version_workflow,
                    #{order_id => <<"o-bad-step-version-1">>},
                    #{run_id => RunId}),
    {ok, State} = beamtrail:await_terminal(RunId, 1000),
    ?assertEqual(failed, maps:get(status, State)),
    ?assertEqual(true, maps:get(terminal, State)),
    Failure = maps:get(failure, State),
    ?assertMatch(#{reason := bad_workflow_callback,
                   callback := step_version,
                   callback_error := #{class := error}},
                 Failure).

idempotency_key_callback_error_fails_terminally() ->
    RunId = <<"bad-idempotency-run-1">>,
    {ok, RunId} = beamtrail:start_workflow(
                    bt_bad_idempotency_workflow,
                    #{order_id => <<"o-bad-idempotency-1">>},
                    #{run_id => RunId}),
    {ok, State} = beamtrail:await_terminal(RunId, 1000),
    ?assertEqual(failed, maps:get(status, State)),
    ?assertEqual(true, maps:get(terminal, State)),
    ?assertMatch(#{reason := bad_workflow_callback,
                   callback := idempotency_key,
                   callback_error := #{class := error}},
                 maps:get(failure, State)),
    {ok, Events} = beamtrail:events(RunId),
    ?assertEqual(0, length([E || E <- Events,
                            maps:get(event_type, E) =:= 'attempt.started'])).

timeout_callback_error_closes_attempt_terminally() ->
    RunId = <<"bad-timeout-run-1">>,
    {ok, RunId} = beamtrail:start_workflow(
                    bt_bad_timeout_workflow,
                    #{order_id => <<"o-bad-timeout-1">>},
                    #{run_id => RunId}),
    {ok, State} = beamtrail:await_terminal(RunId, 1000),
    ?assertEqual(failed, maps:get(status, State)),
    ?assertEqual(true, maps:get(terminal, State)),
    ?assertMatch(#{reason := bad_workflow_callback,
                   callback := timeout_ms,
                   callback_error := #{class := error}},
                 maps:get(failure, State)),
    {ok, Events} = beamtrail:events(RunId),
    ?assertEqual(1, length([E || E <- Events,
                            maps:get(event_type, E) =:= 'attempt.started'])),
    ?assertEqual(1, length([E || E <- Events,
                            maps:get(event_type, E) =:= 'step.failed'])).

retry_policy_failure_fails_terminally_without_retry_loop() ->
    RunId = <<"bad-retry-policy-run-1">>,
    Input = #{order_id => <<"o-bad-retry-policy-1">>, test_pid => self()},
    ?assertEqual({ok, RunId},
                 beamtrail:start_workflow(bt_bad_retry_policy_workflow, Input,
                                          #{run_id => RunId})),
    ?assertEqual({bad_retry_execute, 1}, receive_exec()),
    {ok, State} = beamtrail:await_terminal(RunId, 1000),
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

malformed_retry_policy_map_fails_terminally_without_retry_loop() ->
    RunId = <<"malformed-retry-policy-run-1">>,
    Input = #{order_id => <<"o-malformed-retry-policy-1">>, test_pid => self()},
    ?assertEqual({ok, RunId},
                 beamtrail:start_workflow(bt_malformed_retry_policy_workflow,
                                          Input, #{run_id => RunId})),
    ?assertEqual({malformed_retry_execute, 1}, receive_exec()),
    {ok, State} = beamtrail:await_terminal(RunId, 1000),
    ?assertEqual(failed, maps:get(status, State)),
    ?assertEqual(true, maps:get(terminal, State)),
    Failure = maps:get(failure, State),
    ?assertMatch(#{retry_policy_error :=
                       #{callback := retry_policy,
                         reason := {bad_policy, _}}},
                 Failure),
    {ok, Events} = beamtrail:events(RunId),
    ?assertEqual(1, length([E || E <- Events,
                            maps:get(event_type, E) =:= 'attempt.started'])),
    ?assertEqual(0, length([E || E <- Events,
                            maps:get(event_type, E) =:= 'retry.scheduled'])),
    ?assertEqual({ok, []}, beamtrail:recover_unfinished()),
    {ok, EventsAfterRecovery} = beamtrail:events(RunId),
    ?assertEqual(length(Events), length(EventsAfterRecovery)).

failed_step_decision_is_crash_atomic() ->
    RunId = <<"atomic-failure-run-1">>,
    ok = application:set_env(beamtrail, storage_adapter,
                             bt_decision_append_fail_storage),
    ok = application:set_env(beamtrail, lease_ttl_ms, 20),
    Input = #{order_id => <<"o-atomic-failure-1">>, test_pid => self()},
    ?assertEqual({ok, RunId},
                 beamtrail:start_workflow(bt_single_fail_workflow, Input,
                                          #{run_id => RunId})),
    ?assertEqual({single_fail_execute, 1}, receive_exec()),
    ?assertEqual(timeout, receive_exec_short()),
    {ok, Events} = beamtrail:events(RunId),
    ?assertEqual(1, length([E || E <- Events,
                            maps:get(event_type, E) =:= 'attempt.started'])),
    ?assertEqual(1, length([E || E <- Events,
                            maps:get(event_type, E) =:= 'step.failed'])),
    ?assertEqual(1, length([E || E <- Events,
                            maps:get(event_type, E) =:= 'workflow.failed'])),
    ?assertEqual({ok, []}, beamtrail:recover_unfinished()),
    ?assertEqual(terminal, terminal_status(beamtrail:get_state(RunId))).

open_attempt_recovery_reuses_attempt_budget() ->
    RunId = <<"open-attempt-budget-run-1">>,
    Input = #{order_id => <<"o-open-attempt-budget-1">>, test_pid => self()},
    {ok, _} = beamtrail_memory_storage:append_event(
                RunId, 0, undefined,
                'workflow.instance.created', undefined, undefined,
                undefined,
                #{workflow => bt_single_fail_workflow, input => Input,
                  steps => [charge]}),
    {ok, Lease} = beamtrail_memory_storage:acquire_lease(RunId, stale_worker, 20),
    {ok, _} = beamtrail_memory_storage:append_event(
                RunId, 1, maps:get(fencing_token, Lease),
                'attempt.started', charge, 1,
                {charge, <<"o-open-attempt-budget-1">>}, #{attempt => 1}),
    ok = wait_until_lease_expired(RunId, 1000),
    ?assertMatch({ok, [_]}, beamtrail:recover_unfinished()),
    ?assertEqual({single_fail_execute, 1}, receive_exec()),
    {ok, Events} = beamtrail:events(RunId),
    ?assertEqual(1, length([E || E <- Events,
                            maps:get(event_type, E) =:= 'attempt.started'])),
    State = beamtrail:get_state(RunId),
    ?assertEqual(1, maps:get(charge, maps:get(attempt_counts, State))).

legacy_attempt_started_falls_back_to_workflow_input() ->
    RunId = <<"legacy-step-input-run-1">>,
    Input = #{order_id => <<"o-legacy-step-input-1">>, test_pid => self()},
    {ok, _} = beamtrail_memory_storage:append_event(
                RunId, 0, undefined,
                'workflow.instance.created', undefined, undefined,
                undefined,
                #{workflow => bt_success_workflow, input => Input,
                  steps => [charge]}),
    {ok, Lease} = beamtrail_memory_storage:acquire_lease(RunId, stale_worker, 20),
    {ok, _} = beamtrail_memory_storage:append_event(
                RunId, 1, maps:get(fencing_token, Lease),
                'attempt.started', charge, 1,
                {charge, <<"o-legacy-step-input-1">>}, #{attempt => 1}),
    State = beamtrail:get_state(RunId),
    ?assertEqual(Input, maps:get(step_input, maps:get(pending_attempt, State))),
    ok = wait_until_lease_expired(RunId, 1000),
    ?assertMatch({ok, [_]}, beamtrail:recover_unfinished()),
    ?assertMatch({executed, charge, 1, {charge, <<"o-legacy-step-input-1">>}},
                 receive_exec()).

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

workflow_module_preload_accepts_configured_modules() ->
    ok = application:set_env(beamtrail, workflow_modules,
                             [<<"bt_success_workflow">>,
                              bt_retry_workflow,
                              "bt_timeout_workflow"]),
    ?assertEqual(ok, beamtrail_config:preload_workflows()).

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
    ok = wait_for_status(RunId, completed, 1000),
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

recovery_budget_exceeded_fails_open_attempt() ->
    ok = application:set_env(beamtrail, lease_ttl_ms, 20),
    ok = application:set_env(beamtrail, max_recoveries_per_attempt, 2),
    RunId = <<"recovery-budget-run-1">>,
    Input = #{order_id => <<"o-recovery-budget-1">>},
    {ok, _} = beamtrail_memory_storage:append_event(
                RunId, 0, undefined,
                'workflow.instance.created', undefined, undefined,
                undefined,
                #{workflow => bt_success_workflow, input => Input,
                  steps => [charge]}),
    {ok, Lease} = beamtrail_memory_storage:acquire_lease(RunId, recovery_seed, 10),
    {ok, _} = beamtrail_memory_storage:append_event(
                RunId, 1, maps:get(fencing_token, Lease),
                'attempt.started', charge, 1,
                {charge, <<"o-recovery-budget-1">>}, #{attempt => 1}),
    ok = wait_until_lease_expired(RunId, 1000),
    {ok, requeued} = beamtrail:mark_recovery_requeued(RunId),
    ok = wait_until_lease_expired(RunId, 1000),
    {ok, requeued} = beamtrail:mark_recovery_requeued(RunId),
    ok = wait_until_lease_expired(RunId, 1000),
    ?assertEqual({ok, failed}, beamtrail:mark_recovery_requeued(RunId)),
    State = beamtrail:get_state(RunId),
    ?assertEqual(failed, maps:get(status, State)),
    ?assertEqual(true, maps:get(terminal, State)),
    Failure = maps:get(failure, State),
    ?assertMatch(#{reason := recovery_budget_exceeded,
                   recoveries := 2,
                   max_recoveries := 2},
                 Failure),
    {ok, Events} = beamtrail:events(RunId),
    ?assertEqual(2, length([E || E <- Events,
                            maps:get(event_type, E) =:= 'recovery.requeued'])),
    ?assertEqual(1, length([E || E <- Events,
                            maps:get(event_type, E) =:= 'workflow.failed'])),
    ?assertEqual({ok, []}, beamtrail:recover_unfinished()).

scanner_handles_recovery_budget_failure() ->
    ok = application:set_env(beamtrail, lease_ttl_ms, 20),
    ok = application:set_env(beamtrail, max_recoveries_per_attempt, 1),
    RunId = <<"scanner-recovery-budget-run-1">>,
    Input = #{order_id => <<"o-scanner-recovery-budget-1">>},
    {ok, _} = beamtrail_memory_storage:append_event(
                RunId, 0, undefined,
                'workflow.instance.created', undefined, undefined,
                undefined,
                #{workflow => bt_success_workflow, input => Input,
                  steps => [charge]}),
    {ok, Lease} = beamtrail_memory_storage:acquire_lease(RunId, recovery_seed, 10),
    {ok, _} = beamtrail_memory_storage:append_event(
                RunId, 1, maps:get(fencing_token, Lease),
                'attempt.started', charge, 1,
                {charge, <<"o-scanner-recovery-budget-1">>}, #{attempt => 1}),
    ok = wait_until_lease_expired(RunId, 1000),
    {ok, requeued} = beamtrail:mark_recovery_requeued(RunId),
    ok = wait_until_lease_expired(RunId, 1000),
    ?assertEqual([RunId], beamtrail:list_recoverable()),
    {ok, _Pid} = beamtrail_scanner:start_link(#{interval_ms => infinity,
                                                auto_start => false}),
    ?assertEqual({ok, []}, beamtrail_scanner:scan_now()),
    State = beamtrail:get_state(RunId),
    ?assertEqual(failed, maps:get(status, State)),
    ?assertEqual(true, maps:get(terminal, State)),
    ?assertMatch(#{reason := recovery_budget_exceeded},
                 maps:get(failure, State)).

repeated_recovery_requeued_keeps_open_attempt_metric() ->
    ok = application:set_env(beamtrail, lease_ttl_ms, 50),
    RunId = <<"recov-in-ms-repeat-1">>,
    Input = #{order_id => <<"o-rec-repeat-1">>},
    {ok, _} = beamtrail_memory_storage:append_event(
                RunId, 0, undefined,
                'workflow.instance.created', undefined, undefined,
                undefined,
                #{workflow => bt_versioned_workflow, input => Input,
                  steps => [charge]}),
    {ok, Lease} = beamtrail_memory_storage:acquire_lease(RunId, recovery_seed, 30),
    {ok, _} = beamtrail_memory_storage:append_event(
                RunId, 1, maps:get(fencing_token, Lease),
                'attempt.started', charge, 1,
                {charge, <<"o-rec-repeat-1">>}, #{attempt => 1}),
    ok = wait_until_lease_expired(RunId, 1000),
    {ok, requeued} = beamtrail:mark_recovery_requeued(RunId),
    ok = wait_until_lease_expired(RunId, 1000),
    {ok, requeued} = beamtrail:mark_recovery_requeued(RunId),
    {ok, Events} = beamtrail:events(RunId),
    [First, Second] = [E || E <- Events,
                            maps:get(event_type, E) =:= 'recovery.requeued'],
    FirstRecovered = maps:get(recovered_in_ms, maps:get(payload, First)),
    SecondRecovered = maps:get(recovered_in_ms, maps:get(payload, Second)),
    ?assert(is_integer(FirstRecovered)),
    ?assert(is_integer(SecondRecovered)),
    ?assert(SecondRecovered >= FirstRecovered).

scanner_uses_indexed_recoverable_without_per_run_replay() ->
    %% With every run already terminal, the scanner must not load
    %% (snapshot/replay) any run state to discover there is nothing to recover.
    ok = application:set_env(beamtrail, storage_adapter, bt_counting_storage),
    Complete =
        fun(N) ->
                OrderId = iolist_to_binary(io_lib:format("o-idx-~p", [N])),
                Input = #{order_id => OrderId, test_pid => self()},
                {ok, RunId} = beamtrail:start_workflow(bt_success_workflow, Input),
                _ = receive_exec(), _ = receive_exec(),
                ok = wait_for_status(RunId, completed, 1000),
                ?assertEqual(completed,
                             maps:get(status, beamtrail:get_state(RunId))),
                RunId
        end,
    _ = [Complete(N) || N <- [1, 2, 3]],
    bt_counting_storage:reset_counts(),
    {ok, _Pid} = beamtrail_scanner:start_link(#{interval_ms => infinity,
                                                auto_start => false}),
    ?assertEqual({ok, []}, beamtrail_scanner:scan_now()),
    Counts = bt_counting_storage:counts(),
    ?assertEqual(0, maps:get(read_events, Counts, 0)),
    ?assertEqual(0, maps:get(events, Counts, 0)),
    ?assertEqual(0, maps:get(read_snapshot, Counts, 0)).

recover_unfinished_uses_index_without_full_replay() ->
    %% Manual recovery should use the same indexed path as the scanner instead
    %% of falling back to the old full list_run_ids + per-run replay sweep.
    ok = application:set_env(beamtrail, storage_adapter, bt_counting_storage),
    Complete =
        fun(N) ->
                OrderId = iolist_to_binary(io_lib:format("o-rec-idx-~p", [N])),
                Input = #{order_id => OrderId, test_pid => self()},
                {ok, RunId} = beamtrail:start_workflow(bt_success_workflow, Input),
                _ = receive_exec(), _ = receive_exec(),
                ok = wait_for_status(RunId, completed, 1000),
                ?assertEqual(completed,
                             maps:get(status, beamtrail:get_state(RunId))),
                RunId
        end,
    _ = [Complete(N) || N <- [1, 2, 3]],
    bt_counting_storage:reset_counts(),
    ?assertEqual({ok, []}, beamtrail:recover_unfinished()),
    Counts = bt_counting_storage:counts(),
    ?assertEqual(0, maps:get(read_events, Counts, 0)),
    ?assertEqual(0, maps:get(events, Counts, 0)),
    ?assertEqual(0, maps:get(read_snapshot, Counts, 0)).

list_recoverable_fills_page_after_filtered_candidates() ->
    %% The storage index returns coarse candidates. A migration-blocked run can
    %% pass the indexed filter and then be rejected by recoverable/2. Public
    %% pagination should still fill the final recoverable page before returning.
    seed_migration_blocked(<<"p-fill-01-migration">>),
    seed_created(<<"p-fill-02-running">>, bt_success_workflow, [charge]),
    seed_created(<<"p-fill-03-running">>, bt_success_workflow, [charge]),
    {ok, #{run_ids := FirstPage,
           has_more := true,
           next_cursor := Cursor1}} = beamtrail:list_recoverable(undefined, 1),
    ?assertEqual([<<"p-fill-02-running">>], FirstPage),
    {ok, #{run_ids := SecondPage,
           has_more := false}} = beamtrail:list_recoverable(Cursor1, 1),
    ?assertEqual([<<"p-fill-03-running">>], SecondPage).

list_recoverable_matches_recoverable_states() ->
    %% The indexed candidate query is a coarse superset filtered by the precise
    %% recoverable/2 check (including the live-code migration gate). The final
    %% set must equal the genuinely recoverable runs across every reachable
    %% (status, terminal, next_retry_at, lease, migration) combination.
    Now = erlang:system_time(millisecond),
    %% completed -> terminal -> not recoverable
    seed_created(<<"p-completed">>, bt_success_workflow, []),
    {ok, CompletedLease} =
        beamtrail_memory_storage:acquire_lease(<<"p-completed">>, seed, 1000),
    {ok, _} = beamtrail_memory_storage:append_event(
                <<"p-completed">>, 1, maps:get(fencing_token, CompletedLease),
                'workflow.completed', undefined, undefined, undefined, #{}),
    %% running, no lease -> recoverable
    seed_created(<<"p-running-nolease">>, bt_success_workflow, [charge]),
    %% running, live lease -> not recoverable (owned)
    seed_created(<<"p-running-livelease">>, bt_success_workflow, [charge]),
    {ok, _} = beamtrail_memory_storage:acquire_lease(
                <<"p-running-livelease">>, live_owner, 30000),
    %% retrying, next_retry in the future -> not recoverable yet
    seed_retrying(<<"p-retry-future">>, Now + 60000),
    %% retrying, next_retry in the past, lease expired -> recoverable
    seed_retrying(<<"p-retry-due">>, Now - 1000),
    ok = wait_until_lease_expired(<<"p-retry-due">>, 1000),
    %% terminal failure -> not recoverable
    seed_terminal_failed(<<"p-terminal-failed">>),
    %% open attempt whose step_version diverges from deployed code -> migration
    %% blocked. It passes the coarse projection filter but recoverable/2 rejects.
    seed_created(<<"p-migration">>, bt_versioned_workflow, [charge]),
    {ok, MigLease} =
        beamtrail_memory_storage:acquire_lease(<<"p-migration">>, mig_owner, 20),
    {ok, _} = beamtrail_memory_storage:append_event(
                <<"p-migration">>, 1, maps:get(fencing_token, MigLease),
                'attempt.started', charge, 1, {charge, <<"p-migration">>},
                #{attempt => 1}),
    ok = wait_until_lease_expired(<<"p-migration">>, 1000),
    {ok, #{run_ids := Recoverable}} = beamtrail:list_recoverable(undefined, 100),
    ?assertEqual([<<"p-retry-due">>, <<"p-running-nolease">>], Recoverable).

seed_created(RunId, Workflow, Steps) ->
    {ok, _} = beamtrail_memory_storage:append_event(
                RunId, 0, undefined,
                'workflow.instance.created', undefined, undefined, undefined,
                #{workflow => Workflow, input => #{order_id => RunId},
                  steps => Steps}),
    ok.

decider_state(Command) ->
    RunId = <<"decider-validation-run-1">>,
    Input = #{order_id => <<"o-decider-validation-1">>,
              command => Command},
    beamtrail_reducer:from_events(
      [#{run_id => RunId,
         event_seq => 1,
         event_type => 'workflow.instance.created',
         payload => #{workflow => bt_decider_workflow,
                      input => Input,
                      steps => [charge, ship],
                      decider => module,
                      decider_version => 1},
         occurred_at => 1}]).

seed_migration_blocked(RunId) ->
    seed_created(RunId, bt_versioned_workflow, [charge]),
    {ok, Lease} = beamtrail_memory_storage:acquire_lease(RunId, mig_owner, 20),
    {ok, _} = beamtrail_memory_storage:append_event(
                RunId, 1, maps:get(fencing_token, Lease),
                'attempt.started', charge, 1, {charge, RunId},
                #{attempt => 1}),
    ok = wait_until_lease_expired(RunId, 1000).

seed_retrying(RunId, NextRetryAt) ->
    seed_created(RunId, bt_success_workflow, [charge]),
    {ok, Lease} = beamtrail_memory_storage:acquire_lease(RunId, retry_owner, 20),
    Fence = maps:get(fencing_token, Lease),
    {ok, _} = beamtrail_memory_storage:append_event(
                RunId, 1, Fence, 'attempt.started', charge, 1,
                {charge, RunId}, #{attempt => 1}),
    {ok, _} = beamtrail_memory_storage:append_events(
                RunId, 2, Fence,
                [#{event_type => 'step.failed', step_id => charge,
                   step_version => 1, idempotency_key => {charge, RunId},
                   payload => #{reason => transient, class => transient,
                                attempt => 1}},
                 #{event_type => 'retry.scheduled', step_id => charge,
                   step_version => 1, idempotency_key => {charge, RunId},
                   payload => #{reason => transient, class => transient,
                                attempt => 1, next_retry_at => NextRetryAt}}]),
    ok.

seed_waiting_timer(RunId, FireAtMs) ->
    seed_created(RunId, bt_timer_workflow, []),
    {ok, Lease} = beamtrail_memory_storage:acquire_lease(RunId, timer_owner, 1000),
    Fence = maps:get(fencing_token, Lease),
    {ok, _Events} =
        beamtrail_memory_storage:append_events(
          RunId, 1, Fence,
          [#{event_type => 'timer.scheduled',
             step_id => undefined,
             step_version => undefined,
             idempotency_key => undefined,
             payload => #{timer_id => approval_deadline,
                          fire_at_ms => FireAtMs,
                          scheduled_at => FireAtMs - 1000,
                          source_command => sleep_until,
                          next_wake_at => FireAtMs}},
           #{event_type => 'workflow.waiting',
             step_id => undefined,
             step_version => undefined,
             idempotency_key => undefined,
             payload => #{reason => waiting_for_timer,
                          waiting_since => FireAtMs - 1000}}]),
    ok = beamtrail_memory_storage:release_lease(RunId, Fence),
    ok.

seed_terminal_failed(RunId) ->
    seed_created(RunId, bt_success_workflow, [charge]),
    {ok, Lease} = beamtrail_memory_storage:acquire_lease(RunId, fail_owner, 1000),
    Fence = maps:get(fencing_token, Lease),
    {ok, _} = beamtrail_memory_storage:append_event(
                RunId, 1, Fence, 'attempt.started', charge, 1,
                {charge, RunId}, #{attempt => 1}),
    {ok, _} = beamtrail_memory_storage:append_events(
                RunId, 2, Fence,
                [#{event_type => 'step.failed', step_id => charge,
                   step_version => 1, idempotency_key => {charge, RunId},
                   payload => #{reason => fatal, class => fatal, attempt => 1}},
                 #{event_type => 'workflow.failed', step_id => charge,
                   step_version => 1, idempotency_key => {charge, RunId},
                   payload => #{reason => fatal, class => fatal, attempt => 1}}]),
    ok.

receive_exec() ->
    receive Message -> Message
    after 1000 -> timeout
    end.

receive_exec_short() ->
    receive Message -> Message
    after 50 -> timeout
    end.

terminal_status(#{status := failed, terminal := true}) ->
    terminal;
terminal_status(#{status := Status, terminal := Terminal}) ->
    {Status, Terminal}.

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

wait_for_runner_status(_RunId, _Target, Remaining) when Remaining =< 0 ->
    {error, timeout};
wait_for_runner_status(RunId, Target, Remaining) ->
    case beamtrail_run_registry:lookup(RunId) of
        {ok, #{status := Target}} ->
            ok;
        _ ->
            timer:sleep(20),
            wait_for_runner_status(RunId, Target, Remaining - 20)
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

stop_registered(Name) ->
    _ = catch gen_server:stop(Name, shutdown, 1000),
    ok.
