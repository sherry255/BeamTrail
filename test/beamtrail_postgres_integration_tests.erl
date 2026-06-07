-module(beamtrail_postgres_integration_tests).

-include_lib("eunit/include/eunit.hrl").

postgres_integration_test_() ->
    case pg_config() of
        skip ->
            [];
        {ok, Config} ->
            {foreach,
             fun() -> setup(Config) end,
             fun cleanup/1,
             [fun postgres_workflow_survives_application_restart/0,
              fun postgres_recovery_replays_unfinished_attempt_after_restart/0,
              fun postgres_recovery_budget_exceeded_fails_open_attempt/0,
              fun postgres_signal_wakes_waiting_workflow/0,
              fun postgres_timer_wakes_waiting_workflow/0,
              fun postgres_approval_signal_before_deadline_completes/0,
              fun postgres_list_recoverable_uses_indexed_projection/0,
              fun postgres_release_lease_preserves_fencing/0,
              fun postgres_recoverable_index_excludes_parked_runs/0,
              fun postgres_backfill_reports_per_run_load_errors/0,
              fun postgres_transaction_rolls_back_on_internal_exception/0,
              fun postgres_decode_rejects_unknown_atoms/0,
              fun postgres_append_locks_only_target_run/0,
              fun postgres_append_events_writes_adjacent_events/0,
              fun postgres_expected_seq_conflict_is_per_run/0,
              fun postgres_rejects_zombie_append_after_fence_takeover/0,
              fun postgres_storage_uses_supervised_connection_pool/0,
              fun postgres_pool_recovers_checked_out_connection_after_owner_death/0,
              fun postgres_pool_checkout_times_out_when_exhausted/0]}
    end.

setup(Config) ->
    ok = stop_beamtrail_runtime(),
    ok = application:unset_env(beamtrail, worker_max_children),
    ok = application:unset_env(beamtrail, lease_ttl_ms),
    ok = application:unset_env(beamtrail, max_recoveries_per_attempt),
    ok = application:set_env(beamtrail, storage_adapter,
                             beamtrail_postgres_storage),
    ok = application:set_env(beamtrail, postgres, Config),
    ok = application:set_env(beamtrail, postgres_pool_size, 2),
    ok = application:set_env(beamtrail, postgres_pool_checkout_timeout_ms, 50),
    ok = beamtrail_postgres_storage:init_schema(),
    ok = drop_slow_event_trigger(Config),
    ok = truncate_tables(Config),
    {ok, _Apps} = application:ensure_all_started(beamtrail),
    ok.

cleanup(_) ->
    ok = stop_beamtrail_runtime(),
    ok = application:unset_env(beamtrail, worker_max_children),
    ok = application:unset_env(beamtrail, lease_ttl_ms),
    ok = application:unset_env(beamtrail, max_recoveries_per_attempt),
    ok = application:unset_env(beamtrail, storage_adapter),
    ok = application:unset_env(beamtrail, postgres),
    ok = application:unset_env(beamtrail, postgres_pool_size),
    ok = application:unset_env(beamtrail, postgres_pool_checkout_timeout_ms).

postgres_workflow_survives_application_restart() ->
    RunId = unique_run_id("pg-complete"),
    Input = #{order_id => RunId, test_pid => self()},
    {ok, RunId} = beamtrail:start_workflow(bt_success_workflow, Input,
                                           #{run_id => RunId}),
    ?assertMatch({executed, charge, 1, {charge, RunId}}, receive_exec()),
    ?assertMatch({executed, ship, 1, {ship, RunId}}, receive_exec()),
    {ok, #{status := completed}} = beamtrail:await_terminal(RunId, 1000),
    ok = restart_beamtrail(),
    State = beamtrail:get_state(RunId),
    ?assertEqual(completed, maps:get(status, State)),
    {ok, Events} = beamtrail:events(RunId),
    ?assertEqual(lists:seq(1, 12), [maps:get(event_seq, E) || E <- Events]).

postgres_recovery_replays_unfinished_attempt_after_restart() ->
    RunId = unique_run_id("pg-recover"),
    Input = #{order_id => RunId, test_pid => self()},
    {ok, _} =
        beamtrail_postgres_storage:append_event(
          RunId, 0, undefined,
          'workflow.instance.created', undefined, undefined,
          undefined,
          #{workflow => bt_success_workflow, input => Input, steps => [charge]}),
    {ok, Lease} = beamtrail_postgres_storage:acquire_lease(RunId, stale_worker, 100),
    {ok, _} =
        beamtrail_postgres_storage:append_event(
          RunId, 1, maps:get(fencing_token, Lease),
          'attempt.started', charge, 1,
          {charge, RunId}, #{attempt => 1}),
    timer:sleep(120),
    ok = restart_beamtrail(),
    {ok, {requeued, RecoveryLease}} = beamtrail:mark_recovery_requeued_with_lease(RunId),
    ?assertMatch({ok, #{status := completed}},
                 beamtrail:dispatch(RunId, RecoveryLease)),
    ?assertMatch({executed, charge, 1, {charge, RunId}}, receive_exec()).

postgres_recovery_budget_exceeded_fails_open_attempt() ->
    ok = application:set_env(beamtrail, lease_ttl_ms, 100),
    ok = application:set_env(beamtrail, max_recoveries_per_attempt, 1),
    RunId = unique_run_id("pg-recovery-budget"),
    Input = #{order_id => RunId},
    {ok, _} =
        beamtrail_postgres_storage:append_event(
          RunId, 0, undefined,
          'workflow.instance.created', undefined, undefined,
          undefined,
          #{workflow => bt_success_workflow, input => Input, steps => [charge]}),
    {ok, Lease} = beamtrail_postgres_storage:acquire_lease(RunId, stale_worker, 100),
    {ok, _} =
        beamtrail_postgres_storage:append_event(
          RunId, 1, maps:get(fencing_token, Lease),
          'attempt.started', charge, 1,
          {charge, RunId}, #{attempt => 1}),
    ok = wait_until_pg_lease_expired(RunId, 1000),
    ?assertEqual({ok, requeued}, beamtrail:mark_recovery_requeued(RunId)),
    ok = wait_until_pg_lease_expired(RunId, 1000),
    ?assertEqual({ok, failed}, beamtrail:mark_recovery_requeued(RunId)),
    State = beamtrail:get_state(RunId),
    ?assertEqual(failed, maps:get(status, State)),
    ?assertEqual(true, maps:get(terminal, State)),
    ?assertMatch(#{reason := recovery_budget_exceeded,
                   recoveries := 1,
                   max_recoveries := 1},
                 maps:get(failure, State)),
    {ok, Events} = beamtrail_postgres_storage:events(RunId),
    ?assertEqual(1, length([E || E <- Events,
                            maps:get(event_type, E) =:= 'workflow.failed'])).

postgres_signal_wakes_waiting_workflow() ->
    RunId = unique_run_id("pg-signal"),
    Input = #{order_id => RunId, test_pid => self()},
    {ok, RunId} = beamtrail:start_workflow(
                    bt_signal_workflow,
                    Input,
                    #{run_id => RunId}),
    {ok, Waiting} = wait_for_state(RunId, waiting, 1000),
    ?assertMatch(#{terminal := false,
                   wait_reason := waiting_for_approval}, Waiting),
    {ok, _Signalled} = beamtrail:signal_run(
                         RunId,
                         approved,
                         #{approved_by => <<"ops">>}),
    ?assertMatch({signal_workflow_executed, fulfill, _}, receive_exec()),
    {ok, State} = beamtrail:await_terminal(RunId, 1000),
    ?assertMatch(#{status := completed,
                   workflow_result := #{approved_by := <<"ops">>}}, State),
    {ok, Events} = beamtrail_postgres_storage:events(RunId),
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
       [maps:get(event_type, E) || E <- Events]).

postgres_timer_wakes_waiting_workflow() ->
    RunId = unique_run_id("pg-timer-wake"),
    Input = #{order_id => RunId,
              timer_id => approval_deadline,
              fire_at_ms => erlang:system_time(millisecond) - 1},
    {ok, RunId} = beamtrail:start_workflow(bt_timer_workflow, Input,
                                           #{run_id => RunId}),
    {ok, Waiting} = wait_for_state(RunId, waiting, 1000),
    ?assertMatch(#{next_wake_at := WakeAt} when is_integer(WakeAt), Waiting),
    ok = wait_until_pg_lease_expired(RunId, 1000),
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

postgres_approval_signal_before_deadline_completes() ->
    RunId = unique_run_id("pg-approval"),
    Input = #{order_id => RunId,
              deadline_at_ms => erlang:system_time(millisecond) + 60000,
              test_pid => self()},
    {ok, RunId} = beamtrail:start_workflow(
                    bt_approval_deadline_workflow,
                    Input,
                    #{run_id => RunId}),
    {ok, Waiting} = wait_for_state(RunId, waiting, 1000),
    ?assertMatch(#{wait_reason := waiting_for_approval,
                   next_wake_at := WakeAt} when is_integer(WakeAt),
                 Waiting),
    {ok, _Signalled} = beamtrail:signal_run(
                         RunId, approved, #{approved_by => <<"ops">>}),
    ?assertMatch({approval_workflow_executed, fulfill, _}, receive_exec()),
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

postgres_list_recoverable_uses_indexed_projection() ->
    {ok, Config} = application:get_env(beamtrail, postgres),
    Now = erlang:system_time(millisecond),
    %% running, no lease -> recoverable
    seed_created_pg(<<"pg-rec-1-running">>, [charge]),
    %% completed -> terminal -> not recoverable
    seed_created_pg(<<"pg-rec-2-completed">>, []),
    {ok, L2} = beamtrail_postgres_storage:acquire_lease(
                 <<"pg-rec-2-completed">>, seed, 1000),
    {ok, _} = beamtrail_postgres_storage:append_event(
                <<"pg-rec-2-completed">>, 1, maps:get(fencing_token, L2),
                'workflow.completed', undefined, undefined, undefined, #{}),
    %% running, live lease -> not recoverable (owned)
    seed_created_pg(<<"pg-rec-3-livelease">>, [charge]),
    {ok, _} = beamtrail_postgres_storage:acquire_lease(
                <<"pg-rec-3-livelease">>, live_owner, 30000),
    %% waiting for signal -> not recoverable
    seed_waiting_pg(<<"pg-rec-3b-waiting">>),
    %% waiting for signal with future timer -> not recoverable yet
    seed_waiting_timer_pg(<<"pg-rec-3c-waiting-timer-future">>, Now + 60000),
    %% waiting for signal with due timer -> recoverable
    seed_waiting_timer_pg(<<"pg-rec-3d-waiting-timer-due">>, Now - 1000),
    %% retrying, next_retry in the future -> not recoverable yet
    seed_retry_pg(<<"pg-rec-4-retry-future">>, Now + 60000),
    %% retrying, next_retry past, lease expired -> recoverable
    seed_retry_pg(<<"pg-rec-5-retry-due">>, Now - 1000),
    ok = wait_until_pg_lease_expired(<<"pg-rec-5-retry-due">>, 1000),
    %% terminal failure -> not recoverable
    seed_terminal_failed_pg(<<"pg-rec-6-terminal-failed">>),
    {ok, #{run_ids := Recoverable}} = beamtrail:list_recoverable(undefined, 100),
    ?assertEqual([<<"pg-rec-1-running">>,
                  <<"pg-rec-3d-waiting-timer-due">>,
                  <<"pg-rec-5-retry-due">>],
                 Recoverable),
    IndexedNow = erlang:system_time(millisecond),
    {ok, #{run_ids := IndexedCandidates}} =
        beamtrail_postgres_storage:list_recoverable_run_ids(undefined, 100, IndexedNow),
    ?assertNot(lists:member(<<"pg-rec-3b-waiting">>, IndexedCandidates)),
    ?assertNot(lists:member(<<"pg-rec-3c-waiting-timer-future">>, IndexedCandidates)),
    ?assert(lists:member(<<"pg-rec-3d-waiting-timer-due">>, IndexedCandidates)),
    %% Projection columns mirror the reduced state derived by the reducer.
    ?assertEqual({<<"completed">>, true, null, null, false},
                 run_projection_row(Config, <<"pg-rec-2-completed">>)),
    ?assertMatch({<<"retrying">>, false, NextRetry, null, false}
                   when is_integer(NextRetry),
                 run_projection_row(Config, <<"pg-rec-4-retry-future">>)),
    ?assertEqual({<<"waiting">>, false, null, null, false},
                 run_projection_row(Config, <<"pg-rec-3b-waiting">>)),
    ?assertMatch({<<"waiting">>, false, null, FutureWake, false}
                   when is_integer(FutureWake),
                 run_projection_row(Config, <<"pg-rec-3c-waiting-timer-future">>)),
    ?assertMatch({<<"waiting">>, false, null, DueWake, false}
                   when is_integer(DueWake),
                 run_projection_row(Config, <<"pg-rec-3d-waiting-timer-due">>)),
    ?assertEqual({<<"failed">>, true, null, null, false},
                 run_projection_row(Config, <<"pg-rec-6-terminal-failed">>)).

postgres_release_lease_preserves_fencing() ->
    RunId = unique_run_id("pg-release-lease"),
    seed_created_pg(RunId, [charge]),
    {ok, Lease1} = beamtrail_postgres_storage:acquire_lease(RunId, owner1, 30000),
    Fence1 = maps:get(fencing_token, Lease1),
    ok = beamtrail_postgres_storage:release_lease(RunId, Fence1),
    {ok, Lease2} = beamtrail_postgres_storage:acquire_lease(RunId, owner2, 30000),
    ?assertEqual(Fence1 + 1, maps:get(fencing_token, Lease2)),
    ?assertEqual({error, stale_fence},
                 beamtrail_postgres_storage:release_lease(RunId, Fence1)).

postgres_recoverable_index_excludes_parked_runs() ->
    RunId = unique_run_id("pg-parked-index"),
    seed_created_pg(RunId, [charge]),
    {ok, Lease} = beamtrail_postgres_storage:acquire_lease(RunId, owner1, 30000),
    {ok, State0} = beamtrail_state:load(RunId, beamtrail_postgres_storage),
    Parked =
        #{event_type => 'workflow.parked',
          step_id => undefined,
          step_version => undefined,
          idempotency_key => undefined,
          payload => #{reason => maintenance,
                       parked_at => erlang:system_time(millisecond)}},
    {ok, [_]} =
        beamtrail_postgres_storage:append_events(
          RunId, maps:get(last_event_seq, State0),
          maps:get(fencing_token, Lease), [Parked]),
    ok = beamtrail_postgres_storage:release_lease(
           RunId, maps:get(fencing_token, Lease)),
    {ok, #{run_ids := Candidates}} =
        beamtrail_postgres_storage:list_recoverable_run_ids(
          undefined, 100, erlang:system_time(millisecond)),
    ?assertNot(lists:member(RunId, Candidates)).

postgres_backfill_reports_per_run_load_errors() ->
    {ok, Config} = application:get_env(beamtrail, postgres),
    RunId = <<"pg-backfill-corrupt-run">>,
    ok = insert_corrupt_backfill_run(Config, RunId),
    ?assertMatch(
       {error, {backfill_failed,
                [#{run_id := RunId, reason := bad_external_term}]}},
       beamtrail_postgres_storage:backfill_run_projections()).

postgres_transaction_rolls_back_on_internal_exception() ->
    ok = application:set_env(beamtrail, postgres_pool_size, 1),
    ok = restart_beamtrail(),
    RunId = <<"pg-transaction-exception-run">>,
    ?assertMatch(
       {error, {transaction_failed, error, {badkey, event_type}}},
       beamtrail_postgres_storage:append_events(RunId, 0, undefined, [#{}])),
    ?assertMatch({ok, #{event_seq := 1}}, append_created_event(RunId)),
    {ok, #{run_ids := RunIds}} = beamtrail_postgres_storage:list_run_ids(undefined, 10),
    ?assertEqual([RunId], RunIds).

postgres_decode_rejects_unknown_atoms() ->
    {ok, Config} = application:get_env(beamtrail, postgres),
    UnknownTypeRun = <<"pg-unknown-event-type-run">>,
    UnknownStepRun = <<"pg-unknown-step-run">>,
    ok = insert_raw_event(Config, UnknownTypeRun, <<"made.up.event">>, null),
    ok = insert_raw_event(Config, UnknownStepRun, <<"attempt.started">>,
                          <<"step_atom_that_should_not_exist_987654">>),
    ?assertEqual({error, {unknown_event_type, <<"made.up.event">>}},
                 beamtrail_postgres_storage:events(UnknownTypeRun)),
    ?assertEqual({error,
                  {unknown_step_id,
                   <<"step_atom_that_should_not_exist_987654">>}},
                 beamtrail_postgres_storage:events(UnknownStepRun)).

postgres_append_locks_only_target_run() ->
    {ok, Config} = application:get_env(beamtrail, postgres),
    SlowRun = unique_run_id("pg-slow-lock"),
    FastRun = unique_run_id("pg-fast-lock"),
    ok = install_slow_event_trigger(Config, SlowRun),
    Parent = self(),
    {SlowPid, SlowRef} =
        spawn_monitor(
          fun() ->
                  Parent ! {slow_append_result, self(),
                            append_created_event(SlowRun)}
          end),
    ok = wait_for_slow_trigger(Config),
    StartedAt = erlang:monotonic_time(millisecond),
    FastResult = append_created_event(FastRun),
    ElapsedMs = erlang:monotonic_time(millisecond) - StartedAt,
    SlowResult = receive_slow_append(SlowPid, SlowRef),
    ok = drop_slow_event_trigger(Config),
    ?assertMatch({ok, #{event_seq := 1}}, FastResult),
    ?assertMatch({ok, #{event_seq := 1}}, SlowResult),
    ?assert(ElapsedMs < 900).

postgres_append_events_writes_adjacent_events() ->
    RunId = unique_run_id("pg-batch-append"),
    {ok, _} = append_created_event(RunId),
    {ok, Lease} = beamtrail_postgres_storage:acquire_lease(RunId, worker_a, 1000),
    Fence = maps:get(fencing_token, Lease),
    EventSpecs =
        [#{event_type => 'attempt.started',
           step_id => charge,
           step_version => 1,
           idempotency_key => {charge, RunId},
           payload => #{attempt => 1}},
         #{event_type => 'step.failed',
           step_id => charge,
           step_version => 1,
           idempotency_key => {charge, RunId},
           payload => #{reason => transient, class => transient, attempt => 1}}],
    ?assertMatch({ok, [#{event_seq := 2}, #{event_seq := 3}]},
                 beamtrail_postgres_storage:append_events(RunId, 1, Fence,
                                                          EventSpecs)),
    ?assertMatch({error, {conflict, #{expected_seq := 1, actual_seq := 3}}},
                 beamtrail_postgres_storage:append_events(RunId, 1, Fence,
                                                          EventSpecs)),
    {ok, Events} = beamtrail_postgres_storage:events(RunId),
    ?assertEqual([1, 2, 3], [maps:get(event_seq, E) || E <- Events]).

postgres_expected_seq_conflict_is_per_run() ->
    RunId = unique_run_id("pg-conflict"),
    {ok, _} = append_created_event(RunId),
    ?assertMatch({error, {conflict, #{expected_seq := 0, actual_seq := 1}}},
                 append_created_event(RunId)).

postgres_rejects_zombie_append_after_fence_takeover() ->
    RunId = unique_run_id("pg-zombie-fence"),
    {ok, _} = beamtrail_postgres_storage:append_event(
                RunId, 0, undefined,
                'workflow.instance.created', undefined, undefined,
                undefined,
                #{workflow => bt_success_workflow, input => #{},
                  steps => [charge]}),
    {ok, Lease1} = beamtrail_postgres_storage:acquire_lease(RunId, worker_a, 200),
    Fence1 = maps:get(fencing_token, Lease1),
    {ok, _} = beamtrail_postgres_storage:append_event(
                RunId, 1, Fence1,
                'attempt.started', charge, 1,
                {charge, RunId}, #{attempt => 1}),
    ok = wait_until_pg_lease_expired(RunId, 1000),
    {ok, Lease2} = beamtrail_postgres_storage:acquire_lease(RunId, worker_b, 1000),
    Fence2 = maps:get(fencing_token, Lease2),
    ?assert(Fence2 > Fence1),
    {ok, _} = beamtrail_postgres_storage:append_event(
                RunId, 2, Fence2,
                'recovery.requeued', undefined, undefined,
                undefined, #{requeued_at => erlang:system_time(millisecond)}),
    ?assertEqual({error, stale_fence},
                 beamtrail_postgres_storage:append_event(
                   RunId, 3, Fence1,
                   'step.succeeded', charge, 1,
                   {charge, RunId}, #{result => zombie_late_success})),
    {ok, Events} = beamtrail_postgres_storage:events(RunId),
    ?assertEqual(3, length(Events)),
    ?assertEqual(0, length([E || E <- Events,
                            maps:get(event_type, E) =:= 'step.succeeded'])).

postgres_storage_uses_supervised_connection_pool() ->
    Info0 = beamtrail_postgres_pool:info(),
    ?assertEqual(2, maps:get(size, Info0)),
    RunId = unique_run_id("pg-pool"),
    {ok, _} = append_created_event(RunId),
    {ok, [_]} = beamtrail_postgres_storage:events(RunId),
    Info1 = beamtrail_postgres_pool:info(),
    ?assert(maps:get(checkouts, Info1) >= maps:get(checkouts, Info0) + 2),
    ?assertEqual(2, maps:get(available, Info1)),
    ?assertEqual(0, maps:get(busy, Info1)).

postgres_pool_recovers_checked_out_connection_after_owner_death() ->
    Parent = self(),
    {Pid, Ref} =
        spawn_monitor(
          fun() ->
                  {ok, _C} = beamtrail_postgres_pool:checkout(),
                  Parent ! {checked_out, self()},
                  receive stop -> ok end
          end),
    receive
        {checked_out, Pid} -> ok
    after 1000 ->
            error(checkout_timeout)
    end,
    Info0 = beamtrail_postgres_pool:info(),
    ?assertEqual(1, maps:get(busy, Info0)),
    exit(Pid, kill),
    receive
        {'DOWN', Ref, process, Pid, killed} -> ok
    after 1000 ->
            error(owner_down_timeout)
    end,
    ?assertEqual(ok, wait_for_pool_idle(2, 1000)).

postgres_pool_checkout_times_out_when_exhausted() ->
    {ok, C1} = beamtrail_postgres_pool:checkout(),
    {ok, C2} = beamtrail_postgres_pool:checkout(),
    Parent = self(),
    {Pid, Ref} =
        spawn_monitor(
          fun() ->
                  Parent ! {checkout_result, self(),
                            beamtrail_postgres_pool:checkout()}
          end),
    Result =
        receive
            {checkout_result, Pid, CheckoutResult} ->
                CheckoutResult
        after 1000 ->
                exit(Pid, kill),
                timeout
        end,
    receive
        {'DOWN', Ref, process, Pid, _Reason} -> ok
    after 1000 ->
            error(checkout_process_down_timeout)
    end,
    beamtrail_postgres_pool:checkin(C1),
    beamtrail_postgres_pool:checkin(C2),
    ?assertEqual({error, checkout_timeout}, Result).

restart_beamtrail() ->
    ok = stop_beamtrail_runtime(),
    {ok, _Apps} = application:ensure_all_started(beamtrail),
    ok.

stop_beamtrail_runtime() ->
    _ = application:stop(beamtrail),
    ok = stop_registered(beamtrail_scanner),
    ok = stop_registered(beamtrail_worker_sup),
    ok.

stop_registered(Name) ->
    case whereis(Name) of
        undefined ->
            ok;
        Pid ->
            Ref = erlang:monitor(process, Pid),
            unlink(Pid),
            exit(Pid, shutdown),
            receive
                {'DOWN', Ref, process, Pid, _Reason} ->
                    ok
            after 250 ->
                    exit(Pid, kill),
                    receive
                        {'DOWN', Ref, process, Pid, _Reason} ->
                            ok
                    after 5000 ->
                            erlang:demonitor(Ref, [flush]),
                            {error, {stop_timeout, Name}}
                    end
            end
    end.

append_created_event(RunId) ->
    beamtrail_postgres_storage:append_event(
      RunId, 0, undefined,
      'workflow.instance.created', undefined, undefined,
      undefined,
      #{workflow => bt_success_workflow, input => #{}, steps => []}).

seed_created_pg(RunId, Steps) ->
    {ok, _} = beamtrail_postgres_storage:append_event(
                RunId, 0, undefined,
                'workflow.instance.created', undefined, undefined, undefined,
                #{workflow => bt_success_workflow,
                  input => #{order_id => RunId}, steps => Steps}),
    ok.

seed_retry_pg(RunId, NextRetryAt) ->
    seed_created_pg(RunId, [charge]),
    {ok, Lease} = beamtrail_postgres_storage:acquire_lease(RunId, retry_owner, 100),
    Fence = maps:get(fencing_token, Lease),
    {ok, _} = beamtrail_postgres_storage:append_event(
                RunId, 1, Fence, 'attempt.started', charge, 1,
                {charge, RunId}, #{attempt => 1}),
    {ok, _} = beamtrail_postgres_storage:append_events(
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

seed_waiting_pg(RunId) ->
    seed_created_pg(RunId, [charge]),
    {ok, Lease} = beamtrail_postgres_storage:acquire_lease(RunId, waiting_owner, 1000),
    Fence = maps:get(fencing_token, Lease),
    {ok, _} = beamtrail_postgres_storage:append_event(
                RunId, 1, Fence, 'workflow.waiting', undefined, undefined,
                undefined,
                #{reason => waiting_for_approval,
                  waiting_since => erlang:system_time(millisecond)}),
    ok = beamtrail_postgres_storage:release_lease(RunId, Fence),
    ok.

seed_waiting_timer_pg(RunId, FireAtMs) ->
    seed_created_pg(RunId, []),
    {ok, Lease} = beamtrail_postgres_storage:acquire_lease(RunId, timer_owner, 1000),
    Fence = maps:get(fencing_token, Lease),
    {ok, _} = beamtrail_postgres_storage:append_events(
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
    ok = beamtrail_postgres_storage:release_lease(RunId, Fence),
    ok.

seed_terminal_failed_pg(RunId) ->
    seed_created_pg(RunId, [charge]),
    {ok, Lease} = beamtrail_postgres_storage:acquire_lease(RunId, fail_owner, 1000),
    Fence = maps:get(fencing_token, Lease),
    {ok, _} = beamtrail_postgres_storage:append_event(
                RunId, 1, Fence, 'attempt.started', charge, 1,
                {charge, RunId}, #{attempt => 1}),
    {ok, _} = beamtrail_postgres_storage:append_events(
                RunId, 2, Fence,
                [#{event_type => 'step.failed', step_id => charge,
                   step_version => 1, idempotency_key => {charge, RunId},
                   payload => #{reason => fatal, class => fatal, attempt => 1}},
                 #{event_type => 'workflow.failed', step_id => charge,
                   step_version => 1, idempotency_key => {charge, RunId},
                   payload => #{reason => fatal, class => fatal, attempt => 1}}]),
    ok.

run_projection_row(Config, RunId) ->
    with_pg(Config,
            fun(C) ->
                    {ok, _Cols, [Row]} =
                        epgsql:equery(
                          C,
                          "SELECT status, terminal, next_retry_at_ms, "
                          "next_wake_at_ms, parked "
                          "FROM workflow_runs WHERE run_id = $1",
                          [RunId]),
                    Row
            end).

insert_corrupt_backfill_run(Config, RunId) ->
    Now = erlang:system_time(millisecond),
    with_pg(Config,
            fun(C) ->
                    {ok, 1} =
                        epgsql:equery(
                          C,
                          "INSERT INTO workflow_runs "
                          "(run_id, created_at_ms, updated_at_ms) "
                          "VALUES ($1,$2,$2)",
                          [RunId, Now]),
                    {ok, 1} =
                        epgsql:equery(
                          C,
                          "INSERT INTO workflow_events "
                          "(run_id, event_seq, event_type, step_id, "
                          "step_version, idempotency_key, payload, "
                          "fencing_token, occurred_at_ms) "
                          "VALUES ($1,1,'workflow.instance.created',"
                          "NULL,NULL,NULL,$2,NULL,$3)",
                          [RunId, <<"not-an-external-term">>, Now]),
                    ok
            end).

insert_raw_event(Config, RunId, EventType, StepId) ->
    Now = erlang:system_time(millisecond),
    Payload = term_to_binary(#{}),
    with_pg(Config,
            fun(C) ->
                    {ok, 1} =
                        epgsql:equery(
                          C,
                          "INSERT INTO workflow_runs "
                          "(run_id, created_at_ms, updated_at_ms) "
                          "VALUES ($1,$2,$2)",
                          [RunId, Now]),
                    {ok, 1} =
                        epgsql:equery(
                          C,
                          "INSERT INTO workflow_events "
                          "(run_id, event_seq, event_type, step_id, "
                          "step_version, idempotency_key, payload, "
                          "fencing_token, occurred_at_ms) "
                          "VALUES ($1,1,$2,$3,NULL,NULL,$4,NULL,$5)",
                          [RunId, EventType, StepId, Payload, Now]),
                    ok
            end).

truncate_tables(Config) ->
    case epgsql:connect(Config) of
        {ok, C} ->
            try
                case epgsql:squery(
                       C,
                       "TRUNCATE workflow_events, workflow_snapshots, "
                       "workflow_leases, workflow_runs") of
                    {ok, _, _} -> ok;
                    {error, Reason} -> {error, Reason}
                end
            after
                catch epgsql:close(C)
            end;
        {error, Reason} ->
            {error, Reason}
    end.

install_slow_event_trigger(Config, SlowRun) ->
    HexRun = unicode:characters_to_list(binary:encode_hex(SlowRun)),
    Sql = lists:flatten(
            io_lib:format(
              "CREATE OR REPLACE FUNCTION beamtrail_test_slow_event() "
              "RETURNS trigger AS $$ "
              "BEGIN "
              "IF NEW.run_id = decode('~s', 'hex') THEN "
              "PERFORM pg_advisory_lock(918273645); "
              "PERFORM pg_sleep(1.5); "
              "PERFORM pg_advisory_unlock(918273645); "
              "END IF; "
              "RETURN NEW; "
              "END; "
              "$$ LANGUAGE plpgsql",
              [HexRun])),
    with_pg(Config,
            fun(C) ->
                    ok = squery_ok(C, Sql),
                    squery_ok(C,
                              "CREATE TRIGGER beamtrail_test_slow_event "
                              "BEFORE INSERT ON workflow_events "
                              "FOR EACH ROW EXECUTE FUNCTION "
                              "beamtrail_test_slow_event()")
            end).

drop_slow_event_trigger(Config) ->
    with_pg(Config,
            fun(C) ->
                    ok = squery_ok(
                           C,
                           "DROP TRIGGER IF EXISTS beamtrail_test_slow_event "
                           "ON workflow_events"),
                    squery_ok(C,
                              "DROP FUNCTION IF EXISTS "
                              "beamtrail_test_slow_event()")
            end).

wait_for_slow_trigger(Config) ->
    Deadline = erlang:monotonic_time(millisecond) + 3000,
    with_pg(Config, fun(C) -> wait_for_slow_trigger(C, Deadline) end).

wait_for_slow_trigger(C, Deadline) ->
    case epgsql:equery(C, "SELECT pg_try_advisory_lock(918273645)", []) of
        {ok, _Cols, [{false}]} ->
            ok;
        {ok, _Cols, [{true}]} ->
            _ = epgsql:equery(C, "SELECT pg_advisory_unlock(918273645)", []),
            case erlang:monotonic_time(millisecond) < Deadline of
                true ->
                    timer:sleep(20),
                    wait_for_slow_trigger(C, Deadline);
                false ->
                    {error, slow_trigger_timeout}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

receive_slow_append(SlowPid, SlowRef) ->
    receive
        {slow_append_result, SlowPid, Result} ->
            receive
                {'DOWN', SlowRef, process, SlowPid, _Reason} -> ok
            after 0 ->
                    ok
            end,
            Result;
        {'DOWN', SlowRef, process, SlowPid, Reason} ->
            {error, {slow_append_down, Reason}}
    after 4000 ->
            {error, slow_append_timeout}
    end.

wait_until_pg_lease_expired(_RunId, Remaining) when Remaining =< 0 ->
    {error, timeout};
wait_until_pg_lease_expired(RunId, Remaining) ->
    {ok, Lease} = beamtrail_postgres_storage:read_lease(RunId),
    case maps:get(lease_until, Lease) =< erlang:system_time(millisecond) of
        true -> ok;
        false ->
            timer:sleep(20),
            wait_until_pg_lease_expired(RunId, Remaining - 20)
    end.

with_pg(Config, Fun) ->
    case epgsql:connect(Config) of
        {ok, C} ->
            try Fun(C)
            after
                catch epgsql:close(C)
            end;
        {error, Reason} ->
            {error, Reason}
    end.

squery_ok(C, Sql) ->
    case epgsql:squery(C, Sql) of
        {ok, _, _} -> ok;
        {ok, _Count} -> ok;
        {error, Reason} -> {error, Reason}
    end.

pg_config() ->
    case os:getenv("BEAMTRAIL_PG_TEST") of
        "1" ->
            {ok, #{host => env_string("BEAMTRAIL_PG_HOST", "localhost"),
                   port => env_int("BEAMTRAIL_PG_PORT", 5432),
                   username => env_string("BEAMTRAIL_PG_USER", "beamtrail"),
                   password => env_string("BEAMTRAIL_PG_PASSWORD", "beamtrail"),
                   database => env_string("BEAMTRAIL_PG_DATABASE", "beamtrail")}};
        _ ->
            skip
    end.

env_string(Name, Default) ->
    case os:getenv(Name) of
        false -> Default;
        Value -> Value
    end.

env_int(Name, Default) ->
    case os:getenv(Name) of
        false -> Default;
        Value -> list_to_integer(Value)
    end.

unique_run_id(Prefix) ->
    iolist_to_binary(io_lib:format("~s-~p-~p",
                                  [Prefix,
                                   erlang:system_time(millisecond),
                                   erlang:unique_integer([positive])])).

receive_exec() ->
    receive Message -> Message
    after 1000 -> timeout
    end.

wait_for_state(_RunId, _Target, Remaining) when Remaining =< 0 ->
    {error, timeout};
wait_for_state(RunId, Target, Remaining) ->
    State = beamtrail:get_state(RunId),
    case maps:get(status, State) of
        Target ->
            {ok, State};
        _ ->
            timer:sleep(20),
            wait_for_state(RunId, Target, Remaining - 20)
    end.

wait_for_pool_idle(_Size, Remaining) when Remaining =< 0 ->
    {error, timeout};
wait_for_pool_idle(Size, Remaining) ->
    Info = beamtrail_postgres_pool:info(),
    case {maps:get(available, Info), maps:get(busy, Info)} of
        {Size, 0} -> ok;
        _ ->
            timer:sleep(20),
            wait_for_pool_idle(Size, Remaining - 20)
    end.
