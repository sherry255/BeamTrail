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
              fun postgres_append_locks_only_target_run/0,
              fun postgres_expected_seq_conflict_is_per_run/0]}
    end.

setup(Config) ->
    ok = stop_beamtrail_runtime(),
    ok = application:unset_env(beamtrail, worker_max_children),
    ok = application:set_env(beamtrail, storage_adapter,
                             beamtrail_postgres_storage),
    ok = application:set_env(beamtrail, postgres, Config),
    ok = beamtrail_postgres_storage:init_schema(),
    ok = drop_slow_event_trigger(Config),
    ok = truncate_tables(Config),
    {ok, _Apps} = application:ensure_all_started(beamtrail),
    ok.

cleanup(_) ->
    ok = stop_beamtrail_runtime(),
    ok = application:unset_env(beamtrail, worker_max_children),
    ok = application:unset_env(beamtrail, storage_adapter),
    ok = application:unset_env(beamtrail, postgres).

postgres_workflow_survives_application_restart() ->
    RunId = unique_run_id("pg-complete"),
    Input = #{order_id => RunId, test_pid => self()},
    {ok, RunId} = beamtrail:start_workflow(bt_success_workflow, Input,
                                           #{run_id => RunId}),
    ?assertMatch({executed, charge, 1, {charge, RunId}}, receive_exec()),
    ?assertMatch({executed, ship, 1, {ship, RunId}}, receive_exec()),
    ok = restart_beamtrail(),
    State = beamtrail:get_state(RunId),
    ?assertEqual(completed, maps:get(status, State)),
    {ok, Events} = beamtrail:events(RunId),
    ?assertEqual([1, 2, 3, 4, 5, 6], [maps:get(event_seq, E) || E <- Events]).

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

postgres_expected_seq_conflict_is_per_run() ->
    RunId = unique_run_id("pg-conflict"),
    {ok, _} = append_created_event(RunId),
    ?assertMatch({error, {conflict, #{expected_seq := 0, actual_seq := 1}}},
                 append_created_event(RunId)).

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
