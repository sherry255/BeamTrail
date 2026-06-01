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
              fun postgres_recovery_replays_unfinished_attempt_after_restart/0]}
    end.

setup(Config) ->
    ok = stop_beamtrail_runtime(),
    ok = application:unset_env(beamtrail, worker_max_children),
    ok = application:set_env(beamtrail, storage_adapter,
                             beamtrail_postgres_storage),
    ok = application:set_env(beamtrail, postgres, Config),
    ok = beamtrail_postgres_storage:init_schema(),
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

truncate_tables(Config) ->
    case epgsql:connect(Config) of
        {ok, C} ->
            try
                case epgsql:squery(
                       C,
                       "TRUNCATE workflow_events, workflow_snapshots, workflow_leases") of
                    {ok, _, _} -> ok;
                    {error, Reason} -> {error, Reason}
                end
            after
                catch epgsql:close(C)
            end;
        {error, Reason} ->
            {error, Reason}
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
