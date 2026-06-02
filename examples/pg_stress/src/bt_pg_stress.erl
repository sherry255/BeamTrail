-module(bt_pg_stress).

-export([run/4]).

run(Count0, SleepMs0, PoolSize0, Port0) ->
    Count = normalize_int(Count0),
    SleepMs = normalize_int(SleepMs0),
    PoolSize = normalize_int(PoolSize0),
    Port = normalize_int(Port0),
    ok = warmup_atoms(),
    configure(Port, PoolSize, Count),
    ok = init_runtime(),
    StartedAt = erlang:monotonic_time(millisecond),
    RunIds = start_runs(Count, SleepMs),
    Results = wait_runs(RunIds, 30000, #{}),
    FinishedAt = erlang:monotonic_time(millisecond),
    Summary = summarize(Results),
    io:format("runs=~p pool_size=~p sleep_ms=~p elapsed_ms=~p~n",
              [Count, PoolSize, SleepMs, FinishedAt - StartedAt]),
    io:format("completed=~p failed=~p timeout=~p other=~p~n",
              [maps:get(completed, Summary, 0),
               maps:get(failed, Summary, 0),
               maps:get(timeout, Summary, 0),
               maps:get(other, Summary, 0)]),
    io:format("pool=~p~n", [beamtrail_postgres_pool:info()]),
    halt(exit_code(Summary)).

configure(Port, PoolSize, Count) ->
    ok = application:set_env(beamtrail, storage_adapter,
                             beamtrail_postgres_storage),
    ok = application:set_env(beamtrail, postgres,
                             #{host => "localhost",
                               port => Port,
                               username => "beamtrail",
                               password => "beamtrail",
                               database => "beamtrail"}),
    ok = application:set_env(beamtrail, postgres_pool_size, PoolSize),
    ok = application:set_env(beamtrail, postgres_pool_checkout_timeout_ms, 1000),
    ok = application:set_env(beamtrail, workflow_modules,
                             [bt_pg_stress_workflow]),
    ok = application:set_env(beamtrail, run_max_children, max(Count, 64)),
    ok = application:set_env(beamtrail, scanner_interval_ms, infinity),
    ok.

init_runtime() ->
    {ok, _} = application:ensure_all_started(epgsql),
    ok = beamtrail_postgres_storage:init_schema(),
    {ok, _} = application:ensure_all_started(beamtrail),
    ok.

start_runs(Count, SleepMs) ->
    [start_run(N, SleepMs) || N <- lists:seq(1, Count)].

start_run(N, SleepMs) ->
    RunId = iolist_to_binary(io_lib:format("pg-stress-~p-~p",
                                           [erlang:system_time(millisecond), N])),
    Input = #{order_id => RunId, sleep_ms => SleepMs},
    case beamtrail:start_workflow(bt_pg_stress_workflow, Input,
                                  #{run_id => RunId}) of
        {ok, RunId} ->
            RunId;
        {error, Reason} ->
            io:format("start_failed run=~s reason=~p~n", [RunId, Reason]),
            RunId
    end.

wait_runs([], _RemainingMs, Results) ->
    Results;
wait_runs(RunIds, RemainingMs, Results) when RemainingMs =< 0 ->
    lists:foldl(fun(RunId, Acc) -> maps:put(RunId, timeout, Acc) end,
                Results, RunIds);
wait_runs(RunIds, RemainingMs, Results) ->
    {Done, Pending} = partition_terminal(RunIds, [], []),
    Results1 =
        lists:foldl(fun({RunId, Status}, Acc) -> maps:put(RunId, Status, Acc) end,
                    Results, Done),
    case Pending of
        [] ->
            Results1;
        _ ->
            timer:sleep(50),
            wait_runs(Pending, RemainingMs - 50, Results1)
    end.

partition_terminal([], Done, Pending) ->
    {Done, Pending};
partition_terminal([RunId | Rest], Done, Pending) ->
    case beamtrail:get_state(RunId) of
        #{terminal := true, status := Status} ->
            partition_terminal(Rest, [{RunId, Status} | Done], Pending);
        {error, Reason} ->
            partition_terminal(Rest, [{RunId, {error, Reason}} | Done], Pending);
        _ ->
            partition_terminal(Rest, Done, [RunId | Pending])
    end.

summarize(Results) ->
    maps:fold(fun(_RunId, Status, Acc) -> bump(summary_key(Status), Acc) end,
              #{}, Results).

summary_key(completed) -> completed;
summary_key(failed) -> failed;
summary_key(timeout) -> timeout;
summary_key(_Other) -> other.

bump(Key, Acc) ->
    maps:put(Key, maps:get(Key, Acc, 0) + 1, Acc).

exit_code(Summary) ->
    Bad = maps:get(failed, Summary, 0)
        + maps:get(timeout, Summary, 0)
        + maps:get(other, Summary, 0),
    case Bad of
        0 -> 0;
        _ -> 2
    end.

warmup_atoms() ->
    {module, bt_pg_stress_workflow} = code:ensure_loaded(bt_pg_stress_workflow),
    lists:foreach(fun(Name) -> _ = list_to_atom(Name), ok end,
                  ["bt_pg_stress_workflow", "work", "order_id", "sleep_ms",
                   "slept_ms"]),
    ok.

normalize_int(N) when is_integer(N) ->
    N;
normalize_int(N) when is_list(N) ->
    list_to_integer(N).
