-module(bt_pg_stress).

-export([run/4, run/5]).

run(Count0, SleepMs0, PoolSize0, Port0) ->
    run(Count0, SleepMs0, PoolSize0, Port0, "8").

run(Count0, SleepMs0, PoolSize0, Port0, DescribeSample0) ->
    Count = normalize_int(Count0),
    SleepMs = normalize_int(SleepMs0),
    PoolSize = normalize_int(PoolSize0),
    Port = normalize_int(Port0),
    DescribeSample = normalize_int(DescribeSample0),
    ok = warmup_atoms(),
    configure(Port, PoolSize, Count),
    ok = init_runtime(),
    StartedAt = erlang:monotonic_time(millisecond),
    RunIds = start_runs(Count, SleepMs),
    {Results, DescribeStats} =
        wait_runs(RunIds, 30000, #{}, empty_probe_stats(), DescribeSample),
    FinishedAt = erlang:monotonic_time(millisecond),
    Summary = summarize(Results),
    Latencies = result_latencies(Results),
    io:format("runs=~p pool_size=~p sleep_ms=~p describe_sample=~p elapsed_ms=~p~n",
              [Count, PoolSize, SleepMs, DescribeSample, FinishedAt - StartedAt]),
    io:format("completed=~p failed=~p timeout=~p other=~p~n",
              [maps:get(completed, Summary, 0),
               maps:get(failed, Summary, 0),
               maps:get(timeout, Summary, 0),
               maps:get(other, Summary, 0)]),
    print_latency("terminal_latency_ms", Latencies),
    print_probe_stats(DescribeStats),
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
            #{run_id => RunId,
              started_at => erlang:monotonic_time(millisecond)};
        {error, Reason} ->
            io:format("start_failed run=~s reason=~p~n", [RunId, Reason]),
            #{run_id => RunId,
              started_at => erlang:monotonic_time(millisecond)}
    end.

wait_runs([], _RemainingMs, Results, ProbeStats, _DescribeSample) ->
    {Results, ProbeStats};
wait_runs(Runs, RemainingMs, Results, ProbeStats, _DescribeSample)
  when RemainingMs =< 0 ->
    Results1 =
        lists:foldl(fun(#{run_id := RunId}, Acc) ->
                            maps:put(RunId, #{status => timeout}, Acc)
                    end,
                    Results, Runs),
    {Results1, ProbeStats};
wait_runs(Runs, RemainingMs, Results, ProbeStats, DescribeSample) ->
    ProbeStats1 = probe_pending(Runs, DescribeSample, ProbeStats),
    {Done, Pending} = partition_terminal(Runs, [], []),
    Results1 =
        lists:foldl(fun({RunId, Status, LatencyMs}, Acc) ->
                            maps:put(RunId,
                                     #{status => Status,
                                       latency_ms => LatencyMs},
                                     Acc)
                    end,
                    Results, Done),
    case Pending of
        [] ->
            {Results1, ProbeStats1};
        _ ->
            timer:sleep(50),
            wait_runs(Pending, RemainingMs - 50, Results1, ProbeStats1,
                      DescribeSample)
    end.

partition_terminal([], Done, Pending) ->
    {Done, Pending};
partition_terminal([#{run_id := RunId, started_at := StartedAt} = Run | Rest],
                   Done, Pending) ->
    case beamtrail:get_state(RunId) of
        #{terminal := true, status := Status} ->
            LatencyMs = erlang:monotonic_time(millisecond) - StartedAt,
            partition_terminal(Rest, [{RunId, Status, LatencyMs} | Done],
                               Pending);
        {error, Reason} ->
            LatencyMs = erlang:monotonic_time(millisecond) - StartedAt,
            partition_terminal(Rest, [{RunId, {error, Reason}, LatencyMs} | Done],
                               Pending);
        _ ->
            partition_terminal(Rest, Done, [Run | Pending])
    end.

summarize(Results) ->
    maps:fold(fun(_RunId, Result, Acc) ->
                      bump(summary_key(maps:get(status, Result)), Acc)
              end,
              #{}, Results).

summary_key(completed) -> completed;
summary_key(failed) -> failed;
summary_key(timeout) -> timeout;
summary_key(_Other) -> other.

bump(Key, Acc) ->
    maps:put(Key, maps:get(Key, Acc, 0) + 1, Acc).

result_latencies(Results) ->
    maps:fold(fun(_RunId, #{latency_ms := LatencyMs}, Acc) ->
                      [LatencyMs | Acc];
                 (_RunId, _Result, Acc) ->
                      Acc
              end,
              [], Results).

empty_probe_stats() ->
    #{calls => 0,
      errors => 0,
      latencies => []}.

probe_pending(_Pending, SampleSize, Stats) when SampleSize =< 0 ->
    Stats;
probe_pending(Pending, SampleSize, Stats) ->
    probe_runs(take(SampleSize, Pending), Stats).

probe_runs([], Stats) ->
    Stats;
probe_runs([#{run_id := RunId} | Rest], Stats) ->
    StartedAt = erlang:monotonic_time(millisecond),
    Result = (catch beamtrail_query:describe(RunId)),
    LatencyMs = erlang:monotonic_time(millisecond) - StartedAt,
    Calls = maps:get(calls, Stats) + 1,
    Errors0 = maps:get(errors, Stats),
    Errors =
        case Result of
            #{run_id := RunId} -> Errors0;
            _ -> Errors0 + 1
        end,
    Stats1 = Stats#{calls => Calls,
                    errors => Errors,
                    latencies => [LatencyMs | maps:get(latencies, Stats)]},
    probe_runs(Rest, Stats1).

take(_N, []) ->
    [];
take(N, _List) when N =< 0 ->
    [];
take(N, [Item | Rest]) ->
    [Item | take(N - 1, Rest)].

print_probe_stats(Stats) ->
    Latencies = maps:get(latencies, Stats),
    io:format("describe_ms=calls=~p errors=~p ",
              [maps:get(calls, Stats), maps:get(errors, Stats)]),
    print_latency_values(Latencies).

print_latency(Label, Latencies) ->
    io:format("~s=", [Label]),
    print_latency_values(Latencies).

print_latency_values([]) ->
    io:format("samples=0 p50=undefined p95=undefined max=undefined~n");
print_latency_values(Latencies) ->
    Sorted = lists:sort(Latencies),
    io:format("samples=~p p50=~p p95=~p max=~p~n",
              [length(Sorted),
               percentile(Sorted, 50),
               percentile(Sorted, 95),
               lists:last(Sorted)]).

percentile(Sorted, Percent) ->
    Count = length(Sorted),
    Index = max(1, ceil_div(Count * Percent, 100)),
    lists:nth(Index, Sorted).

ceil_div(N, D) ->
    (N + D - 1) div D.

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
