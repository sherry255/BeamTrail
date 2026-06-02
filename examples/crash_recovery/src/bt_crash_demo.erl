-module(bt_crash_demo).

-export([start_and_block/4, recover/4]).

start_and_block(RunId0, ModeFile, MarkerFile, Port0) ->
    RunId = normalize_run_id(RunId0),
    ok = warmup_atoms(),
    configure(normalize_port(Port0)),
    ok = init_runtime(),
    Input = #{order_id => RunId,
              mode_file => ModeFile,
              marker_file => MarkerFile},
    {ok, RunId} =
        beamtrail:start_workflow(bt_crash_demo_workflow, Input,
                                 #{run_id => RunId}),
    io:format("Started run ~s in blocking VM.~n", [RunId]),
    timer:sleep(infinity).

recover(RunId0, _ModeFile, MarkerFile, Port0) ->
    RunId = normalize_run_id(RunId0),
    ok = warmup_atoms(),
    configure(normalize_port(Port0)),
    ok = init_runtime(),
    State = wait_terminal(RunId, 15000),
    {ok, Events} = beamtrail:events(RunId),
    io:format("~nMarker log:~n"),
    print_file(MarkerFile),
    io:format("~nFinal status: ~p~n", [maps:get(status, State)]),
    io:format("Terminal: ~p~n", [maps:get(terminal, State)]),
    io:format("Attempts: ~p~n", [maps:get(attempts, State)]),
    io:format("~nEvent log:~n"),
    print_events(Events),
    halt(0).

configure(Port) ->
    ok = application:set_env(beamtrail, storage_adapter,
                             beamtrail_postgres_storage),
    ok = application:set_env(beamtrail, postgres,
                             #{host => "localhost",
                               port => Port,
                               username => "beamtrail",
                               password => "beamtrail",
                               database => "beamtrail"}),
    ok = application:set_env(beamtrail, postgres_pool_size, 2),
    ok = application:set_env(beamtrail, postgres_pool_checkout_timeout_ms, 1000),
    ok = application:set_env(beamtrail, workflow_modules,
                             [bt_crash_demo_workflow]),
    ok = application:set_env(beamtrail, lease_ttl_ms, 700),
    ok = application:set_env(beamtrail, scanner_interval_ms, 200),
    ok = application:set_env(beamtrail, max_recoveries_per_attempt, 3),
    ok.

init_runtime() ->
    {ok, _} = application:ensure_all_started(epgsql),
    ok = beamtrail_postgres_storage:init_schema(),
    {ok, _} = application:ensure_all_started(beamtrail),
    ok.

wait_terminal(RunId, RemainingMs) when RemainingMs =< 0 ->
    io:format("Timed out waiting for ~s to become terminal.~n", [RunId]),
    halt(2);
wait_terminal(RunId, RemainingMs) ->
    case beamtrail:get_state(RunId) of
        #{terminal := true} = State ->
            State;
        {error, _Reason} = Error ->
            io:format("state load while waiting: ~p~n", [Error]),
            maybe_requeue(RunId),
            timer:sleep(250),
            wait_terminal(RunId, RemainingMs - 250);
        _State ->
            maybe_requeue(RunId),
            timer:sleep(250),
            wait_terminal(RunId, RemainingMs - 250)
    end.

maybe_requeue(RunId) ->
    case beamtrail:mark_recovery_requeued_with_lease(RunId) of
        {ok, {requeued, Lease}} ->
            io:format("recovery requeued; dispatching supervised runner~n"),
            log_dispatch_result(beamtrail_run_sup:dispatch(RunId, Lease));
        {ok, skipped} ->
            ok;
        {ok, {failed, _State}} ->
            io:format("recovery budget exceeded while waiting~n");
        {error, {migration_required, _State}} ->
            io:format("recovery blocked by workflow version migration~n");
        {error, leased} ->
            ok;
        {error, Reason} ->
            io:format("recovery requeue error while waiting: ~p~n", [Reason])
    end.

warmup_atoms() ->
    %% PostgreSQL decodes payloads with binary_to_term(..., [safe]). Keep every
    %% atom this demo writes into persisted input/attempt/result terms present
    %% before recovery reads old events in a fresh VM.
    {module, bt_crash_demo_workflow} = code:ensure_loaded(bt_crash_demo_workflow),
    lists:foreach(fun ensure_demo_atom/1,
                  ["bt_crash_demo_workflow", "charge", "order_id",
                   "mode_file", "marker_file", "owner_node", "node", "pid",
                   "runner", "beamtrail_run", "attempt", "step", "recovered",
                   "nonode@nohost"]),
    ok.

ensure_demo_atom(Name) ->
    _ = list_to_atom(Name),
    ok.

log_dispatch_result({ok, _Pid}) ->
    ok;
log_dispatch_result({ok, _Pid, _Existing}) ->
    ok;
log_dispatch_result(Other) ->
    io:format("runner dispatch result: ~p~n", [Other]).

print_file(Path) ->
    case file:read_file(Path) of
        {ok, Bin} -> io:format("~s", [Bin]);
        {error, Reason} -> io:format("could not read ~s: ~p~n", [Path, Reason])
    end.

print_events(Events) ->
    lists:foreach(fun print_event/1, Events).

print_event(Event) ->
    io:format("  seq=~p type=~p step=~p payload=~p~n",
              [maps:get(event_seq, Event),
               maps:get(event_type, Event),
               maps:get(step_id, Event, undefined),
               maps:get(payload, Event, #{})]).

normalize_run_id(RunId) when is_binary(RunId) ->
    RunId;
normalize_run_id(RunId) when is_list(RunId) ->
    list_to_binary(RunId).

normalize_port(Port) when is_integer(Port) ->
    Port;
normalize_port(Port) when is_list(Port) ->
    list_to_integer(Port).
