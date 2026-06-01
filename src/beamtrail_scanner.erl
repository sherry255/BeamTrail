-module(beamtrail_scanner).
-behaviour(gen_server).

%% Background recovery scanner. Periodically scans for unfinished runs and
%% re-dispatches them via beamtrail_worker_sup. The append-only event log
%% remains the only source of truth for run progress, but the scanner does
%% append durable recovery.requeued / recovery.skipped markers (and acquires
%% a lease) so each scan decision is observable in the inspector. Dispatch
%% execution happens on a separate worker process, so a slow run cannot
%% stall the scanner.

-export([start_link/0, start_link/1, scan_now/0, set_interval/1, last_scan/0, stop/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(DEFAULT_INTERVAL_MS, 5000).
-define(DEFAULT_BATCH_SIZE, 100).

start_link() ->
    start_link(#{interval_ms => ?DEFAULT_INTERVAL_MS, auto_start => true}).

start_link(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Opts, []).

scan_now() ->
    gen_server:call(?MODULE, scan_now, 30000).

set_interval(Ms) ->
    gen_server:call(?MODULE, {set_interval, Ms}).

last_scan() ->
    gen_server:call(?MODULE, last_scan).

stop() ->
    gen_server:stop(?MODULE).

init(Opts) ->
    Interval = maps:get(interval_ms, Opts, ?DEFAULT_INTERVAL_MS),
    BatchSize = maps:get(batch_size, Opts, ?DEFAULT_BATCH_SIZE),
    Auto = maps:get(auto_start, Opts, true),
    State =
        #{interval_ms => Interval,
          batch_size => BatchSize,
          cursor => undefined,
          timer => undefined,
          last_scan_at => undefined,
          last_requeued => []},
    State1 = case Auto of
                 true -> arm_timer(State);
                 false -> State
             end,
    {ok, State1}.

handle_call(scan_now, _From, State) ->
    {Reply, State1} = do_scan(State),
    {reply, Reply, State1};
handle_call({set_interval, Ms}, _From, State) ->
    cancel_timer(State),
    State1 = arm_timer(State#{interval_ms := Ms}),
    {reply, ok, State1};
handle_call(last_scan, _From, State) ->
    {reply, #{at => maps:get(last_scan_at, State),
              requeued => maps:get(last_requeued, State),
              cursor => maps:get(cursor, State),
              batch_size => maps:get(batch_size, State)}, State};
handle_call(_, _, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_, State) -> {noreply, State}.

handle_info(scan_tick, State) ->
    {_R, State1} = do_scan(State),
    State2 = arm_timer(State1),
    {noreply, State2};
handle_info(_, State) -> {noreply, State}.

terminate(_, State) ->
    cancel_timer(State),
    ok.

code_change(_, S, _) -> {ok, S}.

do_scan(State) ->
    %% Detect one cursor page synchronously, then hand recoverable runs to a
    %% supervised worker via beamtrail_worker_sup. Execution does NOT happen
    %% on the scanner process, so a slow run can't stall scans.
    Cursor = maps:get(cursor, State, undefined),
    BatchSize = maps:get(batch_size, State, ?DEFAULT_BATCH_SIZE),
    Result =
        try beamtrail:list_recoverable(Cursor, BatchSize) of
            {ok, #{run_ids := RunIds} = Page} ->
                Spawned = [requeue(RunId) || RunId <- RunIds],
                {ok, [R || {R, ok} <- Spawned], Page}
        catch
            _:Reason -> {error, Reason}
        end,
    Requeued1 = case Result of {ok, Rs, _Page} -> Rs; _ -> [] end,
    Cursor1 = case Result of
                  {ok, _, #{has_more := true, next_cursor := Next}} -> Next;
                  {ok, _, _} -> undefined;
                  _ -> Cursor
              end,
    Reply = case Result of
                {ok, Runs, _} -> {ok, Runs};
                Error -> Error
            end,
    {Reply, State#{last_scan_at := erlang:system_time(millisecond),
                   last_requeued := Requeued1,
                   cursor := Cursor1}}.

requeue(RunId) ->
    try
        case beamtrail:mark_recovery_requeued_with_lease(RunId) of
            {ok, {requeued, Lease}} ->
                Spawn = case whereis(beamtrail_worker_sup) of
                            undefined ->
                                _ = proc_lib:spawn(beamtrail_worker, run, [RunId, Lease]),
                                ok;
                            _Pid ->
                                case beamtrail_worker_sup:dispatch_async(RunId, Lease) of
                                    {ok, _} -> ok;
                                    {ok, _, _} -> ok;
                                    Other -> Other
                                end
                        end,
                {RunId, Spawn};
            {ok, skipped} ->
                {RunId, skipped_lease_contention}
        end
    catch
        _:R -> {RunId, {error, R}}
    end.

arm_timer(#{interval_ms := infinity} = State) ->
    State#{timer := undefined};
arm_timer(#{interval_ms := Ms} = State) when is_integer(Ms), Ms > 0 ->
    TRef = erlang:send_after(Ms, self(), scan_tick),
    State#{timer := TRef}.

cancel_timer(#{timer := undefined}) -> ok;
cancel_timer(#{timer := T}) -> erlang:cancel_timer(T), ok.
