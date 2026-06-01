-module(beamtrail_memory_storage).
-behaviour(gen_server).
-behaviour(beamtrail_storage).

-export([start_link/0, reset/0]).
-export([append_event/8, read_events/3, events/1, list_run_ids/0, list_run_ids/2]).
-export([write_snapshot/4, read_snapshot/1]).
-export([acquire_lease/3, renew_lease/3, read_lease/1]).
-export([telemetry_counters/0]).
-export([bump_counter/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%% In-memory append-only storage. Events are the source of truth; runtime
%%% state/read models are derived through beamtrail_reducer.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

reset() ->
    gen_server:call(?MODULE, reset).

append_event(RunId, ExpectedSeq, FencingToken,
             EventType, StepId, StepVersion, IdempotencyKey, Payload) ->
    gen_server:call(?MODULE,
        {append_event, RunId, ExpectedSeq, FencingToken,
         EventType, StepId, StepVersion, IdempotencyKey, Payload}).

read_events(RunId, FromSeq, Limit) ->
    gen_server:call(?MODULE, {read_events, RunId, FromSeq, Limit}).

events(RunId) ->
    read_events(RunId, 1, infinity).

list_run_ids() ->
    gen_server:call(?MODULE, list_run_ids).

list_run_ids(Cursor, Limit) ->
    gen_server:call(?MODULE, {list_run_ids, Cursor, Limit}).

write_snapshot(RunId, State, SnapshotSeq, SnapshotRevision) ->
    gen_server:call(?MODULE,
        {write_snapshot, RunId, State, SnapshotSeq, SnapshotRevision}).

read_snapshot(RunId) ->
    gen_server:call(?MODULE, {read_snapshot, RunId}).

acquire_lease(RunId, Owner, TtlMs) ->
    gen_server:call(?MODULE, {acquire_lease, RunId, Owner, TtlMs}).

renew_lease(RunId, FencingToken, TtlMs) ->
    gen_server:call(?MODULE, {renew_lease, RunId, FencingToken, TtlMs}).

read_lease(RunId) ->
    gen_server:call(?MODULE, {read_lease, RunId}).

telemetry_counters() ->
    gen_server:call(?MODULE, telemetry_counters).

bump_counter(Name, By) ->
    gen_server:cast(?MODULE, {bump_counter, Name, By}).

init([]) ->
    {ok, empty_state()}.

empty_state() ->
    #{events => #{},
      event_counts => #{},
      snapshots => #{},
      leases => #{},
      counters => #{}}.

handle_call(reset, _From, _State) ->
    {reply, ok, empty_state()};
handle_call({append_event, RunId, ExpectedSeq, FencingToken,
             EventType, StepId, StepVersion, IdempotencyKey, Payload}, _From, State) ->
    EventsByRun = maps:get(events, State),
    EventCounts = maps:get(event_counts, State),
    RunEvents = maps:get(RunId, EventsByRun, []),
    ActualSeq = maps:get(RunId, EventCounts, 0),
    case ExpectedSeq =:= ActualSeq of
        false ->
            {reply, {error, {conflict, #{expected_seq => ExpectedSeq,
                                         actual_seq => ActualSeq}}},
             State};
        true ->
            case validate_fencing(RunId, EventType, FencingToken, State) of
                ok ->
                    EventSeq = ActualSeq + 1,
                    Event =
                        #{run_id => RunId,
                          event_seq => EventSeq,
                          event_type => EventType,
                          step_id => StepId,
                          step_version => StepVersion,
                          idempotency_key => IdempotencyKey,
                          fencing_token => FencingToken,
                          payload => Payload,
                          occurred_at => erlang:system_time(millisecond)},
                    State1 =
                        State#{events := maps:put(RunId, [Event | RunEvents], EventsByRun),
                               event_counts := maps:put(RunId, EventSeq, EventCounts)},
                    {reply, {ok, Event}, State1};
                {error, _} = Error ->
                    {reply, Error, State}
            end
    end;
handle_call({read_events, RunId, FromSeq, Limit}, _From, State) ->
    RunEvents = lists:reverse(maps:get(RunId, maps:get(events, State), [])),
    Tail = [E || E <- RunEvents, maps:get(event_seq, E) >= FromSeq],
    Result =
        case Limit of
            infinity -> Tail;
            N when is_integer(N), N >= 0 -> lists:sublist(Tail, N)
        end,
    {reply, {ok, Result}, State};
handle_call(list_run_ids, _From, State) ->
    {reply, {ok, maps:keys(maps:get(events, State))}, State};
handle_call({list_run_ids, Cursor, Limit}, _From, State) ->
    All = lists:sort(maps:keys(maps:get(events, State))),
    AfterCursor = case Cursor of
                      undefined -> All;
                      _ -> [RunId || RunId <- All, RunId > Cursor]
                  end,
    Page = lists:sublist(AfterCursor, Limit),
    HasMore = length(AfterCursor) > length(Page),
    NextCursor = case Page of
                     [] -> undefined;
                     _ -> lists:last(Page)
                 end,
    {reply, {ok, #{run_ids => Page,
                   next_cursor => NextCursor,
                   has_more => HasMore}},
     State};
handle_call({write_snapshot, RunId, SnapshotState, SnapshotSeq, SnapshotRevision}, _From, State) ->
    Snapshot =
        #{run_id => RunId,
          snapshot_seq => SnapshotSeq,
          snapshot_revision => SnapshotRevision,
          state => SnapshotState,
          written_at => erlang:system_time(millisecond)},
    Snapshots = maps:put(RunId, Snapshot, maps:get(snapshots, State)),
    {reply, ok, State#{snapshots := Snapshots}};
handle_call({read_snapshot, RunId}, _From, State) ->
    Reply =
        case maps:find(RunId, maps:get(snapshots, State)) of
            {ok, Snapshot} -> {ok, Snapshot};
            error -> not_found
        end,
    {reply, Reply, State};
handle_call({acquire_lease, RunId, Owner, TtlMs}, _From, State) ->
    Now = erlang:system_time(millisecond),
    Leases = maps:get(leases, State),
    Current = maps:get(RunId, Leases, undefined),
    case lease_available(Current, Now) of
        true ->
            FencingToken = next_fencing_token(Current),
            Lease =
                #{run_id => RunId,
                  owner_node => Owner,
                  lease_until => Now + TtlMs,
                  fencing_token => FencingToken,
                  acquired_at => Now},
            {reply, {ok, Lease}, State#{leases := maps:put(RunId, Lease, Leases)}};
        false ->
            {reply, {error, leased}, State}
    end;
handle_call({renew_lease, RunId, FencingToken, TtlMs}, _From, State) ->
    Now = erlang:system_time(millisecond),
    Leases = maps:get(leases, State),
    case maps:get(RunId, Leases, undefined) of
        undefined ->
            {reply, {error, no_lease}, State};
        #{lease_until := LeaseUntil} when LeaseUntil =< Now ->
            {reply, {error, lease_expired}, State};
        #{fencing_token := FencingToken} = Current when is_integer(FencingToken) ->
            Renewed = Current#{lease_until := Now + TtlMs,
                               renewed_at => Now},
            {reply, {ok, Renewed},
             State#{leases := maps:put(RunId, Renewed, Leases)}};
        #{fencing_token := CurrentFence} when FencingToken < CurrentFence ->
            {reply, {error, stale_fence}, State};
        #{fencing_token := CurrentFence} ->
            {reply, {error, {invalid_fence, #{provided => FencingToken,
                                              current => CurrentFence}}},
             State}
    end;
handle_call({read_lease, RunId}, _From, State) ->
    Reply =
        case maps:find(RunId, maps:get(leases, State)) of
            {ok, Lease} -> {ok, Lease};
            error -> not_found
        end,
    {reply, Reply, State};
handle_call(telemetry_counters, _From, State) ->
    {reply, maps:get(counters, State), State};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast({bump_counter, Name, By}, State) ->
    Counters = maps:get(counters, State),
    Updated = maps:update_with(Name, fun(V) -> V + By end, By, Counters),
    {noreply, State#{counters := Updated}};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

lease_available(undefined, _Now) -> true;
lease_available(Lease, Now) -> maps:get(lease_until, Lease) =< Now.

next_fencing_token(undefined) -> 1;
next_fencing_token(Lease) -> maps:get(fencing_token, Lease) + 1.

validate_fencing(_RunId, 'workflow.instance.created', undefined, _State) ->
    ok;
validate_fencing(RunId, _EventType, FencingToken, State) when is_integer(FencingToken) ->
    Now = erlang:system_time(millisecond),
    case maps:get(RunId, maps:get(leases, State), undefined) of
        undefined ->
            {error, no_lease};
        #{lease_until := LeaseUntil} when LeaseUntil =< Now ->
            {error, lease_expired};
        #{fencing_token := FencingToken} ->
            ok;
        #{fencing_token := Current} when FencingToken < Current ->
            {error, stale_fence};
        #{fencing_token := Current} ->
            {error, {invalid_fence, #{provided => FencingToken,
                                      current => Current}}}
    end;
validate_fencing(_RunId, _EventType, undefined, _State) ->
    {error, missing_fence}.
