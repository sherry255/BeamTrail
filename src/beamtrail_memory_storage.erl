-module(beamtrail_memory_storage).
-behaviour(gen_server).
-behaviour(beamtrail_storage).

-export([start_link/0, reset/0]).
-export([append_event/8, append_events/4, read_events/3, events/1,
         list_run_ids/0, list_run_ids/2, list_recoverable_run_ids/3]).
-export([write_snapshot/4, read_snapshot/1]).
-export([acquire_lease/3, renew_lease/3, release_lease/2, read_lease/1]).
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

append_events(RunId, ExpectedSeq, FencingToken, EventSpecs) ->
    gen_server:call(?MODULE,
                    {append_events, RunId, ExpectedSeq, FencingToken, EventSpecs}).

read_events(RunId, FromSeq, Limit) ->
    gen_server:call(?MODULE, {read_events, RunId, FromSeq, Limit}).

events(RunId) ->
    read_events(RunId, 1, infinity).

list_run_ids() ->
    gen_server:call(?MODULE, list_run_ids).

list_run_ids(Cursor, Limit) ->
    gen_server:call(?MODULE, {list_run_ids, Cursor, Limit}).

list_recoverable_run_ids(Cursor, Limit, NowMs) ->
    gen_server:call(?MODULE, {list_recoverable_run_ids, Cursor, Limit, NowMs}).

write_snapshot(RunId, State, SnapshotSeq, SnapshotRevision) ->
    gen_server:call(?MODULE,
        {write_snapshot, RunId, State, SnapshotSeq, SnapshotRevision}).

read_snapshot(RunId) ->
    gen_server:call(?MODULE, {read_snapshot, RunId}).

acquire_lease(RunId, Owner, TtlMs) ->
    gen_server:call(?MODULE, {acquire_lease, RunId, Owner, TtlMs}).

renew_lease(RunId, FencingToken, TtlMs) ->
    gen_server:call(?MODULE, {renew_lease, RunId, FencingToken, TtlMs}).

release_lease(RunId, FencingToken) ->
    gen_server:call(?MODULE, {release_lease, RunId, FencingToken}).

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
      counters => #{},
      %% Per-run reduced state, folded through beamtrail_reducer at append time.
      %% Used only as a recovery-scan index; the event log stays authoritative.
      projections => #{}}.

handle_call(reset, _From, _State) ->
    {reply, ok, empty_state()};
handle_call({append_event, RunId, ExpectedSeq, FencingToken,
             EventType, StepId, StepVersion, IdempotencyKey, Payload}, _From, State) ->
    Spec = #{event_type => EventType,
             step_id => StepId,
             step_version => StepVersion,
             idempotency_key => IdempotencyKey,
             payload => Payload},
    case append_events_to_state(RunId, ExpectedSeq, FencingToken, [Spec], State) of
        {{ok, [Event]}, State1} -> {reply, {ok, Event}, State1};
        {{error, _} = Error, State1} -> {reply, Error, State1}
    end;
handle_call({append_events, RunId, ExpectedSeq, FencingToken, EventSpecs}, _From, State) ->
    {Reply, State1} =
        append_events_to_state(RunId, ExpectedSeq, FencingToken, EventSpecs, State),
    {reply, Reply, State1};
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
handle_call({list_recoverable_run_ids, Cursor, Limit, NowMs}, _From, State) ->
    Projections = maps:get(projections, State),
    Leases = maps:get(leases, State),
    Candidates =
        lists:sort(
          [RunId
           || RunId <- maps:keys(Projections),
              recoverable_candidate(maps:get(RunId, Projections),
                                    maps:get(RunId, Leases, undefined),
                                    NowMs)]),
    AfterCursor = case Cursor of
                      undefined -> Candidates;
                      _ -> [RunId || RunId <- Candidates, RunId > Cursor]
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
handle_call({release_lease, RunId, FencingToken}, _From, State) ->
    Now = erlang:system_time(millisecond),
    Leases = maps:get(leases, State),
    case maps:get(RunId, Leases, undefined) of
        undefined ->
            {reply, {error, no_lease}, State};
        #{fencing_token := FencingToken} = Lease ->
            Lease1 = Lease#{lease_until := Now},
            {reply, ok, State#{leases := maps:put(RunId, Lease1, Leases)}};
        #{fencing_token := Current} when FencingToken < Current ->
            {reply, {error, stale_fence}, State};
        #{fencing_token := Current} ->
            {reply, {error, {invalid_fence, #{provided => FencingToken,
                                              current => Current}}},
             State}
    end;
handle_call(telemetry_counters, _From, State) ->
    {reply, maps:get(counters, State), State};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

append_events_to_state(_RunId, _ExpectedSeq, _FencingToken, [], State) ->
    {{ok, []}, State};
append_events_to_state(RunId, ExpectedSeq, FencingToken, EventSpecs, State) ->
    EventsByRun = maps:get(events, State),
    EventCounts = maps:get(event_counts, State),
    RunEvents = maps:get(RunId, EventsByRun, []),
    ActualSeq = maps:get(RunId, EventCounts, 0),
    case ExpectedSeq =:= ActualSeq of
        false ->
            {{error, {conflict, #{expected_seq => ExpectedSeq,
                                  actual_seq => ActualSeq}}},
             State};
        true ->
            case validate_fencing_for_specs(RunId, FencingToken, EventSpecs, State) of
                ok ->
                    Events = build_events(RunId, ActualSeq, FencingToken, EventSpecs),
                    EventSeq = ActualSeq + length(Events),
                    State1 =
                        State#{events := maps:put(RunId, lists:reverse(Events) ++ RunEvents,
                                                  EventsByRun),
                               event_counts := maps:put(RunId, EventSeq, EventCounts),
                               projections := update_projection(RunId, Events,
                                                                maps:get(projections, State))},
                    {{ok, Events}, State1};
                {error, _} = Error ->
                    {Error, State}
            end
    end.

build_events(RunId, ActualSeq, FencingToken, EventSpecs) ->
    {Events, _Seq} =
        lists:mapfoldl(fun(Spec, Seq) ->
                               EventSeq = Seq + 1,
                               {#{run_id => RunId,
                                  event_seq => EventSeq,
                                  event_type => maps:get(event_type, Spec),
                                  step_id => maps:get(step_id, Spec, undefined),
                                  step_version => maps:get(step_version, Spec, undefined),
                                  idempotency_key => maps:get(idempotency_key, Spec, undefined),
                                  fencing_token => FencingToken,
                                  payload => maps:get(payload, Spec),
                                  occurred_at => erlang:system_time(millisecond)},
                                EventSeq}
                       end,
                       ActualSeq,
                       EventSpecs),
    Events.

validate_fencing_for_specs(_RunId, _FencingToken, [], _State) ->
    ok;
validate_fencing_for_specs(RunId, FencingToken, [Spec | Rest], State) ->
    EventType = maps:get(event_type, Spec),
    case validate_fencing(RunId, EventType, FencingToken, State) of
        ok -> validate_fencing_for_specs(RunId, FencingToken, Rest, State);
        {error, _} = Error -> Error
    end.

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

%% Fold the newly appended events onto the run's prior reduced state. The
%% reducer is the authority for run state; this only keeps a derived copy for
%% the recovery-scan index. Migration is deliberately excluded: it depends on
%% live deployed code, so beamtrail_state:enrich applies it at load time and
%% recoverable/2 re-checks it on candidates.
update_projection(RunId, Events, Projections) ->
    Prior = maps:get(RunId, Projections, beamtrail_reducer:new()),
    Reduced = lists:foldl(fun(Event, Acc) ->
                                  beamtrail_reducer:apply_event(Acc, Event)
                          end,
                          Prior,
                          Events),
    maps:put(RunId, Reduced, Projections).

%% Coarse recovery candidate test mirroring beamtrail:recoverable_by_status/1
%% and lease_recoverable/1, minus the migration gate (applied later from live
%% code). It is a safe over-approximation: it never excludes a genuinely
%% recoverable run.
recoverable_candidate(Reduced, Lease, NowMs) ->
    maps:get(parked, Reduced, false) =/= true
        andalso candidate_status(Reduced, NowMs)
        andalso lease_open(Lease, NowMs).

candidate_status(Reduced, NowMs) ->
    case maps:get(terminal, Reduced, false) of
        true ->
            false;
        false ->
            case maps:get(status, Reduced) of
                retrying -> maps:get(next_retry_at, Reduced, 0) =< NowMs;
                waiting -> false;
                _ -> true
            end
    end.

lease_open(undefined, _NowMs) -> true;
lease_open(Lease, NowMs) -> maps:get(lease_until, Lease) =< NowMs.

lease_available(undefined, _Now) -> true;
lease_available(Lease, Now) -> maps:get(lease_until, Lease) =< Now.

next_fencing_token(undefined) -> 1;
next_fencing_token(Lease) -> maps:get(fencing_token, Lease) + 1.

validate_fencing(_RunId, 'workflow.instance.created', undefined, _State) ->
    ok;
validate_fencing(_RunId, 'signal.received', undefined, _State) ->
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
