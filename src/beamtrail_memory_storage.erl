-module(beamtrail_memory_storage).
-behaviour(gen_server).
-behaviour(beamtrail_storage).

-export([start_link/0, reset/0]).
-export([append_event/6, read_events/3, events/1, list_run_ids/0]).
-export([write_snapshot/4, read_snapshot/1]).
-export([acquire_lease/3, read_lease/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

reset() ->
    gen_server:call(?MODULE, reset).

append_event(RunId, EventType, StepId, StepVersion, IdempotencyKey, Payload) ->
    gen_server:call(?MODULE, {append_event, RunId, EventType, StepId, StepVersion, IdempotencyKey, Payload}).

read_events(RunId, FromSeq, Limit) ->
    gen_server:call(?MODULE, {read_events, RunId, FromSeq, Limit}).

events(RunId) ->
    read_events(RunId, 1, infinity).

list_run_ids() ->
    gen_server:call(?MODULE, list_run_ids).

write_snapshot(RunId, State, SnapshotSeq, SnapshotRevision) ->
    gen_server:call(?MODULE, {write_snapshot, RunId, State, SnapshotSeq, SnapshotRevision}).

read_snapshot(RunId) ->
    gen_server:call(?MODULE, {read_snapshot, RunId}).

acquire_lease(RunId, Owner, TtlMs) ->
    gen_server:call(?MODULE, {acquire_lease, RunId, Owner, TtlMs}).

read_lease(RunId) ->
    gen_server:call(?MODULE, {read_lease, RunId}).

init([]) ->
    {ok, #{events => #{}, snapshots => #{}, leases => #{}}}.

handle_call(reset, _From, _State) ->
    {reply, ok, #{events => #{}, snapshots => #{}, leases => #{}}};
handle_call({append_event, RunId, EventType, StepId, StepVersion, IdempotencyKey, Payload}, _From, State) ->
    EventsByRun = maps:get(events, State),
    RunEvents = maps:get(RunId, EventsByRun, []),
    EventSeq = length(RunEvents) + 1,
    Event =
        #{run_id => RunId,
          event_seq => EventSeq,
          event_type => EventType,
          step_id => StepId,
          step_version => StepVersion,
          idempotency_key => IdempotencyKey,
          payload => Payload,
          occurred_at => erlang:system_time(millisecond)},
    UpdatedEvents = maps:put(RunId, RunEvents ++ [Event], EventsByRun),
    {reply, {ok, Event}, State#{events := UpdatedEvents}};
handle_call({read_events, RunId, FromSeq, Limit}, _From, State) ->
    RunEvents = maps:get(RunId, maps:get(events, State), []),
    Tail = [Event || Event <- RunEvents, maps:get(event_seq, Event) >= FromSeq],
    Result =
        case Limit of
            infinity -> Tail;
            N when is_integer(N), N >= 0 -> lists:sublist(Tail, N)
        end,
    {reply, {ok, Result}, State};
handle_call(list_run_ids, _From, State) ->
    {reply, maps:keys(maps:get(events, State)), State};
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
                  fencing_token => FencingToken},
            {reply, {ok, Lease}, State#{leases := maps:put(RunId, Lease, Leases)}};
        false ->
            {reply, {error, leased}, State}
    end;
handle_call({read_lease, RunId}, _From, State) ->
    Reply =
        case maps:find(RunId, maps:get(leases, State)) of
            {ok, Lease} -> {ok, Lease};
            error -> not_found
        end,
    {reply, Reply, State};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

lease_available(undefined, _Now) ->
    true;
lease_available(Lease, Now) ->
    maps:get(lease_until, Lease) =< Now.

next_fencing_token(undefined) ->
    1;
next_fencing_token(Lease) ->
    maps:get(fencing_token, Lease) + 1.
