-module(beamtrail_memory_storage).
-behaviour(gen_server).
-behaviour(beamtrail_storage).

-export([start_link/0, reset/0]).
-export([append_event/8, read_events/3, events/1, list_run_ids/0, list_run_ids/2]).
-export([write_snapshot/4, read_snapshot/1]).
-export([acquire_lease/3, renew_lease/3, read_lease/1]).
-export([read_instance/1, read_attempts/1, telemetry_counters/0]).
-export([bump_counter/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%% In-memory append-only storage. Events are the source of truth;
%%% instances/attempts are read models derived from the same events.

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

read_instance(RunId) ->
    gen_server:call(?MODULE, {read_instance, RunId}).

read_attempts(RunId) ->
    gen_server:call(?MODULE, {read_attempts, RunId}).

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
      instances => #{},
      attempts => #{},
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
                    State2 = project_event(State1, Event),
                    {reply, {ok, Event}, State2};
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
handle_call({read_instance, RunId}, _From, State) ->
    Reply =
        case maps:find(RunId, maps:get(instances, State)) of
            {ok, Instance} -> {ok, Instance};
            error -> not_found
        end,
    {reply, Reply, State};
handle_call({read_attempts, RunId}, _From, State) ->
    Reply =
        case maps:find(RunId, maps:get(attempts, State)) of
            {ok, Attempts} -> {ok, Attempts};
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
validate_fencing(_RunId, 'recovery.skipped', undefined, _State) ->
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

%%% Read-model projection. Derived from events; never the primary write.
project_event(State, Event) ->
    State1 = project_instance(State, Event),
    project_attempt(State1, Event).

project_instance(State, Event) ->
    RunId = maps:get(run_id, Event),
    Instances = maps:get(instances, State),
    Current = maps:get(RunId, Instances, #{run_id => RunId,
                                           workflow => undefined,
                                           status => new,
                                           current_step => undefined,
                                           last_event_seq => 0,
                                           next_retry_at => undefined,
                                           failure => undefined}),
    Updated = update_instance(Current, Event),
    State#{instances := maps:put(RunId, Updated, Instances)}.

update_instance(Inst, #{event_type := 'workflow.instance.created'} = E) ->
    P = maps:get(payload, E),
    Steps = maps:get(steps, P, []),
    Inst#{workflow => maps:get(workflow, P),
          steps => Steps,
          status => running,
          current_step => first_step(Steps),
          completed_steps => 0,
          last_event_seq => maps:get(event_seq, E),
          created_at => maps:get(occurred_at, E)};
update_instance(Inst, #{event_type := 'attempt.started'} = E) ->
    Inst#{status => running,
          current_step => maps:get(step_id, E),
          next_retry_at => undefined,
          last_event_seq => maps:get(event_seq, E)};
update_instance(Inst, #{event_type := 'step.succeeded'} = E) ->
    CompletedSteps = maps:get(completed_steps, Inst, 0) + 1,
    Steps = maps:get(steps, Inst, []),
    Inst#{status => running,
          current_step => next_step(Steps, CompletedSteps),
          completed_steps => CompletedSteps,
          last_event_seq => maps:get(event_seq, E),
          failure => undefined};
update_instance(Inst, #{event_type := 'step.failed'} = E) ->
    Inst#{status => failed,
          last_event_seq => maps:get(event_seq, E),
          failure => maps:get(payload, E)};
update_instance(Inst, #{event_type := 'retry.scheduled'} = E) ->
    P = maps:get(payload, E),
    Inst#{status => retrying,
          next_retry_at => maps:get(next_retry_at, P, undefined),
          last_event_seq => maps:get(event_seq, E)};
update_instance(Inst, #{event_type := 'workflow.completed'} = E) ->
    Inst#{status => completed,
          current_step => undefined,
          last_event_seq => maps:get(event_seq, E),
          completed_at => maps:get(occurred_at, E)};
update_instance(Inst, #{event_type := 'workflow.failed'} = E) ->
    Inst#{status => failed,
          current_step => undefined,
          last_event_seq => maps:get(event_seq, E),
          terminal => true,
          failure => maps:get(payload, E)};
update_instance(Inst, E) ->
    Inst#{last_event_seq => maps:get(event_seq, E)}.

project_attempt(State, #{event_type := 'attempt.started'} = E) ->
    RunId = maps:get(run_id, E),
    Attempts = maps:get(attempts, State),
    RunAttempts = maps:get(RunId, Attempts, []),
    Attempt =
        #{step_id => maps:get(step_id, E),
          step_version => maps:get(step_version, E),
          idempotency_key => maps:get(idempotency_key, E),
          attempt => maps:get(attempt, maps:get(payload, E), 1),
          status => unknown,
          started_event_seq => maps:get(event_seq, E),
          started_at => maps:get(occurred_at, E)},
    State#{attempts := maps:put(RunId, RunAttempts ++ [Attempt], Attempts)};
project_attempt(State, #{event_type := EType} = E)
  when EType =:= 'step.succeeded'; EType =:= 'step.failed' ->
    RunId = maps:get(run_id, E),
    Attempts = maps:get(attempts, State),
    RunAttempts = maps:get(RunId, Attempts, []),
    Updated = mark_latest_attempt(RunAttempts, maps:get(step_id, E),
                                  status_for(EType), E),
    State#{attempts := maps:put(RunId, Updated, Attempts)};
project_attempt(State, _E) ->
    State.

status_for('step.succeeded') -> succeeded;
status_for('step.failed') -> failed.

%% Marks the *latest* unknown attempt for StepId. Walks the list from
%% the tail (latest first) using foldl over the reversed input; prepending
%% to the accumulator naturally restores chronological order without a
%% final reverse.
mark_latest_attempt(Attempts, StepId, Status, Event) ->
    {Acc, _Done} =
        lists:foldl(
          fun(A, {AccIn, false}) ->
                  case maps:get(step_id, A) =:= StepId
                       andalso maps:get(status, A) =:= unknown of
                      true ->
                          P = maps:get(payload, Event),
                          Updated = A#{status => Status,
                                       completed_event_seq => maps:get(event_seq, Event),
                                       completed_at => maps:get(occurred_at, Event),
                                       result => maps:get(result, P, undefined),
                                       reason => maps:get(reason, P, undefined)},
                          {[Updated | AccIn], true};
                      false ->
                          {[A | AccIn], false}
                  end;
             (A, {AccIn, true}) ->
                  {[A | AccIn], true}
          end,
          {[], false},
          lists:reverse(Attempts)),
    Acc.

first_step([]) -> undefined;
first_step([S | _]) -> S.

next_step(Steps, CompletedSteps) when CompletedSteps >= length(Steps) ->
    undefined;
next_step(Steps, CompletedSteps) ->
    lists:nth(CompletedSteps + 1, Steps).
