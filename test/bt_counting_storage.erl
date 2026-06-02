-module(bt_counting_storage).
-behaviour(beamtrail_storage).

-export([reset_counts/0, counts/0]).
-export([append_event/8, append_events/4, read_events/3, events/1, write_snapshot/4,
         read_snapshot/1, acquire_lease/3, renew_lease/3, read_lease/1,
         list_run_ids/0, list_run_ids/2, list_recoverable_run_ids/3]).
-export([telemetry_counters/0]).

-define(TABLE, bt_counting_storage_counts).

reset_counts() ->
    ensure_table(),
    ets:delete_all_objects(?TABLE),
    ok.

counts() ->
    ensure_table(),
    maps:from_list(ets:tab2list(?TABLE)).

append_event(RunId, ExpectedSeq, FencingToken, EventType, StepId,
             StepVersion, IdempotencyKey, Payload) ->
    beamtrail_memory_storage:append_event(RunId, ExpectedSeq, FencingToken,
                                          EventType, StepId, StepVersion,
                                          IdempotencyKey, Payload).

append_events(RunId, ExpectedSeq, FencingToken, EventSpecs) ->
    beamtrail_memory_storage:append_events(RunId, ExpectedSeq, FencingToken,
                                           EventSpecs).

read_events(RunId, FromSeq, Limit) ->
    bump(read_events),
    beamtrail_memory_storage:read_events(RunId, FromSeq, Limit).

events(RunId) ->
    bump(events),
    beamtrail_memory_storage:events(RunId).

write_snapshot(RunId, State, SnapshotSeq, SnapshotRevision) ->
    beamtrail_memory_storage:write_snapshot(RunId, State, SnapshotSeq,
                                            SnapshotRevision).

read_snapshot(RunId) ->
    bump(read_snapshot),
    beamtrail_memory_storage:read_snapshot(RunId).

acquire_lease(RunId, Owner, TtlMs) ->
    beamtrail_memory_storage:acquire_lease(RunId, Owner, TtlMs).

renew_lease(RunId, FencingToken, TtlMs) ->
    beamtrail_memory_storage:renew_lease(RunId, FencingToken, TtlMs).

read_lease(RunId) ->
    beamtrail_memory_storage:read_lease(RunId).

list_run_ids() ->
    beamtrail_memory_storage:list_run_ids().

list_run_ids(Cursor, Limit) ->
    beamtrail_memory_storage:list_run_ids(Cursor, Limit).

list_recoverable_run_ids(Cursor, Limit, NowMs) ->
    beamtrail_memory_storage:list_recoverable_run_ids(Cursor, Limit, NowMs).

telemetry_counters() ->
    beamtrail_memory_storage:telemetry_counters().

bump(Key) ->
    ensure_table(),
    ets:update_counter(?TABLE, Key, {2, 1}, {Key, 0}),
    ok.

ensure_table() ->
    case ets:info(?TABLE) of
        undefined ->
            try ets:new(?TABLE, [named_table, public, set]) of
                _ -> ok
            catch
                error:badarg -> ok
            end;
        _ ->
            ok
    end.
