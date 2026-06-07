-module(bt_blocking_storage).
-behaviour(beamtrail_storage).

-export([reset/0, block_read_snapshot/1, block_append_event/1]).
-export([append_event/8, append_events/4, read_events/3, events/1,
         write_snapshot/4, read_snapshot/1, acquire_lease/3, renew_lease/3,
         release_lease/2, read_lease/1, list_run_ids/0, list_run_ids/2,
         list_recoverable_run_ids/3]).
-export([telemetry_counters/0]).

-define(TABLE, bt_blocking_storage_state).

reset() ->
    ensure_table(),
    ets:delete_all_objects(?TABLE),
    ok.

block_read_snapshot(TestPid) when is_pid(TestPid) ->
    ensure_table(),
    ets:insert(?TABLE, {read_snapshot_blocker, TestPid}),
    ok.

block_append_event(TestPid) when is_pid(TestPid) ->
    ensure_table(),
    ets:insert(?TABLE, {append_event_blocker, TestPid}),
    ok.

append_event(RunId, ExpectedSeq, FencingToken, EventType, StepId,
             StepVersion, IdempotencyKey, Payload) ->
    maybe_block_append_event(),
    beamtrail_memory_storage:append_event(RunId, ExpectedSeq, FencingToken,
                                          EventType, StepId, StepVersion,
                                          IdempotencyKey, Payload).

append_events(RunId, ExpectedSeq, FencingToken, EventSpecs) ->
    beamtrail_memory_storage:append_events(RunId, ExpectedSeq, FencingToken,
                                           EventSpecs).

read_events(RunId, FromSeq, Limit) ->
    beamtrail_memory_storage:read_events(RunId, FromSeq, Limit).

events(RunId) ->
    beamtrail_memory_storage:events(RunId).

write_snapshot(RunId, State, SnapshotSeq, SnapshotRevision) ->
    beamtrail_memory_storage:write_snapshot(RunId, State, SnapshotSeq,
                                            SnapshotRevision).

read_snapshot(RunId) ->
    maybe_block_read_snapshot(),
    beamtrail_memory_storage:read_snapshot(RunId).

acquire_lease(RunId, Owner, TtlMs) ->
    beamtrail_memory_storage:acquire_lease(RunId, Owner, TtlMs).

renew_lease(RunId, FencingToken, TtlMs) ->
    beamtrail_memory_storage:renew_lease(RunId, FencingToken, TtlMs).

release_lease(RunId, FencingToken) ->
    beamtrail_memory_storage:release_lease(RunId, FencingToken).

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

maybe_block_read_snapshot() ->
    ensure_table(),
    case ets:take(?TABLE, read_snapshot_blocker) of
        [{read_snapshot_blocker, TestPid}] ->
            TestPid ! {bt_blocking_storage, read_snapshot_blocked, self()},
            receive
                {bt_blocking_storage, continue} -> ok
            end;
        [] ->
            ok
    end.

maybe_block_append_event() ->
    ensure_table(),
    case ets:take(?TABLE, append_event_blocker) of
        [{append_event_blocker, TestPid}] ->
            TestPid ! {bt_blocking_storage, append_event_blocked, self()},
            receive
                {bt_blocking_storage, continue} -> ok
            end;
        [] ->
            ok
    end.

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
