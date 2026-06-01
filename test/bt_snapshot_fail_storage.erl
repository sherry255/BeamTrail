-module(bt_snapshot_fail_storage).
-behaviour(beamtrail_storage).

-export([append_event/8, read_events/3, events/1, write_snapshot/4,
         read_snapshot/1, acquire_lease/3, renew_lease/3, read_lease/1,
         list_run_ids/0, list_run_ids/2]).

append_event(RunId, ExpectedSeq, FencingToken,
             EventType, StepId, StepVersion, IdempotencyKey, Payload) ->
    beamtrail_memory_storage:append_event(RunId, ExpectedSeq, FencingToken,
                                          EventType, StepId, StepVersion,
                                          IdempotencyKey, Payload).

read_events(RunId, FromSeq, Limit) ->
    beamtrail_memory_storage:read_events(RunId, FromSeq, Limit).

events(RunId) ->
    beamtrail_memory_storage:events(RunId).

write_snapshot(_RunId, _State, _SnapshotSeq, _SnapshotRevision) ->
    {error, snapshot_write_failed}.

read_snapshot(RunId) ->
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
