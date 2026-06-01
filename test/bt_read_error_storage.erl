-module(bt_read_error_storage).
-behaviour(beamtrail_storage).

-export([append_event/8, read_events/3, events/1, write_snapshot/4,
         read_snapshot/1, acquire_lease/3, renew_lease/3, read_lease/1,
         list_run_ids/0, list_run_ids/2]).
-export([telemetry_counters/0]).

append_event(RunId, ExpectedSeq, FencingToken, EventType, StepId,
             StepVersion, IdempotencyKey, Payload) ->
    beamtrail_memory_storage:append_event(RunId, ExpectedSeq, FencingToken,
                                          EventType, StepId, StepVersion,
                                          IdempotencyKey, Payload).

read_events(RunId, FromSeq, Limit) ->
    beamtrail_memory_storage:read_events(RunId, FromSeq, Limit).

events(RunId) ->
    beamtrail_memory_storage:events(RunId).

write_snapshot(RunId, State, SnapshotSeq, SnapshotRevision) ->
    beamtrail_memory_storage:write_snapshot(RunId, State, SnapshotSeq,
                                            SnapshotRevision).

read_snapshot(<<"state-read-error-run">>) ->
    {error, state_read_failed};
read_snapshot(RunId) ->
    beamtrail_memory_storage:read_snapshot(RunId).

acquire_lease(RunId, Owner, TtlMs) ->
    beamtrail_memory_storage:acquire_lease(RunId, Owner, TtlMs).

renew_lease(RunId, FencingToken, TtlMs) ->
    beamtrail_memory_storage:renew_lease(RunId, FencingToken, TtlMs).

read_lease(<<"query-lease-error-run">>) ->
    {error, lease_read_failed};
read_lease(RunId) ->
    beamtrail_memory_storage:read_lease(RunId).

list_run_ids() ->
    beamtrail_memory_storage:list_run_ids().

list_run_ids(Cursor, Limit) ->
    beamtrail_memory_storage:list_run_ids(Cursor, Limit).

telemetry_counters() ->
    beamtrail_memory_storage:telemetry_counters().
