-module(beamtrail_storage).

-callback append_event(RunId :: binary(),
                       ExpectedSeq :: non_neg_integer(),
                       FencingToken :: pos_integer() | undefined,
                       EventType :: atom(),
                       StepId :: atom() | undefined,
                       StepVersion :: non_neg_integer() | undefined,
                       IdempotencyKey :: term(),
                       Payload :: map()) -> {ok, map()} | {error, term()}.
-callback read_events(RunId :: binary(), FromSeq :: pos_integer(), Limit :: pos_integer() | infinity) ->
    {ok, [map()]} | {error, term()}.
-callback write_snapshot(RunId :: binary(), State :: map(), SnapshotSeq :: non_neg_integer(),
                         SnapshotRevision :: pos_integer()) -> ok | {error, term()}.
-callback read_snapshot(RunId :: binary()) -> {ok, map()} | not_found | {error, term()}.
-callback acquire_lease(RunId :: binary(), Owner :: term(), TtlMs :: pos_integer()) ->
    {ok, map()} | {error, leased}.
-callback read_lease(RunId :: binary()) -> {ok, map()} | not_found.
-callback list_run_ids() -> [binary()].

%% Optional read-model accessors. Derived from the append-only event log,
%% never written as the primary source of truth.
-optional_callbacks([read_instance/1, read_attempts/1, telemetry_counters/0]).
-callback read_instance(RunId :: binary()) -> {ok, map()} | not_found.
-callback read_attempts(RunId :: binary()) -> {ok, [map()]} | not_found.
-callback telemetry_counters() -> map().
