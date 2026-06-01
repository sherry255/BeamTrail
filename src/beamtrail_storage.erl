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
-callback events(RunId :: binary()) -> {ok, [map()]} | {error, term()}.
-callback write_snapshot(RunId :: binary(), State :: map(), SnapshotSeq :: non_neg_integer(),
                         SnapshotRevision :: pos_integer()) -> ok | {error, term()}.
-callback read_snapshot(RunId :: binary()) -> {ok, map()} | not_found | {error, term()}.
-callback acquire_lease(RunId :: binary(), Owner :: term(), TtlMs :: pos_integer()) ->
    {ok, map()} | {error, term()}.
-callback renew_lease(RunId :: binary(), FencingToken :: pos_integer(), TtlMs :: pos_integer()) ->
    {ok, map()} | {error, no_lease | stale_fence | term()}.
-callback read_lease(RunId :: binary()) -> {ok, map()} | not_found | {error, term()}.
-callback list_run_ids() -> {ok, [binary()]} | {error, term()}.
-callback list_run_ids(Cursor :: binary() | undefined, Limit :: pos_integer()) ->
    {ok, #{run_ids := [binary()],
           next_cursor := binary() | undefined,
           has_more := boolean()}}.

%% Optional observability accessors. Never part of the primary source of truth.
-optional_callbacks([telemetry_counters/0]).
-callback telemetry_counters() -> map().
