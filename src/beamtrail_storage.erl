-module(beamtrail_storage).

-export_type([run_id/0, event/0, event_spec/0, snapshot/0, lease/0,
              run_id_page/0]).

-type run_id() :: beamtrail_workflow:run_id().
-type event() :: map().
-type event_spec() ::
    #{event_type := atom(),
      step_id => beamtrail_workflow:step_id() | undefined,
      step_version => beamtrail_workflow:step_version() | undefined,
      idempotency_key => term(),
      payload => map()}.
-type snapshot() :: map().
-type lease() :: map().
-type run_id_page() ::
    #{run_ids := [run_id()],
      next_cursor := run_id() | undefined,
      has_more := boolean()}.

-callback append_event(RunId :: run_id(),
                       ExpectedSeq :: non_neg_integer(),
                       FencingToken :: pos_integer() | undefined,
                       EventType :: atom(),
                       StepId :: beamtrail_workflow:step_id() | undefined,
                       StepVersion :: beamtrail_workflow:step_version() | undefined,
                       IdempotencyKey :: term(),
                       Payload :: map()) -> {ok, event()} | {error, term()}.
-callback append_events(RunId :: run_id(),
                        ExpectedSeq :: non_neg_integer(),
                        FencingToken :: pos_integer() | undefined,
                        EventSpecs :: [event_spec()]) ->
    {ok, [event()]} | {error, term()}.
-callback read_events(RunId :: run_id(), FromSeq :: pos_integer(),
                       Limit :: pos_integer() | infinity) ->
    {ok, [event()]} | {error, term()}.
-callback events(RunId :: run_id()) -> {ok, [event()]} | {error, term()}.
-callback write_snapshot(RunId :: run_id(), State :: map(), SnapshotSeq :: non_neg_integer(),
                         SnapshotRevision :: pos_integer()) -> ok | {error, term()}.
-callback read_snapshot(RunId :: run_id()) -> {ok, snapshot()} | not_found | {error, term()}.
-callback acquire_lease(RunId :: run_id(), Owner :: term(), TtlMs :: pos_integer()) ->
    {ok, lease()} | {error, term()}.
-callback renew_lease(RunId :: run_id(), FencingToken :: pos_integer(), TtlMs :: pos_integer()) ->
    {ok, lease()} | {error, no_lease | stale_fence | term()}.
-callback release_lease(RunId :: run_id(), FencingToken :: pos_integer()) ->
    ok | {error, no_lease | stale_fence | term()}.
-callback read_lease(RunId :: run_id()) -> {ok, lease()} | not_found | {error, term()}.
-callback list_run_ids() -> {ok, [run_id()]} | {error, term()}.
-callback list_run_ids(Cursor :: run_id() | undefined, Limit :: pos_integer()) ->
    {ok, run_id_page()} | {error, term()}.

%% Optional indexed recovery scan. Returns a page of run ids that are coarse
%% recovery candidates at NowMs: not terminal, not parked, not waiting on a
%% future retry, and not currently leased. This avoids per-run snapshot/replay
%% during scans; the event log remains the source of truth, and
%% beamtrail:list_recoverable/2 still applies the precise recoverable/2 check
%% (including the live-code migration gate) to each returned candidate. Adapters
%% that do not export it fall back to the replay-based scan in beamtrail.
-callback list_recoverable_run_ids(Cursor :: run_id() | undefined,
                                   Limit :: pos_integer(),
                                   NowMs :: integer()) ->
    {ok, run_id_page()} | {error, term()}.

%% Optional observability accessors. Never part of the primary source of truth.
-optional_callbacks([release_lease/2, list_recoverable_run_ids/3,
                     telemetry_counters/0]).
-callback telemetry_counters() -> map().
