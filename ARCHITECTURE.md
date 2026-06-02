# Architecture and Failure Model

BeamTrail is a durable step runner for Erlang/OTP systems. It uses OTP
processes for the hot execution path and PostgreSQL for the durable recovery
boundary.

This document explains where each boundary sits and why the event log exists
even though Erlang already has supervisors and state machines.

## Design Position

OTP supervision is the right tool for recovering live processes inside one
running VM. It does not persist business progress across VM restart, node loss,
deployment replacement, or disk failure on the application host.

BeamTrail therefore treats the PostgreSQL event log as the source of truth. A
supervised runner can cache reduced state and own timers while it is alive, but
the runner is a fast path. If it disappears, another process can rebuild the run
from persisted events and continue from the last recorded boundary.

The split is intentional:

- OTP owns live execution, timers, isolation, and process cleanup.
- PostgreSQL owns durable history, append ordering, leases, fencing, and
  cross-process recovery.

## Runtime Layers

`beamtrail_run` is the active per-run `gen_statem`. It owns the local execution
loop for one run: acquiring or reusing a lease, preparing the next transition,
starting step execution, handling retry timers, renewing the lease, and stopping
when the run reaches a terminal state.

`beamtrail:start_workflow/3` dispatches to this supervised runner path by
default. Its `{ok, RunId}` return means the run creation event was committed and
the runner was accepted for execution; completion is observed later through
`beamtrail:get_state/1`, `beamtrail_query:describe/1`, events, or telemetry.
The synchronous `beamtrail:dispatch/1` path remains an explicit low-level driver
for tests and fallback tooling.

Run-control APIs (`cancel_run/2`, `park_run/2`, `resume_run/1`, and
`requeue_run/2`) are durable state transitions. A call first targets a local
active runner, if one exists, so the runner can stop in-flight step execution and
append the control event with its current lease. If there is no local runner, the
API falls back to acquiring the storage lease and appending the event directly.
Control APIs do not force a live remote owner; cross-node handoff still happens
through lease expiry and fencing.

`beamtrail_transition` is the durable transition layer. It decides which events
to append for a state transition and applies those events to the reduced state.
It is deliberately separate from `beamtrail_run` so the same transition rules can
be used by active runners and fallback dispatch paths.

`beamtrail_state` loads a run by reading the latest compatible snapshot and
replaying the remaining event tail through `beamtrail_reducer`. Snapshots are an
optimization only. If a snapshot revision is obsolete, the run is rebuilt from
events.

`beamtrail_storage` is the storage contract. The PostgreSQL adapter is the
durable adapter; the memory adapter is for tests and local development.

`beamtrail_scanner` is a recovery trigger. It scans for runs that are unfinished
and whose lease is missing or expired, records a `recovery.requeued` marker after
acquiring a lease, and dispatches the run through the active runner supervisor.

## Event Log

Each run is an append-only stream keyed by `{run_id, event_seq}`. The reducer is
the only authority for deriving run state from that stream.

Important event types:

- `workflow.instance.created` records the workflow module, original input, and
  the linear step list.
- `attempt.started` records that a step attempt crossed the execution boundary.
- `step.succeeded` records a completed step and advances to the next step.
- `step.failed` records a failed attempt.
- `retry.scheduled` records the retry decision and `next_retry_at`.
- `workflow.completed` and `workflow.failed` are terminal decisions.
- `workflow.cancelled` is a terminal operator decision.
- `workflow.parked` and `workflow.resumed` gate automatic dispatch and recovery
  without changing the underlying run status.
- `recovery.requeued` records an observable recovery takeover decision.

Failure decisions are written atomically. A failed step and its retry-or-terminal
decision are appended in one storage call, so a crash cannot leave a persistent
state where `step.failed` is recorded but the retry/terminal decision is missing.

Workflow callback errors are captured at the engine boundary. `steps/1` errors
return a structured create failure before the run is written. Runtime callback
errors from `step_version/1`, `idempotency_key/3`, or `timeout_ms/1` are written
as terminal workflow failures; if an attempt was already opened, it is closed
with `step.failed` in the same batch.

## Concurrency Control

BeamTrail uses three layers for write safety.

First, every append includes `expected_seq`. Storage rejects the append if the
current event stream tail is not the expected sequence. This catches concurrent
writers that made decisions from stale state.

Second, PostgreSQL serializes appends for one run with a per-run row lock in
`workflow_runs`. Different runs can append concurrently, but one run has one
serialized append path.

Third, non-creation appends require a valid fencing token from the current
lease. If an old owner resumes after a newer owner acquired the run, stale writes
are rejected even if the old process is still alive.

These layers are complementary. The local registry avoids duplicate active
runners on one BEAM node, but it is not the correctness boundary. Storage
fencing is.

## Leases and Fencing

A lease grants temporary ownership of a run. Each successful acquisition carries
a monotonically increasing fencing token. Append operations carry that token, and
storage validates that it is still current and unexpired.

Active runners renew their lease while waiting for retry timers and while a step
is executing. If renewal fails, the runner stops advancing the run. A later
scanner or runner can acquire a newer lease and continue from the event log.

Lease expiration is a recovery mechanism, not a clock-based correctness proof.
Correctness comes from fencing token validation at append time.

## Recovery

Recovery is needed when an active owner disappears after recording progress but
before reaching a terminal state.

The scanner does not execute workflow code itself. It identifies a recoverable
run, acquires a lease, appends `recovery.requeued`, and hands the run to an
active runner. The runner can then replay the event stream and continue.

To find recoverable runs without replaying every run, storage maintains a
`run projection` index: per-run `status`, `terminal`, and `next_retry_at`,
derived from the reducer and kept beside the run row. A scan queries that index
for coarse candidates (not terminal, not waiting on a future retry, not currently
leased, not parked) and pages over them. The lease is read live (a join, never
denormalized, since leases are written in a separate transaction). The projection
is a scan optimization in the same tier as snapshots: the event log stays
authoritative, and the precise `recoverable/2` check is still applied to each
candidate before a takeover. Because the index can only over-approximate (it
omits the migration gate, which depends on live code), it never hides a genuinely
recoverable run.

An open `attempt.started` without a closing event is retried as the same attempt
number with the same idempotency key. That preserves the attempt budget, but it
means callback execution is at-least-once.

To avoid an unbounded recovery loop, BeamTrail counts `recovery.requeued` events
for the open attempt. Once `max_recoveries_per_attempt` is reached, recovery
appends a terminal failure with reason `recovery_budget_exceeded` instead of
requeueing the attempt again.

## Callback Semantics

BeamTrail does not provide exactly-once callback execution. If a callback
performs an external side effect and the VM dies before BeamTrail records the
attempt outcome, that same attempt can be re-entered.

Workflow code must use the `idempotency_key` in the execution context when it
calls external systems. BeamTrail keeps the key stable across retries of the
same logical step.

`max_attempts` is a crash-safe upper bound on attempt numbers the engine starts,
because failed-attempt decisions are appended atomically. It is not a guarantee
that an external side effect happened only once.

## Versioning Boundary

Each attempt records the `step_version` that was current when it started. If a
pending attempt must be replayed and the current workflow module reports a
different version for that step, BeamTrail marks the run as requiring migration
and refuses to advance it automatically.

Completed steps are not re-executed, so completed-step version changes do not
block later steps.

## Run Control Boundary

Cancellation is terminal. Once `workflow.cancelled` is appended, dispatch and
recovery treat the run as closed and will not append `workflow.completed`.

Parking is a separate gate, not a status. A parked run keeps its current
underlying status (`running`, `retrying`, or `failed`), but dispatch and recovery
refuse to advance it until `workflow.resumed` is appended. This is useful for
operator intervention, migration review, and poison-run triage without losing the
run's original execution state.

Manual requeue uses the same durable recovery path as the scanner:
`recovery.requeued` plus supervised runner dispatch. It refuses terminal and
parked runs. Recovery budgets still apply to automatic recovery decisions.

## Current Limits

BeamTrail currently supports linear step lists. It does not yet support dynamic
workflow control flow, branching, DAGs, fan-out/fan-in, child workflows, signals,
or step-result dataflow into later steps.

The recovery scan is driven by the `run projection` index (see Recovery), so it
avoids snapshot/replay work for every historical run. The PostgreSQL adapter
keeps `status`, `terminal`, `next_retry_at_ms`, and `parked` columns on
`workflow_runs`, updated in the same transaction as the append, with a partial
index on non-terminal, non-parked runs and an index on lease expiry. Adapters
that do not implement the optional
`list_recoverable_run_ids/3` callback fall back to paging all run ids and
replaying each, which is still correct. Runs created before the projection
columns existed default to a safe over-approximating state and can be normalized
once with `beamtrail_postgres_storage:backfill_run_projections/0`.

The projection is deliberately limited to the fields the scan filters on.
Migration-required is not projected: it is recomputed from live deployed code at
load time, so it stays a per-candidate check rather than a stored column.

The PostgreSQL payload format uses Erlang external term format for replay
fidelity. Operational projections are added as structured columns, never by
querying payload blobs directly.
