# Changelog

All notable changes to BeamTrail are documented in this file.

BeamTrail follows semantic versioning once public releases begin. `0.x` versions
are pre-stable and may include breaking API or storage changes.

## [Unreleased]

No changes yet.

## [0.2.0-pre.2] - 2026-06-14

### Added

- Reducer-level step result history and optional workflow result state, exposed
  through `beamtrail_query:describe/1`, as the first dataflow foundation for the
  planned decider layer.
- Per-attempt `step_input` persisted on `attempt.started` and exposed through
  the pending attempt query view, with legacy logs falling back to the original
  workflow input.
- Internal legacy decider adapter that represents linear workflow progress as
  `run_step` or `complete` commands without changing existing behavior.
- Optional workflow `decide/1` callback with command validation and dispatch
  routing for one-command-at-a-time dataflow.
- Optional workflow `decider_version/0` callback. Decider mode and version are
  recorded at creation, exposed through `beamtrail_query:describe/1`, and used
  to gate old histories as migration-required when orchestration logic changes.
- Executable decider dataflow coverage showing a workflow that reads a prior
  step result and passes a derived input into the next step.
- Minimal durable signal support: `beamtrail:signal_run/3` appends
  `signal.received`, `{wait, Reason}` appends `workflow.waiting`, and deciders
  can resume from persisted signals.
- Scanner-driven durable timers: deciders can return `{sleep, TimerId,
  DelayMs}` or `{sleep_until, TimerId, FireAtMs}`, the runtime appends
  `timer.scheduled`, due timers materialize as `timer.fired` under lease and
  fencing, and `next_wake_at_ms` drives recovery scans for waiting runs.
- Human approval deadline pattern coverage showing signals and timers composed
  into approve, reject, timeout, and stale-terminal safety paths.
- Crash recovery demo approval scenario showing a waiting approval run surviving
  VM death, then resuming through either an approval signal or a deadline timer.
- External step effects: workflow modules can mark steps as `external`, the
  runtime records pending effects instead of executing them locally, and workers
  can list and complete those effects through the public API.
- External effect claim leases with `claim_token` fencing. Active claims hide
  work until `claim_until_ms`, expired claims become visible again, and stale
  claim tokens are rejected.
- PostgreSQL `workflow_effects` operational index for visible external effects,
  maintained in the same transaction as the event append and rebuildable with
  `beamtrail_postgres_storage:backfill_effect_index/0`.
- Optional external effect deadlines via `external_effect_timeout_ms`, allowing
  abandoned external work to fail through the step retry policy.
- `beamtrail_external_worker:run_once/2`, a minimal in-process external worker
  loop that lists, claims, handles, and completes one visible external effect.
- External worker handoff demo showing claim expiry, stale-token rejection, and
  binary-safe worker result payloads against PostgreSQL.

## [0.2.0-pre.1] - 2026-06-03

### Added

- PostgreSQL storage adapter with append-only events, snapshots, leases,
  fencing token checks, per-run append locking, and connection pooling.
- Supervised active run processes (`beamtrail_run`) as the default execution
  path for `start_workflow/3`.
- Scanner recovery that requeues unfinished runs through active runners.
- Crash-atomic failure decisions via batch event append:
  `step.failed` is committed with the retry-or-terminal decision in one storage
  operation.
- Indexed recovery scans using run projections (`status`, `terminal`,
  `next_retry_at_ms`) to avoid replaying every run during scanner sweeps.
- Recovery fuse with `max_recoveries_per_attempt` to stop poison recovery loops.
- `beamtrail:await_terminal/2` for callers that need to wait for an asynchronous
  run to reach a terminal state.
- `beamtrail_query:describe/1` inspector data for state, attempts, snapshots,
  recovery, and active runner visibility.
- GitHub Actions CI covering EUnit, xref, PostgreSQL integration tests, and
  gitleaks secret scanning.

### Changed

- `start_workflow/3` now dispatches to a supervised active runner by default.
  `{ok, RunId}` means the run was durably created and accepted for execution,
  not that it has completed.
- Scanner recovery no longer depends on the worker supervisor for normal
  execution; active runners are the canonical execution path.
- Completed-step workflow version changes no longer block later in-flight steps;
  version mismatch gating applies only to pending attempts that may replay.
- Workflow callback failures are captured as structured engine failures:
  `steps/1` failures return a create error, while runtime callback failures
  terminally fail the run.
- Recovery scan projection is treated as an optimization only; the event log and
  precise recoverability check remain authoritative.

### Fixed

- Closed the crash window between recording `step.failed` and recording the
  retry-or-terminal decision.
- Prevented healthy, actively leased runs from receiving repeated
  `recovery.skipped` events.
- Hardened Postgres transactions so exceptions rollback instead of returning a
  dirty transaction connection to the pool.
- Hardened Postgres decoding with safe external-term decode and event-type
  whitelisting.
- Preserved supplied lease TTLs during active runner heartbeats.
- Prevented snapshot write failures from aborting already committed transitions.
- Kept snapshot writes monotonic so stale writers cannot replace newer
  snapshots.

### Notes

- This release is still an MVP. It supports linear step lists, not DAGs,
  fan-out, signals, queries, HTTP APIs, or a browser UI.
- Workflow callback execution remains at-least-once. External side effects must
  use the provided idempotency key.
- Historical PostgreSQL runs require workflow modules and step atoms to remain
  loadable in the release while those runs can still be recovered.

## [0.1.0] - 2026-06-01

### Added

- Initial BeamTrail MVP with event-sourced workflow state, memory storage,
  retry handling, timeouts, leases, snapshots, and early PostgreSQL hardening.

[Unreleased]: https://github.com/sherry255/BeamTrail/compare/v0.2.0-pre.2...HEAD
[0.2.0-pre.2]: https://github.com/sherry255/BeamTrail/compare/v0.2.0-pre.1...v0.2.0-pre.2
[0.2.0-pre.1]: https://github.com/sherry255/BeamTrail/compare/v0.1.0...v0.2.0-pre.1
[0.1.0]: https://github.com/sherry255/BeamTrail/releases/tag/v0.1.0
