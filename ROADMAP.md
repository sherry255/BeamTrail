# Roadmap

Last updated: 2026-06-03

BeamTrail's long-term goal is to be the Erlang/OTP-native durable step runner:
embedded in a BEAM release, backed by a durable event log, and shaped around OTP
supervision instead of an external workflow service.

This roadmap is intentionally staged. BeamTrail should first become small and
correct, then mature as a public library, and only then expand toward richer
workflow semantics.

## Current Position

BeamTrail is a serious MVP. It has the core durable runtime shape:

- PostgreSQL-backed append-only event log
- reducer-based replay and snapshots
- expected-sequence append checks
- per-run PostgreSQL append locking
- leases and fencing tokens
- supervised active run processes
- scanner recovery
- crash-atomic failure decisions
- bounded poison recovery
- indexed recovery scans
- durable run control APIs (`cancel_run/2`, `park_run/2`, `resume_run/1`,
  `requeue_run/2`)
- structured workflow callback failure handling

It is still early. It supports linear step lists, not DAGs, fan-out, signals,
child workflows, HTTP APIs, or a browser UI.

## Near Term: v0.2.x Library Credibility

The next milestone is making the project easier to review, package, and trust as
an Erlang library.

- Publish `v0.2.0-pre.1` as a GitHub pre-release.
- Add Hex package metadata and verify package contents.
- Add `-spec` annotations on the public API and storage/workflow contracts.
- Add Dialyzer to CI once the first useful specs are in place.
- Keep xref checks in CI to catch undefined and deprecated calls.
- Add coverage reporting for the reducer, transitions, and storage adapters.
- Keep README, CHANGELOG, and ARCHITECTURE aligned with behavior changes.
- Document the exact supported OTP/PostgreSQL version matrix.

## Runtime Hardening

BeamTrail should make failure and recovery operations explicit enough for real
operators.

Implemented in v0.3:

- `cancel_run/2` writes a durable terminal event.
- `park_run/2` / `resume_run/1` gate and un-gate automatic dispatch and
  recovery.
- `requeue_run/2` provides a manual recovery trigger with fencing semantics.

Remaining:

- Add dead-letter style inspection for runs that exceeded recovery budget.
- Emit structured `logger` events for runner stop reasons and recovery actions.
- Expose standard telemetry measurements for run duration, step duration,
  retries, lease renewals, recovery latency, and storage latency.
- Define shutdown behavior for active runners during OTP application stop.

## Workflow Expressiveness

BeamTrail should grow workflow semantics carefully, without losing replay
clarity.

- Implement the v0.3 event-sourced decider described in
  [DECIDER.md](DECIDER.md).
- Pass prior step results into later steps through an explicit dataflow model.
- Add conditional steps through decider commands before adding arbitrary dynamic
  control flow.
- Add compensation hooks for saga-style workflows.
- Explore fan-out/fan-in only after the linear runtime remains stable.
- Keep workflow definition versioning explicit; never hide replay hazards behind
implicit code changes.

## Operations And Visibility

BeamTrail should be inspectable without requiring users to decode Erlang terms by
hand.

- Extend PostgreSQL run projections for operational queries.
- Add a documented SQL view or query module for stuck runs, failed runs, and
  long-running attempts.
- Add OpenTelemetry spans for workflow runs and step attempts.
- Build a minimal read-only inspector before any write-capable UI.
- Keep ETF payload storage for exact replay, but project operational fields into
  structured columns.

## Multi-Node Direction

BeamTrail's correctness boundary is PostgreSQL leases and fencing. Multi-node
coordination should build on that, not replace it with local process registry
assumptions.

- Document multi-node safety and recovery latency explicitly.
- Avoid every node scanning the same full candidate set at high scale.
- Add shard-aware scanner coordination, likely using PostgreSQL advisory locks or
  deterministic run-id partitions.
- Keep local `beamtrail_run_registry` as a fast path only; storage remains the
  correctness boundary.
- Research BEAM-native replicated storage (`ra` / `khepri`) as an optional future
  backend, not as a replacement for the PostgreSQL path.

## Community Goals

BeamTrail should invite Erlang developers to review and shape the design.

- Keep issues small and technically specific.
- Prefer failing tests or failure traces over broad feature requests.
- Document design tradeoffs instead of only documenting APIs.
- Be explicit about what BeamTrail does not guarantee.
- Favor maintainable OTP code over feature count.

## Non-Goals For Now

- Reimplementing Temporal's full service architecture.
- Multi-language SDKs.
- Exactly-once external side effects.
- A mandatory standalone server.
- Browser UI before the runtime API is stable.
- Arbitrary dynamic workflow code replay.
