# Contributing

BeamTrail is an early Erlang/OTP durable runtime project. Contributions are
welcome, especially around correctness, failure modeling, tests, documentation,
and OTP ergonomics.

## Project Direction

BeamTrail aims to be a BEAM-native durable step runner: embedded in an OTP
release, driven by supervised active run processes, and backed by a durable event
log.

Good contributions usually strengthen one of these boundaries:

- event log correctness
- reducer replay determinism
- lease and fencing safety
- recovery behavior
- active runner lifecycle
- storage adapter consistency
- observability and operator workflows
- public API clarity

Feature additions should preserve BeamTrail's current scope unless the roadmap
explicitly calls for expanding it.

## Development Setup

Requirements:

- Erlang/OTP 28
- rebar3 3.27 or newer
- Docker, if running PostgreSQL integration tests locally

Run the default test suite:

```sh
rebar3 eunit
```

Run PostgreSQL integration tests:

```sh
docker run --name beamtrail-pg-test \
  -e POSTGRES_USER=beamtrail \
  -e POSTGRES_PASSWORD=beamtrail \
  -e POSTGRES_DB=beamtrail \
  -p 55432:5432 \
  -d postgres:16

BEAMTRAIL_PG_TEST=1 BEAMTRAIL_PG_PORT=55432 rebar3 eunit

docker rm -f beamtrail-pg-test
```

## Testing Expectations

Runtime behavior changes should start with a failing test.

Preferred tests:

- focused EUnit tests for public API behavior
- reducer tests for event replay semantics
- storage tests for expected sequence, fencing, and transaction behavior
- fault-injection tests for crash windows and recovery edges

For failure-mode changes, include the failure sequence in the test name or
nearby comment. The project values tests that prove a real race or crash window
over broad happy-path coverage.

Before opening a pull request, run:

```sh
rebar3 eunit
git diff --check
```

If your change touches PostgreSQL storage, run the PostgreSQL integration tests
as well.

## Design Guidelines

Keep the event log authoritative.

Snapshots, projections, active runner state, and registry entries are
optimizations. They must not become the source of truth.

Keep OTP and durable boundaries separate.

OTP supervision owns live process lifecycle. PostgreSQL owns durable history,
append ordering, lease ownership, and fencing. Local process registries are fast
paths, not distributed locks.

Prefer explicit failure semantics.

If a callback, storage call, lease renewal, or recovery action can fail, model
the outcome explicitly. Avoid silent retries that hide persistent poison states.

Avoid feature sprawl.

BeamTrail is not trying to clone Temporal feature-for-feature. New workflow
expressiveness should be added only when replay, versioning, recovery, and
operator behavior are clear.

## Pull Request Checklist

- The change has a focused rationale.
- Behavior changes include tests.
- Storage changes preserve expected sequence and fencing semantics.
- Recovery changes describe what happens after process crash, VM restart, and
  lease expiration.
- Documentation is updated when public behavior changes.
- `rebar3 eunit` passes.
- `git diff --check` is clean.

## Reporting Issues

Useful issue reports include:

- BeamTrail version or commit SHA
- storage adapter used
- relevant event list or `beamtrail_query:describe/1` output
- expected behavior
- actual behavior
- whether a lease, retry, recovery, or callback failure was involved

For correctness bugs, a concrete event sequence is the most useful form of
report.
