# BeamTrail

[![CI](https://github.com/sherry255/BeamTrail/actions/workflows/ci.yml/badge.svg)](https://github.com/sherry255/BeamTrail/actions/workflows/ci.yml)

BeamTrail is a PostgreSQL-backed durable workflow runtime for Erlang/OTP.

It records each workflow run as an append-only event stream, rebuilds state by
reducing events, and executes a linear list of workflow steps with retries,
timeouts, leases, fencing, snapshots, active runners, and scanner recovery.

## Status

BeamTrail is an MVP. The durable path is implemented through
`beamtrail_postgres_storage`; the default `beamtrail_memory_storage` adapter is
for local development and tests only.

Current scope:

- Linear step lists
- Durable event log and snapshots
- Expected-sequence append checks
- Per-run PostgreSQL append locking
- Leases and fencing tokens
- Supervised active run processes
- Retry backoff and step/workflow timeouts
- Scanner recovery for unfinished attempts
- Version mismatch gating during replay
- Durable run control: cancel, park/resume, and manual requeue
- Inspector data through `beamtrail_query:describe/1`

Not in scope yet:

- Branching, DAGs, or fan-out
- HTTP API or browser UI
- SQL-native JSON inspection
- Built-in external side-effect deduplication
- Exactly-once execution of workflow callbacks

## Why BeamTrail

BeamTrail's goal is to become the Erlang/OTP-native durable step runner: smaller
than Temporal, embedded in the BEAM, and built around OTP supervision plus a
durable event log.

Temporal and Cadence are broad workflow platforms. They provide service
boundaries, SDKs, queues, visibility APIs, and rich workflow semantics. BeamTrail
starts from the opposite direction: an Erlang application that already has an
OTP release, supervisors, telemetry, and PostgreSQL, and wants durable business
process execution without adding a separate workflow service.

Oban is a durable job queue for Elixir. BeamTrail is lower level: each run is an
event stream with replayed state, leases, fencing, snapshots, and supervised
active runners. It is useful when the workflow history itself is the primary
artifact, not only a queued job record.

BeamTrail aims to win on the BEAM-specific axis first: embedded deployment, OTP
process ownership, transparent failure semantics, and a small operational
surface. It is not trying to match every Temporal feature before it becomes
useful.

For the OTP/process boundary and recovery model, see [Architecture and Failure
Model](ARCHITECTURE.md).

For the planned decider and step-result dataflow layer, see
[Decider and Dataflow Design](DECIDER.md).

For planned work and contribution areas, see [Roadmap](ROADMAP.md) and
[Contributing](CONTRIBUTING.md).

## Guarantees

With the PostgreSQL adapter, BeamTrail guarantees:

- Workflow history is stored as append-only events.
- Appends are rejected when `expected_seq` is stale.
- Appends are rejected when the fencing token is missing, expired, or stale.
- Different runs can append concurrently; one run is serialized by a per-run
  PostgreSQL lock.
- Snapshots are only an optimization. If a snapshot revision is obsolete,
  BeamTrail ignores it and replays from events.
- Active runner processes are a fast path. PostgreSQL events, leases, fencing
  tokens, and snapshots remain the recovery boundary.
- `start_workflow/3` dispatches to a supervised active runner by default.
  `{ok, RunId}` means the run was durably created and accepted for execution;
  it does not mean the workflow has already reached a terminal state.
- Workflow callback failures are captured as structured engine failures.
  `steps/1` failures return a structured create error before a run is written;
  runtime callback failures terminally fail the run.

BeamTrail does not guarantee exactly-once side effects. Workflow code must use
the idempotency key in the execution context when it calls external systems.

Step failure and the retry-or-terminal decision are appended atomically, so
`max_attempts` is a crash-safe upper bound on the attempt numbers BeamTrail
starts. If a VM or node dies after a callback performs an external side effect
but before BeamTrail records the attempt outcome, the same attempt number can be
re-entered with the same idempotency key.

Open attempts have a recovery fuse. If the same attempt is requeued too many
times without reaching an outcome, BeamTrail terminally fails the run with
`recovery_budget_exceeded` rather than looping forever.

## Quickstart With PostgreSQL

Start PostgreSQL:

```sh
docker run --name beamtrail-pg-test \
  -e POSTGRES_USER=beamtrail \
  -e POSTGRES_PASSWORD=beamtrail \
  -e POSTGRES_DB=beamtrail \
  -p 55432:5432 \
  -d postgres:16
```

Start a shell:

```sh
rebar3 shell
```

Configure storage, install the schema, and start the application:

```erlang
application:set_env(beamtrail, storage_adapter, beamtrail_postgres_storage),
application:set_env(beamtrail, postgres,
                    #{host => "localhost",
                      port => 55432,
                      username => "beamtrail",
                      password => "beamtrail",
                      database => "beamtrail"}).
application:set_env(beamtrail, postgres_pool_size, 5).
application:set_env(beamtrail, workflow_modules, [my_workflow]).

{ok, _} = application:ensure_all_started(epgsql).
ok = beamtrail_postgres_storage:init_schema().
{ok, _} = application:ensure_all_started(beamtrail).
```

Run a workflow:

```erlang
{ok, RunId} = beamtrail:start_workflow(my_workflow,
                                       #{order_id => <<"o-1">>}).

{ok, State} = beamtrail:await_terminal(RunId, 5000).
View = beamtrail_query:describe(RunId).
```

For explicit synchronous driving in tests or low-level tooling:

```erlang
{ok, RunId} = beamtrail:start_workflow(my_workflow,
                                       #{order_id => <<"o-1">>},
                                       #{auto_dispatch => false}).
{ok, FinalState} = beamtrail:dispatch(RunId).
```

Run control APIs are durable events, not in-memory flags:

```erlang
{ok, Cancelled} = beamtrail:cancel_run(RunId, operator_cancel).
{ok, Parked} = beamtrail:park_run(RunId, maintenance).
{ok, Resumed} = beamtrail:resume_run(RunId).
{ok, requeued} = beamtrail:requeue_run(RunId, manual).
```

`cancel_run/2` writes a terminal `workflow.cancelled` event. `park_run/2`
writes `workflow.parked` and prevents dispatch and recovery from advancing the
run. `resume_run/1` clears the parked gate with `workflow.resumed` and dispatches
through the supervised runner path. `requeue_run/2` is a manual recovery trigger
and refuses terminal or parked runs.

Control calls first target a live active runner on the local BEAM node. If there
is no local runner, they acquire the storage lease and append directly. They do
not force-control a live runner on another node; in that case fencing and lease
expiry remain the handoff boundary.

Clean up the local container:

```sh
docker rm -f beamtrail-pg-test
```

## Memory Mode

For local development without PostgreSQL:

```sh
rebar3 shell --apps beamtrail
```

The default storage adapter is `beamtrail_memory_storage`. It is a single
in-memory process. It is not the durable storage path.

## Workflow Module

```erlang
-module(my_workflow).
-behaviour(beamtrail_workflow).

-export([steps/1, step_version/1, retry_policy/1, timeout_ms/1,
         idempotency_key/3, execute/4]).

steps(_Input) ->
    [charge, ship].

step_version(_StepId) ->
    1.

retry_policy(_StepId) ->
    #{max_attempts => 3,
      backoff_ms => 1000,
      retryable_errors => [transient]}.

timeout_ms(_StepId) ->
    5000.

idempotency_key(_RunId, StepId, Input) ->
    {StepId, maps:get(order_id, Input)}.

execute(StepId, StepVersion, Input, Ctx) ->
    IdempotencyKey = maps:get(idempotency_key, Ctx),
    {ok, #{step => StepId,
           version => StepVersion,
           idempotency_key => IdempotencyKey,
           input => Input}}.
```

Step ids must be atoms. BeamTrail rejects workflow definitions whose `steps/1`
return value is not a proper list of atoms. This keeps the memory and PostgreSQL
storage adapters on the same contract and avoids hidden replay incompatibilities.

`execute/4` should treat `Ctx.idempotency_key` as the key for any external
side effect.

## Querying

```erlang
State = beamtrail:get_state(RunId).
{ok, Events} = beamtrail:events(RunId).
View = beamtrail_query:describe(RunId).
{ok, Requeued} = beamtrail_scanner:scan_now().
```

`beamtrail_query:describe/1` returns the current reduced state, attempts,
snapshot metadata, replay tail length, lease/fencing metadata, active runner
metadata, run-control metadata, recovery metadata, and the event list.

## Configuration

Set application environment before starting `beamtrail`:

- `storage_adapter`: storage module, default `beamtrail_memory_storage`
- `postgres`: PostgreSQL connection map for `beamtrail_postgres_storage`
- `postgres_pool_size`: supervised PostgreSQL connection pool size, default `5`
- `postgres_pool_checkout_timeout_ms`: checkout wait timeout, default `5000`
- `postgres_pool_reconnect_interval_ms`: reconnect interval after a pooled
  connection replacement fails, default `1000`
- `workflow_modules`: workflow modules to preload on application start. This
  keeps PostgreSQL's safe external-term decoding from failing on atoms whose
  modules are present in the release but have not been loaded yet.
- `scanner_interval_ms`: recovery scan interval, default `5000`
- `worker_max_children`: concurrent dispatch workers, default `64`
- `run_max_children`: concurrent active run processes, default `64`
- `lease_ttl_ms`: dispatch lease TTL, default `30000`
- `max_recoveries_per_attempt`: recovery requeue budget for one open attempt,
  default `3`; set to `infinity` to disable the fuse

For PostgreSQL-backed production use, size `postgres_pool_size` for expected
active runner concurrency. The default is intentionally small for local
development; high `run_max_children` / `worker_max_children` values with a small
pool will add checkout latency and can make active runners less responsive under
load.

## Tests

```sh
rebar3 eunit
```

Crash recovery demo:

```sh
examples/crash_recovery/run.sh
```

The demo starts PostgreSQL, kills an Erlang VM while a step attempt is open, then
starts a second VM and uses the scanner's recovery primitive to complete the same
attempt. See `examples/crash_recovery/README.md`.

PostgreSQL stress harness:

```sh
examples/pg_stress/run.sh
```

The stress harness runs many short workflows against a real PostgreSQL adapter
and prints completion counts, terminal latency percentiles, sampled
`describe/1` latency, elapsed time, and pool state. It is a pressure smoke test,
not a benchmark. See `examples/pg_stress/README.md`.

PostgreSQL integration tests:

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

GitHub Actions runs both the default EUnit suite and the PostgreSQL integration
suite on every push to `main` and on pull requests.

## Storage Format

PostgreSQL stores payloads, idempotency keys, leases, and snapshots as Erlang
external-term-format `bytea` values. Replay fidelity comes first. Operational
projections that the engine needs to query — the recovery-scan index columns
(`status`, `terminal`, `next_retry_at_ms`) on `workflow_runs` — are kept as
structured columns derived from the reducer, never by inspecting payload blobs.

BeamTrail decodes these terms with `binary_to_term/2` in safe mode. Configure
`workflow_modules` in releases so workflow module atoms and step atoms are
available before recovery scans replay old events. Historical workflow modules
and step atoms are a replay compatibility boundary: do not remove or rename them
while runs that reference them may still be recovered.

The schema lives in `priv/sql/postgres.sql`.

## License

MIT.
