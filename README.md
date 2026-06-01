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
- Inspector data through `beamtrail_query:describe/1`

Not in scope yet:

- Branching, DAGs, or fan-out
- HTTP API or browser UI
- SQL-native JSON inspection
- Built-in external side-effect deduplication
- Exactly-once execution of workflow callbacks

## Why BeamTrail

BeamTrail is aimed at Erlang systems that want durable step execution without
running a separate workflow service. Temporal and Cadence are broader systems:
they provide service boundaries, SDKs, queues, visibility APIs, and richer
workflow semantics. BeamTrail keeps the surface smaller and stays inside an OTP
release.

Oban is a durable job queue for Elixir. BeamTrail is lower level: each run is an
event stream with replayed state, leases, fencing, snapshots, and supervised
active runners. It is useful when the workflow history itself is the primary
artifact, not only a queued job record.

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

BeamTrail does not guarantee exactly-once side effects. Workflow code must use
the idempotency key in the execution context when it calls external systems.

Retry limits are enforced by persisted events, but they are not crash-atomic
across the small window between recording `step.failed` and recording the next
retry or terminal decision. If the VM or node dies in that window, recovery may
re-enter the failed step. External side effects must therefore be idempotent
against the stable idempotency key.

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

{ok, _} = application:ensure_all_started(epgsql).
ok = beamtrail_postgres_storage:init_schema().
{ok, _} = application:ensure_all_started(beamtrail).
```

Run a workflow:

```erlang
{ok, RunId} = beamtrail:start_workflow(my_workflow,
                                       #{order_id => <<"o-1">>}).

State = beamtrail:get_state(RunId).
View = beamtrail_query:describe(RunId).
```

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
metadata, recovery metadata, and the event list.

## Configuration

Set application environment before starting `beamtrail`:

- `storage_adapter`: storage module, default `beamtrail_memory_storage`
- `postgres`: PostgreSQL connection map for `beamtrail_postgres_storage`
- `postgres_pool_size`: supervised PostgreSQL connection pool size, default `5`
- `scanner_interval_ms`: recovery scan interval, default `5000`
- `worker_max_children`: concurrent dispatch workers, default `64`
- `run_max_children`: concurrent active run processes, default `64`
- `lease_ttl_ms`: dispatch lease TTL, default `30000`

## Tests

```sh
rebar3 eunit
```

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
external-term-format `bytea` values. Replay fidelity comes first; SQL-level
inspection can be added later through read models.

The schema lives in `priv/sql/postgres.sql`.

## License

MIT.
