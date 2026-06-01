# BeamTrail

[![CI](https://github.com/sherry255/BeamTrail/actions/workflows/ci.yml/badge.svg)](https://github.com/sherry255/BeamTrail/actions/workflows/ci.yml)

BeamTrail is an Erlang/OTP durable workflow runtime built around
event-sourced execution.

It stores each workflow run as an append-only event stream, derives state by
reducing events, and runs a static list of workflow steps with retry, timeout,
lease, and recovery metadata recorded in the log.

The in-memory adapter is the default for local development. A PostgreSQL
adapter is included for durable event, snapshot, and lease storage.

## Current State

- `beamtrail_memory_storage` is the default adapter for local development and
  tests. It is a single in-memory process and is not the production durability
  path.
- `beamtrail_postgres_storage` uses epgsql and the schema in
  `priv/sql/postgres.sql`.
- PostgreSQL payloads, idempotency keys, leases, and snapshots are stored as
  Erlang external-term-format `bytea` values. This keeps replay exact; SQL-level
  JSON inspection is not implemented yet.
- Snapshot reads are revision-gated. If a stored snapshot uses an obsolete
  revision, BeamTrail ignores it and replays from the append-only event stream.
- Workflows are linear step lists. There is no branching, DAG execution, or
  fan-out.
- Idempotency keys are recorded and passed to workflow code. Deduplicating
  external side effects is still the workflow's job.
- There is no HTTP API or browser UI.

## What Works

- Start a workflow from an Erlang module implementing `beamtrail_workflow`.
- Append workflow events with expected-sequence checks and fencing tokens.
- Rebuild run state from events and snapshots.
- Retry failed steps according to a per-step retry policy.
- Record step and workflow timeouts.
- Recover unfinished in-flight attempts through the scanner.
- Renew leases while a step is running.
- Query a run's events, attempts, snapshot, lease, recovery metadata, and
  telemetry counters.

## Run

```sh
rebar3 shell --apps beamtrail
```

The application starts the in-memory storage process and recovery scanner under
the OTP supervision tree.

## Test

```sh
rebar3 eunit
```

Build output is written under `_build/` and ignored by Git.

PostgreSQL integration tests are disabled by default. To run them locally:

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

## Configuration

Set these application environment values before starting `beamtrail`:

- `storage_adapter`: storage module, default `beamtrail_memory_storage`.
- `scanner_interval_ms`: recovery scan interval, default `5000`.
- `worker_max_children`: concurrent dispatch workers, default `64`.

For PostgreSQL:

```erlang
application:set_env(beamtrail, storage_adapter, beamtrail_postgres_storage),
application:set_env(beamtrail, postgres,
                    #{host => "localhost",
                      port => 5432,
                      username => "beamtrail",
                      password => "beamtrail",
                      database => "beamtrail"}).

ok = beamtrail_postgres_storage:init_schema().
```

## Production Notes

- Use PostgreSQL for durable storage. The memory adapter is useful for local
  development and tests, but it is still process memory.
- PostgreSQL append uses expected sequence checks, fencing tokens, and per-run
  row locks so different runs can append concurrently.
- External side effects must be idempotent at the workflow boundary. BeamTrail
  records stable idempotency keys but does not deduplicate calls to outside
  systems.

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

## Example

```erlang
{ok, RunId} = beamtrail:start_workflow(my_workflow,
                                       #{order_id => <<"o-1">>}).

State = beamtrail:get_state(RunId).

{ok, Events} = beamtrail:events(RunId).

View = beamtrail_query:describe(RunId).

{ok, Requeued} = beamtrail_scanner:scan_now().
```
