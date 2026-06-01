# BeamTrail

BeamTrail is an Erlang/OTP durable workflow runtime built around
event-sourced execution.

It stores each workflow run as an append-only event stream, derives state by
reducing events, and runs a static list of workflow steps with retry, timeout,
lease, and recovery metadata recorded in the log.

The current repository still has missing durable-storage work. The only
working storage adapter is currently in-memory.

## Current State

- `beamtrail_memory_storage` is the only supported adapter.
- `beamtrail_postgres_storage` is a stub and returns `{error, not_implemented}`.
- `priv/sql/postgres.sql` sketches the intended PostgreSQL schema.
- Workflows are linear step lists. There is no branching, DAG execution, or
  fan-out.
- Idempotency keys are recorded and passed to workflow code. Deduplicating
  external side effects is still the workflow's job.
- Lease/fencing is enforced in the in-memory adapter. Multi-node durable
  takeover still needs a real storage backend.
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

## Configuration

Set these application environment values before starting `beamtrail`:

- `storage_adapter`: storage module, default `beamtrail_memory_storage`.
- `scanner_interval_ms`: recovery scan interval, default `5000`.
- `worker_max_children`: concurrent dispatch workers, default `64`.

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
