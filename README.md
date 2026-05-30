# BeamTrail

BeamTrail is an Erlang/OTP library for running workflows on top of an
append-only event stream.

The runtime records workflow events, derives read state by reducing
events and snapshots, and keeps retry, timeout, idempotency, lease, and
recovery metadata in the event history.

## Status

- The in-memory storage adapter is the only working adapter.
- `beamtrail_postgres_storage` is a stub and returns
  `{error, not_implemented}`.
- `priv/sql/postgres.sql` defines the intended PostgreSQL schema.
- There is no HTTP API or browser UI.
- Lease and fencing tokens exist, but multi-node takeover is not
  implemented.

## Features

- Append-only workflow event streams.
- Snapshot plus tail replay.
- Retry policies with stable idempotency keys.
- Step and workflow timeouts.
- Step-version replay for recovery.
- Background recovery scanner and worker supervisor.
- Query view for events, attempts, snapshots, leases, recovery state,
  and telemetry counters.
- Configurable storage adapter via the `beamtrail` application
  environment.

## Run

```sh
rebar3 shell --apps beamtrail
```

The application starts the in-memory storage process and recovery
scanner under the OTP supervision tree.

## Test

```sh
rebar3 eunit
```

Build output is written under `_build/` and ignored by Git.

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

{ok, Events} = beamtrail:events(RunId).

View = beamtrail_query:describe(RunId).

{ok, Requeued} = beamtrail_scanner:scan_now().
```
