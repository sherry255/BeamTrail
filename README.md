# BeamTrail

Minimal Erlang/OTP durable workflow runtime prototype.

## What Is Included

- Append-only `workflow_events` as the source of truth.
- Event reducer that derives workflow state, attempts, retry state, failures, and terminal state.
- Periodic snapshots every 5 events plus terminal/recovery handoff snapshots.
- In-memory OTP storage adapter for tests and local development.
- Storage behaviour and PostgreSQL schema for `workflow_events`, `workflow_snapshots`, `workflow_leases`, and read-model tables.
- Workflow callback behaviour with stable idempotency key generation.
- At-least-once step dispatch, retry policy, step timeout, and unknown-attempt recovery.
- Versioned replay for in-flight work using the `step_version` recorded on `attempt.started`.

## Run

```sh
rebar3 shell --apps beamtrail
```

## Test

```sh
rebar3 eunit
```

## Minimal Workflow Module

```erlang
-module(my_workflow).
-behaviour(beamtrail_workflow).

-export([steps/1, step_version/1, retry_policy/1, timeout_ms/1,
         idempotency_key/3, execute/4]).

steps(_Input) -> [charge, ship].
step_version(_StepId) -> 1.
retry_policy(_StepId) -> #{max_attempts => 3, backoff_ms => 1000, retryable_errors => [transient]}.
timeout_ms(_StepId) -> 5000.
idempotency_key(_RunId, StepId, Input) -> {StepId, maps:get(order_id, Input)}.

execute(StepId, StepVersion, Input, Ctx) ->
    %% Pass maps:get(idempotency_key, Ctx) to external services.
    {ok, #{step => StepId, version => StepVersion, input => Input}}.
```

Start and query:

```erlang
{ok, RunId} = beamtrail:start_workflow(my_workflow, #{order_id => <<"o-1">>}).
State = beamtrail:get_state(RunId).
{ok, Events} = beamtrail:events(RunId).
```
