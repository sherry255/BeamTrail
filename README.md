# BeamTrail

Durable workflow runtime for Erlang/OTP — append-only event log, periodic
snapshots, versioned replay, retry / timeout / idempotency semantics.

This iteration is a **library-first MVP**. The HTML prototype is a design
reference for what the inspector and query API should eventually look
like; this codebase ships the runtime + a structured query API that
returns the same shape, not a browser UI. See "Known gaps" below.

## What works in this iteration

- `workflow_events` is the source of truth; `workflow_instances` and
  `step_attempts` are read models projected on event append, never
  written as primary state.
- Reducer rebuilds state from snapshot + tail events for both fresh
  dispatches and recovery.
- Periodic snapshots every 5 events plus terminal / recovery handoff.
- `beamtrail_scanner` runs as an OTP `gen_server` in the supervision
  tree. Each tick it (a) lists recoverable runs without dispatching,
  (b) appends a durable `recovery.requeued` event with lease + fencing
  metadata, then (c) hands the run to a transient worker spawned under
  `beamtrail_worker_sup` (a `simple_one_for_one` pool). Slow runs
  cannot block the scanner.
- Storage adapter is application-configurable via
  `application:set_env(beamtrail, storage_adapter, Module)`; the
  default is `beamtrail_memory_storage`. The supervisor only starts the
  in-memory adapter ad-hoc; durable adapters are expected to wire their
  own connection pool / supervision.
- Stable, attempt-insensitive `idempotency_key` is recorded on
  `attempt.started` and reused across retries and recovery dispatches.
- Step timeout (`timeout_ms/1`) and workflow timeout
  (optional `workflow_timeout_ms/0`) both surface as observable
  `step.failed` / `workflow.failed` events with structured reasons.
- Versioned replay: `attempt.started` records `step_version`; recovery
  dispatches the recorded version, and the query API flags
  `migration_required_for_version_change` when the deployed module's
  current `step_version/1` differs from the recorded one.
- `beamtrail_query:describe/1` returns a single map matching the
  prototype: event ledger, attempts, snapshot info, lease, telemetry
  counters, version-mismatch flag, recovered-in-ms, and a copyable
  `query` body.
- Telemetry events (`[beamtrail, attempt, started]`,
  `[beamtrail, retry, scheduled]`, `[beamtrail, recovery, requeued]`,
  `[beamtrail, snapshot, written]`) are emitted via
  `beamtrail_telemetry:execute/3` and also tallied as in-memory
  counters surfaced through the query API.
- Lease + fencing token primitives via `beamtrail_lease_manager` —
  boundary only; multi-node takeover is explicitly out of MVP scope.

## Known gaps (honestly declared)

- **PostgreSQL adapter is a stub**: `priv/sql/postgres.sql` ships the
  schema and `beamtrail_postgres_storage` ships the behaviour wiring,
  but every callback returns `{error, not_implemented}`. The in-memory
  adapter is the only working backend. Wiring `epgsql` + connection
  pool, txn boundaries, a JSON codec, and a Postgres `ct` suite is a
  follow-up. The stub returns loudly instead of silently falling back
  so this gap can't be missed.
- **Inspector UI**: the HTML prototype is a design target. This pass
  exposes the same structured data via `beamtrail_query:describe/1`;
  rendering a browser inspector is a follow-up. `.panel/app.json` is
  a `rebar3 shell` service with no `entry_url` because there is no
  HTTP surface yet.
- **Recovery scanner cadence**: default 5s tick is conservative and
  hits the in-memory store on every interval. For the durable backend
  the scan should be cursor-driven over the events table.
- **Multi-node takeover**: lease + fencing token exist as a boundary,
  but contended takeover, split-brain, and stale-fence rejection are
  not implemented.

## Run

```sh
rebar3 shell --apps beamtrail
```

The application supervisor starts the in-memory storage and the
background recovery scanner automatically.

## Test

```sh
rebar3 eunit
```

`_build/` is in `.gitignore`; tests are reproducible from a clean
state (no stale include symlinks).

## Minimal workflow module

```erlang
-module(my_workflow).
-behaviour(beamtrail_workflow).

-export([steps/1, step_version/1, retry_policy/1, timeout_ms/1,
         idempotency_key/3, execute/4]).

steps(_Input) -> [charge, ship].
step_version(_StepId) -> 1.
retry_policy(_StepId) ->
    #{max_attempts => 3, backoff_ms => 1000, retryable_errors => [transient]}.
timeout_ms(_StepId) -> 5000.
idempotency_key(_RunId, StepId, Input) ->
    {StepId, maps:get(order_id, Input)}.

execute(StepId, StepVersion, Input, Ctx) ->
    %% Pass maps:get(idempotency_key, Ctx) to external services for dedup.
    {ok, #{step => StepId, version => StepVersion, input => Input}}.
```

Start, query, recover:

```erlang
{ok, RunId}    = beamtrail:start_workflow(my_workflow, #{order_id => <<"o-1">>}).
QueryView      = beamtrail_query:describe(RunId).
{ok, Events}   = beamtrail:events(RunId).
{ok, Requeued} = beamtrail_scanner:scan_now().
```
