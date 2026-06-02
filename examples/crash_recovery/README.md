# Crash Recovery Demo

This demo kills an Erlang VM while a BeamTrail step attempt is still open, then
starts a second VM and lets BeamTrail recover the run from PostgreSQL.

It demonstrates one narrow but important guarantee:

- `attempt.started` is durable before the callback runs;
- if the VM dies before the attempt records an outcome, recovery re-enters the
  same attempt number with the same idempotency key;
- PostgreSQL leases and the same recovery primitive used by the scanner let a
  new VM take over after the old lease expires;
- the run can finish without starting attempt 2.

It does not demonstrate exactly-once side effects. The workflow callback can be
entered more than once for the same attempt, so real workflows must use the
idempotency key when calling external systems.

## Run

Requirements:

- Docker
- Erlang/OTP and `rebar3`
- local port `55432` available, unless overridden

```sh
examples/crash_recovery/run.sh
```

Optional environment variables:

```sh
BEAMTRAIL_DEMO_PG_PORT=55433 examples/crash_recovery/run.sh
KEEP_BEAMTRAIL_DEMO=1 examples/crash_recovery/run.sh
```

## Expected Output

The output should include:

```text
==> Killing VM 1 while attempt 1 is still open
==> Starting VM 2 and waiting for recovery

Marker log:
started step=charge version=1 attempt=1 ...
started step=charge version=1 attempt=1 ...
completed step=charge attempt=1

Final status: completed

Event log:
  seq=1 type='workflow.instance.created' ...
  seq=2 type='attempt.started' ...
  seq=3 type='recovery.requeued' ...
  seq=4 type='step.succeeded' ...
  seq=5 type='workflow.completed' ...
```

The repeated marker line is expected: it is the same attempt being re-entered
after VM death, not a new attempt consuming retry budget.
