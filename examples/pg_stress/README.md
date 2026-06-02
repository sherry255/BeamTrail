# PostgreSQL Stress Harness

This harness runs many short workflows against the PostgreSQL adapter. It is not
a benchmark. It is a smoke-pressure tool for the active runner, connection pool,
append path, and terminal-state polling under concurrent load.

The current goal is to make pool pressure visible before refactoring the runner
or storage I/O path.

## Run

```sh
examples/pg_stress/run.sh
```

Default settings:

- `BEAMTRAIL_STRESS_RUNS=32`
- `BEAMTRAIL_STRESS_POOL_SIZE=5`
- `BEAMTRAIL_STRESS_SLEEP_MS=25`
- `BEAMTRAIL_STRESS_PG_PORT=55432`

Try a smaller pool:

```sh
BEAMTRAIL_STRESS_RUNS=64 \
BEAMTRAIL_STRESS_POOL_SIZE=2 \
BEAMTRAIL_STRESS_SLEEP_MS=50 \
examples/pg_stress/run.sh
```

Expected output:

```text
runs=32 pool_size=5 sleep_ms=25 elapsed_ms=...
completed=32 failed=0 timeout=0 other=0
pool=#{busy => ...,size => ...,waiting => ...,available => ...,checkouts => ...}
```

If `failed`, `timeout`, or `other` is non-zero, the harness exits with status 2.
That is a signal to inspect pool sizing, checkout timeout, runner limits, or
PostgreSQL availability.
