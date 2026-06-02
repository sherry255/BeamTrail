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
- `BEAMTRAIL_STRESS_SLEEP_MS=100`
- `BEAMTRAIL_STRESS_PG_PORT=55432`
- `BEAMTRAIL_STRESS_DESCRIBE_SAMPLE=8`

Try a smaller pool:

```sh
BEAMTRAIL_STRESS_RUNS=64 \
BEAMTRAIL_STRESS_POOL_SIZE=2 \
BEAMTRAIL_STRESS_SLEEP_MS=50 \
examples/pg_stress/run.sh
```

Expected output:

```text
runs=32 pool_size=5 sleep_ms=100 describe_sample=8 elapsed_ms=...
completed=32 failed=0 timeout=0 other=0
terminal_latency_ms=samples=32 p50=... p95=... max=...
describe_ms=calls=... errors=0 samples=... p50=... p95=... max=...
pool=#{busy => ...,size => ...,waiting => ...,available => ...,checkouts => ...}
```

If `failed`, `timeout`, or `other` is non-zero, the harness exits with status 2.
That is a signal to inspect pool sizing, checkout timeout, runner limits, or
PostgreSQL availability.

`terminal_latency_ms` measures time from accepted `start_workflow/3` to terminal
state. `describe_ms` is sampled while runs are in flight and exercises
`beamtrail_query:describe/1` under the same pool pressure. Non-zero
`describe_ms` errors or high p95/max values are a signal that active runner
inspection is getting stuck behind storage I/O or connection checkout pressure.
