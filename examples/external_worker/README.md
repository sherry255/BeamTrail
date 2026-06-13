# External worker example

This example shows BeamTrail's minimal external-effect loop:

1. A workflow schedules `charge` as an external step.
2. Worker A claims the effect and then "crashes" before completion.
3. The claim expires.
4. Worker B claims the same effect.
5. Worker A's old claim token is rejected with `{error, stale_claim}`.
6. Worker B completes the effect with the current claim token.
7. BeamTrail resumes the workflow and runs the local `ship` step.

Run it from the repository root:

```sh
examples/external_worker/run.sh
```

The script starts a disposable PostgreSQL container, compiles the example
modules, runs the workflow and worker loop, prints the event log, and removes
the container unless `KEEP_BEAMTRAIL_EXTERNAL_WORKER=1` is set.

The demo uses the low-level claim and complete APIs directly so it can show
Worker A's stale claim token being rejected after Worker B reclaims the effect.
For ordinary in-process workers, `beamtrail_external_worker:run_once/2` wraps the
same list, claim, handle, and complete loop.

The external worker result payload and claim owner use binary values on purpose.
PostgreSQL payloads and effect-index terms are decoded with Erlang's safe
external-term mode, so dynamic atoms created by a separate worker node are a
replay compatibility hazard unless every reader VM already has those atoms
loaded.
