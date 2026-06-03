# Decider and Dataflow Design

This document sketches BeamTrail's next workflow-expressiveness boundary.

BeamTrail currently has a durable execution boundary: attempts, retries, leases,
fencing, snapshots, recovery, and run control are persisted and replayed. What it
does not yet have is a durable orchestration boundary. `steps/1` is evaluated at
run creation, stored in `workflow.instance.created`, and then interpreted as a
fixed linear plan. Step results are stored in `step.succeeded`, but they are not
fed into later step selection.

The v0.3 goal is to add a small deterministic decider layer without turning
BeamTrail into a Temporal-style deterministic code replay engine.

## Implemented Foundation

The first dataflow foundation is implemented:

- reducer state now keeps ordered `results`
- reducer state keeps optional `workflow_result`
- `attempt.started` now persists the exact `step_input` for the attempt
- open-attempt replay reuses the persisted `step_input`
- old logs without `step_input` fall back to the original workflow input
- the legacy linear plan is represented as decider commands internally:
  `{run_step, StepId, StepInput}` or `complete`
- workflow modules may implement optional `decide/1`
- decider commands are validated before appending anything
- dispatch routes validated commands through `run_step`, `complete`,
  `{complete, Result}`, `{fail, Reason}`, or `{wait, Reason}`
- invalid decider commands and decider callback crashes terminally fail the run
  before opening an attempt
- `beamtrail_query:describe/1` exposes results, workflow result, and the
  current pending attempt
- test coverage includes a result-branching decider workflow that reads a
  prior `charge` result and passes a derived shipment input to `ship`
- minimal signal delivery is implemented with `signal.received` events,
  `{wait, Reason}` commands, and ordered `signals` in the decision view
- snapshot schema revision includes the new state keys and attempt keys, so old
  snapshots replay from events instead of loading an incompatible shape

The fields are durable and inspectable today. Legacy linear workflows still set
`StepInput = WorkflowInput`. Decider workflows can choose explicit step inputs
from the reduced view. Decider mode and `decider_version` are persisted on run
creation and exposed through `beamtrail_query:describe/1`.

## Position

BeamTrail should grow through an event-sourced decider:

- The event log remains the source of truth.
- The reducer builds a stable decision view from events.
- Workflow code receives that view and returns the next command.
- The command becomes durable only when BeamTrail appends the resulting event.

This is different from Temporal/Cadence code replay. BeamTrail will not try to
re-run arbitrary workflow code and intercept nondeterministic operations. Instead,
the workflow author provides a pure decider callback over a reduced view.

The design is intentionally narrower:

- one command at a time in v0.3
- no fan-out/fan-in yet
- only minimal external signal delivery; no signal subscriptions, cursors, or
  typed mailbox API yet
- no first-class durable timers yet
- no separate activity task queues yet

Those features should grow from the same command boundary later.

## Compatibility

Existing workflows stay valid.

If a workflow module does not implement a decider callback, BeamTrail uses the
legacy linear decider:

```erlang
steps(Input) -> [StepId].
```

The stored step list remains the execution order. After each `step.succeeded`,
the next step is the next item in that stored list. Each step receives the
original workflow input, exactly as it does today.

For workflows that implement the new decider callback, `steps/1` changes role:
it declares the allowed step ids for that workflow run. It is still evaluated at
creation and stored in `workflow.instance.created`, but it is no longer the
execution order. The decider chooses the next step from that declared set.

This keeps the storage contract stable:

- step ids remain atoms
- step ids are known at creation time
- PostgreSQL can keep decoding `step_id` safely as an existing atom
- `step_version/1`, `retry_policy/1`, and `timeout_ms/1` stay per-step

## Decider Callback

The proposed callback shape is:

```erlang
-callback decide(beamtrail_workflow:decision_view()) ->
    beamtrail_workflow:decision_command().
```

`decide/1` is optional. If it is absent, BeamTrail uses the legacy linear
decider.

The decision view is a map derived only from persisted events and stable engine
metadata:

```erlang
#{run_id := binary(),
  workflow := module(),
  input := term(),
  steps := [atom()],
  status := atom(),
  current_step := atom() | undefined,
  results := [map()],
  workflow_result := term() | undefined,
  signals := [map()],
  wait_reason := term() | undefined,
  attempts := [map()],
  failure := term() | undefined}.
```

`results` is ordered by successful step completion. Each item should preserve at
least the step id, attempt number, completion event sequence, and result:

```erlang
#{step_id := atom(),
  attempt := pos_integer(),
  event_seq := non_neg_integer(),
  result := term()}
```

A repeated step id can appear more than once. Helper APIs can later provide
`latest_result(View, StepId)`, but the stored model should not collapse repeated
steps into a map by default.

The initial v0.3 command set should be:

```erlang
complete
{complete, Result}
{run_step, StepId}
{run_step, StepId, StepInput}
{fail, Reason}
{wait, Reason}
```

Reserved future commands:

```erlang
{sleep_until, TimerId, UnixMs}
{run_many, [StepCommand]}
```

Reserved commands must be rejected with a structured engine error until their
subsystems exist. The timer command design is tracked in
[Durable Timer Design](TIMER.md).

## Step Input

Today `execute/4` receives the original workflow input as its third argument.
With a decider, the third argument becomes the step input selected by the
decider command:

```erlang
{run_step, charge, #{order_id => <<"o-1">>, amount => 5000}}
```

For legacy linear workflows, BeamTrail sets `StepInput = WorkflowInput`, so
existing modules keep their behavior.

`idempotency_key/3` should receive the same `StepInput` that `execute/4` will
receive. That key is computed once before `attempt.started` is appended and then
persisted on the attempt, as it is today.

The chosen step input must be persisted in `attempt.started` payload:

```erlang
#{attempt => AttemptNo,
  owner_node => node(),
  step_input => StepInput}
```

On retry or recovery of an open attempt, BeamTrail must reuse the persisted
`step_input`. It must not call `decide/1` again for the already-open attempt.

This preserves the existing recovery rule:

- `attempt.started` is the execution boundary
- an open attempt can be re-entered with the same attempt number
- the attempt uses the same idempotency key and the same step input

For old event logs whose `attempt.started` payload has no `step_input`, BeamTrail
uses the original workflow input as the compatibility default.

## Reducer State

The reducer should expose step results as first-class state:

```erlang
results => [#{step_id => StepId,
              attempt => AttemptNo,
              event_seq => EventSeq,
              result => Result}],
workflow_result => undefined | term()
```

On `step.succeeded`, append one result item to `results`. On
`workflow.completed`, store the optional workflow result in `workflow_result`.
Do not infer results from the attempt list in multiple places.

The state schema revision must change because `results` is a new state key. The
key must be added to both:

- `beamtrail_reducer:new/0`
- `beamtrail_state:state_keys/0`

This makes old snapshots incompatible and forces a safe event replay.

## Transition Flow

The transition loop becomes:

1. If the run is terminal, parked, retrying before `next_retry_at`, or migration
   blocked, stop as today.
2. If there is a pending attempt for the current step, run or resume that
   attempt as today.
3. If no attempt is pending, build the decision view from reducer state.
4. Call the decider:
   - legacy linear decider if `decide/1` is absent
   - workflow `decide/1` if present
5. Validate the command.
6. Append the event implied by the command:
   - `complete` / `{complete, Result}` -> `workflow.completed`
   - `{fail, Reason}` -> `workflow.failed`
   - `{wait, Reason}` -> `workflow.waiting`
   - `{run_step, StepId, StepInput}` -> `attempt.started`
7. Execute the attempt only after `attempt.started` is durably appended.

The decider is never called while an attempt is open. The open attempt is already
the durable command.

## Determinism Rules

`decide/1` must be deterministic over the decision view.

Workflow authors must not use wall-clock time, random values, process ids, ETS
state, network calls, or database reads inside `decide/1`. If the workflow needs
time or external input, the engine must model that as an explicit event later
through timer or signal commands.

BeamTrail should enforce only the practical boundary in v0.3:

- call `decide/1` through the same safe callback wrapper style as other workflow
  callbacks
- reject malformed commands before appending anything
- terminally fail the run with a structured workflow callback error if the
  decider raises or returns an invalid command

Full static verification of determinism is out of scope.

## Versioning

Dynamic deciders make workflow definition versioning more important than static
linear plans.

The implemented v0.3 rule is conservative:

- Add optional `decider_version/0`.
- If `decide/1` is implemented, record the current `decider_version` in
  `workflow.instance.created`.
- Before calling `decide/1` on a non-terminal run, compare the recorded version
  with the current callback result.
- If they differ, set the existing migration-required gate and refuse automatic
  progress.

Default version is `1` when the callback is absent. A run created without
decider metadata is treated as `decider => legacy, decider_version => 1`, so old
logs do not start using a newly-added `decide/1` callback by accident. If
`decider_version/0` raises or returns a value that is not a non-negative integer,
run creation fails before `workflow.instance.created` is appended.

This is not a migration system. It is a safety gate. Actual migration can later
be implemented through explicit operator actions or workflow-defined migration
callbacks.

Step versioning remains unchanged. Each `attempt.started` records
`step_version`; a pending attempt still replays against its recorded step
version and blocks if the current step version differs.

## Events

The minimum event changes for v0.3:

- `workflow.instance.created` payload may include:
  - `decider => legacy | module`
  - `decider_version => non_neg_integer()`
- `attempt.started` payload includes:
  - `step_input => term()`
- `workflow.completed` payload may include:
  - `result => term()`

No `decision.made` event is required for v0.3. The durable decision is the event
that crosses the next boundary: `attempt.started`, `workflow.completed`, or
`workflow.failed`.

If a future feature needs auditability for every decider evaluation, a
`workflow.decision.recorded` event can be added later. It should not be required
for the first dataflow/decider milestone.

## Query View

`beamtrail_query:describe/1` should expose:

- `results`
- `workflow_result`
- `decider`
- `decider_version`
- current pending attempt, including `step_input`

This keeps the inspector useful without requiring users to decode raw events.

## Non-Goals

v0.3 should not implement:

- Temporal-style arbitrary workflow code replay
- parallel command batches
- durable timers
- typed mailbox cursors or signal subscription helpers
- child workflows
- activity task queues
- automatic workflow code migration

The purpose of v0.3 is to add the missing orchestration boundary while keeping
the current recovery and append semantics intact.

## Implementation Order

1. Done: add reducer `results` state and snapshot schema coverage.
2. Done: persist `step_input` in `attempt.started`, defaulting to original input
   for legacy logs.
3. Done: add the legacy decider adapter and keep existing tests green.
4. Done: add optional `decide/1` behaviour callback and command validation.
5. Done: route dispatch through decider commands when no attempt is pending.
6. Done: add `decider_version/0` safety gate.
7. Done: expose decider metadata in `beamtrail_query:describe/1`.
8. Done: add an executable result-branching example test that uses previous
   step results to select the next step input.
9. Done: add minimal durable signal delivery and `{wait, Reason}` commands.

Each step should keep the event log replayable and PostgreSQL/memory adapters in
lockstep.
