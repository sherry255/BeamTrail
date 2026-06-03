# Human Approval Deadline Pattern

BeamTrail does not yet have a first-class human task subsystem. The first
approval milestone is intentionally smaller: prove that existing decider
primitives can model a real approval deadline without new runtime semantics.

## Goal

Show that a workflow can wait for either:

- an external approval signal, or
- a durable timer deadline.

The event log remains the source of truth. Approval input is persisted as
`signal.received`; deadlines are persisted as `timer.scheduled` and
`timer.fired`.

## Pattern

An approval decider should:

1. schedule a stable deadline timer with `{sleep_until, TimerId, FireAtMs}`;
2. return `{wait, waiting_for_approval}` once the timer is scheduled;
3. on `approved` signal, run the next step or complete;
4. on `rejected` signal, fail with an approval rejection reason;
5. on deadline timer fired, fail with an approval timeout reason.

The decider must treat signals and fired timers as durable input. It must not
compare wall-clock time directly.

## Scope

This pattern does not add:

- assignment, RBAC, forms, comments, or escalation;
- timer cancellation;
- a browser UI;
- a human-task storage table.

Those belong to a future first-class human task layer. This milestone only
validates that the lower-level durable process model can express the workflow
correctly.

## Acceptance Criteria

- Approving before the deadline completes the workflow.
- Rejecting before the deadline terminally fails the workflow.
- A due deadline wakes a waiting workflow and terminally fails it.
- Terminal runs ignore stale later signals or timers.
- The example remains crash-recoverable through scanner-driven timer firing.
