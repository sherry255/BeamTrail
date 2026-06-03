# Design Rationale

BeamTrail is a BEAM-native durable step runner. This document explains the main
design choices and the trade-offs behind them.

It is intentionally separate from [Architecture and Failure Model](ARCHITECTURE.md).
The architecture document describes how the runtime works. This document explains
why it is shaped this way.

## Core Position

BeamTrail is not trying to be a Temporal-compatible server, a generic DAG
scheduler, or a replacement for OTP supervision.

The intended shape is smaller:

- run inside an Erlang/OTP release;
- use OTP processes for live execution, timers, and isolation;
- use PostgreSQL as the durable recovery boundary;
- keep workflow history as an append-only event stream;
- make crashes, retries, waits, signals, and operator decisions observable.

The project starts as a durable step runner because that is the smallest useful
unit that can be made precise. Broader workflow features are only valuable if
the recovery model underneath them is already correct.

## Why OTP Is Not Enough

OTP supervision is excellent at restarting live processes inside a running VM.
It does not remember business progress after a VM restart, deployment
replacement, node loss, or host disk failure.

For a long-running business process, "the process restarted" is not enough. The
runtime also needs to know:

- which step crossed the execution boundary;
- which attempt number and idempotency key were used;
- whether a failure already decided to retry or terminate;
- which signals and timers were recorded;
- whether an operator cancelled, parked, resumed, or requeued the run.

Those facts need durable storage. BeamTrail uses OTP for the hot path and a
PostgreSQL event log for the recovery boundary.

## Why Not Temporal

Temporal is a broad workflow platform with a server cluster, SDK protocol, task
queues, visibility APIs, and mature multi-language support. BeamTrail is not
trying to match that surface area.

BeamTrail is for teams that already run Erlang/OTP and PostgreSQL and want a
small embedded runtime rather than another workflow service.

The trade-off is explicit:

- BeamTrail has a smaller operational surface.
- BeamTrail can use normal OTP supervision and process ownership.
- BeamTrail does not yet provide Temporal's workflow expression power, service
  maturity, SDK ecosystem, or visibility plane.

The long-term opportunity is not "Temporal, but written in Erlang." It is an OTP
runtime for durable business processes: supervised while active, event-sourced
while durable, and inspectable as a timeline.

## Why Not Oban

Oban is a durable job system for Elixir applications. A job is usually a unit of
work that succeeds, fails, retries, or gets discarded.

BeamTrail models a run as a durable event stream. The run has history,
attempts, signals, timers, state derived by a reducer, and a supervised active
owner. This is useful when the process history is the artifact, not only a queued
job row.

For simple background work, a job queue is the better tool. BeamTrail is aimed at
business processes where operators need to answer: what happened, what is it
waiting for, who owns it now, and what boundary can safely be retried?

## Why Event Log First

The event log is the source of truth because recovery needs facts, not guesses.

BeamTrail records boundary-crossing facts:

- `workflow.instance.created` records the initial plan and input.
- `attempt.started` records the exact attempt and step input that crossed into
  user code.
- `step.succeeded` and `step.failed` record attempt outcomes.
- `retry.scheduled` and `workflow.failed` record decisions.
- `signal.received`, `timer.scheduled`, and `timer.fired` record external and
  time-based inputs.

State is derived by reducing the event stream. Snapshots and indexed
projections are optimizations; they are not the authority.

This is why failure decisions are appended atomically. A failed step without the
retry-or-terminal decision is an ambiguous durable state, so BeamTrail writes
the fact and decision in one storage call.

## Why Active Runners Exist

If the event log is authoritative, a naive implementation could make every
dispatch load state, reduce events, run one step, and exit.

BeamTrail still uses active `gen_statem` runners because OTP is good at live
ownership:

- a run can keep reduced state hot between transitions;
- retry timers can fire without waiting for the scanner tick;
- step execution is isolated from other runs;
- lease heartbeats are owned by the live runner;
- local run control can stop in-flight execution before appending an operator
  event.

The active runner is a fast path, not the source of truth. If it dies, another
runner can rebuild from PostgreSQL and continue from the last recorded event.

## Why PostgreSQL

PostgreSQL is a pragmatic durable boundary:

- many Erlang and Elixir applications already operate it;
- transactions and row locks are enough to serialize one run's append path;
- indexed projections support scanner recovery without replaying every run;
- the event table gives a durable audit trail.

BeamTrail uses `expected_seq` and fencing tokens on top of PostgreSQL locks.
The lock serializes the append transaction. `expected_seq` rejects decisions
based on stale state. The fencing token rejects writes from old owners after a
new owner has taken the run.

Distributed Erlang, `global`, `pg`, or a local registry can help route live
processes, but they are not the durable correctness boundary. They do not
replace storage fencing after VM restart, node loss, or network partition.

## Why Linear First

BeamTrail began with linear step lists because linear execution is the smallest
model that still exercises the hard parts:

- append ordering;
- idempotent attempt boundaries;
- crash-atomic failure decisions;
- lease takeover;
- retry and timeout recovery;
- snapshots and replay;
- operator controls.

The decider layer now makes the transition loop more general: a workflow can
return one command at a time, wait for a signal, or schedule a durable timer.
That is the path toward richer orchestration.

DAGs, fan-out, child workflows, and richer dataflow are future expression
features. They should be built on top of the same durable boundary, not before
it.

## Why At-Least-Once

BeamTrail cannot guarantee exactly-once external side effects. If a VM dies
after user code calls an external service but before BeamTrail records the
attempt outcome, the same attempt can be re-entered during recovery.

The runtime therefore provides stable idempotency keys and attempt identity.
Workflow code must use those keys when calling external systems.

What BeamTrail does guarantee is narrower and useful: step failure and the
retry-or-terminal decision are crash-atomic, so `max_attempts` is a hard upper
bound on the attempt numbers the engine starts.

## Why Scanner Recovery

The scanner is a recovery trigger, not the correctness boundary.

It finds unfinished runs with missing or expired leases, records an observable
`recovery.requeued` marker after acquiring a lease, and dispatches the run
through the active runner supervisor. Correctness still comes from lease
fencing, expected sequence checks, and event replay.

The indexed scanner path exists so recovery is based on projected run state and
lease deadlines instead of replaying every run on every tick.

## Intentional MVP Limits

These are current limits, not accidental omissions:

- no general DAG or parallel command batch yet;
- no first-class human task assignment UI yet;
- no hosted control plane;
- no SQL-native JSON payload inspection;
- no built-in external side-effect deduplication;
- no multi-language worker protocol.

The near-term goal is to make the embedded BEAM + PostgreSQL runtime credible
for production trials before expanding the expression model.

## What Would Change With A Wider Goal

If BeamTrail becomes a general durable process runtime rather than a durable
step runner, the next structural layers are:

- durable mailbox semantics for signals, webhooks, approvals, and agent events;
- richer decider commands, including child runs and parallel command batches;
- first-class human tasks and operator workflows;
- cluster-aware scheduling and global backpressure;
- a visibility/control plane with timeline, retry, cancel, park, and audit
  workflows.

Those features should not weaken the current invariant: business progress is a
durable event stream, and OTP processes are the live execution fast path.
