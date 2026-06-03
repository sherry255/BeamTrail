# Durable Timer Design

This document defines the next event-sourced decider primitive after durable
signals: durable timers.

Signals let an external actor wake a waiting run. Timers let time wake it. The
two must compose from the first implementation because approval-style workflows
often wait for either a signal or a deadline.

## Goals

- Add a deterministic timer boundary for decider workflows.
- Keep the event log as the source of truth.
- Let a run wait for an external signal and a timer deadline at the same time.
- Materialize timer firing as a persisted event before the decider sees it.
- Preserve PostgreSQL fencing and expected-sequence protection for timer firing.
- Keep the first implementation scanner-driven; local fast timers are optional
  future work.

## Non-Goals

- Recurring timers or cron.
- Timer cancellation and rescheduling.
- Millisecond-accurate wakeups. Scanner interval granularity is acceptable.
- Parallel command batches, DAG execution, or fan-out/fan-in.
- Replaying arbitrary workflow code.

## Commands

The decider command set gains two commands:

```erlang
{sleep_until, TimerId, FireAtMs}
{sleep, TimerId, DelayMs}
```

`TimerId` is an atom or binary. It must be stable and inspectable. `FireAtMs` is
a non-negative Unix millisecond timestamp. `DelayMs` is a non-negative duration
in milliseconds.

`{sleep, TimerId, DelayMs}` is a convenience command. The transition layer
converts it to an absolute `FireAtMs` at append time and persists only the
absolute deadline. Reducer replay never reads the wall clock.

The transition layer schedules one timer command at a time, then immediately
re-enters the decider under the same lease with the updated reduced state. This
lets a workflow express "schedule a deadline, then wait for a signal" without
adding multi-command batches:

```erlang
decide(View0) -> {sleep, approval_deadline, 86400000}.
decide(View1) -> {wait, waiting_for_approval}.
```

## Events

Timer scheduling is persisted as:

```erlang
timer.scheduled
#{timer_id => TimerId,
  fire_at_ms => FireAtMs,
  scheduled_at => NowMs,
  source_command => sleep | sleep_until,
  next_wake_at => NextWakeAt}
```

Timer firing is persisted as:

```erlang
timer.fired
#{timer_id => TimerId,
  fire_at_ms => FireAtMs,
  fired_at => NowMs,
  next_wake_at => NextWakeAt}
```

`timer.fired` is the only way a decider observes a timer as fired. The decider
must not compare wall-clock time itself.

## Reducer State

The reducer adds timer state:

```erlang
timers => #{TimerId => #{timer_id => TimerId,
                         status => scheduled | fired,
                         fire_at_ms => FireAtMs,
                         scheduled_event_seq => EventSeq,
                         scheduled_at => ScheduledAt,
                         fired_event_seq => EventSeq | undefined,
                         fired_at => FiredAt | undefined}},
next_wake_at => undefined | non_neg_integer()
```

`next_wake_at` is the minimum `fire_at_ms` over scheduled timers. It is
independent of `status`. A run may have:

- `status => waiting`
- `wait_reason => waiting_for_approval`
- `next_wake_at => DeadlineMs`

That state means the run is waiting for a signal, but it is also recoverable once
the timer deadline is due.

The decision view adds `timers` and `next_wake_at` alongside the existing
`signals` and `wait_reason` fields.

The state schema revision must change because `timers` and `next_wake_at` are
new reducer keys. Add both keys to:

- `beamtrail_reducer:new/0`
- `beamtrail_state:state_keys/0`

## Timer Idempotence

Scheduling rules for one `TimerId`:

- If no timer exists, append `timer.scheduled`.
- If a scheduled timer exists with the same `fire_at_ms`, treat the command as a
  no-op and re-enter the decider.
- If a scheduled timer exists with a different `fire_at_ms`, terminally fail the
  run with an invalid timer command error.
- If a fired timer exists, the decider must use a new `TimerId` for a new timer.
  Reusing a fired id terminally fails the run.

This avoids silent rescheduling and keeps v0.4 replay rules small. Timer
cancellation and rescheduling can be added later as explicit event types.

## Recoverability

Exact recoverability remains the correctness boundary. Indexed storage queries
only provide a coarse candidate set.

The exact rule becomes:

```erlang
terminal => false
parked => false
migration_required_for_version_change => false

status = retrying:
  next_retry_at =< Now

status = waiting:
  next_wake_at =/= undefined andalso next_wake_at =< Now

status = completed:
  false

status = failed:
  terminal =/= true

all other non-terminal statuses:
  true
```

This is the critical difference from the current signal-only model. `waiting`
does not automatically mean unrecoverable. It means unrecoverable unless a
durable timer is due.

## Indexed Recovery

PostgreSQL run projections add:

```sql
next_wake_at_ms bigint
```

`timer.scheduled` updates `next_wake_at_ms` to the minimum pending timer
deadline. `timer.fired` recomputes it from remaining scheduled timers or clears
it to `NULL`.

The indexed recoverable query keeps the live lease join and changes the waiting
filter from an absolute exclusion to a wake gate:

```sql
AND (
  r.status <> 'waiting'
  OR r.next_wake_at_ms <= $1
)
AND (
  r.status <> 'retrying'
  OR r.next_retry_at_ms <= $1
)
AND (
  l.lease_until_ms IS NULL
  OR l.lease_until_ms <= $1
)
```

Memory storage mirrors the same projection from reduced state. Neither adapter
stores `migration_required_for_version_change` because that value depends on
current workflow code and must be computed by `beamtrail_state`.

## Firing Timers

`timer.fired` is appended by the dispatch path while holding a valid run lease.

The ordering inside `dispatch_locked` is:

1. Load or receive current reduced state.
2. Reject terminal, parked, stale-lease, or migration-required states.
3. Find due scheduled timers with `fire_at_ms =< Now`.
4. Append one or more `timer.fired` events with the current expected sequence and
   the lease fencing token.
5. Apply those events to state.
6. Snapshot opportunistically.
7. Continue into the normal decider transition with the updated state.

Two scanners cannot fire the same timer twice because the write uses the same
per-run lock, expected sequence, and fencing token discipline as other durable
transitions.

`timer.fired` must update reducer state so the due timer no longer contributes
to `next_wake_at`. Otherwise the run would stay recoverable and repeatedly wake.

## Waiting, Signals, And Stale Timers

`workflow.waiting` still releases the active runner lease and stops the runner.
If the run also has a `next_wake_at`, the scanner can wake it when that deadline
is due.

`signal_run/3` may wake a waiting run before a timer fires. Until timer
cancellation exists, the scheduled timer can still fire later if the run is
non-terminal. This is acceptable in the first timer version. The decider must
treat timer events as input and ignore stale timers that no longer matter.

If the signal path reaches a terminal event before the deadline, terminal
recoverability prevents the timer from firing.

## Test Matrix

Core tests:

- A decider returning `{sleep, TimerId, DelayMs}` schedules `timer.scheduled`.
- Scheduling a timer and then returning `{wait, Reason}` produces
  `timer.scheduled` followed by `workflow.waiting`.
- A waiting run with a future `next_wake_at` is not recoverable.
- A waiting run with a due `next_wake_at` is recoverable.
- Dispatching a due timer appends `timer.fired` before the decider continues.
- A signal before the deadline can complete the workflow; the later timer does
  not fire after terminal completion.
- Reusing a timer id with a different deadline terminally fails the run.
- Reusing a fired timer id terminally fails the run.

PostgreSQL tests:

- `timer.scheduled` and `timer.fired` encode, decode, and replay correctly.
- The indexed recoverable query excludes future waiting timers and includes due
  waiting timers.
- `next_wake_at_ms` projection mirrors reducer state.
- Backfill computes `next_wake_at_ms` from existing timer events.

Crash and concurrency tests:

- Two recovery attempts cannot append duplicate `timer.fired` events.
- A run scheduled before an application restart fires after `scan_now/0` once the
  timer is due.
- A due timer under an expired lease is fired only by the process that acquires
  the new lease.

Timing tests should use `beamtrail_scanner:scan_now/0` or a lowered scanner
interval. The default scanner interval is five seconds, so a 50 ms timer should
not be expected to wake by itself within 50 ms.

## Implementation Order

1. Add reducer state keys and decision view fields.
2. Add command validation for `sleep` and `sleep_until`.
3. Implement `timer.scheduled` transition and timer idempotence checks.
4. Materialize due timers before the decider runs.
5. Update exact recoverability for waiting runs with due timers.
6. Update memory projection and indexed candidates.
7. Update PostgreSQL schema, projection, decode, backfill, and indexed query.
8. Add tests from the matrix above.
9. Update README, ARCHITECTURE, DECIDER, and CHANGELOG.

## Future Work

- Local active-runner `send_after` fast path for low-latency timers.
- Timer cancellation and rescheduling events.
- Recurring timers and cron.
- Human approval deadlines built on top of signal plus timer composition.
