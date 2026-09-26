# Durable dispatch action context (Task 009)

This note defines the action context that is bound to a dispatch, and how it is bound. Code:
`MissionStore.prepare_step_dispatch`, `MissionStore.get_dispatch_context` and the
`authorize_step_send` precondition in `task_resilience/task_resilience/storage.py`, the
`DispatchContextRecord` in `model.py`, and three errors in `errors.py`. Tests:
`task_resilience/test/test_dispatch_context.py`.

Schema version 4 adds exactly one table, `dispatch_contexts`. It adds no other table, column or
index, and it changes no existing row. Task 010 has since moved the schema to version 5 with one more
table, `dispatch_authorizations` (`docs/dispatch_authorization_model.md`), Task 011 to version 6 with the
evidence table, and Task 012 to version 7 with the reconciliation context of a mission
(`docs/mission_reconciliation_model.md`). None of that changes anything described here: this note is about what version 4 added, and the context is bound, read and
protected exactly as below.

The contract in one sentence: **any context created by supported schema version 4 code was
committed atomically with its dispatch's first preparation.** There is no later binding, no
backfill and no repair.

## What is bound

Three exact strings, declared for a dispatch **before** its send can be authorized:

| Field | Rule |
|---|---|
| `action_name` | non-empty `str` |
| `action_type` | non-empty `str` |
| `robot_namespace` | `str`; the empty string is allowed |

Nothing is normalized or inferred: `/robot1` and `robot1` differ, and so do `''` and `/`. These are
the rules of Task 007's `ExpectedActionContext`, and that type is what
`prepare_step_dispatch` takes. `storage.py` imports it from `reconciliation.py`. That adds no cycle:
`reconciliation.py` still imports only the standard library and the state definitions, and
`storage.py` imports nothing else from it.

`DispatchContextRecord` (`model.py`) is what a read returns: `dispatch_id`, `mission_id`,
`step_key` and the three strings, frozen and validated. It carries no time, no source and no
history, because none is stored.

## Declared intent, not proof

The context records **which action the dispatch was declared to reach**. It does not show where
any request was actually sent. Nothing in the store observes a send, and `DISPATCHED` already means
only that an authorization committed (`docs/send_authorization_model.md`). A program that sends
somewhere other than what it declared is not detected by this binding.

What the binding does give a later reader is an answer that no longer depends on the process doing
the recovery: which action the dispatch was meant for, fixed before the send was allowed, and not
supplied afterwards by whoever happens to be recovering.

## The table

```sql
CREATE TABLE dispatch_contexts (
    dispatch_id     TEXT NOT NULL PRIMARY KEY
                    REFERENCES step_dispatches (dispatch_id) ON DELETE CASCADE,
    action_name     TEXT NOT NULL CHECK (action_name <> ''),
    action_type     TEXT NOT NULL CHECK (action_type <> ''),
    robot_namespace TEXT NOT NULL
)
```

The absence of a row means **no durable context is bound**. There is no placeholder row and no
NULL that stands for "unknown".

Not stored, on purpose: a server epoch or incarnation, any time, a goal body or hash, a retry
field, an owner, lease or fencing field, or any evidence. `NOT NULL` on the key matches
`step_dispatches`.

### What the primary key does and does not do

The primary key gives **one context row per dispatch**. It does **not** make the row's contents
physically immutable: SQLite will accept an `UPDATE` of `action_name` from any connection, and a
test shows that it does. The immutability is a contract of the supported code:

- a context is inserted in exactly one place, the first preparation of a step, in the same
  transaction as the dispatch row and the step's state change;
- there is no public method that replaces, updates, adds or repairs one;
- production code contains no `UPDATE`, `DELETE`, `REPLACE` or `ON CONFLICT` for the table, and a
  test reads the source to check it.

A writer that bypasses the code, such as an older build or raw SQL, is not stopped by the schema.

## Preparation binds three things together

`prepare_step_dispatch(mission_id, step_key, action)` now requires the action. For a `PENDING`
step, in **one** write transaction:

1. the state machine validates `PENDING` to `DISPATCH_PREPARED`;
2. a step that already has a dispatch is reported as corrupt, as before;
3. a UUIDv4 is generated;
4. the `step_dispatches` row is inserted;
5. the `dispatch_contexts` row is inserted for the same UUID;
6. the step is moved to `DISPATCH_PREPARED`;
7. the `DispatchRecord` is returned.

The UUID, the context and `DISPATCH_PREPARED` commit together. A failure anywhere, including at
`COMMIT`, rolls all three back: never a UUID without a context, a context without a UUID, or a
prepared step without either. The `action` argument is type-checked before the database is touched.
Preparation still consumes no attempt, and **preparation alone still authorizes no send**:
`authorize_step_send` is the only operation that does. This first preparation is the only operation
that ever inserts a context.

## Replay

A step that is already `DISPATCH_PREPARED` and has its dispatch:

| Replay | Result |
|---|---|
| Same three strings | The existing `DispatchRecord`. Nothing is written, timestamps included. |
| Any of the three different | `DispatchContextConflict`, naming the fields that differ. Nothing is written. |
| No context is bound | `DispatchContextMissing`. Nothing is written, and **no context is inserted**. |
| The bound context is malformed | `CorruptDispatchContext`, not a conflict. Nothing is written. |

A replay only reads. A missing context is reported before the strings are compared, so not even a
different context is answered with a conflict. The context is never replaced through a supported
API. A step that has already progressed past `DISPATCH_PREPARED` is refused by the state machine
exactly as before (`InvalidStateTransition`), whatever context is offered, and nothing is bound.

### A missing context fails closed

A `DISPATCH_PREPARED` step with a dispatch UUID and no context can arise in two ways:

- **A.** a step prepared under schema version 3, which never had a context;
- **B.** a step prepared under version 4 whose context row was later removed by something outside
  the supported code, such as raw SQL or a damaged file.

Schema version 4 stores nothing that tells them apart: both are a prepared step, one dispatch row and
no context row, and a test builds both and checks that they leave the same durable shape and get the
same answers. The store therefore does not guess. Treating every missing context as a legacy row
would let a replay silently reconstruct a context that was lost, from whatever action the caller
supplies now, which is exactly what binding before the send exists to prevent. So both fail the same
way: `DispatchContextMissing`, from a replay, from `get_dispatch_context` and from
`authorize_step_send`, with nothing written.

## Reading

`get_dispatch_context(mission_id, step_key)` returns the frozen record. It never binds or repairs
anything, and it executes no write. What it reports:

| Situation | Error |
|---|---|
| The mission does not exist | `MissionNotFound` |
| The step does not exist | `StepNotFound` |
| The step has no dispatch | `DispatchNotFound` |
| The dispatch is duplicated or its UUID is not canonical | `CorruptDispatchRecord` |
| The dispatch exists and no context is bound | `DispatchContextMissing` |
| A context row exists but is not a valid context | `CorruptDispatchContext` |
| Otherwise | the `DispatchContextRecord` |

`CorruptDispatchContext` covers an empty name or type, and a value that is not text. The schema's
own checks should make it unreachable, so it means the row was changed from outside.

## The authorization gate

`authorize_step_send` remains the only operation that consumes an attempt, enters `DISPATCHED` and
returns a `SendAuthorization`. Task 009 adds exactly one precondition (Task 010 later adds a check
after it, for an existing authorization record, and the context check keeps its place and its
error). After the checks for the
mission, the step state, the unknown-outcome gate and the dispatch identity, the dispatch must have a
valid bound context:

- absent: `DispatchContextMissing`;
- malformed: `CorruptDispatchContext`.

It comes **before** the state-machine check and before the attempt is taken. A refused
authorization writes nothing at all: `attempts_used`, the mission's timestamp, the step (still
`DISPATCH_PREPARED`) and the dispatch are exactly as they were. Attempt semantics, the refund policy
and the receipt are unchanged, and no retry is permitted by it.

## Migration

Version 3 to version 4 is a pure `CREATE TABLE`. It does not rebuild, alter, drop or fill anything.
The chain is one transaction whatever the starting point, so no version in between is ever
committed:

```text
fresh -> 4
v3 -> v4
v2 -> v3 -> v4
v1 -> v2 -> v3 -> v4
```

Since Task 010 every one of these chains carries on to version 5 in the same transaction, and a
database that is already at version 4 runs only the version 5 step.

Migration creates **zero** context rows and invents no action name, type or namespace. A
migrated database has the same schema text as a fresh database of the same version (4 when this
note was written, 7 since Task 012). The tests build their
version 3 database from the version 3 schema written out in full. That text was checked, when this
task was written, against what the committed version 3 code creates: the two were identical.

## Rows that already exist

| Row, as migrated from version 3 | What happens |
|---|---|
| `PENDING` | Nothing. When it is first prepared under version 4 it gets a UUID, a context and `DISPATCH_PREPARED` together, like any new step. |
| `DISPATCH_PREPARED` with a UUID and no context | Stays **permanently unbound**. `get_dispatch_context`, `authorize_step_send` and a `prepare_step_dispatch(..., action)` replay all raise `DispatchContextMissing`. No attempt is consumed and no row, state or timestamp changes. It can still be cancelled or failed by an ordinary transition. |
| `DISPATCHED`, `ACCEPTED`, `EXECUTING`, `CANCEL_REQUESTED`, `OUTCOME_UNKNOWN`, or finished, with a UUID and no context | Stay unbound. Ordinary state behavior is unchanged, and preparation refuses them with `InvalidStateTransition`. |
| A version 2 step with no UUID | Unchanged from Task 005: no dispatch, so no context. |

### There is no late binding

Task 009 has no supported way to give an existing dispatch a context, for any state. A design
that let a replay bind a still-`DISPATCH_PREPARED` migrated step was considered and rejected,
because the missing context it would act on is ambiguous, as above: the store cannot know that the
row is legacy rather than damaged.

A version 3 step that is stuck this way is not repaired. If it ever needs to be, an explicit
legacy or operator-attestation mechanism could be designed later. That is separate work: it would
have to say who attests, what is recorded about the attestation, and that it declares an intent
without proving where an older or bypassing program may have used that UUID. It is not designed or
implemented here.

A dispatch that stays unbound is unavailable to any future evidence-backed reconciliation until
such a mechanism exists. That includes every migrated dispatch that was already authorized, in
flight, unknown or finished under version 3.

## Correlation is not the same as proving the goal

The context is for **action correlation**: which action a result or status is about. It is not a
proof that the correct goal body was sent. `GetResult` does not return the goal, so evidence
obtained by UUID cannot show what the original request contained, and Task 006 found a UUID the
server had forgotten accepted again as a new goal, so a UUID does not pin the request body. The
project also has no durable per-step goal specification yet.
So no goal body and no goal hash is stored, and binding the goal is deferred until such a
specification exists.

## Errors

| Error | Meaning |
|---|---|
| `DispatchContextMissing(mission_id, step_key)` | The dispatch exists and has no bound context. |
| `DispatchContextConflict(mission_id, step_key, fields)` | The dispatch is already bound and the requested context differs. `fields` names the fields, never the values. |
| `CorruptDispatchContext(mission_id, step_key, reason, value)` | A context row exists and cannot be read as a valid context. |

They are separate on purpose: corruption is never reported as a conflict, and a missing or corrupt
dispatch identity keeps `DispatchNotFound` and `CorruptDispatchRecord`.

## Crashes and concurrency

The tests kill a real child process at chosen statements of the real code, before and after each
one, and reopen the database:

- around a first preparation, the UUID, the context and `DISPATCH_PREPARED` all survive or none
  does, and only a kill after `COMMIT` leaves them.

That is the only operation with a crash window, because it is the only one that writes a context. A
replay, a read and a refused authorization write nothing.

Threads with one connection each race several preparations of one step, with the same context and
with different contexts: exactly one context is bound each time, and every caller that offered a
different one gets `DispatchContextConflict`. Threads also replay preparation and attempt
authorization at once against many unbound prepared steps: every caller gets
`DispatchContextMissing`, no context is created, no authorization is issued and no attempt is
consumed. These kill processes and run threads. They say nothing about power loss or a filesystem
that does not honor `fsync`.

## What this task does not do

Task 009 stores the declared context. It does **not** query ROS, construct a
`RecoveryObservation`, call `decide_reconciliation`, record evidence, or apply a terminal outcome,
and `authorizes_send` on a decision stays `False`. Task 010 has since added the durable record that
supported code committed a send authorization for a dispatch (`docs/dispatch_authorization_model.md`),
and it does not touch the context. Task 011 has since built Task 007's `ExpectedActionContext` from this
durable binding and compares recovery observations against it before recording anything
(`docs/recovery_evidence_model.md`). A dispatch with no context takes no evidence at all: it fails with
`DispatchContextMissing`, and nothing is stored. Task 011 changes nothing about how a context is bound.
Task 012's reconciliation context belongs to a mission, not to a dispatch, and reads no dispatch context.

Not decided here: any mission-level consequence of recorded evidence; whether an
explicit legacy or operator-attestation mechanism should exist for a dispatch that has no context;
retries; goal-body binding; leases and fencing; the ROS adapter.
