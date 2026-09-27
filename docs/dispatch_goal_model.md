# Durable dispatch goal binding (Task 013)

This note defines the exact external goal specification that is bound to a dispatch, and how it
is bound. Code: `MissionStore.prepare_step_dispatch`, `MissionStore.get_dispatch_goal` and the
`authorize_step_send` precondition in `task_resilience/task_resilience/storage.py`, the
`ExpectedGoalSpecification` and `DispatchGoalRecord` in `model.py`, and three errors in
`errors.py`. Tests: `task_resilience/test/test_dispatch_goal.py`.

Schema version 8 adds exactly one table, `dispatch_goals`. It adds no other table, column or
index, and it changes no existing row. Tasks 009-012 added `dispatch_contexts` (version 4),
`dispatch_authorizations` (version 5), `recovery_evidence` (version 6) and
`mission_reconciliation_contexts` (version 7); none of that changes anything described here, and
this task changes nothing about any of them.

The contract in one sentence: **any goal specification created by supported schema version 8
code was committed atomically with its dispatch's first preparation.** There is no later
binding, no rebind, no replace, no update and no repair.

## The gap this closes

Task 009's action context says **where** a dispatch is declared to reach: an action name, type
and namespace. It has never said **what** the dispatch is declared to carry: no goal body and no
goal hash was stored, and `docs/dispatch_context_model.md` said so explicitly, deferring it until
a durable per-step goal specification existed. A design pass proved the resulting gap directly,
against the real committed store: two hypothetical senders can each locally construct a
different `NavigateToPose` goal for the same durable `dispatch_id`, and the full seven-table
database dump is byte-identical before and after either construction. The database had no way to
say which goal, if either, the dispatch was intended to carry. A second experiment showed that
even an accepted mission payload that happens to encode something pose-shaped is not enough by
itself: two different (but equally plausible) translators can read the identical stored bytes
into two different goals, because nothing durable pinned the mapping from payload to goal, or its
version.

A mission's accepted request (`MissionRequest`, `missions.request_json`) and a dispatch's goal
specification (`ExpectedGoalSpecification`, `dispatch_goals.goal_json`) are two different things
kept apart on purpose, the same way `dispatch_contexts` is kept apart from `step_dispatches`: the
mission's request is what the caller asked for; a dispatch's goal is what one specific external
send is declared to carry. Neither is derived from the other by any code in this package.

## What is bound

Exactly two things, declared for a dispatch **before** its send can be authorized:

| Field | Rule |
|---|---|
| `goal_format` | non-empty `str`, opaque to the core |
| `payload` | any value in the strict JSON domain (`_check_json` in `model.py`): `None`, `bool`, `int`, finite `float`, `str`, `list`, `dict` with `str` keys, no cycles, no subclasses |

`goal_format` is a caller-chosen tag naming which adapter format/version the payload means (for
example `nav2/navigate_to_pose/v1`), never interpreted by the core. It is exact and never
normalized: whitespace and case are kept as given, the same way `action_name`/`action_type` are.
`ExpectedGoalSpecification` (`model.py`) is the caller's input, and follows exactly the
established pattern of `MissionRequest`: the payload is accepted through an `InitVar` and is not
kept, only its canonical JSON text (`goal_json`, sorted keys, compact separators, ASCII escapes,
finite floats only) and a SHA-256 fingerprint. **The fingerprint is over the complete
specification, not the payload alone**:

```json
{"goal_format": "<goal_format>", "payload": <payload>}
```

so the complete specification, including both `goal_format` and `payload`, is the input to the
SHA-256 content identity, exactly as `MissionRequest.fingerprint` is over `{"max_attempts": ...,
"payload": ...}`, not the payload alone. Replay equality is decided from exact `goal_format` and
canonical `goal_json`, never the fingerprint alone (see "Replay" below). `DispatchGoalRecord`
(`model.py`) is what a read returns: `dispatch_id`, `mission_id`,
`step_key`, `goal_format`, a decoded `payload` and `goal_fingerprint`, frozen and validated. The
decoded `payload` is a snapshot, in the same limited sense `MissionRecord.payload` is: an ordinary
mutable `dict`/`list` locally, but mutating it never writes through to SQLite, and it is never
re-read from a caller's own object once the record exists.

## Declared intent, not proof

The goal specification records **what external request the dispatch is declared to carry**. It
does not show what was actually sent, still less what a server executed. Nothing in the store
constructs a ROS message, sends anything, or reads this table for any purpose but binding and
replay comparison. A program that sends something other than what it declared is not detected by
this binding, the same limit the action context already has.

What the binding does give a later reader is an answer that no longer depends on the process
doing the recovery: exactly which goal the dispatch was meant to carry, fixed before the send was
allowed, and not reconstructed afterwards by whoever happens to be recovering, from a payload, a
translator, or a guess.

## The table

```sql
CREATE TABLE dispatch_goals (
    dispatch_id      TEXT NOT NULL PRIMARY KEY
                     REFERENCES step_dispatches (dispatch_id) ON DELETE CASCADE,
    goal_format      TEXT NOT NULL CHECK (goal_format <> ''),
    goal_json        TEXT NOT NULL,
    goal_fingerprint TEXT NOT NULL
)
```

The absence of a row means **no durable goal is bound**. There is no placeholder row and no NULL
that stands for "unknown". No timestamp: the dispatch's own `created_at`, in `step_dispatches`,
is when this was bound, because binding is atomic with dispatch creation. No explicit index,
trigger or view; only the primary key's automatic index exists.

Not stored, on purpose: any ROS message type, any lease, owner or fencing field, an attempt
number, or any evidence. `dispatch_contexts` is unchanged and still stores no goal, hash, body,
lease, owner or fence field either; that guard predates this task and stays scoped to that table.

### What the primary key does and does not do

The primary key gives **one goal row per dispatch**. It does **not** make the row's contents
physically immutable: SQLite accepts an `UPDATE` of `goal_json` from any connection that bypasses
the store. The immutability is a contract of the supported code:

- a goal is inserted in exactly one place, the first preparation of a step, in the same
  transaction as the dispatch row, the context row and the step's state change;
- there is no public method that replaces, updates, adds or repairs one (no `bind_goal`,
  `set_goal`, `update_goal`, `replace_goal`, `repair_goal`, `backfill_goal` or `rebind_goal`);
- production code contains no `UPDATE`, `DELETE`, `REPLACE` or `ON CONFLICT` for the table, and a
  test reads the source to check it.

A writer that bypasses the code, such as an older build or raw SQL, is not stopped by the schema.

## Preparation binds four things together

`prepare_step_dispatch(mission_id, step_key, action, goal)` now requires the goal specification
as well as the action context. For a `PENDING` step, in **one** write transaction:

1. the state machine validates `PENDING` to `DISPATCH_PREPARED`;
2. a step that already has a dispatch is reported as corrupt, as before;
3. a UUIDv4 is generated;
4. the `step_dispatches` row is inserted;
5. the `dispatch_contexts` row is inserted for the same UUID;
6. the `dispatch_goals` row is inserted for the same UUID;
7. the step is moved to `DISPATCH_PREPARED`;
8. the `DispatchRecord` is returned.

The UUID, the context, the goal and `DISPATCH_PREPARED` commit together. A failure anywhere,
including at `COMMIT`, rolls all four back: never a UUID without a goal, a goal without a UUID,
or a prepared step without all of them. Both the `action` and the `goal` arguments are
type-checked before the database is touched. Preparation still consumes no attempt, and
**preparation alone still authorizes no send**: `authorize_step_send` is the only operation that
does. This first preparation is the only operation that ever inserts a goal row.

## Replay

A step that is already `DISPATCH_PREPARED` and has its dispatch, its context judged first
(preserving Task 009's diagnostic precedence), then its goal:

| Replay | Result |
|---|---|
| Same `goal_format` and the same canonical `goal_json` | The existing `DispatchRecord`. Nothing is written, timestamps included. |
| A different `goal_format` | `DispatchGoalConflict`, naming `('goal_format',)`. Nothing is written. |
| The same `goal_format` but a different canonical `goal_json` | `DispatchGoalConflict`, naming `('goal_content',)`. Nothing is written. |
| Both differ | `DispatchGoalConflict`, naming both. |
| No goal is bound | `DispatchGoalMissing`. Nothing is written, and **no goal is inserted**. |
| The bound goal is malformed | `CorruptDispatchGoal`, not a conflict. Nothing is written. |

**Equality is decided from the format and the complete canonical JSON text, reconstructed
through the same canonicalizer that built it, never from the fingerprint alone.** A fingerprint
match is not treated as proof of identity; only the stored bytes are. Two dictionary key orders
that canonicalize to the same text are an identical replay; a different list order, a different
`goal_format`, or a different payload value (`1` and `1.0` included, since the canonicalizer
keeps them apart as different JSON kinds) is a conflict. A step that has already progressed past
`DISPATCH_PREPARED` is refused by the state machine exactly as before (`InvalidStateTransition`),
whatever goal is offered, and nothing is bound: `prepare_step_dispatch` is never a goal-recovery
or update operation for a step that has moved on.

### A missing goal fails closed, and is never backfilled

A `DISPATCH_PREPARED` step with a dispatch UUID, a context, and no goal can arise in two ways:

- **A.** a step prepared under schema version 7 or earlier, which never had a goal table;
- **B.** a step prepared under version 8 whose goal row was later removed by something outside
  the supported code, such as raw SQL or a damaged file.

Schema version 8 stores nothing that tells them apart, exactly as version 4 could not tell a
version-3 dispatch with no context from one whose context was removed. The store therefore does
not guess, and it does not use a later `prepare_step_dispatch` call as an opportunity to bind the
newly supplied goal to that old identity: the store cannot know that the body a caller now
supplies is what the dispatch was originally meant to execute, and treating a replay as a
backfill point is exactly the silent reconstruction that binding before the send exists to
prevent. So both fail the same way: `DispatchGoalMissing`, from a replay, from
`get_dispatch_goal` and from `authorize_step_send`, with nothing written, forever.

## Reading

`get_dispatch_goal(mission_id, step_key)` returns the frozen record. It never binds or repairs
anything, and it executes no write. What it reports:

| Situation | Error |
|---|---|
| The mission does not exist | `MissionNotFound` |
| The step does not exist | `StepNotFound` |
| The step has no dispatch | `DispatchNotFound` |
| The dispatch is duplicated or its UUID is not canonical | `CorruptDispatchRecord` |
| The dispatch exists and no goal is bound | `DispatchGoalMissing` |
| A goal row exists but is not a valid goal | `CorruptDispatchGoal` |
| Otherwise | the `DispatchGoalRecord` |

`CorruptDispatchGoal` covers: `goal_format` empty or not text; `goal_json` not text, not valid
JSON, not in the strict JSON domain once parsed, or not canonical for the payload it decodes to;
`goal_fingerprint` not text, or not equal to the SHA-256 of `{"goal_format": ..., "payload":
...}` recomputed from the decoded payload. The schema's own checks should make most of this
unreachable, so reaching it means the row was changed from outside. Nothing is repaired: a
bypassing writer that changes both the content and the fingerprint consistently produces a row
this store cannot distinguish from a valid one, and this table is not tamper-evident, the same
limit every other table in this schema has.

## The authorization gate

`authorize_step_send` remains the only operation that consumes an attempt, enters `DISPATCHED`
and returns a `SendAuthorization`. Task 013 adds exactly one precondition, checked immediately
after the existing context check and before the existing authorization-provenance precheck, the
budget check and every write:

- absent: `DispatchGoalMissing`;
- malformed: `CorruptDispatchGoal`.

A refused authorization writes nothing at all: `attempts_used`, the mission's timestamp, the step
(still `DISPATCH_PREPARED`) and the dispatch are exactly as they were. Attempt semantics, the
refund policy and the receipt are unchanged, and no retry is permitted by it. Historical
query/recovery paths (`get_dispatch_context`, `get_dispatch_authorization`,
`apply_recovery_observation`, `get_recovery_evidence`, and every Task 012 reconciliation
operation) gained **no** goal requirement: none of them ever read this table, before or after
this task, because correlating a result with a dispatch only needs the durable dispatch UUID and
action context, which Task 013 does not touch.

## Migration

Version 7 to version 8 is a pure `CREATE TABLE`. It does not rebuild, alter, drop or fill
anything. The chain is one transaction whatever the starting point, so no version in between is
ever committed:

```text
fresh -> 8
v7 -> v8
v6 -> v7 -> v8
v5 -> v6 -> v7 -> v8
v4 -> v5 -> v6 -> v7 -> v8
v1 -> ... -> v8
```

Migration creates **zero** goal rows and invents no format or payload for any existing dispatch.
A migrated database has the same schema text as a fresh database of the same version.

## Rows that already exist

| Row, as migrated | What happens |
|---|---|
| `PENDING` | Nothing. When it is first prepared under version 8 it gets a UUID, a context, a goal and `DISPATCH_PREPARED` together, like any new step. |
| `DISPATCH_PREPARED` with a UUID and a context, no goal | Stays **permanently unbound**. `get_dispatch_goal`, `authorize_step_send` and a `prepare_step_dispatch(..., goal)` replay all raise `DispatchGoalMissing`. No attempt is consumed and no row, state or timestamp changes. It can still be cancelled or failed by an ordinary transition. |
| `DISPATCHED`, `ACCEPTED`, `EXECUTING`, `CANCEL_REQUESTED`, `OUTCOME_UNKNOWN`, or finished, with a UUID and no goal | Stay unbound. Ordinary state behavior is unchanged, and preparation refuses them with `InvalidStateTransition`. Every historical read (context, authorization, evidence, Task 012 reconciliation) is unaffected: none of it needs a goal. |

### There is no late binding

Task 013 has no supported way to give an existing dispatch a goal, for any state. A design that
let a replay bind a still-`DISPATCH_PREPARED` migrated step was considered and rejected, for the
same reason Task 009 rejected it for the action context: the missing goal it would act on is
ambiguous, and the store cannot know that the row is legacy rather than damaged, or that the
newly supplied body is what was originally meant.

A pre-version-8 step that is stuck this way is not repaired. If it ever needs to be, an explicit
legacy or operator-attestation mechanism could be designed later, the same way Task 009 already
left that door open and closed for the action context. It is not designed or implemented here.

## Historical query/recovery is unaffected

Goal binding gates a **new physical send**. It does not gate reading history. A migrated
dispatch that is already `DISPATCHED` or later may lack a goal row forever, and that absence does
not retroactively make its recorded history corrupt: `get_dispatch_context`,
`get_dispatch_authorization`, `apply_recovery_observation` and `get_recovery_evidence` correlate
a result with a dispatch by its durable UUID and its Task 009 action context alone, exactly as
before this task, and none of them was changed by it. The distinction is exactly "permission to
send" versus "ability to query": the first now additionally requires a bound goal; the second
never depended on one.

## Retry representation is deliberately left open

The current schema permits one dispatch per step and implements no retry mechanism. If multiple
dispatches are supported later, each dispatch must retain its own immutable goal binding. This
task does not define how retries are represented.

## Not a claim about ROS timestamps in general

The design pass that led to this task inspected the installed Jazzy `NavigateToPose` goal and the
project's own working sender, and found that `pose.header.stamp` is left at zero on purpose in
the one format the sender currently produces (its own code comment says so). That is a fact about
that one sender's convention, not a general claim that a ROS message timestamp is always
transport-only. This task's core is ROS-agnostic and says nothing about ROS header semantics at
all: `goal_format` and `payload` are opaque JSON to it. When a future adapter task defines what
`goal_format` values such as `nav2/navigate_to_pose/v1` mean, that task is where the exact
value/construction rule for any timestamp field belongs, reviewed on its own terms.

## Errors

| Error | Meaning |
|---|---|
| `DispatchGoalMissing(mission_id, step_key)` | The dispatch exists and has no bound goal specification. |
| `DispatchGoalConflict(mission_id, step_key, fields)` | The dispatch is already bound and the requested goal differs. `fields` names `'goal_format'` and/or `'goal_content'`, never the values. |
| `CorruptDispatchGoal(mission_id, step_key, reason, value)` | A goal row exists and cannot be read as a valid goal. |

They are separate on purpose: corruption is never reported as a conflict, and a missing or
corrupt dispatch identity keeps `DispatchNotFound` and `CorruptDispatchRecord`.

## Crashes and concurrency

The tests kill a real child process at chosen statements of the real code, before and after each
one, and reopen the database:

- around a first preparation, the UUID, the context, the goal and `DISPATCH_PREPARED` all
  survive or none does, and only a kill after `COMMIT` leaves them.

That is the only operation with a crash window, because it is the only one that writes a goal. A
replay, a read and a refused authorization write nothing.

Threads with one connection each race several preparations of one step, with the same goal and
with different goals: exactly one goal is bound each time, and every caller that offered a
different one gets `DispatchGoalConflict`. Because the identity, the context and the goal are one
atomic commit, a step is never observed as `DISPATCH_PREPARED` with a context and no goal, even
under a real race between preparation and a concurrent reader. These kill processes and run
threads. They say nothing about power loss or a filesystem that does not honor `fsync`.

## What this task does not do

Task 013 binds an opaque goal specification. It does **not** interpret it, does not define what
any particular `goal_format` means, does not import `rclpy`, `nav2_msgs`, `geometry_msgs` or any
ROS package, does not construct or send anything, does not add process ownership, a lease, or a
supervisor, and does not touch `state_machine.py` or `reconciliation.py`. Tasks 011 and 012 are
unaffected: recovery evidence and mission reconciliation read no goal, and this task reads
neither evidence nor a reconciliation context.

Not decided here: which `goal_format` values a future adapter defines, or what they mean; how a
future retry is represented; single-supervisor execution ownership (deferred to a later task);
the ROS adapter and the supervisor (later tasks); cancellation.
