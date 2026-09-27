# Mission-level reconciliation (Task 012)

This note defines how a mission that entered `NEEDS_RECONCILIATION` because a step's outcome was
unknown leaves it again, without losing whether it should resume as `RUNNING` or `CANCELLING`, without
inventing history, and without giving any caller a way to choose a state. Code:
`MissionStore.enter_mission_reconciliation`, `resolve_mission_reconciliation`,
`request_cancellation_during_reconciliation` and `get_mission_reconciliation_context` in
`task_resilience/task_resilience/storage.py`, `MissionReconciliationContextRecord` in `model.py`, and
five errors in `errors.py`. Tests: `task_resilience/test/test_mission_reconciliation.py`.

Schema version 7 adds exactly one table, `mission_reconciliation_contexts`. It adds no other table, no
column, no index, no trigger and no view, and it changes no existing row. The pure state machine
(`state_machine.py`) and the decision module (`reconciliation.py`) are unchanged.

The contract in one sentence: **a mission enters `NEEDS_RECONCILIATION` only through
`enter_mission_reconciliation`, because a persisted step is `OUTCOME_UNKNOWN`, and it leaves only through
`resolve_mission_reconciliation`, which restores exactly the state recorded at entry once no persisted step
is `OUTCOME_UNKNOWN`.**

## Why version 7 needs a stored resume state

Before this task nothing recorded where a reconciling mission came from. Two missions that went
`RUNNING` to `NEEDS_RECONCILIATION` and `RUNNING` to `CANCELLING` to `NEEDS_RECONCILIATION` leave
byte-identical rows in every one of the six version 6 tables once their ids and times are masked. Entering
`CANCELLING` writes the mission row and nothing else, so no step, dispatch or evidence row can carry the
origin either, and `updated_at` is overwritten by the entry itself. A test builds both histories and shows
exactly that (`test_the_older_tables_cannot_tell_a_running_origin_from_a_cancelling_one_and_the_context_can`).
Restoring "the" state, or asking the caller which one, would therefore either lose a cancellation or hand the
caller a way to choose the state. So the mission's intent is stored, in one small table, in the same
transaction that moves the mission.

Timestamps and step states are never used to reconstruct it.

## What `NEEDS_RECONCILIATION` means

Under supported version 7 code a mission is `NEEDS_RECONCILIATION` because, when it entered, at least one
persisted step was `OUTCOME_UNKNOWN`. That is an objective reason with an objective clearing condition:
**no persisted step is `OUTCOME_UNKNOWN`**. It is the same predicate as
`state_machine.requires_reconciliation`, which already existed and is now used by the store.

The pure graph still has `RUNNING -> NEEDS_RECONCILIATION` and `CANCELLING -> NEEDS_RECONCILIATION`, and
still has no edge out. But the store protects the way in, as Task 008 protects `DISPATCHED`: a plain
`transition_mission_state(mission_id, MissionState.NEEDS_RECONCILIATION)` raises
`MissionReconciliationEntryRequired`, for every mission state, a same-state replay included, and **before any
database access**, so even a mission that does not exist reports the entry error and not `MissionNotFound`.
This deliberately replaces the older committed contract that entering it was left to the caller: a state
that any caller could enter for any reason cannot have a clearing condition. Every generic exit from
`NEEDS_RECONCILIATION` is still `InvalidStateTransition`.

## The table

```sql
CREATE TABLE mission_reconciliation_contexts (
    mission_id   TEXT NOT NULL PRIMARY KEY
                 REFERENCES missions (mission_id) ON DELETE CASCADE,
    resume_state TEXT NOT NULL CHECK (
        resume_state IN ('RUNNING', 'CANCELLING')
    )
)
```

`mission_id` is the mission being reconciled: at most one row per mission, removed with the mission.
`resume_state` is the durable mission state to restore after the blocker clears.

It is **durable active intent**, not history, evidence, authorization or a permission. There is no
timestamp: no `entered_at` was added, because nothing needs one for correctness, and there is no reason
field, no episode identifier and no history table. While a mission stays blocked its own `updated_at` is the
time it entered, and nothing relies on that.

Unlike the tables of Tasks 010 and 011, this row is meant to change, so its writers are stated exactly:

| Mutation | Only this operation | Statement |
|---|---|---|
| `INSERT` | `enter_mission_reconciliation` | one row, `resume_state` = the mission's own state |
| `UPDATE` | `request_cancellation_during_reconciliation` | `SET resume_state = 'CANCELLING' WHERE mission_id = ? AND resume_state = 'RUNNING'` |
| `DELETE` | `resolve_mission_reconciliation` | one row, in the transaction that restores the mission |

and the foreign key's `ON DELETE CASCADE` when a mission is deleted. There is no `REPLACE`, no upsert, no
repair, no backfill, no arbitrary update and no public setter. The `UPDATE` is monotonic in SQL itself: a
row that already says `CANCELLING` matches nothing, so nothing can move it back to `RUNNING`.

Under supported code a row exists exactly while its mission is `NEEDS_RECONCILIATION`. The deliberate
exception is a mission that was already `NEEDS_RECONCILIATION` before version 7: it has no row, and fails
closed (below).

The table is **not tamper-evident**. A writer that bypasses `MissionStore` can insert, alter or remove a row,
and such writes are outside the supported contract.

## The operations

Each takes a mission ID and nothing else. None takes a state, a target, a reason or an episode.

### `enter_mission_reconciliation(mission_id) -> MissionReconciliationContextRecord`

One `BEGIN IMMEDIATE`, in this order:

1. the mission exists (`MissionNotFound`, or `CorruptStoredState` for a stored state that is not a state);
2. the context row, if any, is read and parsed strictly;
3. if the mission is `NEEDS_RECONCILIATION`: no context is `MissionReconciliationContextMissing`; a valid
   context is an **exact, state-based replay**: it is returned and nothing is written;
4. if the mission is anything else and a context exists: `CorruptMissionReconciliationContext`;
5. every persisted step is read;
6. the pure graph decides whether the source state may enter (`transition_mission`): only `RUNNING` and
   `CANCELLING` may, and any other state is `InvalidStateTransition`;
7. `requires_reconciliation` must be true, or `NoUnknownStepToReconcile`;
8. `INSERT` the context, its `resume_state` being the mission's current state;
9. one `_utc_now()` reading;
10. `UPDATE` the mission to `NEEDS_RECONCILIATION`, **last**;
11. the context is read back, and returned only after `COMMIT`.

Entry needs no evidence, dispatch or authorization lookup: the reason is the persisted `OUTCOME_UNKNOWN`
state itself. It therefore also succeeds when that step is *held* and can never be settled (see below).
It writes no step, attempt, authorization or evidence.

### `resolve_mission_reconciliation(mission_id) -> MissionRecord`

One `BEGIN IMMEDIATE`:

1. the mission exists; the context is read strictly;
2. if the mission is not `NEEDS_RECONCILIATION`: a context is `CorruptMissionReconciliationContext`, no context
   is `MissionNotAwaitingReconciliation(mission_id, mission_state)`;
3. `NEEDS_RECONCILIATION` with no context is `MissionReconciliationContextMissing`: nothing is guessed,
   adopted or backfilled;
4. every persisted step is read, in step order;
5. if any is `OUTCOME_UNKNOWN`: the **existing** `MissionReconciliationRequired`, naming those steps in step
   order;
6. the target is exactly `context.resume_state`, and never a caller's value;
7. `DELETE` the context;
8. one `_utc_now()` reading;
9. `UPDATE` the mission to the target, **last**;
10. the mission is read back, and returned after `COMMIT`.

This is a dedicated, non-ordinary storage exit, `NEEDS_RECONCILIATION -> RUNNING` or
`NEEDS_RECONCILIATION -> CANCELLING`, in the same way Task 011 has a dedicated evidence-only
`OUTCOME_UNKNOWN` resolution. It does not call `transition_mission`, and `state_machine.py` gains no edge:
`transition_mission(NEEDS_RECONCILIATION, ...)` still refuses every target.

It reads **only the persisted step states**. It never reads `recovery_evidence`, dispatches or authorizations,
infers provenance, or looks at an evidence outcome. It never chooses `COMPLETED`, `FAILED` or `CANCELLED`,
never authorizes a send, and never touches an attempt.

**Retry semantics.** Resolution is a **state-based operation on the current reconciliation of the mission**,
not a command keyed by an ID, and it does not claim exact replay. If a commit succeeded and its reply was
lost, the retry finds a mission that is no longer `NEEDS_RECONCILIATION` and has no context, and raises
`MissionNotAwaitingReconciliation` carrying the state it is in. A caller treats that as "already resolved, by
anyone", and reads the mission. If a completely new reconciliation began before the retry, the retry acts on
that one, under the same clearing rule: it resolves it if the blocker has cleared, and raises
`MissionReconciliationRequired` otherwise. No receipt or episode table exists.

### `request_cancellation_during_reconciliation(mission_id) -> MissionReconciliationContextRecord`

One `BEGIN IMMEDIATE`: the mission and its context are judged as for resolution
(`MissionNotAwaitingReconciliation`, `CorruptMissionReconciliationContext`,
`MissionReconciliationContextMissing`). A context that already says `CANCELLING` is returned unchanged and
nothing is written (an exact idempotent replay). One that says `RUNNING` runs only the guarded `UPDATE`
above, and if that does not change exactly one row it is `CorruptMissionReconciliationContext`. The context
is read back and returned.

It reads no step, writes no mission row (so `updated_at` does not move), and touches no attempt, step,
dispatch, authorization or evidence. It means only: **"when this mission is eventually allowed to leave
reconciliation, resume as `CANCELLING` rather than `RUNNING`."** It does not mean that the mission is now
`CANCELLING`, that a ROS cancel was sent, that a goal or a step was cancelled, that return-to-base is
authorized, that an unknown step may be discarded, or that reconciliation may now finish. It is not a
cancellation protocol. The intent never moves the other way: there is no operation that writes `RUNNING`
into an existing context.

### `get_mission_reconciliation_context(mission_id) -> MissionReconciliationContextRecord`

Reads the mission state and the context in one deferred read snapshot, so a concurrent entry or
resolution cannot make them contradict each other, and writes nothing. A mission that is not
`NEEDS_RECONCILIATION` with no context is `MissionNotAwaitingReconciliation`; with a context it is
`CorruptMissionReconciliationContext`. A `NEEDS_RECONCILIATION` mission with no context is
`MissionReconciliationContextMissing`. Otherwise the context is returned.

## Strict parsing and corruption

A stored `resume_state` must be exactly the text `RUNNING` or `CANCELLING`. Any other state, a lower-case or
padded spelling, a number or a blob is `CorruptMissionReconciliationContext`, and nothing is repaired. A
context on a mission that is not `NEEDS_RECONCILIATION` is corruption whatever it says, and is reported
before the graph or the reason is consulted, so neither can hide it. A duplicate is impossible (the primary
key).

| Situation | Result |
|---|---|
| `NEEDS_RECONCILIATION` and a valid context | normal active reconciliation |
| `NEEDS_RECONCILIATION` and no context | `MissionReconciliationContextMissing` (a mission from before version 7, or a row removed by something outside the store) |
| not `NEEDS_RECONCILIATION` and a context | `CorruptMissionReconciliationContext` |
| a `resume_state` that is not exactly one of the two | `CorruptMissionReconciliationContext` |
| a stored mission or step state that is not a state | `CorruptStoredState`, as everywhere else |

Nothing is inferred from `missions.updated_at`, step states or evidence rows.

## The clearing condition, and evidence

Whether the reason of this reconciliation remains is decided by the persisted step states alone. No step has
to be terminal: a `RUNNING` mission may resume with `PENDING` or prepared future work, and a `CANCELLING`
mission may resume `CANCELLING` with cancellation work still pending.

| Situation | Blocks resolution? |
|---|---|
| a step is `OUTCOME_UNKNOWN`, authorized, then settled by Task 011 evidence | no: the step is terminal |
| a step is `OUTCOME_UNKNOWN` and **held** (no authorization row) | **yes, for as long as the step is unknown** |
| a terminal step with a Task 011 `CONFLICT` row | no |
| a step that is not `OUTCOME_UNKNOWN` with `UNPROVEN_AUTHORIZATION` evidence | no |

A `CONFLICT` row is immutable and there is no way to adjudicate one, so making it a blocker would create a
permanent freeze with nothing to clear it. It stays discoverable through `get_recovery_evidence`. A new step
may become unknown while the mission is blocked (`transition_step_state` is not gated by the mission state);
the resolution transaction reads the steps again, so it sees it.

## Held steps and legacy missions fail closed

Two situations can keep a mission in `NEEDS_RECONCILIATION` indefinitely. Both are **accepted, intentional
outcomes of this task**, and neither has a way out here.

* **A held unknown step.** A step that became `OUTCOME_UNKNOWN` with no authorization row (Task 011's
  prepared-step anomaly, or a migrated version 4 step) is never settled by Task 011. If the mission enters
  reconciliation because of it, the mission stays `NEEDS_RECONCILIATION`, its context stays present, and
  `resolve_mission_reconciliation` keeps raising `MissionReconciliationRequired` naming it. This is not
  corruption. Before entering, the mission still has the ordinary `FAILED` and `CANCELLING` edges; nothing
  here changes that.
* **A mission that was `NEEDS_RECONCILIATION` under version 6.** The store cannot know whether it was to
  resume as `RUNNING` or as `CANCELLING`, so migration gives it no row and it has no trustworthy resume
  state. Getting, entering, resolving and requesting cancellation on it all raise
  `MissionReconciliationContextMissing`, and its generic exits are as closed as they were. No context is
  guessed, adopted or synthesized.

Deferred, and not a prerequisite for the ROS adapter unless integration shows otherwise: an operator
attestation, an abandon or override path, legacy adoption, and any way to synthesize provenance. Task 012
adds none of them.

## Terminal outcomes, attempts and sends

Restoring a mission is a **state restoration only**. Resolution never transitions to `COMPLETED`, `FAILED` or
`CANCELLED`, and never duplicates the completion invariant: after a `RUNNING` restore, completion goes through
the ordinary edge and its unchanged all-`SUCCEEDED` check, and after a `CANCELLING` restore the ordinary
policy decides. A finished mission is never reopened (`InvalidStateTransition` on entry,
`MissionNotAwaitingReconciliation` on the others).

Task 012 never consumes or refunds an attempt (only `authorize_step_send` consumes one), never inserts or
deletes an authorization row, and never returns a `SendAuthorization`.

| Mission | `authorize_step_send` |
|---|---|
| `NEEDS_RECONCILIATION` | `MissionNotRunning` |
| restored `RUNNING` | Task 008's gates decide, as always |
| restored `CANCELLING` | `MissionNotRunning` |

**Restoring `RUNNING` does not authorize motion.** A later supervisor decides whether and when to continue
work, and Task 008's gates stay authoritative. A `RUNNING` mission may hold an `OUTCOME_UNKNOWN` step: a
crash between the two transactions that make a step unknown and move the mission, or a resolution that wins
an instant before another step becomes unknown. That is a supported state. The step-level gate then refuses the
next send with `MissionReconciliationRequired`, and a new reconciliation may be entered.

The ordinary policy that lets a mission become `FAILED`, or `CANCELLED` after `CANCELLING`, while a step is
unknown and it has **not** entered reconciliation is unchanged and out of scope.

## Transactions, write order, races and crashes

Every writing operation is one `BEGIN IMMEDIATE` and nothing external happens inside it. Entry inserts the
context and updates the mission last; resolution deletes the context and updates the mission last;
the cancellation request updates only the context. **The transaction is what makes each of them atomic**: the
order is chosen so that a failure at any statement is easy to place and the mission row is always the last
write, not because the order itself protects anything.

`BEGIN IMMEDIATE` serializes every supported writer, so each race has a small set of outcomes, all tested:
two entries (both succeed, one row); entry against a plain cancellation; entry against the last unknown step
being settled; resolution against that; against another step becoming unknown; against a cancellation
request (the request either lands first and the mission resumes `CANCELLING`, or is refused with
`MissionNotAwaitingReconciliation` and the caller uses the ordinary edge, and it is never accepted and lost);
two resolutions (one succeeds); against a send authorization (never authorized while reconciling); against
a plain `FAILED` transition; against the context being removed out of band (`MissionReconciliationContextMissing`,
the mission stays blocked, nothing is guessed); and two cancellation requests.

The tests fail each statement of the three writing operations and kill a real child process, by exit and by
`SIGKILL`, around `BEGIN`, the context write or delete, the mission update, `COMMIT` and after the call
returned. Before `COMMIT` nothing is durable; at `COMMIT` and after it, all of it is. This is a killed
process, not a power loss, and it says nothing about `fsync` or a filesystem that does not honor it.

## Migration

Version 6 to version 7 is one `CREATE TABLE mission_reconciliation_contexts` and the new `user_version`. A native
version 6 database executes only those two statements. It creates zero rows, changes no existing row or table
definition, and uses no `ALTER TABLE`. A version 1, 2, 3, 4 or 5 database goes through every step up to 7 in the
one transaction it already used. A fresh database and a migrated one have the same schema text. A migrated
`NEEDS_RECONCILIATION` mission receives no row.

**A later schema note.** Task 013 has since moved the current schema to version 8, adding
`dispatch_goals`. None of this task's four operations (`enter_mission_reconciliation`,
`resolve_mission_reconciliation`, `request_cancellation_during_reconciliation`,
`get_mission_reconciliation_context`) reads that table, and this task's contract is unchanged. See
`docs/dispatch_goal_model.md`.

## What this task does not do

It does not query ROS, send, cancel or retry anything, run an adapter or a supervisor, change a step, read
evidence, authorize a send, or add an edge to the state machine. It does not add an operator override, an
abandon exit, legacy adoption, provenance synthesis, a way to gate `FAILED` or `CANCELLED` on unknown steps, a
conflict adjudication, an episode history or an `entered_at`. The ROS adapter and supervisor come after this.
