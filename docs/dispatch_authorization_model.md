# Durable send-authorization provenance (Task 010)

This note defines the one durable fact that Task 010 adds for each dispatch, and how it is written and
read. Code: `MissionStore.authorize_step_send` and `MissionStore.get_dispatch_authorization` in
`task_resilience/task_resilience/storage.py`, the `DispatchAuthorizationRecord` in `model.py`, and
two errors in `errors.py`. Tests: `task_resilience/test/test_dispatch_authorization.py`.

Schema version 5 adds exactly one table, `dispatch_authorizations`. It adds no other table, column or
index, and it changes no existing row.

The contract in one sentence: **within the supported `MissionStore` API, an authorization row is
created only by `authorize_step_send`, in the very transaction that commits that dispatch's send
authorization.** Nothing in the API writes one any other way. There is no later insertion, no backfill
and no repair. Under the supported-store contract, a valid row is durable provenance that
`authorize_step_send` committed authorization for this dispatch.

**The table is not tamper-evident.** A writer that bypasses `MissionStore` can forge or alter even a
canonically formatted row; such writes are outside the supported contract. The schema cannot prevent
it, and Task 010 adds no signature, MAC, hash, permission or ownership field to try to. Task 011 may
rely on the row under this same trusted-store assumption.

## What a row means

| A row means | A row does not mean |
|---|---|
| `authorize_step_send` committed a send authorization for this dispatch (under the supported-store contract). | `send_goal` was invoked. |
| The authorization, the attempt it took and `DISPATCHED` were committed together. | A request left the process, or reached anything. |
| | A server received, accepted or executed the goal. |
| | A result exists, or that one is still retained anywhere. |
| | Permission to send anything. It is not a token, a lease or a capability, and no API accepts it. |

Nothing in the store observes a send, and a row does not change that. It records the store's own
decision, made before the caller acts. The zero-send window of Task 008 still exists: if `COMMIT`
succeeds and the process dies before the caller sends anything, the row is there and no request ever
existed. A row therefore cannot be read as "it was sent".

## Why it exists

Before Task 010 the store kept, for a dispatch that ended `OUTCOME_UNKNOWN`, only the step's state and
the mission-wide `attempts_used`. `PREPARED` to `OUTCOME_UNKNOWN` is an ordinary edge, so a step could
be unknown having crossed the authorization boundary or never having crossed it, and no stored cell
said which. A test builds both histories, the second with the counter value that an earlier version
could leave behind, masks every UUID and time, and finds the version 4 tables identical. The one row
is what tells them apart, and it is the only thing it adds.

Task 010 does not decide what either case means for recovery. It only makes the difference durable so
that a later task can use it (see "What this task does not do").

## The table

```sql
CREATE TABLE dispatch_authorizations (
    dispatch_id   TEXT NOT NULL PRIMARY KEY
                  REFERENCES step_dispatches (dispatch_id) ON DELETE CASCADE,
    authorized_at TEXT NOT NULL
)
```

Not stored, on purpose: the attempt number, a mission ID or step key (the dispatch already names
them), any server or ROS time, any evidence or result, a retry field, an owner, lease or fencing
field, a goal body or hash. There is no explicit index.

### What the primary key does and does not do

The primary key gives **one row per dispatch**, so a second authorization of a dispatch cannot leave a
second row. It does **not** make a row physically immutable: SQLite accepts an `UPDATE` of
`authorized_at` from any connection, and a test shows that it does. The immutability is a contract of
the supported code:

- a row is inserted in exactly one place, `authorize_step_send`, in the same transaction as the attempt
  and the state change;
- there is no public method that updates, replaces, adds, repairs or deletes one;
- production code contains no `UPDATE`, `DELETE`, `REPLACE`, `OR IGNORE` or `ON CONFLICT` for the
  table, and a test reads the source to check it. A row goes away only through the foreign key's
  cascade, together with its dispatch.

A writer that bypasses the code, such as an older build or raw SQL, is not stopped by the schema, and
it can insert or change a row that is syntactically valid. The supported code's one defence is that it
refuses to read a row that no longer has the exact canonical form it writes. That catches a malformed
row. It does not catch a well-formed forgery, which is why the table is not tamper-evident (see the top
of this note).

## `authorized_at`

It is the store's own wall-clock UTC time for the authorization transaction, and it is the very same
text that `authorize_step_send` writes to `missions.updated_at` and `mission_steps.updated_at` and
returns as `SendAuthorization.authorized_at`. It is read once. The receipt takes it from the row read
back inside the transaction, not from a second reading of the clock.

What it is for: a durable audit fact. `updated_at` on the mission and the step is overwritten by every
later change, so before this task the time of the authorization was lost as soon as anything else
happened.

What it is **not**:

- not the time a request was sent, received or accepted by any server;
- not the time anything executed, or a result was produced;
- not proof that a result is still retained, or that one has expired. Nothing may use it to reason
  about how long a server keeps a goal, and it must never be used to decide that a retry is allowed;
- not a monotonic clock: wall clocks step backwards. It is never compared with another timestamp for
  correctness, so a value earlier than the mission's creation, or in the future, is still a valid row.

### Strictly canonical text

A stored value is accepted only if it parses and prints back to exactly the text the store writes,
`datetime.now(timezone.utc).isoformat(timespec='microseconds')`, for example
`2026-03-04T05:06:07.123456+00:00`. Anything else is `CorruptDispatchAuthorization`: text that is
empty or garbage, a value that is not text (a blob), a naive time, an offset other than `+00:00`,
`Z`, a time without microseconds or with a different count, a space or a lower-case `t` in place of
`T`, and any other spelling of a time that would parse. This mirrors the canonical-UUID rule for the
dispatch ID: only what this code writes is read.

## The attempt number is not kept

`SendAuthorization.attempt_number` is still the mission's `attempts_used` after the authorization,
which can be more than 1 for the first authorization of a migrated mission (`docs/send_authorization_model.md`).
That number is **not** stored per dispatch, and nothing may reconstruct it from the rows: not from the
row ID or insertion order, not from a count of rows, and not from sorting `authorized_at`. The counter
holds attempts that left no row, so the two would disagree, and a test shows a mission whose counter is
5 and whose rows number 2. The record has no `attempt_number` field.

## The authorization operation

`authorize_step_send(mission_id, step_key)` keeps its signature and its receipt. It is still one
`BEGIN IMMEDIATE` transaction, the checks are still in the same order and each still raises without
having written anything. Task 010 adds one check and one write:

| # | Step | Error |
|---|---|---|
| 1 | the mission exists and has a valid state | `MissionNotFound`, `CorruptStoredState` |
| 2 | the mission is exactly `RUNNING` | `MissionNotRunning` |
| 3 | the step exists and has a valid state | `StepNotFound`, `CorruptStoredState` |
| 4 | the step is exactly `DISPATCH_PREPARED` | `StepNotAwaitingSend` |
| 5 | no step of the mission is `OUTCOME_UNKNOWN` | `MissionReconciliationRequired` |
| 6 | the step has exactly one dispatch with a canonical UUID | `CorruptDispatchRecord` |
| 7 | the dispatch has a valid bound context | `DispatchContextMissing`, `CorruptDispatchContext` |
| 8 | the dispatch has a valid bound goal specification (added by Task 013) | `DispatchGoalMissing`, `CorruptDispatchGoal` |
| **9** | **the dispatch has no authorization row yet** | `CorruptDispatchAuthorization` |
| 10 | the state machine allows `DISPATCH_PREPARED` to `DISPATCHED` | `InvalidStateTransition` |
| 11 | an attempt remains, and one is taken | `AttemptLimitReached` |
| **12** | **the authorization row is inserted, with the same timestamp** | |
| 13 | the step is moved to `DISPATCHED`, with the same timestamp | |
| 14 | the row is read back for the receipt | |
| 15 | `COMMIT` | |
| 16 | the receipt is returned, only after the commit | |

The writes are the attempt `UPDATE`, the authorization `INSERT` and the step `UPDATE`, in that order,
and the step update is the last write. Before `COMMIT` a failure at any of them, or at `COMMIT`
itself, rolls all of it back: the attempt count is unchanged, there is no row, and the step is still
`DISPATCH_PREPARED`. After `COMMIT` all three survive. There is no window in which one exists without
the others, and a test injects a failure at every statement and kills real processes at each.

**Check 8 (Task 013).** A dispatch also needs a valid bound goal specification, checked immediately
after the context and before this table's own precheck (check 9). Task 013 changed only the
prerequisite for creating a **new** authorization; it added no column, no check and no write to
`dispatch_authorizations` itself, which is exactly as Task 010 left it. A row already written under
version 5 through 7, for a dispatch that has since lost its goal row (which only something outside the
supported code can do), still reads back exactly as before: `get_dispatch_authorization` and every
historical use of a row are unaffected by a missing or corrupt goal, because neither reads
`dispatch_goals`. Only the creation of a *new* row, at check 8, can now also fail with
`DispatchGoalMissing` or `CorruptDispatchGoal`. See `docs/dispatch_goal_model.md`.

**Check 9.** A step that is `DISPATCH_PREPARED` and already has a row is a contradiction, because a row
is only ever written together with the move out of `DISPATCH_PREPARED`. It is reported as corruption.
It is not answered with an earlier receipt, the row is not deleted or replaced, no attempt is taken and
the step does not move. The check comes after the context and the goal (so a missing context or goal is
reported first) and before the state machine and the budget (so a corrupt row is reported even when the
budget is spent). Every check of Tasks 008 and 009 keeps its place and its error.

`transition_step_state(..., DISPATCHED)` still raises `SendAuthorizationRequired`, so the only way to
enter `DISPATCHED` is the operation that writes the row. Preparation, an ordinary transition and a
read never write one.

## Reading

`get_dispatch_authorization(mission_id, step_key)` returns a frozen `DispatchAuthorizationRecord`
(`dispatch_id`, `mission_id`, `step_key`, `authorized_at`). It executes no write and never creates,
repairs or backfills a row.

| Situation | Error |
|---|---|
| The mission does not exist | `MissionNotFound` |
| The step does not exist | `StepNotFound` |
| The step has no dispatch | `DispatchNotFound` |
| The dispatch is duplicated or its UUID is not canonical | `CorruptDispatchRecord` |
| The step is `PENDING` and has a dispatch | `CorruptDispatchRecord` |
| The step is `DISPATCH_PREPARED` and has a row | `CorruptDispatchAuthorization` |
| The dispatch has no row | `DispatchAuthorizationMissing` |
| A row exists but its time is not canonical | `CorruptDispatchAuthorization` |
| Otherwise | the record |

The reads run in one deferred read transaction, so the step's state and the row are compared as they
were at one moment. Without it, an authorization committing between the two reads would make a
`DISPATCH_PREPARED` step and its brand-new row look like a corruption. A test makes exactly that
happen and shows that the other writer cannot commit inside the read.

A `PENDING` step that has a dispatch is a fault of the dispatch record. It is reported as that, and
is not hidden behind the authorization: the check for it comes before the row is looked at.

### No row is not "it never happened"

For a step that has progressed past `DISPATCH_PREPARED`, a valid row is possible and its absence is
**not** corruption, so the reader raises `DispatchAuthorizationMissing`, not a corruption error.
Migrated schema version 4 rows have no row whatever happened to them, and version 5 cannot tell.
`DispatchAuthorizationMissing` therefore means only that this store holds no durable proof. It does not
mean no authorization was committed, and nothing may treat it as that. For a step that is still
`DISPATCH_PREPARED`, absence is the ordinary case: it has not been authorized.

## Migration

Version 4 to version 5 is a pure `CREATE TABLE`. It does not rebuild, alter, drop or fill anything and
it creates **zero** rows. It changes no mission, step, dispatch or context row. The chain is one
transaction whatever the starting point, so no version in between is ever committed:

```text
fresh -> 5
v4 -> v5
v3 -> v4 -> v5
v2 -> v3 -> v4 -> v5
v1 -> v2 -> v3 -> v4 -> v5
```

A database at version 4 runs only the version 5 step, never the version 3 to 4 one. A migrated
database has the same schema text as a fresh version 5 database, and opening it again changes nothing.

Migration never infers an old authorization. `DISPATCHED` or a later state, `attempts_used` and the
timestamps do not tell an authorization committed under version 4 from a state reached some other way,
so no row is written for any dispatch, however far it progressed.

| Dispatch, as migrated from version 4 | What happens |
|---|---|
| `DISPATCH_PREPARED` with a valid UUID and context | Authorizes normally under version 5: it takes an attempt now, writes its row now and becomes `DISPATCHED`, with the usual receipt. This is a new authorization, not a backfill. |
| `PENDING` | Nothing. It gets a UUID and a context when first prepared, as before. |
| `DISPATCHED`, `ACCEPTED`, `EXECUTING`, `CANCEL_REQUESTED`, `OUTCOME_UNKNOWN`, or finished | Stays without a row. `get_dispatch_authorization` raises `DispatchAuthorizationMissing`. Ordinary state behavior is unchanged. |

The migration tests build older databases from schema text written out in full, and the test suite
does not depend on Git history. The written-out version 4 schema was checked, when this task was
written, against what the committed version 4 code creates: the two were identical.

## Legacy attempt counters

A mission migrated with a nonzero `attempts_used` keeps it. Its first authorization under version 5
takes the next value, so `attempt_number` can be more than 1, and the row it writes holds no number.
The earlier attempts left no row and none is made up.

## `OUTCOME_UNKNOWN`

The state machine is unchanged, and `DISPATCH_PREPARED` to `OUTCOME_UNKNOWN` is still an ordinary
edge. What a later reader may conclude from a step that is `OUTCOME_UNKNOWN`:

| Row | Meaning |
|---|---|
| Valid row | Under the supported-store contract, `authorize_step_send` committed an authorization for this dispatch, before the later lifecycle. |
| No row | Version 5 has no durable proof. Either it never happened, or it happened under version 4, or the row was removed. |

When a step and an authorization race, the outcomes are these, and a test checks that they are the
only two: authorization first, so the step is unknown **with** its row and one attempt used; the
move to unknown first, so it is unknown **without** a row and the authorization is refused with no
attempt taken.

Neither line above is a recovery decision. Task 010 does not read the row for any decision. Task 011
does (`docs/recovery_evidence_model.md`): it applies a terminal result to an `OUTCOME_UNKNOWN` step only
when the row is there, and holds the step, with the evidence recorded as `UNPROVEN_AUTHORIZATION`, when it
is not. Task 010 itself still does not read the row for any decision, and it
does not change what an unknown step may do.

## Crashes and concurrency

The tests kill a real child process before and after each write and `COMMIT`, by a hard exit at
every one of those points and by `SIGKILL` at the boundaries that matter (after each write, around
`COMMIT`, and after the receipt), and reopen the database. Before `COMMIT`: no row, no attempt,
`DISPATCH_PREPARED`. After
`COMMIT`, including after the receipt was returned and before any send: the row, the attempt and
`DISPATCHED`, and no later caller is ever granted a second authorization.

Threads with one connection each race, for 25 rounds each: many callers for one dispatch (one
receipt, one row, one attempt); two steps with one attempt left (one row, for the winner); an
authorization against a step cancellation and against a mission cancellation (a row and an attempt
exist exactly when the authorization won, and a later cancellation refunds neither); an authorization
against the move to unknown; an authorization against the removal of the context by another
connection (a row never exists without a receipt); and readers against an authorization (each read is
"no row yet" or the whole row, never a corruption). These kill processes and run threads. They say
nothing about power loss or a filesystem that does not honor `fsync`.

## What this task does not do

Task 010 stores the fact that an authorization was committed, under the supported-store contract, and
nothing more. It stores **no evidence**: there is no evidence table, no observation, no result and no server time. It does not
query ROS, build a `RecoveryObservation`, call `decide_reconciliation`, apply a reconciliation
outcome, or change any mission or step state. `authorizes_send` on a decision stays `False`, and no row
authorizes anything.

Task 011 has since added terminal recovery evidence and its application to a step's state. It reads a
missing row conservatively, as "no proof" and not as proof that nothing was authorized: the evidence is
recorded and not applied, and it invents no row. It relies on a present row under the same trusted-store
assumption: the row is not tamper-evident, and no authentication mechanism is added. This table still
stores no evidence. Task 012 (`docs/mission_reconciliation_model.md`) never reads this table: whether a
mission can leave `NEEDS_RECONCILIATION` depends on the persisted step states alone, so a step with no row
that stays `OUTCOME_UNKNOWN` keeps its mission blocked, and nothing here is written, backfilled or attested.

Not decided here: any mission-level consequence; whether an explicit legacy or operator-attestation mechanism should exist
for dispatches authorized before version 5; retries; leases and fencing; the ROS adapter.
