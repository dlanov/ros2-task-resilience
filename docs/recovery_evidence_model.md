# Durable terminal recovery evidence (Task 011)

This note defines what is recorded when a recovery attempt supplies the store with an observation of a
terminal result for a dispatch, and how that result is applied to the step. Code: `MissionStore.apply_recovery_observation` and
`MissionStore.get_recovery_evidence` in `task_resilience/task_resilience/storage.py`, the
`RecoveryEvidenceRecord` and `EvidenceOutcome` in `model.py`, and four errors in `errors.py`. Tests:
`task_resilience/test/test_recovery_evidence.py`.

Schema version 6 adds exactly one table, `recovery_evidence`. It adds no other table, column or index,
and it changes no existing row. The pure decision module (`reconciliation.py`) and the state machine
(`state_machine.py`) are unchanged.

The contract in one sentence: **a terminal result response is recorded whenever it correlates with the
dispatch, and it is applied to the step only when the dispatch has durable authorization provenance.**

## What a row means

A recovery-evidence row durably records a terminal `RESULT_RESPONSE` supplied to
`apply_recovery_observation` and successfully correlated with this dispatch's durable action context.

Under the future adapter contract, that observation is expected to come from an action-server result
response. `MissionStore` itself does not authenticate or independently verify the observation's
external origin. What the store does verify is the durable mission, step and dispatch, the Task 010
provenance shape, the Task 009 context, the exact correlation of the observation with that context and
Task 007's decision. There is no signature, certificate, MAC, server authentication or source field.

A row is not proof that the physical task happened, it is not by itself a change to the step, and it is
not permission to send or resend anything. `outcome` says what recording it did.

The table is **not tamper-evident**. A writer that bypasses `MissionStore` can forge or alter even a
well-formed row, and such writes are outside the supported contract. The supported code inserts a row in
one place, `apply_recovery_observation`, and never updates or deletes one. The primary key gives one row
per evidence ID. It does not make a row immutable.

## Terminal results only

Only an observation of kind `RESULT_RESPONSE` with status `SUCCEEDED`, `CANCELED` or `ABORTED` is
recordable. Everything else raises `ObservationNotRecordable`, **before** the write transaction is
opened, so it takes no lock and reads nothing:

| Not recordable | Why |
|---|---|
| `STATUS_SAMPLE`, whatever its status | Advisory only (Task 007). |
| `LOCAL_TIMEOUT`, `SERVICE_UNAVAILABLE`, `TRANSPORT_ERROR` | They establish nothing about the goal. |
| A result whose status is `UNKNOWN` | The server has no record now, which is not an outcome. |
| A result whose status is `ACCEPTED`, `EXECUTING` or `CANCELING` | Not terminal. |

Those remain useful to an adapter or a log. They have no state consequence, and they would need a
retention policy that this task does not invent. The set of recordable statuses is a list, not a
mapping: which step state a status means is Task 007's to say, and a test compares the list with what
`decide_reconciliation` calls a terminal candidate for every `ActionStatus`.

Task 006 measured retention only for a goal that finished by success. The mappings of `CANCELED` and
`ABORTED` follow the meaning of those statuses in Task 007 and were not measured. The row keeps
`observed_status`, so a later review can revisit that without losing data.

## Task 007 decides

`apply_recovery_observation` accepts the raw `RecoveryObservation`. A `ReconciliationDecision` is never
accepted as an input, and no public parameter is annotated with one. Inside the transaction the store
builds Task 007's `DurableSnapshot` from the stored mission, step and dispatch, and its
`ExpectedActionContext` from the Task 009 context, and calls `decide_reconciliation`. It reads
`decision.kind` and `decision.candidate_step_state` and nothing else. **Storage contains no mapping from
a status to a step state**: the target of an application is the decision's candidate, and a test makes
Task 007 name a state the status would not, to show that the store follows it.

`SUCCEEDED` maps to `SUCCEEDED`, `CANCELED` to `CANCELLED` and `ABORTED` to `FAILED`, exactly as in
Task 007. An observation that does not correlate (dispatch UUID, action name, action type or namespace
differing or absent) is refused with `EvidenceNotCorrelated`, and nothing is written.

## Provenance gates application, not recording

Whether the dispatch has a durable authorization row (Task 010) decides whether a result may be applied.
It never decides whether the result is recorded.

| Authorization row | A terminal candidate |
|---|---|
| A valid row | It may be applied. |
| No row | The evidence is recorded as `UNPROVEN_AUTHORIZATION` and the result is **not** applied. |
| A row that is malformed | `CorruptDispatchAuthorization`. Nothing is stored. |
| A `DISPATCH_PREPARED` step that has a row | `CorruptDispatchAuthorization`. Nothing is stored. |

**No row is no durable proof. It is not proof that nothing was authorized.** A dispatch that was
authorized under schema version 4 has no row, and version 5 cannot tell it from one that was never
authorized. That is why the outcome is called `UNPROVEN_AUTHORIZATION` and not something that claims the
dispatch was unauthorized. Nothing here invents a row, and nothing consumes or refunds an attempt.

A terminal step is not changed by any evidence, so provenance does not matter for it: a matching result
is `ALREADY_SETTLED` and a contradicting one is `CONFLICT`, whether or not a row exists. That is Task 007's
classification and it is kept as it is. The missing row remains discoverable through
`get_dispatch_authorization`.

### The outcomes

| Outcome | Meaning |
|---|---|
| `APPLIED` | The step was moved to the terminal state the result maps to. |
| `ALREADY_SETTLED` | The step was already in the state the result maps to. Nothing changed. |
| `CONFLICT` | The step was already in another terminal state. Nothing changed, and the contradiction is kept. |
| `UNPROVEN_AUTHORIZATION` | The result could have settled the step, but there is no durable authorization row. It was withheld. |

`step_state_before` records the state the step was in when the evidence was recorded. It is the only
record of that, and it is what tells the direct prepared anomaly from a migrated unknown step.

## A prepared step that is seen to finish

`DISPATCH_PREPARED` with no authorization row and a correlated terminal result is not a supported
history: authorization commits atomically with `DISPATCHED`, so there is no way under supported code to
send from a prepared step. If such an observation is supplied, either something outside the supported
path sent the request or the observation is not what it claims to be. The store cannot tell which, and
it does not decide.

The store records the evidence as `UNPROVEN_AUTHORIZATION` with `step_state_before = DISPATCH_PREPARED`,
and in the same transaction moves the step `DISPATCH_PREPARED` to `OUTCOME_UNKNOWN` through the existing
ordinary edge. That move is required: without it the step stays prepared, and `authorize_step_send`
could later authorize a send for a UUID that an observation says the server has already finished. Task
006 found that resubmitting a UUID the server still tracked aborted the whole Nav2 container, and that a
forgotten UUID started a second execution.

With the step `OUTCOME_UNKNOWN`, `authorize_step_send` and the supported `MissionStore` path can no
longer authorize that dispatch, and `authorize_step_send` refuses every other step of the mission too
(Task 008's mission-wide gate), while the mission stays `RUNNING`. That is a statement about the
supported path: a sender that bypasses `MissionStore` is outside the contract, and nothing here can
stop it. Attempts and the mission row are not touched, and this is the only state change an unproven
result ever causes.

The guarantee is about evidence that has been durably accepted. If an authorization commits before the
evidence is recorded, the dispatch is `DISPATCHED` with a row when the evidence arrives, and the
evidence is then applied. The store cannot know that evidence existed.

## A held unknown step

A step that is `OUTCOME_UNKNOWN` with no valid authorization row is **held**: terminal evidence is
recorded as `UNPROVEN_AUTHORIZATION` and the step stays `OUTCOME_UNKNOWN`, however often it is observed
and whatever it says. A repeated poll with the same result, a different one or a new evidence ID never
settles it.

The hold is derived from the absence of the authorization row and from nothing else. It is not derived
from evidence rows: a step that has never had evidence recorded is held the same way, deleting every
evidence row releases nothing, and an evidence row written by something outside the supported code does
not hold a step that has an authorization. It is permanent under supported code, because a row can only
be created by `authorize_step_send`, which needs the step to be `DISPATCH_PREPARED`.

A step that was in flight when a version 4 database was migrated keeps its state, and its ordinary exits,
when unproven evidence arrives. A migrated version 4 step that is `OUTCOME_UNKNOWN` is held and
**cannot be settled by Task 011**. Doing something about that would need an explicit legacy or operator
attestation mechanism, which nobody has designed, and it is not implemented here.

## Applying a result that has provenance

For a step that is `DISPATCHED`, `ACCEPTED`, `EXECUTING` or `CANCEL_REQUESTED` the mapped state is
validated with `transition_step` and applied. The ordinary graph already permits all three targets from
each of them.

For a step that is `OUTCOME_UNKNOWN` the mapped state is applied by a dedicated evidence-only write.
**The state machine gains no edge:** `OUTCOME_UNKNOWN` still has no ordinary exit, and
`transition_step_state` still refuses to leave it. Getting out takes a result with provenance and
nothing else.

## An evidence ID is bound to one observation

`evidence_id` is a caller-generated `uuid.UUID`. It is the durable idempotency key, and it must be
supplied by the caller so that a caller that died after the commit can retry the same call and get the
same record. It is checked before the database is touched and stored as canonical text.

| The same ID and | Result |
|---|---|
| The same dispatch and the same complete observation | An exact replay: the stored record is returned. Nothing is written and nothing is applied again. |
| Any changed fact | `EvidenceIdConflict`. Nothing is written. |
| Another dispatch | `EvidenceIdConflict`. |

The facts are the dispatch, the terminal status, the `result_code` and the whole message (its length and
its SHA-256, and the stored prefix is checked too). The action name, type and namespace are not stored:
they must equal the immutable context, so a replay that changes one is refused as `EvidenceNotCorrelated`
before the ID is even looked up. `outcome` and `step_state_before` are the consequences of the first
application and are not compared: a replay returns the original row even though Task 007 would now call
the same result `ALREADY_SETTLED`.

**A different ID with identical facts is a different, corroborating observation, and is stored as its own
row.** Nothing is folded onto another row, because if an ID were absorbed and never stored it could later
be reused for different facts. A caller that keeps generating fresh IDs for the same result grows
the table. That is accepted here, and there is no cap, retention or compaction. The adapter should reuse
one evidence ID when it retries the same store operation, and stop polling for a result once it has been
durably accepted, unless it has a reason to gather corroboration.

### Every call is revalidated

An exact replay is not answered from a fast path. Every call, replays included, reads and checks the
mission, the step, the dispatch, the authorization judgement, the context and the correlation before the
ID is looked up. If something outside the supported code deleted or damaged a required row since, the
replay fails closed with the error for that row, while the evidence itself stays readable through
`get_recovery_evidence` as long as the step's dispatch is valid.

### The order of the checks

Before the transaction: the types of the arguments, then recordability, then the conversion of the
result. Then, in one `BEGIN IMMEDIATE`: the mission, the step, the dispatch, the dispatch and
authorization judgement, the context, Task 007's decision, the evidence ID, the outcome, the insert of
the evidence, the update of the step (only if the state changes), a read-back of the row, and `COMMIT`.

The dispatch and its provenance are judged **before** the context, so that a `PENDING` step that has a
dispatch stays a corruption of the dispatch record (`CorruptDispatchRecord`) and is not reported as a
missing context. `authorize_step_send` keeps its own order, which puts the context first.

| Situation | Error |
|---|---|
| The mission or step does not exist | `MissionNotFound`, `StepNotFound` |
| `PENDING`, no dispatch | `DispatchNotFound`. No dispatch or context is ever invented. |
| `PENDING`, with a dispatch | `CorruptDispatchRecord` |
| In flight, no dispatch (a version 2 database) | `CorruptDispatchRecord`, as for every other writer |
| Unknown or terminal, no dispatch | `DispatchNotFound` |
| The dispatch is duplicated or not canonical | `CorruptDispatchRecord` |
| No context (a version 3 dispatch) | `DispatchContextMissing`. No evidence is stored. |
| A malformed context | `CorruptDispatchContext` |
| Not correlated | `EvidenceNotCorrelated` |

The evidence is inserted first and the step is updated last, in one transaction, with **one** clock
reading: it is `recorded_at` and, when the step changes, the step's `updated_at`. `recorded_at` is this
store's own wall-clock time, not a server or observation time, and it is never compared for
correctness. A step that is not changed keeps its `updated_at`. No mission row and no attempt is written.

## The table

```sql
CREATE TABLE recovery_evidence (
    evidence_id           TEXT NOT NULL PRIMARY KEY,
    dispatch_id           TEXT NOT NULL
                          REFERENCES step_dispatches (dispatch_id) ON DELETE CASCADE,
    observed_status       TEXT NOT NULL CHECK (
        observed_status IN ('SUCCEEDED', 'CANCELED', 'ABORTED')
    ),
    result_code           TEXT,
    result_message_prefix BLOB,
    result_message_bytes  INTEGER,
    result_message_sha256 BLOB,
    step_state_before     TEXT NOT NULL CHECK (step_state_before IN (...nine states...)),
    outcome               TEXT NOT NULL CHECK (outcome IN (
        'APPLIED', 'ALREADY_SETTLED', 'CONFLICT', 'UNPROVEN_AUTHORIZATION')),
    recorded_at           TEXT NOT NULL,
    CHECK (the three message columns are all NULL, or all set with the right storage classes,
           a prefix of at most 1024 bytes, a size not below the prefix, and a 32-byte digest)
)
```

The mission, the step, the action fields and the authorization time are not stored: they join through
`dispatch_id`. No server time, no goal body or hash, no decision, no retry field and no server epoch is
stored, and there is no index.

`CHECK` constraints pass when they evaluate to NULL, so the message constraint is written with explicit
`typeof()` tests, and a test tries every combination of the three columns being NULL, and every wrong
storage class, on a raw connection. The strict reader re-checks everything the schema cannot: it is not
enough that a row got past `CHECK`.

### `result_code`

`result_code` is opaque. Task 007 accepts any Python integer except a `bool`, and a SQLite `INTEGER`
cannot hold all of them (a 64-bit column refuses even a ROS `uint64` above 2**63 - 1). It is stored as
**canonical hexadecimal text**, `hex(code)`: `0` is `0x0`, `5` is `0x5` and `-5` is `-0x5`. It is read back
with `int(stored, 16)` and accepted only if `hex()` of the result equals the stored text, so `05`, `+0x5`,
` 0x5` and `0X5` are corruption. There is no size limit, and hexadecimal does not meet the limit that
Python puts on decimal conversion. A code of `0` is **not** success: nothing reads the number.

### The message

Only a prefix of the message is kept, at most 1024 bytes, together with the exact length of the whole
message in bytes and the SHA-256 of the whole message:

- the message is encoded with UTF-8 and `surrogatepass`, so any Python string round-trips, a lone
  surrogate included, and nothing is escaped or replaced;
- the prefix is built by walking Python characters and stopping before the character that would cross
  1024 bytes. The encoded bytes are never sliced, so no partial character is silently dropped;
- the prefix is stored as a `BLOB` and read back as a `str` (`result_message_prefix`);
- `result_message_truncated` is true when the length is more than the prefix's;
- no message at all, and an empty message, are different: three `NULL` columns against `''`, 0 and the
  digest of nothing.

A terminal result is recorded whatever the size of its diagnostic. A replay under the same evidence ID
is compared on the length, the digest and the stored prefix of the message, so a replay whose message
differs later than its first 1024 bytes is detected through the length or the digest, and is a conflict.

**The SHA-256 is a compact content identity, used to distinguish message contents and to detect changed
replay content. It is not authentication, tamper evidence or a proof of uniqueness, it is not
authorization provenance and it is not a security boundary.** Anyone who can write the table can write a
matching digest.

## Reading

`get_recovery_evidence(mission_id, step_key)` returns a tuple of frozen `RecoveryEvidenceRecord`s, or an
empty tuple. It validates the mission, step and dispatch as the other readers do, fetches the rows of the
dispatch and parses each one strictly. A malformed stored field, whether a UUID, a status, a state, an
outcome, a result code, the message columns or the time, raises `CorruptRecoveryEvidence`, and one
unreadable row makes the whole read fail. Nothing is repaired, backfilled, updated or deleted.

The order is `ORDER BY evidence_id`. It is deterministic and **not chronological**, and `recorded_at` is a
wall clock, so it is not a sequence either. The rowid is not part of the contract. No deferred snapshot is
needed: the rows are append-only, each is judged on its own, and the reader makes no claim about the step
as it is now. It does not infer that an `APPLIED` row means the step is still in that state.

## Migration

Version 5 to version 6 is a pure `CREATE TABLE`. It creates zero rows, changes no existing row and adds
no index. The chain is one transaction whatever the starting point, and a database already at version 5
runs only that step:

```text
fresh -> 6
v5 -> v6
v4 -> v5 -> v6
v3 -> v4 -> v5 -> v6
v2 -> v3 -> v4 -> v5 -> v6
v1 -> v2 -> v3 -> v4 -> v5 -> v6
```

A migrated database has the same schema text as a fresh version 6 database. Nothing is inferred about
what was observed before the table existed.

## Legacy databases

| Migrated dispatch | What happens to evidence |
|---|---|
| Version 4, valid context, no authorization row, in flight | Recorded as `UNPROVEN_AUTHORIZATION`. The step keeps its state and its ordinary exits. |
| Version 4, `DISPATCH_PREPARED` | Recorded as unproven, and the step becomes `OUTCOME_UNKNOWN`. |
| Version 4, `OUTCOME_UNKNOWN` | Recorded as unproven. The step stays held. |
| Version 3, no context | `DispatchContextMissing`. Nothing is stored. |
| Version 2, in flight, no dispatch | `CorruptDispatchRecord`. Nothing is stored. |

## Attempts and the mission

Task 011 never runs the attempt statement, changes `attempts_used` or `max_attempts`, refunds an attempt
or creates a `SendAuthorization`. It never writes a mission row and never moves a mission: no completion,
failure or cancellation, and no way out of `NEEDS_RECONCILIATION`. The mission state plays no part, as in
Task 007: late evidence is recorded and applied whether the mission is running, cancelling, cancelled,
failed or waiting for reconciliation (each is tested). `OUTCOME_UNKNOWN` alone is enough to close Task 008's authorization gate for
the mission. A discrepancy caused by an unsupported execution remains a historical discrepancy.

## Crashes and concurrency

The tests kill a real child process, by exit and by `SIGKILL`, at each statement of the write and around
`COMMIT`, for both an applied result and a held one. Before `COMMIT` nothing is durable: no evidence, no
state change, no attempt. At `COMMIT` and after it all of it is. A retry of the same call with the same
evidence ID returns the same record. Nothing in any window creates a send authorization.

`BEGIN IMMEDIATE` serializes every supported writer, and each decision is made and written inside one
such transaction, so no revision column is needed. Threads with one connection each race, for 25 rounds:
the same ID and facts, the same ID and different facts (one row, the rest a conflict), different IDs with
identical facts (every one stored, one applied), contradictory results (both kept, the first applied),
evidence against an ordinary transition, a mission transition and `authorize_step_send`, concurrent
evidence on an authorized and on a held unknown step, and the context being removed. These kill processes
and run threads. They say nothing about power loss or a filesystem that does not honor `fsync`.

## What this task does not do

It does not query ROS, send, cancel or retry anything, run an adapter or a supervisor, change a mission,
or add anything to the state machine. It stores no evidence that is not a terminal result, and it does not
authenticate or independently verify where an observation came from.

**Task 012** should address mission-level, evidence-backed reconciliation. In particular
`NEEDS_RECONCILIATION` currently has no exit: evidence can now settle the steps of such a mission, and
nothing can move the mission on. The ROS adapter and supervisor come after that, unless a review finds a
better sequence.

Not decided here: a legacy or operator-attestation mechanism for a migrated version 4 dispatch; a
retention, cap or compaction policy for evidence rows; recording non-terminal observations; retries;
leases and fencing.
