# Send authorization and attempt semantics (Task 008)

This note settles what a mission's attempt counter means and defines the one operation that
lets a caller send a prepared step's request externally. Code:
`MissionStore.authorize_step_send` in `task_resilience/task_resilience/storage.py`, the
`SendAuthorization` receipt in `model.py`, and four errors in `errors.py`. Tests:
`task_resilience/test/test_send_authorization.py`.

Task 008 itself changed no schema: version 3 stayed as it was, with no migration, column or table.
Task 009 later moved the schema to version 4 and added one precondition to this operation, the
bound action context (see "The operation" and `docs/dispatch_context_model.md`). Task 010 then moved
it to version 5 and made the operation also commit a durable per-dispatch record that the
authorization was committed (see "The operation" and `docs/dispatch_authorization_model.md`).

## The attempt

> **One attempt is one committed authorization to send one external step request.**

The counter is mission-wide: `attempts_used` against `max_attempts` on the mission row.
Preparing a step consumes none. Only `authorize_step_send` increases `attempts_used`, and the
public `consume_attempt` no longer exists. The old operation could burn budget with no prepared
dispatch, which made "attempts used equals authorizations committed" false.

A committed authorization is never refunded automatically, whatever happens next (see "No
refund").

**Counters from before this task.** Every new increment made by `authorize_step_send` is a
committed send authorization. A database created or used before Task 008 can already hold a
nonzero `attempts_used` produced by the removed `consume_attempt`. Those values are kept when the
database is opened or migrated, and they cannot truthfully be reclassified as historical send
authorizations. They remain spent budget, their cause is not reconstructed, and they are never
refunded. `SendAuthorization.attempt_number` is the mission's resulting counter value, so it can
include that inherited consumption: a mission that had used 2 attempts before this task gets
`attempt_number == 3` for its first authorization under this API.

## Prepared versus dispatched

| State | Means |
|---|---|
| `DISPATCH_PREPARED` | A dispatch UUID was durably reserved, and since Task 009 the action context it is declared to reach is bound with it (a step prepared under version 3 has none, and none is added later; see `docs/dispatch_context_model.md`). No send authorization has been committed. The supported execution path must not send yet. |
| `DISPATCHED` | A send authorization was **durably committed**. The external request may or may not actually have been sent or received. Since Task 010 the same transaction also wrote the dispatch's authorization record (a step authorized under version 4 has none; see `docs/dispatch_authorization_model.md`). |

This is a rule for the supported path, not something the state can prove. Nothing in the schema
or the code stops a caller that sends without asking, or an older or bypassing writer (see
"Schema"), so `DISPATCH_PREPARED` does not by itself show that no request left the
process.

`DISPATCHED` does **not** mean sent, received, accepted or executing. Later states record what
was observed. A step can be `DISPATCHED` with no request ever having left the process, and the
durable state cannot tell that apart from a request that was sent.

## The operation

```python
MissionStore.authorize_step_send(mission_id: str, step_key: str) -> SendAuthorization

@dataclass(frozen=True)
class SendAuthorization:
    mission_id: str
    step_key: str
    dispatch_id: uuid.UUID     # the goal UUID the caller must use
    attempt_number: int        # the mission's attempts_used after this authorization; see "The attempt"
    max_attempts: int
    authorized_at: datetime    # timezone-aware UTC; the value written to both rows and the record
```

The whole sequence is one `BEGIN IMMEDIATE` transaction. The checks come in this order, and
the first that fails raises without having written anything:

| # | Check | Error |
|---|---|---|
| 1 | the mission exists | `MissionNotFound` |
| 2 | its stored state is valid | `CorruptStoredState` |
| 3 | its state is exactly `RUNNING` | `MissionNotRunning(mission_id, mission_state)` |
| 4 | the step exists (and its stored state is valid) | `StepNotFound`, `CorruptStoredState` |
| 5 | the step is exactly `DISPATCH_PREPARED` | `StepNotAwaitingSend(mission_id, step_key, step_state)` |
| 6 | no step of the mission is `OUTCOME_UNKNOWN` | `MissionReconciliationRequired(mission_id, step_keys)` |
| 7 | the step has exactly one dispatch row with a canonical UUID | `CorruptDispatchRecord` |
| 8 | the dispatch has a valid bound action context (added by Task 009) | `DispatchContextMissing`, `CorruptDispatchContext` |
| 9 | the dispatch has no authorization record yet (added by Task 010) | `CorruptDispatchAuthorization` |
| 10 | the state machine still allows `DISPATCH_PREPARED` to `DISPATCHED` | `InvalidStateTransition` |
| 11 | an attempt remains, and one is taken | `AttemptLimitReached` |
| 12 | the authorization record is inserted (added by Task 010) | |
| 13 | the step is moved to `DISPATCHED` | |
| 14 | the receipt is returned, only after the transaction commits | |

Check 8 is the only change Task 009 makes to this operation. It comes before anything is written,
so a refusal takes no attempt and changes no cell. The order of every other check is as before,
and an exhausted mission with no bound context is told about the context, not the budget.

Nothing binds a context afterwards, so a prepared step that has none, whether it was prepared under
version 3 or its context was removed outside the supported code, cannot be authorized. It fails
closed with `DispatchContextMissing` and the store does not try to tell the two apart.

Check 9 and the insert at step 12 are Task 010's. A step that is `DISPATCH_PREPARED` cannot already
have an authorization record, so one that does is corruption: it is not answered with an earlier
receipt, is not deleted or replaced, and takes no attempt. The check comes after the context and
before the state machine and the budget, so nothing else changes its place or its error.

The writes are exactly three, in this order and with one shared timestamp: on the mission row,
`attempts_used` and `updated_at`; one new row in `dispatch_authorizations`, holding the dispatch
ID and that timestamp; on the step row, `step_state` and `updated_at`. The dispatch row, its
context, every `created_at`, and every other step are untouched. The receipt's `authorized_at` is
the stored value, read back inside the transaction, not a second reading of the clock.

The limit check is the same atomic `UPDATE ... WHERE attempts_used < max_attempts` the counter
always had, so the limit holds even if the checks above are ever reordered.

A replay is never answered with the earlier receipt. A step that is not `DISPATCH_PREPARED`,
`DISPATCHED` included, raises `StepNotAwaitingSend`. This is the opposite of
`prepare_step_dispatch`, whose replay returns the same record: preparing again is harmless,
authorizing again would be a second send.

The receipt is a record of a committed fact. It is not persisted, no API accepts it, and building
one gives no permission. It is not a token, a lease or a capability. What is persisted, since
Task 010, is the separate authorization record of the dispatch. It is not a receipt: this operation
does not return it, and no API accepts it either.

## At most one successful return

The durable guarantee is **at most one successful authorization return for one prepared
dispatch**. The linearization point is the `COMMIT` of the transaction that moves the step from
`DISPATCH_PREPARED` to `DISPATCHED` and takes the attempt.

`BEGIN IMMEDIATE` takes the database's write lock before the first read. In SQLite's rollback
journal mode, which the store uses, there is one writer at a time across processes. Callers
therefore run the read, check and write sequence one after another. The first commits. Every
later caller reads `DISPATCHED` and is refused without writing. Different steps of one mission
are serialized by the same lock, so the budget is exact for the whole mission, not per step.

A caller that cannot get the lock before the busy timeout gets `sqlite3.OperationalError` and no
authorization. Retrying the *call* is always safe. A `COMMIT` error also raises and returns no
receipt.

## Crashes

The tests kill a real child process at chosen points in the real code, reopen the database, and
try to authorize again.

| Where the process dies | Durable facts after reopening | Another authorization? |
|---|---|---|
| Before the transaction | unchanged | yes: nobody held permission |
| After the attempt update, or after the record insert, before the step update | rolled back: `DISPATCH_PREPARED`, attempts unchanged, no record | yes |
| After all three writes, before `COMMIT` (exit and SIGKILL) | rolled back | yes |
| **After `COMMIT`, before the return** (exit and SIGKILL) | `DISPATCHED`, attempt consumed, record written | **no** |
| After the return, before any send | `DISPATCHED`, attempt consumed, record written | **no** |
| After the send, before any result is stored | `DISPATCHED`, attempt consumed, record written | **no** |

The last three rows leave identical durable facts. That is the point of the meaning of
`DISPATCHED`.

**The zero-send window.** If `COMMIT` succeeds and the process dies before its caller receives the
receipt, no one holds a permission, nothing was sent, and no later caller may be given another.
The step stays `DISPATCHED`. This can produce zero actual sends. It must not produce a second
authorization, and it does not. Getting out of it is observation and reconciliation (Task 007),
never a retry.

These tests kill processes. They say nothing about power loss or a filesystem that does not honor
`fsync`.

## No refund

An authorization is not refunded by a restart, a crash after the commit, a rejection, a timeout,
an unavailable service, a transport error, a server `UNKNOWN`, or a later cancellation. Every
later state change leaves `attempts_used` alone. Legacy counter values from before this task stay
as they are and are never refunded. No retry is authorized by this task.

## The mission gate

Only a `RUNNING` mission may authorize a send.

| Mission state | Authorizes | Why |
|---|---|---|
| `ACCEPTED` | no | Not started. `RUNNING` is the explicit start. |
| `RUNNING` | **yes** | |
| `CANCELLING` | no | New motion would contradict the cancellation. |
| `COMPLETED` | no | Finished. |
| `FAILED` | no | Finished. |
| `CANCELLED` | no | Finished. |
| `NEEDS_RECONCILIATION` | no | An outcome is unresolved, so a send would be blind. |

Preparation is not gated by mission state and is not changed. A prepared step under a mission
that is not `RUNNING` is inert: it authorizes nothing and takes no attempt. Recording a late
observation is separate from authorizing new motion, and stays ungated.

## The unknown-outcome gate

Even when the mission is `RUNNING`, if any persisted step of the mission is `OUTCOME_UNKNOWN`,
authorization is refused and nothing is written. The error names the affected step key or keys in
step order.

This closes a crash window. A step becomes unknown in one transaction, and moving the mission to
`NEEDS_RECONCILIATION` is a separate one. A crash between them leaves a `RUNNING` mission with an
unknown step, and without this gate a restarted executor could send the next step blind.

It is a narrow gate. It does **not** require earlier steps to have succeeded. Running steps in
order remains a later supervisor policy.

## Cancellation

Two serial orders are valid and both are tested:

| Order | Result |
|---|---|
| Cancellation first | `DISPATCH_PREPARED` to `CANCELLED`. A later authorization raises `StepNotAwaitingSend`. No attempt is taken and the dispatch row is kept. |
| Authorization first | `DISPATCH_PREPARED` to `DISPATCHED`, one attempt taken. A later cancellation is a legal state change and refunds nothing. |

If the mission reaches `CANCELLING` first, authorization raises `MissionNotRunning` and takes no
attempt.

A step recorded as `CANCELLED` after its authorization is a decision recorded here. It does not
show that an external request was cancelled or that a robot stopped. The ordinary graph has no
`DISPATCHED` to `CANCEL_REQUESTED` edge, so an authorized step with no acceptance recorded has no
state in which to record a cancel request. This task does not change that.

## Why nothing external happens inside the transaction

`authorize_step_send` takes only a mission ID and a step key. It takes no callback and no
external-work parameter, and it imports and calls nothing that touches a network or a process. The
caller sends after the receipt returns.

A transaction that held the write lock across an external call would block every other writer for
as long as the network took, and would tie the durable decision to something the database cannot
roll back. The order here is the safe one: the decision commits first, then the caller acts. The
cost is the zero-send window above, and that cost is accepted.

## Direct entry to `DISPATCHED`

`transition_step_state(..., DISPATCHED)` always raises `SendAuthorizationRequired`, before any
database access. That covers every current state, a same-state `DISPATCHED` request, and unknown
identifiers. It mirrors the existing guard for `DISPATCH_PREPARED`. The pure edge in the state
machine is unchanged, and `authorize_step_send` still asks the state machine to validate it.

## Attempts for the two-step mission

For `checkpoint_a` then `return_to_base`, a normal run takes **two** attempts.

| `max_attempts` | Consequence |
|---|---|
| 1 | Only one of the two normal sends can be authorized. The mission cannot complete normally. |
| 2 | Exactly enough budget for the two normal sends. |
| more than 2 | Extra budget exists for future additional authorized sends. The schema permits one dispatch identity per step, so those units cannot yet be used as retries. |

Nothing validates that `max_attempts` is at least the number of steps. That would change what a
request means and its fingerprint, and it is outside this task.

## Schema

Task 008 stored nothing new, and version 3 was unchanged by it. Task 009 has since moved the schema
to version 4 with one table for the dispatch's action context (`docs/dispatch_context_model.md`),
and Task 010 to version 5 with one table for the authorization record
(`docs/dispatch_authorization_model.md`). None of the reasoning below depends on either. What Task
008's design rests on:

- one authorization per dispatch comes from the step state, which only moves forward, and from
  the single transaction;
- the budget comes from `attempts_used` and `max_attempts`, with the schema check
  `attempts_used <= max_attempts`;
- identity comes from the one dispatch row per step.

What the schema cannot express for this operation, none of it needed for the guarantee above:

- Task 008 did not keep the authorization time: `updated_at` is overwritten by later transitions.
  Since Task 010 it is kept, in the authorization record, for each dispatch that version 5 code
  authorized. A dispatch authorized earlier has no record, and none is made up;
- an attempt cannot be attributed to a step from step states alone, because `FAILED`, `CANCELLED`
  and `OUTCOME_UNKNOWN` can be reached with or without an authorization. Since Task 010 the
  authorization record says whether supported code committed one for a dispatch, but not which
  attempt number it was, and it is absent for a dispatch authorized before version 5;
- a step cannot have more than one dispatch;
- the database does not stop a writer that ignores this API, such as an older build.

## Retries are not implemented

A future retry would need a new dispatch identity, never the old UUID, and would consume another
mission-wide authorization unit. It needs a later schema and policy. Resubmitting a stored
dispatch UUID is not a retry: Task 006 reported that it can abort the server that hosts the whole
navigation stack.

## Task 007

Task 007's `ReconciliationDecision.authorizes_send` is always `False`, and stays so. Send
authorization is a separate execution and storage operation. `reconciliation.py` does not import
storage, and nothing in a decision can reach `authorize_step_send`. A decision may inform whether
a caller should call it. It never does.

## Not decided here

Retry policy; step ordering; who moves a step stuck at `DISPATCHED` to a reconciled state; a
protocol for cancelling an authorized step; leases, fencing and single-supervisor exclusion; the
ROS adapter that actually sends.
