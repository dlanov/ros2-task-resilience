# Send authorization and attempt semantics (Task 008)

This note settles what a mission's attempt counter means and defines the one operation that
lets a caller send a prepared step's request externally. Code:
`MissionStore.authorize_step_send` in `task_resilience/task_resilience/storage.py`, the
`SendAuthorization` receipt in `model.py`, and four errors in `errors.py`. Tests:
`task_resilience/test/test_send_authorization.py`.

It changes no schema. Schema version 3 is exactly as before, with no migration, column or table.

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
| `DISPATCH_PREPARED` | A dispatch UUID was durably reserved. No send authorization has been committed. The supported execution path must not send yet. |
| `DISPATCHED` | A send authorization was **durably committed**. The external request may or may not actually have been sent or received. |

This is a rule for the supported path, not something the state can prove. Nothing in the schema
or the code stops a caller that sends without asking, or an older or bypassing writer (see
"Schema version 3"), so `DISPATCH_PREPARED` does not by itself show that no request left the
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
    authorized_at: datetime    # timezone-aware UTC; the value written to both rows
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
| 8 | the state machine still allows `DISPATCH_PREPARED` to `DISPATCHED` | `InvalidStateTransition` |
| 9 | an attempt remains, and one is taken | `AttemptLimitReached` |
| 10 | the step is moved to `DISPATCHED` | |
| 11 | the receipt is returned, only after the transaction commits | |

The writes are exactly two updates, with one shared timestamp: on the mission row,
`attempts_used` and `updated_at`; on the step row, `step_state` and `updated_at`. The dispatch
row, every `created_at`, and every other step are untouched.

The limit check is the same atomic `UPDATE ... WHERE attempts_used < max_attempts` the counter
always had, so the limit holds even if the checks above are ever reordered.

A replay is never answered with the earlier receipt. A step that is not `DISPATCH_PREPARED`,
`DISPATCHED` included, raises `StepNotAwaitingSend`. This is the opposite of
`prepare_step_dispatch`, whose replay returns the same record: preparing again is harmless,
authorizing again would be a second send.

The receipt is a record of a committed fact. It is not persisted, no API accepts it, and building
one gives no permission. It is not a token, a lease or a capability.

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
| After the attempt update, before the step update | rolled back: `DISPATCH_PREPARED`, attempts unchanged | yes |
| After both updates, before `COMMIT` (exit and SIGKILL) | rolled back | yes |
| **After `COMMIT`, before the return** (exit and SIGKILL) | `DISPATCHED`, attempt consumed | **no** |
| After the return, before any send | `DISPATCHED`, attempt consumed | **no** |
| After the send, before any result is stored | `DISPATCHED`, attempt consumed | **no** |

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
| more than 2 | Extra budget exists for future additional authorized sends. Schema version 3 permits one dispatch identity per step, so those units cannot yet be used as retries. |

Nothing validates that `max_attempts` is at least the number of steps. That would change what a
request means and its fingerprint, and it is outside this task.

## Schema version 3

Nothing new is stored. What the design rests on:

- one authorization per dispatch comes from the step state, which only moves forward, and from
  the single transaction;
- the budget comes from `attempts_used` and `max_attempts`, with the schema check
  `attempts_used <= max_attempts`;
- identity comes from the one dispatch row per step.

What version 3 cannot express, none of it needed for the guarantee above:

- the authorization time is not kept: `updated_at` is overwritten by later transitions;
- an attempt cannot be attributed to a step from step states alone, because `FAILED`, `CANCELLED`
  and `OUTCOME_UNKNOWN` can be reached with or without an authorization;
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
protocol for cancelling an authorized step; persisting the authorization time; leases, fencing and
single-supervisor exclusion; the ROS adapter that actually sends.
