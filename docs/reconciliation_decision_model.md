# Reconciliation decision model (Task 007)

A deterministic, ROS-independent function that takes a durable step snapshot, the action the
dispatch was expected to reach, and one recovery observation, and says what that observation
**can establish**. Code: `task_resilience/task_resilience/reconciliation.py`. Tests:
`task_resilience/test/test_reconciliation.py`.

This is a decision model only. It reads three immutable values and returns an immutable
assessment. It writes nothing, sends nothing, changes no state, applies no reconciliation, and
never authorizes a resend. Since Task 011, `MissionStore.apply_recovery_observation` calls it from
storage (`docs/recovery_evidence_model.md`). The function is unchanged and still pure.

## Observation versus inference

An **observation** is what a recovery attempt saw, or failed to see: a result response, a status
entry, a timeout, an unavailable service, a transport error. An **inference** is a conclusion
drawn from it. The model keeps the two apart: it never turns a failure to observe into a
positive fact, and it never turns one observation into more than it says.

| Observation kind | What it is | What it does not establish |
|---|---|---|
| `RESULT_RESPONSE` | The server answered a result request for the UUID, with a status. | For `UNKNOWN`: that the goal never ran, or succeeded. For a nonterminal status: an outcome. |
| `STATUS_SAMPLE` | A status entry for the UUID was seen. | Anything current. A sample may be stale. |
| `LOCAL_TIMEOUT` | No response arrived within the local wait. | That the query was delivered, that the goal is active, or that the server is alive. |
| `SERVICE_UNAVAILABLE` | The service was not available for the query attempt. | Anything about the original goal, including that the request was never delivered. |
| `TRANSPORT_ERROR` | The query attempt did not produce a usable response because of a transport failure. | Whether the request reached the server, or anything else about the goal. |

The absence of a status entry is not an observation kind here, because it proves nothing.

## Inputs

All three are frozen dataclasses with keyword-only fields, validated when built. Types are
checked and never coerced: a string is not accepted where an enum or a `uuid.UUID` is expected,
and a `bool` is not accepted as a result code. A wrong type raises `TypeError`; an impossible
field combination raises `ValueError`.

**`DurableSnapshot`**: what was read from the store.

| Field | Notes |
|---|---|
| `mission_state` | A `MissionState`. Carried for later policy; **no decision reads it**. |
| `step_state` | A `StepState`. |
| `dispatch_id` | A `uuid.UUID`, or `None`. It has no default, so "this step has no durable dispatch UUID" must be said explicitly. |
| `mission_id`, `step_key` | Optional, diagnostics only. |

The snapshot has no field for the durable send-authorization record that Task 010 added
(`docs/dispatch_authorization_model.md`), and no decision reads it. Whether supported code committed
an authorization for the dispatch is therefore not something a decision can depend on, and a step
that is `OUTCOME_UNKNOWN` gets the same decision with or without one. Whether and how a later task
uses it is not decided here.

The snapshot is taken as given. It is a point-in-time read and may be stale by the time anyone
acts on a decision. Whether the step state and the dispatch agree (for example, a `PENDING` step
that has a dispatch) is the store's invariant. It is not checked here.

**`ExpectedActionContext`**: `action_name`, `action_type`, `robot_namespace`, all exact strings.
This model takes them as input and is not allowed to guess them. Schema version 3 did not persist
them. Since Task 009 (schema version 4) they are bound to each dispatch when it is first prepared
(`docs/dispatch_context_model.md`); a dispatch that predates version 4 has none, and none is added
later. Nothing here reads the store. Since Task 011 the store builds this type from that binding, for every
observation it records. Nothing is normalized or inferred: `/robot1` and `robot1` differ, and so
do `''` and `/`. `action_name` and `action_type` must be non-empty. An empty `robot_namespace`
is accepted as an exact value; whether it means the root namespace is the caller's convention.

**`RecoveryObservation`**: `kind`, the four correlation fields (`dispatch_id`, `action_name`,
`action_type`, `robot_namespace`, each possibly absent), and, depending on the kind, a `status`
and an opaque `result_code` and `result_message`.

- `RESULT_RESPONSE` and `STATUS_SAMPLE` require a `status`; the other kinds carry none.
- Only `RESULT_RESPONSE` may carry `result_code` and `result_message`.
- For a timeout, an unavailable service or a transport error, the correlation fields describe
  the query that was attempted, for which no usable result response was obtained.
- `result_code` and `result_message` are opaque evidence. No decision reads them: a
  `RESULT_RESPONSE` with status `UNKNOWN` and `result_code=0` is still `UNKNOWN`, and this
  generic module gives no meaning to any code.
- `ActionStatus` (`UNKNOWN`, `ACCEPTED`, `EXECUTING`, `CANCELING`, `SUCCEEDED`, `CANCELED`,
  `ABORTED`) is defined locally with string values. An adapter maps whatever it receives onto it.

No time, freshness, server epoch, goal history or send time is modelled, and none is invented.

## Correlation

An observation is correlated only if all four match exactly: its dispatch UUID equals the
durable one, and its action name, action type and robot namespace equal the expected strings. A
field that is absent is not the same as one that is wrong, but neither correlates.

A goal UUID is chosen by the client and tracked by the action server that received it. The same
UUID seen on another server, another action or another robot is not evidence about this
dispatch, so the action identity is compared as well. That is a design choice for this model.
It is not something Task 006 measured.

Correlation is checked before any outcome is compared. An observation about something else must
never settle a step or contradict it: a wrong-UUID `SUCCEEDED` result against a durable `FAILED`
step is `UNCORRELATED`, not a conflict.

## Decision order

| # | Condition | Kind | Reason |
|---|---|---|---|
| 0 | Arguments are not the three expected types | `TypeError` | |
| 1 | No durable dispatch UUID | `MISSING_DISPATCH_IDENTITY` | `NO_DURABLE_DISPATCH_ID` |
| 2 | Any correlation field missing or different | `UNCORRELATED` | `CORRELATION_MISMATCH` |
| 3 | `LOCAL_TIMEOUT` | `INCONCLUSIVE` | `LOCAL_TIMEOUT` |
| 4 | `SERVICE_UNAVAILABLE` | `INCONCLUSIVE` | `SERVICE_UNAVAILABLE` |
| 5 | `TRANSPORT_ERROR` | `INCONCLUSIVE` | `TRANSPORT_ERROR` |
| 6 | Any `STATUS_SAMPLE`, whatever its status | `STATUS_ONLY` | `STATUS_IS_ADVISORY` |
| 7 | `RESULT_RESPONSE` | see below | |

Rule 1 comes first and does not fabricate an identity. It also means that two absent UUIDs
never count as a match, although `None == None`.

Result responses (rule 7):

| Reported status | Decision |
|---|---|
| `UNKNOWN` | `SERVER_UNKNOWN` (`RESULT_UNKNOWN`) |
| `ACCEPTED`, `EXECUTING`, `CANCELING` | `INCONCLUSIVE` (`NONTERMINAL_RESULT_STATUS`): a result path returned a nonterminal status |
| `SUCCEEDED` | terminal, implies step state `SUCCEEDED` |
| `CANCELED` | terminal, implies step state `CANCELLED` |
| `ABORTED` | terminal, implies step state `FAILED` |

A terminal result is then compared with the durable step state:

| Durable step state | Decision |
|---|---|
| Already the same terminal state | `ALREADY_SETTLED` (`RESULT_MATCHES_DURABLE_STATE`) |
| A different terminal state (`SUCCEEDED`, `FAILED`, `CANCELLED`) | `EVIDENCE_CONFLICT` (`RESULT_CONTRADICTS_DURABLE_STATE`) |
| Anything else, including `OUTCOME_UNKNOWN` | `TERMINAL_RESULT_CANDIDATE` (`DURABLE_STATE_NOT_TERMINAL`), with `candidate_step_state` set |

The terminal set is the state machine's own `is_step_terminal`. `OUTCOME_UNKNOWN` is not in it:
it is unresolved, and a verified result is exactly what could resolve it.

The mission state plays no part. A late observation under a `COMPLETED` or `CANCELLED` mission is
still classified. Recording that evidence, and deciding what it permits, are separate policies.

Every decision carries a reason, and each reason belongs to exactly one kind. `INCONCLUSIVE` has
four (`LOCAL_TIMEOUT`, `SERVICE_UNAVAILABLE`, `TRANSPORT_ERROR`, `NONTERMINAL_RESULT_STATUS`);
every other kind has the single reason shown in the tables above. `ReconciliationDecision`
rejects any other pairing with `ValueError`, so a decision that is built directly, or changed
with `dataclasses.replace()`, cannot state one outcome and give the reason for another.

## Why status is advisory

Any correlated `STATUS_SAMPLE` is `STATUS_ONLY`, even `SUCCEEDED`. Task 006 reported, on its
tested setup, that the status topic kept showing a finished goal as `SUCCEEDED` after the server
had forgotten it, and that a fresh subscriber sometimes received nothing at all. A status sample
is therefore neither a reliable positive nor a reliable negative, and this model does not let
one resolve a step. A sample that contradicts the durable state is not an `EVIDENCE_CONFLICT`
either: it is still only `STATUS_ONLY`.

## Local timeout and server `UNKNOWN` are different things

- A **local timeout** means only that no response was received within the wait. It says nothing
  about delivery, about whether the goal is active, or about whether the server is alive: a live
  server holding an active goal, a hung or dead server, and a lost request all look the same.
- **Service unavailable** means the service was not available for the query attempt. It does not
  establish anything about the original goal.
- **Transport error** means the query attempt did not produce a usable response because of a
  transport failure. Do not infer whether the request reached the server.
- Both are `INCONCLUSIVE`.
- A **server `UNKNOWN`** is an answer: the server has no record now. Task 006 found that this one
  answer covers several histories (never delivered, rejected, expired after retention, server
  restarted) that a fresh probe cannot tell apart. It is not success, and it is not evidence
  that nothing ran.

None of these authorizes another send under the same UUID or under a replacement UUID.

## What a terminal candidate means

`TERMINAL_RESULT_CANDIDATE` says: a correlated result response reported a terminal status, and
the durable step is not yet in a terminal state. `candidate_step_state` is the step state that
report maps to.

- It is what the action server **reported**, at the time it answered. It is not proof that a
  physical task completed, and it says nothing about mission completion or whether motion may
  continue.
- It is **not** a claim that the ordinary transition is legal. The ordinary state graph has no
  `DISPATCH_PREPARED` to `SUCCEEDED` edge, and leaving `OUTCOME_UNKNOWN` takes an explicit
  reconciliation with evidence. This model therefore never calls `transition_step()` to ask.
- `candidate_step_state` is set only for this kind. `ALREADY_SETTLED` has nothing to apply, and
  `EVIDENCE_CONFLICT` deliberately carries none, so a conflicting outcome cannot be mistaken for
  one to apply. Deciding which side of a conflict is right is not done here.

## A candidate is not state application

Nothing here writes to the store, and a decision has no method that could. Applying a candidate
takes a separately reviewed path that re-reads the durable state inside a write transaction (the
snapshot may be stale), verifies the evidence, records it durably, and decides the consequences.
Task 011 is that path for the step (`docs/recovery_evidence_model.md`): the store builds the snapshot
and the context, asks this module what the observation establishes, and applies a candidate only when
the dispatch has durable authorization provenance, which the decision cannot know. The mission-level
consequences are not decided there either. The ordinary state graph still blocks the edges that path
needs, and it still does: applying to an `OUTCOME_UNKNOWN` step is a dedicated evidence-only write,
not an edge.

## Why `authorizes_send` is always `False`

Every decision has an `authorizes_send` value, and it is always `False`. It is a read-only
property, not a field: it cannot be passed to the constructor, set on an instance, or replaced
with `dataclasses.replace()`. This guards against accident. It is not a defence against a caller
that patches the class.

It is `False` because deciding that a resend is safe needs facts this model does not have: who
owns the external send, whether another supervisor could be acting, and the server's retention
and restart state. What an attempt is was settled separately, by Task 008. Task 006 reported that resubmitting a UUID the server
still tracked aborted the whole Nav2 container in 7 of 7 planned trials, and that after the
server had forgotten it the same UUID started a second execution. A newly generated UUID is a
new request, not a retry, and does not resolve what happened to the old one.

The property is not a stepping stone. Task 008 added send authorization as a separate storage
API, `MissionStore.authorize_step_send` (`docs/send_authorization_model.md`). It has its own
inputs and gates, it is not reachable through a reconciliation decision, and `authorizes_send`
stays `False`.

## Deferred boundaries

Task 007 does **not** decide or implement:

- any policy for refunding an attempt (Task 008 never refunds one automatically);
- retry policy;
- external-send ownership;
- leases, fencing, or single-supervisor exclusion;
- a new dispatch;
- replacement UUID generation;
- durable evidence storage;
- schema v4;
- SQLite writes;
- a generic force-state;
- applying a reconciliation state;
- any mission transition out of `NEEDS_RECONCILIATION`;
- automatic mission-state changes;
- whether physical motion may resume.

Task 008 has since settled the meaning of an attempt and how one is consumed, and added send
authorization as a separate storage API. It adds no leases, fencing or single-supervisor
exclusion.

Task 009 has since added schema version 4 with the durable action context of a dispatch, and
nothing else from this list: no evidence storage, no application of a reconciliation state, and no
mission transition. This model is unchanged and still calls nothing.

Task 010 has since added schema version 5 with one durable record per dispatch that supported code
committed a send authorization for it (`docs/dispatch_authorization_model.md`). It stores no
evidence either, and applies no reconciliation state.

Task 011 has since added schema version 6 with one durable table of terminal recovery evidence, and the
atomic application of a terminal result to a step's state, through this model and gated by that
provenance. This model is unchanged. Mission-level reconciliation is not done by this decision model. Task 012 now provides the dedicated store-level exit, while the pure state machine still has no ordinary exit from `NEEDS_RECONCILIATION`.

Task 012 has since added schema version 7 with the durable state a reconciling mission is to resume, and the
one dedicated, non-ordinary exit from `NEEDS_RECONCILIATION` (`docs/mission_reconciliation_model.md`). This
model is unchanged and still calls nothing: that exit reads only persisted step states, never a decision, an
observation or recovery evidence, and the pure state machine still has no edge out of
`NEEDS_RECONCILIATION`. The items in the list above are still not done by this model itself.

Also not done: mapping ROS messages onto observations (an adapter), any notion of time, freshness
or server epoch, and a policy for resolving an `EVIDENCE_CONFLICT`.

## Connection to Task 006

The decisions above are the conservative reading of what `docs/ros_action_recovery_spike.md`
reported. Those measurements are limited to the tested setup (`rclpy` 7.1.11, Nav2 1.3.13
loopback, one host, Fast DDS) and were reported from the original runs. This model is generic and
claims no ROS-wide guarantee.

| Task 006 finding, as reported | Where it shows up here |
|---|---|
| A terminal `get_result` response is evidence the server held the UUID when it answered | `TERMINAL_RESULT_CANDIDATE`, `ALREADY_SETTLED`, `EVIDENCE_CONFLICT` |
| An immediate `UNKNOWN` covers several histories and is not evidence nothing ran | `SERVER_UNKNOWN` |
| A pending call or local timeout establishes only that nothing arrived in the wait | `INCONCLUSIVE` |
| The status topic can be stale or empty | `STATUS_ONLY` |
| Resubmitting a tracked UUID aborted the container; a new UUID is a new request | `authorizes_send` is always `False` |

Two limits on that mapping. Task 006 measured retention only for a goal that finished by
success (its limitations say goals finished by cancel or abort were not tested), so the
`CANCELED` and `ABORTED` mappings follow the general meaning of those statuses, not a
measurement. And the correlation on action name, type and namespace is a design choice of this
model, not a Task 006 result.

## Errors

Wrong types raise `TypeError` and impossible combinations raise `ValueError`, as elsewhere in
the core. That includes a decision whose reason does not belong to its kind, or whose candidate
step state is present, absent or non-terminal against its kind. No new exception class was
added. `decide_reconciliation` itself returns a decision for every valid input and raises only
for invalid arguments.

## Tests

`test_reconciliation.py` writes its expected matrix out by hand rather than importing a table
from the module. It covers correlation (each field wrong and absent, exact-string comparison,
identity checked before outcome), every result status against every durable step state, the
opaque result fields, every non-result observation, the Task 006 patterns, independence from the
mission state, type and combination boundaries, the kind and reason relation (every allowed pair
builds, every other pair is rejected, and every decision the function returns satisfies it),
immutability, and the no-send invariant. A static check pins the module's imports to
`dataclasses`, `enum`, `uuid` and the state definitions, and a fresh-interpreter check confirms
that importing the module loads no ROS, SQLite, storage, network or LLM package.
