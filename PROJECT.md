# ROS 2 Task Resilience

> **Working title:** ROS 2 Task Resilience
> **Working repository name:** `ros2-task-resilience`
> **Initial ROS packages:** `task_resilience_msgs`, `task_resilience`, `task_resilience_demos`
> **Status:** Phase 1 (durable mission core) in progress. The ROS-independent core, the Task 006 recovery spike and the Task 007 reconciliation decision model are committed; Task 008 (send authorization and attempt semantics) is complete; Task 009 (the durable action context of a dispatch, schema version 4) is complete; Task 010 (the durable send-authorization provenance of a dispatch, schema version 5) is complete; Task 011 (durable terminal recovery evidence and its atomic application to a step, schema version 6) is complete; Task 012 (mission-level reconciliation and the durable state to resume, schema version 7) is complete; Task 013 (durable dispatch goal binding, schema version 8) is complete.
> **Last updated:** 2026-09-27
> **Primary maintainer:** Dennis Lanov

## 1. Project purpose

ROS 2 Task Resilience is an independently maintained ROS 2 ecosystem project for **durable mission execution across client, supervisor, and process restarts**.

The project is intended for ordinary ROS 2 applications as well as AI/LLM agents. It is **not** an LLM framework and does not require an LLM.

The core problem is:

> A long-running ROS 2 task should not silently lose unfinished work, reset execution limits, duplicate a request, or convert an uncertain action outcome into a successful result merely because an agent, supervisor, or process restarted.

The initial integration target is Nav2.

## 2. First actionable goal

The first public goal is intentionally narrow:

> Allow a ROS 2 application or AI agent to submit a small navigation mission, restart, reconnect, and recover authoritative mission state without silently losing remaining work or replenishing already-consumed attempt limits.

The first reference mission is:

> **Visit checkpoint A, then return to base.**

The supervisor must preserve the remaining mission work and execution accounting even when the requesting client or the supervisor is interrupted.

## 3. Why this project exists

ROS 2 and Nav2 already provide actions, behavior trees, navigation state, goal checking, pause/resume mechanisms, and other execution primitives.

This project does **not** replace those capabilities.

The missing problem we are targeting is the layer above an individual action:

- durable mission identity;
- durable progress across restarts;
- explicit unfinished work;
- durable execution/attempt accounting;
- duplicate-request handling using explicit request IDs;
- conservative recovery when an action outcome cannot be established;
- an auditable distinction between:
  - requested,
  - dispatched,
  - accepted,
  - executing,
  - completed,
  - failed,
  - cancelled,
  - and outcome unknown.

The authoritative state is held by the runtime, not by an AI model's conversation history.

## 4. Design principles

### 4.1 Model-independent

The runtime must not depend on GPT, Claude, Gemini, LangGraph, MCP, AgenticROS, ROSA, or another AI framework.

Those systems may become clients or adapters later.

### 4.2 ROS-native

The public integration surface should use normal ROS 2 concepts:

- actions;
- messages;
- services;
- parameters;
- lifecycle-friendly nodes where useful;
- standard Nav2 interfaces.

### 4.3 Conservative recovery

If the runtime cannot prove whether an action completed, it must not invent an answer.

The expected state is something explicit such as:

`NEEDS_RECONCILIATION`

or

`OUTCOME_UNKNOWN`

A first release does **not** promise universal exactly-once physical execution.

### 4.4 Durable authoritative state

Restarting a client, changing an AI model, compacting an LLM context, or restarting the supervisor must not automatically:

- erase unfinished mission steps;
- reset attempt/retry counters;
- create a new copy of an existing request;
- turn an unknown result into success.

### 4.5 No physical-safety overclaim

The hardware-free version validates software execution and recovery semantics.

It does not establish:

- physical robot safety;
- braking behavior;
- sensor reliability;
- collision safety;
- certification;
- security against malicious ROS nodes.

### 4.6 Small first release

Do not build a general robotics governance platform in v0.1.

The first release should solve one problem well and demonstrate it reproducibly.

## 5. Explicit non-goals for v0.1

The first release will not attempt to provide:

- a new LLM-to-ROS bridge;
- natural-language planning;
- a new MCP server;
- a general capability ontology;
- multi-robot fleet orchestration;
- arbitrary sensor evidence fusion;
- learned confidence scoring;
- cryptographic proof bundles;
- a web dashboard;
- a distributed database;
- physical emergency-stop logic;
- replacement collision avoidance;
- ROS 2 security policy enforcement;
- a replacement for Nav2 behavior trees;
- production safety certification.

## 6. Development environment

Primary development environment:

- Windows host;
- WSL2;
- Ubuntu 24.04;
- ROS 2 Jazzy;
- Python-first implementation;
- `colcon`;
- `pytest`;
- `launch_testing`;
- Nav2;
- Nav2 loopback simulator.

Additional Ubuntu VMs may later be used for network/process isolation tests, but they are not required for the MVP.

No physical robot is required for v0.1.

## 7. Hardware-free test strategy

Testing is deliberately layered.

### Layer A — pure Python state-machine tests

No ROS graph.

Test:

- legal/illegal state transitions;
- persistence;
- request identity;
- retry/attempt accounting;
- recovery decisions;
- idempotency behavior.

### Layer B — fake ROS action server

Use a deliberately controllable fake action server to produce exact failure conditions:

- delayed goal acceptance;
- goal rejected;
- success;
- failure;
- cancellation;
- server disappears;
- result unavailable;
- client disappears;
- supervisor disappears at selected transition points.

This layer gives deterministic failure injection that a full simulator cannot reliably reproduce.

### Layer C — Nav2 loopback simulation

Use stock Nav2 with `nav2_loopback_sim`.

Purpose:

- prove compatibility with real Nav2 action interfaces;
- execute navigation without a physical robot;
- run a realistic "checkpoint A -> base" mission;
- demonstrate restart behavior.

The loopback simulator intentionally abstracts away physical dynamics. That is appropriate for the project's first software-level objective.

## 8. Initial hardware-free demo

### Demo mission

1. Start Nav2 with its loopback simulator.
2. Start the Task Resilience supervisor.
3. Submit mission `demo-001`:
   - step 1: navigate from base to checkpoint A;
   - step 2: navigate back to base.
4. Persist the accepted mission before execution.
5. Dispatch step 1 through the normal Nav2 navigation action.
6. Record state transitions durably.
7. Interrupt a selected component.
8. Restart/reconnect.
9. Query the mission by its stable ID.
10. Continue only from an authoritative recovered state.

### Required demo cases

#### Case A — normal completion

Expected:

`ACCEPTED -> RUNNING -> COMPLETED`

Both navigation steps complete once.

#### Case B — requesting client restarts

The client disappears while the mission is active.

Expected:

- mission remains active;
- persisted progress remains authoritative;
- reconnecting client retrieves the same mission;
- no duplicate mission is created.

#### Case C — explicit duplicate request ID

The same request ID is submitted again.

Expected:

- supervisor recognizes the existing request;
- it returns/references existing mission state;
- it does not dispatch a second copy merely because the request was replayed.

This guarantee only applies to the defined identity/idempotency mechanism. It does not claim semantic deduplication of arbitrary differently identified requests.

#### Case D — consumed attempt survives restart

A controlled failure consumes an allowed attempt.

Expected:

- consumed attempt remains consumed after supervisor restart;
- the restart does not replenish the budget.

#### Case E — ambiguous dispatch/result

Interrupt the supervisor at a point where available evidence cannot establish the final downstream outcome.

Expected:

- supervisor does not mark the action successful;
- supervisor does not blindly assume failure and resend;
- mission enters an explicit reconciliation/unknown state.

#### Case F — remaining return-to-base work survives interruption

Interrupt after checkpoint A has been reached but before the return-to-base step is verified complete.

Expected:

- `return_to_base` remains unfinished after recovery;
- cancellation or shutdown does not falsely mark it complete;
- recovery policy determines whether execution may resume.

## 9. Definition of success for the first demo

The hardware-free demo is successful when automated tests and a documented manual run show that the defined failure cases do not silently:

1. erase unfinished work;
2. replenish consumed attempt limits;
3. create duplicate dispatches for the same explicitly identified request;
4. report an unknown outcome as success;
5. mark the complete two-step mission finished before all required steps are established complete.

## 10. Proposed repository structure

```text
ros2-task-resilience/
├── task_resilience_msgs/
│   ├── action/
│   ├── msg/
│   ├── srv/
│   ├── CMakeLists.txt
│   └── package.xml
│
├── task_resilience/
│   ├── task_resilience/
│   │   ├── state_machine.py
│   │   ├── storage.py
│   │   ├── attempt_budget.py
│   │   ├── recovery.py
│   │   ├── supervisor_node.py
│   │   └── nav2_adapter.py
│   ├── test/
│   ├── setup.py
│   ├── setup.cfg
│   └── package.xml
│
├── task_resilience_demos/
│   ├── launch/
│   ├── config/
│   ├── scripts/
│   ├── test/
│   └── package.xml
│
├── docs/
│   ├── architecture.md
│   ├── state_model.md
│   └── hardware_free_demo.md
│
├── .github/
│   └── workflows/
│
├── PROJECT.md
├── README.md
├── CONTRIBUTING.md
├── GOVERNANCE.md
└── LICENSE
```

This is a target structure, not a requirement to create every file immediately.

## 11. Proposed ROS interfaces

The exact schemas remain subject to implementation review.

Likely interfaces include:

### Action

`ExecuteMission.action`

Conceptually:

Goal:
- `mission_id`
- stable request/idempotency identifier
- ordered mission steps
- accepted execution limits

Feedback:
- current step
- durable mission status
- attempt accounting

Result:
- final status
- completion/reconciliation reason

### Messages

Candidate messages:

- `MissionState.msg`
- `MissionStep.msg`
- `ExecutionRecord.msg`

### Services

Candidate service:

`GetMissionState.srv`

The first interface set should remain minimal. Do not create messages merely because they may be useful later.

## 12. Persisted data

SQLite is the initial persistence mechanism.

### Implemented so far (schema version 8)

The schema version is `PRAGMA user_version`. A version 1, 2, 3, 4, 5, 6 or 7 database migrates to version 8 when it is opened, in one transaction.

- `missions`: mission ID, external request ID, request fingerprint and canonical payload, attempt limit and attempts used, `mission_state`, and created/updated timestamps.
- `mission_steps`: mission ID, caller-supplied stable `step_key`, `step_index` (order), `step_state`, and created/updated timestamps. Nothing else is stored per step yet.
- `step_dispatches`: the durable dispatch UUID of a step, with the mission ID, step key and creation time. At most one per step. A step that was never prepared through the store has none.
- `dispatch_contexts`: the action a dispatch is declared to reach, keyed by its dispatch UUID: `action_name`, `action_type` and `robot_namespace`, exact strings. At most one per dispatch. A dispatch that predates version 4 has none, and none is made up.
- `dispatch_authorizations`: one row per dispatch for which `authorize_step_send` committed a send authorization, keyed by its dispatch UUID, with `authorized_at`. Under the supported-store contract only that operation writes one, and the table is not tamper-evident. Nothing else is stored in it: no attempt number, no evidence. A dispatch authorized before version 5 has none, and none is made up.
- `recovery_evidence`: one row per recorded terminal observation, keyed by the caller's evidence ID, for a dispatch: the observed terminal status, the opaque `result_code` (canonical hexadecimal text), a bounded message prefix with its exact length and the SHA-256 of the whole message, the step's state before, the outcome, and the store's own receipt time. Not tamper-evident. Migration creates none, and a dispatch from an older version has none.
- `mission_reconciliation_contexts`: one row per mission that is `NEEDS_RECONCILIATION` under supported code, keyed by the mission ID, with `resume_state` (`RUNNING` or `CANCELLING`): the mission state to restore when the reconciliation is resolved. Durable active intent, not history, evidence, authorization or a permission. Only `enter_mission_reconciliation` inserts a row, only `request_cancellation_during_reconciliation` updates one (`RUNNING` to `CANCELLING`, never back), and only `resolve_mission_reconciliation` deletes one. No timestamp, no reason, no history. Not tamper-evident. Migration creates none: a mission that was already `NEEDS_RECONCILIATION` has none and fails closed.
- `dispatch_goals`: the exact external goal specification a dispatch is declared to carry, keyed by its dispatch UUID: `goal_format` (an opaque, caller-chosen tag) and a canonical `goal_json`, with `goal_fingerprint` the SHA-256 of the complete specification (format and payload together, not the payload alone). At most one per dispatch, bound atomically with its first preparation, alongside its action context. A dispatch UUID and action context identify the declared external action, while a committed authorization says a send was permitted; this table records what that send was declared to carry. A dispatch that predates version 8 has none, and none is made up; a new send authorization requires one, but reading history (context, authorization, evidence, reconciliation) never has.

Every ordinary state change is validated by the state model (section 13) inside a write transaction. Task 011 additionally has a dedicated evidence-only resolution of `OUTCOME_UNKNOWN`, gated by correlated terminal evidence and durable send-authorization provenance; it does not add an ordinary state-machine edge. Task 012 adds a second dedicated exit, `NEEDS_RECONCILIATION` to the mission state recorded in its reconciliation context, gated by the clearing condition that no persisted step is `OUTCOME_UNKNOWN`. The target is never supplied by a caller, and this exit adds no ordinary state-machine edge either.

### Dispatch identity

Preparing a step for dispatch reserves a durable UUID, binds the action context the dispatch is declared to reach, and moves the step from `PENDING` to `DISPATCH_PREPARED` in one transaction. That commit only reserves the identity and declares the action, and both stay whether or not anything is ever sent. `DISPATCH_PREPARED` does not authorize an external send: an external send may be attempted only by the caller that successfully returns from `authorize_step_send`, described under "Send authorization and attempts" below. Even a committed authorization (`DISPATCHED`) does not prove that a send actually happened. Preparing the same step again with the same context returns the same UUID, a different context raises `DispatchContextConflict`, and a dispatch with no context raises `DispatchContextMissing` and is not repaired. Preparation does not consume an attempt.

The durable invariants:

- A `PENDING` step never has a dispatch row through the normal APIs.
- `prepare_step_dispatch` commits `DISPATCH_PREPARED`, the dispatch row and its action context atomically. A plain state transition cannot enter `DISPATCH_PREPARED`; it raises `DispatchPreparationRequired`.
- Any context created by supported schema version 4 code was committed atomically with its dispatch's first preparation. No supported operation binds, backfills, updates or repairs one afterwards.
- A later state keeps the dispatch row, `CANCELLED` included. The row is historical evidence that preparation occurred, and it is not deleted because the step's current state changes.
- A step already stored as `DISPATCH_PREPARED` by an earlier schema version may have no dispatch. It is migrated as it was and is not repaired; no identity is made up for it.
- A `DISPATCH_PREPARED` step that has a dispatch UUID and no context is not repaired and is not guessed to be a migrated row: it fails closed with `DispatchContextMissing` (see "Dispatch action context" below).

Migrated in-flight step states that predate durable dispatch IDs are preserved, but ordinary progression is blocked until a later reconciliation mechanism resolves the missing dispatch identity.

`DISPATCH_PREPARED` means only that this identity was reserved and its action context declared. No send has been authorized: under the supported path an external send is not permitted while the durable state is `DISPATCH_PREPARED`, and an authorization first commits `DISPATCHED`. Nothing stops an older or bypassing program from sending anyway, so the state does not by itself prove that no request left the process. After an authorization, `DISPATCHED` can remain in the store although a send happened, if the process crashed before a later acceptance or execution state was recorded, and it can equally remain although no send happened. So recovery of `DISPATCHED` and of any later unresolved state still needs reconciliation, and a blind resend is not safe.

The future ROS adapter is expected to convert the durable `dispatch_id` into the explicit `goal_uuid` argument of `rclpy`'s `ActionClient.send_goal_async`. A stable goal UUID helps correlate the same external request. It does not guarantee exactly-once execution if the action server restarts or forgets the goal, so recovery stays conservative. The Task 006 spike reported measurements of this on stock Nav2 under the tested conditions: see `docs/ros_action_recovery_spike.md`.

### Dispatch action context

Task 009. Full description: `docs/dispatch_context_model.md`.

- The context is **declared intent, not proof** of where a request was actually sent. It is bound before the send can be authorized, so a later reader does not depend on whichever process is recovering.
- The primary key gives one context row per dispatch. It does not make the row physically immutable. Immutability is a contract of the supported code: only the first preparation of a step inserts a context, there is no public update, replace or repair method, and production code never updates or deletes a row of the table.
- Migration to version 4 is a pure `CREATE TABLE`. It creates no context for an existing dispatch and invents no action name, type or namespace.
- There is **no late binding and no backfill**. A missing context is ambiguous: it may be a dispatch prepared under version 3 that never had one, or a version 4 dispatch whose context was removed outside the supported code, and version 4 stores nothing that tells them apart. Supported code therefore fails closed. For a `DISPATCH_PREPARED` step with a UUID and no context, `get_dispatch_context`, `authorize_step_send` and a `prepare_step_dispatch(..., action)` replay all raise `DispatchContextMissing`, consume no attempt, and change no row, state or timestamp. A step that was `PENDING` when migrated gets a UUID and a context together when it is first prepared.
- Every migrated dispatch that predates a context, whatever its state, stays unbound and is unavailable to a future evidence-backed reconciliation until some separately reviewed mechanism exists. An explicit legacy or operator-attestation mechanism could be designed later if it is needed. It is not designed or implemented.
- Correlating a result to an action is a different problem from proving the correct goal body was sent, and this table alone still does not: it says WHERE a dispatch is declared to reach, never WHAT it carries. Task 013 has since added that durable per-dispatch goal specification (see "Dispatch goal binding" below); it reads and changes nothing in this table.
- Task 009 stores the declared context and nothing more. Task 011 has since recorded terminal recovery evidence and applied it (see "Recovery evidence" below).

### Dispatch authorization record

Task 010. Full description: `docs/dispatch_authorization_model.md`.

- Within the supported `MissionStore` API, a row is created only by `authorize_step_send`, when that authorization commits, in the same transaction as the attempt it took and `DISPATCHED`. Under the supported-store contract, a valid row is durable provenance that `authorize_step_send` committed authorization for this dispatch. It does not mean `send_goal` was called, that a request left the process, that anything received or accepted it, or that a result exists, and it is not a permission: no API accepts it. The zero-send window stays valid.
- The table is **not tamper-evident**. A writer that bypasses `MissionStore` can forge or alter even a canonically formatted row; such writes are outside the supported contract, the schema cannot prevent them, and no signature, MAC, hash, permission or ownership field is added. Task 011 may rely on the row under this same trusted-store assumption.
- `authorized_at` is the store's own wall-clock UTC time for that transaction, the same text as `missions.updated_at`, `mission_steps.updated_at` and `SendAuthorization.authorized_at`. It is not a send, server or result time, it is not proof that a result is retained or has expired, and it must never be used to reason about server retention or to authorize a retry.
- The attempt number is deliberately not stored, and must not be reconstructed from row order, a count of rows or `authorized_at`.
- The primary key gives one row per dispatch. It does not make the row physically immutable. Only `authorize_step_send` inserts one, and production code never updates or deletes one. A stored time must be exactly the canonical UTC text the store writes, or the row is `CorruptDispatchAuthorization`.
- `get_dispatch_authorization(mission_id, step_key)` reads it and writes nothing. A `DISPATCH_PREPARED` step that already has a row is corruption. For a step that has progressed, no row is `DispatchAuthorizationMissing`, not corruption: it means this store has no durable proof, **not** that no authorization was committed.
- Migration to version 5 is a pure `CREATE TABLE` that creates zero rows. It never infers an old authorization from `DISPATCHED`, `attempts_used` or a timestamp. A prepared step that version 4 wrote is authorized normally under version 5, taking an attempt and writing its row then.
- `OUTCOME_UNKNOWN` with a row means, under the supported-store contract, that `authorize_step_send` committed an authorization for the dispatch before the later lifecycle. `OUTCOME_UNKNOWN` with no row means version 5 has no durable proof. Task 010 does not decide what either means for recovery, and the state machine is unchanged.
- This table stores no evidence. Task 011 has since added terminal recovery evidence and its application to a step's state, and relies on a present row under this same trusted-store assumption.

### Recovery evidence

Task 011. Full description: `docs/recovery_evidence_model.md`.

- **Terminal results only.** `MissionStore.apply_recovery_observation(evidence_id, mission_id, step_key, observation)` records a `RESULT_RESPONSE` with status `SUCCEEDED`, `CANCELED` or `ABORTED`, and nothing else. Any other observation raises `ObservationNotRecordable` before a transaction is opened. It accepts the raw `RecoveryObservation`, never a caller-made `ReconciliationDecision`.
- **Task 007 decides.** Inside one transaction the store builds the durable snapshot and the Task 009 context, calls `decide_reconciliation`, and reads `decision.candidate_step_state`. Storage holds no mapping from a status to a step state. An observation that does not correlate is `EvidenceNotCorrelated`.
- **Provenance gates application, not recording.** With a valid Task 010 authorization row a candidate is applied (`DISPATCHED`, `ACCEPTED`, `EXECUTING` and `CANCEL_REQUESTED` through `transition_step`, and `OUTCOME_UNKNOWN` through a dedicated evidence-only write; the state machine gains no edge). With no row the evidence is recorded as `UNPROVEN_AUTHORIZATION` and is not applied. A malformed row, or a `DISPATCH_PREPARED` step that has a row, is `CorruptDispatchAuthorization` and nothing is stored. No row is no durable proof, not proof that nothing was authorized, and no provenance is invented.
- **A prepared step that is seen to finish** has no supported history. The evidence is recorded as `UNPROVEN_AUTHORIZATION` and the step moves `DISPATCH_PREPARED` to `OUTCOME_UNKNOWN` through the existing ordinary state-machine edge, so `authorize_step_send`, the supported path, can no longer authorize that dispatch, and the mission-wide gate closes. **A step that is `OUTCOME_UNKNOWN` with no row is held**: no observation ever settles it. The hold is derived from the missing authorization row, not from evidence rows.
- **A terminal step is never rewritten.** A matching result is `ALREADY_SETTLED` and a contradicting one is `CONFLICT`, with or without provenance.
- **An evidence ID is bound to one observation for good.** The same ID with the same facts is an exact replay (the stored record, nothing written); any changed fact, or another dispatch, is `EvidenceIdConflict`. A different ID with the same facts is stored as a distinct corroborating row, with no de-duplication, so a caller that keeps generating IDs can grow the table. Every call, replays included, revalidates the durable state first.
- **Exact facts.** `result_code` is any integer, kept as canonical hexadecimal text. Of the message a prefix of at most 1024 bytes is kept, with its exact length and the SHA-256 of the whole message. The SHA-256 is a content identity only: not authentication, not tamper evidence, not authorization provenance. The table is not tamper-evident.
- **Reading.** `get_recovery_evidence(mission_id, step_key)` returns strictly parsed rows ordered by evidence ID: deterministic, not chronological.
- **No mission row and no attempt is ever written**, no send is authorized and nothing external happens. Migration to version 6 is a pure `CREATE TABLE` with zero rows.
- A migrated version 4 step that is `OUTCOME_UNKNOWN` cannot be settled by Task 011 and stays held until some explicit legacy mechanism exists. A mission blocked by one stays `NEEDS_RECONCILIATION` under Task 012 (see "Mission reconciliation").

### Mission reconciliation

Task 012. Full description: `docs/mission_reconciliation_model.md`.

- **Why version 7 stores a resume state.** The six version 6 tables cannot tell a mission that entered `NEEDS_RECONCILIATION` from `RUNNING` from one that entered from `CANCELLING`, so restoring "the" state would lose a cancellation or make the caller choose it. `mission_reconciliation_contexts.resume_state` is durable active intent, not history: `RUNNING` or `CANCELLING`, recorded in the transaction that moves the mission.
- **The reason is objective.** Under supported code a mission is `NEEDS_RECONCILIATION` because a persisted step was `OUTCOME_UNKNOWN` when it entered, and it can leave only when none is. The generic `transition_mission_state(..., NEEDS_RECONCILIATION)` is refused with `MissionReconciliationEntryRequired`, before any database access, for every mission (this replaces the older contract that the caller chose it). The pure state-machine edge is unchanged. Entry is `enter_mission_reconciliation`, for a `RUNNING` or `CANCELLING` mission with an unknown step; an exact replay returns the existing context and writes nothing.
- **Resolution restores, and only restores.** `resolve_mission_reconciliation` is a dedicated storage-level exit that restores exactly the recorded `resume_state`, never a caller's target and never `COMPLETED`, `FAILED` or `CANCELLED`, once no persisted step is `OUTCOME_UNKNOWN` (otherwise the existing `MissionReconciliationRequired`, naming the steps in order). It reads only the step states: no recovery evidence, dispatch or authorization. A `CONFLICT` row or `UNPROVEN` evidence on a step that is not unknown does not block it. It is a state-based operation on the current reconciliation, not an operation keyed by an ID: after a committed resolution a retry is `MissionNotAwaitingReconciliation`.
- **Cancellation intent.** `request_cancellation_during_reconciliation` raises the stored intent from `RUNNING` to `CANCELLING`, in SQL that cannot go back, and touches nothing else: not the mission row, a step, an attempt, or ROS. It cancels nothing and releases nothing; it says only which state to resume.
- **No motion is authorized.** Restoring `RUNNING` restores a state. Task 008's gates stay authoritative, `NEEDS_RECONCILIATION` and a restored `CANCELLING` both refuse a send with `MissionNotRunning`, and nothing here touches an attempt or completes a mission: the completion invariant stays where it is.
- **Held and legacy missions fail closed, on purpose.** A held unknown step (no authorization row) keeps its mission `NEEDS_RECONCILIATION` and blocked indefinitely, and a mission that was `NEEDS_RECONCILIATION` under version 6 has no context and every reconciliation operation on it raises `MissionReconciliationContextMissing`. Operator attestation, an abandon or override exit, legacy adoption and provenance synthesis are deferred.
- The context table is not tamper-evident. Migration to version 7 is one `CREATE TABLE` with zero rows.

### Dispatch goal binding

Task 013. Full description: `docs/dispatch_goal_model.md`.

- **The gap.** Dispatch UUID + action context + a committed authorization say a send was permitted for a declared action; none of them, nor the mission's own accepted request, said what goal body that send was to carry. A design pass proved this directly: two hypothetical senders can each locally construct a different `NavigateToPose` goal for the identical durable `dispatch_id`, with the full database unchanged either way, and even an accepted payload that happens to encode a pose is not enough by itself, because nothing durable pinned the mapping from payload to goal.
- **What is bound.** `ExpectedGoalSpecification(goal_format, payload)`: an opaque, caller-chosen `goal_format` tag and any value in the strict JSON domain, canonicalized exactly as `MissionRequest` already is. `goal_fingerprint` is the SHA-256 of the complete specification, `{"goal_format": ..., "payload": ...}`, not of the payload alone: the fingerprint covers both fields together. Replay equality is decided from exact `goal_format` and canonical `goal_json`, never the fingerprint alone. No ROS type, message or import is used or needed: the core stores opaque JSON.
- **Bound atomically with preparation, once.** `prepare_step_dispatch(mission_id, step_key, action, goal)` now binds the dispatch identity, the action context and the goal specification together, in the same transaction that moves the step to `DISPATCH_PREPARED`. This is the only place a goal is ever inserted; there is no `bind_goal`, `set_goal`, `update_goal`, `replace_goal`, `repair_goal` or `rebind_goal`.
- **Replay compares the canonical text, not the fingerprint alone.** An exact replay (same `goal_format`, same canonical `goal_json`) is a no-op; a different format or content is `DispatchGoalConflict`, naming which; a missing goal is `DispatchGoalMissing`; a malformed row is `CorruptDispatchGoal`. A fingerprint match is never treated as proof of identity by itself.
- **A new send authorization requires a bound goal; historical query and recovery do not.** `authorize_step_send` gains exactly one more precondition, checked right after the existing context check and before the existing authorization-provenance precheck: `DispatchGoalMissing` or `CorruptDispatchGoal` refuses it, with nothing written and no attempt consumed. `get_dispatch_context`, `get_dispatch_authorization`, `apply_recovery_observation`, `get_recovery_evidence` and every Task 012 reconciliation operation read no goal, before or after this task: correlating a result with a dispatch only ever needed the durable UUID and action context.
- **Migrated and legacy dispatches stay unbound, permanently and on purpose.** A dispatch prepared under schema version 7 or earlier has no goal, and none is invented or backfilled by a later `prepare_step_dispatch` replay: the store cannot know that a newly supplied body is what the dispatch was originally meant to execute. It can never receive a new send authorization; its existing history remains fully readable.
- **Retry representation is left open.** The current schema permits one dispatch per step and implements no retry mechanism. If multiple dispatches are supported later, each dispatch must retain its own immutable goal binding. This task does not define how retries are represented.
- The goal table is not tamper-evident. Migration to version 8 is one `CREATE TABLE` with zero rows.

### Send authorization and attempts

**One attempt is one committed authorization to send one external step request.** The counter (`attempts_used` against `max_attempts`) is mission-wide, and preparation consumes none.

`MissionStore.authorize_step_send(mission_id, step_key)` is the only operation that increases `attempts_used`, and the only one that enters `DISPATCHED`. In one write transaction it checks that the mission is `RUNNING`, that the step is exactly `DISPATCH_PREPARED`, that no step of the mission is `OUTCOME_UNKNOWN`, that the step's dispatch UUID exists and is valid, that the dispatch has a valid bound action context, and that an attempt remains. It also checks that the dispatch has no authorization record yet. It then takes one attempt, writes the dispatch's authorization record, and moves the step to `DISPATCHED`, with one UTC timestamp for the mission row, the record and the step row. It returns a `SendAuthorization` receipt only after the commit. Nothing external happens inside the transaction. The public `consume_attempt` was removed, and a plain state transition into `DISPATCHED` raises `SendAuthorizationRequired`. See `docs/send_authorization_model.md`.

- `DISPATCHED` means a send authorization was durably committed. The external request may or may not have been sent or received. It does not mean sent, received, accepted or executing.
- The guarantee is at most one successful authorization return per prepared dispatch. If the process dies after the commit and before its caller receives the receipt, no later caller gets another one. That can mean no send at all, and never a second authorization.
- An attempt is never refunded automatically: not by a restart, a crash, a rejection, a timeout, an unavailable service, a transport error, a server `UNKNOWN`, or a later cancellation. No retry is authorized by this design.
- Every new increment made by `authorize_step_send` is a committed send authorization. A nonzero `attempts_used` that predates Task 008, produced by the removed `consume_attempt`, is kept as spent budget. Its cause is not reconstructed, and it is never refunded.
- The schema allows one dispatch identity per step, so budget beyond the number of steps cannot yet be used as retries. A future retry would consume another mission-wide unit and must use a new dispatch identity.
- Since Task 011 a step can also become `OUTCOME_UNKNOWN` because a terminal result was observed for a prepared dispatch that has no authorization. That closes this gate for the whole mission and for that dispatch for good. Recording evidence never consumes or refunds an attempt.

The concepts below are the longer-term minimum. Only the parts listed above are implemented.

Minimum concepts:

### Mission

- mission ID;
- external request ID;
- accepted timestamp;
- mission status;
- active step;
- completion state;
- schema/version information.

### Step

- step ID;
- mission ID;
- type;
- target parameters;
- status;
- attempt count/limit;
- downstream action goal identifier where available;
- dispatch timestamps;
- result/error information.

### Event

Append-only or effectively append-only state-transition records are desirable for debugging and recovery reasoning.

The implementation should prefer transactions around state transitions that matter for restart behavior.

## 13. Initial state model

The first version should avoid an excessively complex state machine.

Mission states (the current v0.1 state model):

```text
ACCEPTED
RUNNING
CANCELLING
COMPLETED
FAILED
CANCELLED
NEEDS_RECONCILIATION
```

Step states (the current v0.1 state model):

```text
PENDING
DISPATCH_PREPARED
DISPATCHED
ACCEPTED
EXECUTING
SUCCEEDED
FAILED
CANCEL_REQUESTED
CANCELLED
OUTCOME_UNKNOWN
```

The distinction between a locally recorded dispatch intention and evidence that the downstream server accepted/executed it is important. `DISPATCHED` is a committed send authorization, not evidence that anything was sent.

`NEEDS_RECONCILIATION` and `OUTCOME_UNKNOWN` have no ordinary exit. `NEEDS_RECONCILIATION` cannot be entered by a plain transition either: the store enters it with `enter_mission_reconciliation` and leaves it with `resolve_mission_reconciliation`, and an `OUTCOME_UNKNOWN` step is left only by Task 011's evidence (`docs/mission_reconciliation_model.md`, `docs/recovery_evidence_model.md`).

State names may change after implementation review.

## 14. Invariants

Initial invariants:

### I-1 — completion invariant

A mission cannot be `COMPLETED` while a required step remains unfinished.

### I-2 — durable accounting invariant

Restarting the supervisor cannot increase an accepted attempt budget.

### I-3 — duplicate-request invariant

Replaying the same accepted request identity must not create a second logical mission.

### I-4 — uncertainty invariant

Unknown downstream outcome must not be represented as success.

### I-5 — state authority invariant

Client/agent conversation state is not authoritative mission state.

### I-6 — no automatic physical continuation assumption

Recovery of an unfinished obligation does not itself authorize arbitrary physical motion. Recovery policy must decide whether continuation is permitted.

## 15. Estimated initial size

Planning estimate for a credible v0.1:

- approximately 50–70 hand-maintained files;
- approximately 4,000–7,000 handwritten lines including tests;
- three ROS packages;
- one Nav2 adapter;
- one fake action server for deterministic failure tests;
- one hardware-free Nav2 demonstration.

These are planning estimates, not targets.

## 16. Implementation phases

### Phase 0 — prove environment and Nav2 loopback baseline — COMPLETE

Completed as milestone M0 (tag `m0-hardware-free-baseline`).

No Task Resilience logic yet.

Deliverable:

- documented commands that work in WSL;
- Nav2 loopback stack starts;
- robot can accept a programmatically submitted navigation goal;
- goal can complete headlessly;
- environment/version information captured.

### Phase 1 — pure mission core — CURRENT

Implement:

- mission data model;
- state machine;
- SQLite storage;
- durable attempt accounting;
- request identity/idempotency;
- unit tests.

No Nav2 dependency in the core state-machine logic.

### Phase 2 — fake action integration

Implement controlled downstream action behavior and failure injection.

Prove restart/recovery semantics before integrating Nav2.

### Phase 3 — Nav2 adapter

Use standard Nav2 action interfaces.

Run checkpoint-A-and-return mission against loopback simulation.

### Phase 4 — restart/failure system tests

Automate:

- client restart;
- supervisor restart;
- duplicate request;
- consumed-attempt persistence;
- ambiguous result/reconciliation;
- unfinished return-to-base step.

### Phase 5 — documentation and community review

Publish:

- architecture;
- failure semantics;
- explicit limitations;
- reproducible demo;
- tests;
- comparison with existing Nav2 mechanisms.

Ask ROS/Nav2 community for technical feedback before expanding scope.

## 17. Claude development workflow

Claude is used as a coding worker, not as the authority for design decisions.

Workflow:

1. ChatGPT defines a bounded implementation task.
2. Claude inspects the existing repository and environment.
3. Claude proposes or makes the smallest required changes.
4. Claude reports:
   - changed files;
   - design assumptions;
   - commands/tests run;
   - exact results;
   - known limitations.
5. Do not commit until changes are reviewed.
6. ChatGPT independently reviews logic, APIs, test coverage, ROS conventions, and claims.
7. Run tests locally.
8. Only then commit.

### Coding rules for Claude

- Do not invent ROS APIs; inspect installed interfaces/source when uncertain.
- Prefer existing ROS/Nav2 interfaces over custom duplicates.
- Keep comments natural and useful.
- Avoid large abstractions without an immediate requirement.
- Do not modify vendored/upstream Nav2 source.
- Do not commit unless explicitly asked.
- Do not claim tests passed unless they were actually run.
- Preserve failure information; never convert uncertainty into success.
- Keep AI/LLM dependencies out of the core runtime.
- Use type hints where appropriate.
- Add tests with each behavior change.
- Follow ROS 2 package conventions and `ament`/`colcon` tooling.

## 18. Governance and ownership

The project is independently maintained.

Initial governance:

- Dennis Lanov is the lead maintainer.
- The canonical repository remains under maintainer control.
- External contributions are accepted through pull requests.
- Maintainer review is required before merge.
- Public interfaces and compatibility expectations will be documented.

An open-source license permits forks; repository maintainership and project direction remain separate from the rights granted by the license.

The project is licensed under Apache-2.0, which is common in ROS 2. See `LICENSE`.

## 19. ROS ecosystem strategy

The project should begin as an independent ROS 2 ecosystem package.

It does not require ROS Project approval merely to exist.

Potential later steps:

1. publish the public repository;
2. obtain community feedback;
3. establish useful external users;
4. satisfy ROS package/naming/licensing requirements;
5. release with Bloom / submit to `rosdistro` if appropriate;
6. propose Nav2 integration changes only when concrete upstream changes are justified.

The first community pitch should emphasize a specific failure/recovery problem rather than broad claims about "AI safety."

## 20. Current project boundary

Current scope is intentionally:

```text
durable mission state
        +
explicit request identity
        +
attempt accounting
        +
conservative recovery
        +
Nav2 integration
        +
hardware-free failure tests
```

Not:

```text
universal AI safety
or
general robot autonomy framework
```

## 21. Open design questions

These should be resolved by implementation evidence rather than speculation:

1. Exact mission/action schema. (The persisted mission/step schema is implemented as version 7; the ROS action schema is still open.)
2. Exact transaction boundaries around Nav2 dispatch. (The boundary of the send authorization is settled by Task 008; the adapter's send and the recording of its result are not.)
3. How much downstream ROS action state can be reconciled after different crash timings.
4. Whether to expose attempt budgets in the public action interface in v0.1.
5. Whether mission events should be append-only from the beginning.
6. Whether the supervisor should be a managed lifecycle node in v0.1.
7. Final project/repository/package names.
8. Minimum supported ROS 2 distributions beyond Jazzy.
9. ~~Final licensing choice.~~ Resolved: Apache-2.0.
10. Which failure semantics belong in the generic core versus the Nav2 adapter.
11. Mission-level, evidence-backed reconciliation. Resolved for now by Task 012 (complete): a mission leaves `NEEDS_RECONCILIATION` and resumes its recorded state once no step is unknown. Still open, and deferred: what may ever release a mission blocked by a held unknown step or by a version 6 reconciliation (an operator attestation, an abandon exit).

## 22. Milestones

### Current milestone: Phase 1 — durable mission core

Phase 1 (section 16) is the current milestone. It proceeds in bounded tasks:

- Task 002, durable mission identity and attempt accounting: **COMPLETE**. A ROS-independent Python core with SQLite storage.
- Task 003, mission and step state model: **COMPLETE**. Pure state semantics and transition rules.
- Task 004, persistent mission and step state: **COMPLETE**. Mission and step states stored in SQLite (schema version 2), with migration from version 1.
- Task 005, durable dispatch identity and atomic dispatch preparation: **COMPLETE**. A step gets a durable dispatch UUID in the same transaction that marks it `DISPATCH_PREPARED` (schema version 3).
- Task 006, ROS action recovery spike: **COMPLETE**. A research spike, not production code. It reported what a fresh process can learn about a Nav2 `NavigateToPose` goal from its UUID alone (`docs/ros_action_recovery_spike.md`, scripts in `task_resilience_demos/scripts/`). Findings, limited to the inspected `rclpy` 7.1.11 and the tested Nav2 1.3.13 loopback setup on one host: the public `rclpy` API has no way to turn a UUID into a goal handle or fetch a result by UUID; the `/navigate_to_pose/_action/get_result` protocol service answered by UUID for as long as the server retained the goal (900 s in the stock `nav2_params.yaml`; a finished goal's result was reported retrievable at 882.5 s and gone at 902.2 s); in the two tested restarts (one clean, one SIGKILL) the restarted server returned `UNKNOWN` for goals that had been retained or active before the restart and accepted the same UUID again; and resubmitting a UUID the server still tracked aborted the Nav2 container in 7 of 7 planned trials (as reported). A pending or timed-out request is not evidence about the goal. `task_resilience` was not changed.
- Task 007, reconciliation decision model: **COMPLETE**. A pure decision function (`task_resilience/reconciliation.py`) that says what one recovery observation can establish about a dispatch. It changes no state and never authorizes a send (`docs/reconciliation_decision_model.md`).
- Task 008, atomic send authorization and attempt semantics: **COMPLETE**. One attempt is one committed authorization to send one external step request, and `MissionStore.authorize_step_send` is the only way to take one and the only way to enter `DISPATCHED`. No schema change (version 3). See `docs/send_authorization_model.md`.
- Task 009, immutable durable dispatch action context: **COMPLETE**. The action name, type and robot namespace a dispatch is declared to reach are bound to it, together with its UUID and `DISPATCH_PREPARED`, in one transaction (schema version 4, one new table). Declared intent, not proof of where a request was sent. `authorize_step_send` requires a bound context. No evidence is stored and no reconciliation is applied. See `docs/dispatch_context_model.md`.
- Task 010, durable send-authorization provenance: **COMPLETE**. `authorize_step_send` now commits, in one transaction, the attempt, one row in the new `dispatch_authorizations` table (the dispatch and the store's authorization time) and `DISPATCHED` (schema version 5, one new table). Under the supported-store contract the row means only that `authorize_step_send` committed the authorization (the table is not tamper-evident): not that a request was sent, and not a permission. The attempt number is not stored. Migration creates no row for an earlier dispatch, and no row is not proof that nothing was authorized. No evidence is stored in that table and no reconciliation is applied by that task: terminal recovery evidence and its application are Task 011. See `docs/dispatch_authorization_model.md`.
- Task 011, durable terminal recovery evidence and atomic evidence-backed step-state application: **COMPLETE**. `MissionStore.apply_recovery_observation` records a terminal `RESULT_RESPONSE` in the new `recovery_evidence` table (schema version 6, one new table) and applies it to the step through Task 007's decision, but only when the dispatch has Task 010 authorization provenance. Without it the evidence is recorded as `UNPROVEN_AUTHORIZATION` and not applied, a prepared step that is seen to finish becomes `OUTCOME_UNKNOWN` through the existing ordinary edge so the supported path can no longer authorize it, and an unknown step with no provenance is held. Evidence IDs bind to one observation for good, different IDs are distinct corroborating rows, `result_code` is kept exactly as canonical hexadecimal, and a message is kept as a bounded prefix with its exact size and SHA-256 (a content identity only). No mission row or attempt is ever written. See `docs/recovery_evidence_model.md`.
- Task 012, mission-level evidence-backed reconciliation: **COMPLETE**. A mission enters `NEEDS_RECONCILIATION` only through `MissionStore.enter_mission_reconciliation`, because a persisted step is `OUTCOME_UNKNOWN`, and the mission state to resume (`RUNNING` or `CANCELLING`) is recorded in the same transaction in the new `mission_reconciliation_contexts` table (schema version 7, one new table, no timestamp). A plain transition into it is refused with `MissionReconciliationEntryRequired`. `resolve_mission_reconciliation` restores exactly that state, never a caller's target, once no persisted step is `OUTCOME_UNKNOWN`; it never completes, fails or cancels a mission, authorizes a send, touches an attempt or reads recovery evidence. `request_cancellation_during_reconciliation` raises the stored intent from `RUNNING` to `CANCELLING` and never back, and cancels nothing. A held unknown step, or a mission that was `NEEDS_RECONCILIATION` under version 6, keeps the mission blocked: that fail-closed outcome is accepted, and operator attestation or an abandon exit is deferred. `state_machine.py` and `reconciliation.py` are unchanged. See `docs/mission_reconciliation_model.md`. The ROS adapter and supervisor come after this.
- Task 013, durable dispatch goal binding: **COMPLETE**. A design pass proved that dispatch UUID + action context + a committed authorization never said what goal body a send was to carry, even against an accepted payload that happens to encode a pose. `prepare_step_dispatch(mission_id, step_key, action, goal)` now binds an `ExpectedGoalSpecification` (an opaque `goal_format` tag plus any strict-JSON-domain payload, canonicalized exactly as `MissionRequest` already is) atomically with the dispatch identity and action context, in the new `dispatch_goals` table (schema version 8, one new table, no timestamp). The fingerprint is over the complete specification, not the payload alone. Replay compares the canonical text, never the fingerprint alone: an exact replay is a no-op, a different format or content is `DispatchGoalConflict`, a missing goal is `DispatchGoalMissing`, and none is ever bound, replaced or backfilled outside that one atomic preparation. `authorize_step_send` gains exactly one more precondition, checked after the existing context check and before the existing authorization-provenance precheck; a migrated dispatch that predates this table can never receive a new send authorization, but every historical query and recovery path (context, authorization, evidence, Task 012 reconciliation) reads no goal and is unaffected. `state_machine.py` and `reconciliation.py` are unchanged; no ROS import was added; single-supervisor execution ownership, the ROS adapter and the supervisor remain later tasks. See `docs/dispatch_goal_model.md`.

### M0: Hardware-Free Nav2 Baseline — COMPLETE

Baseline tag: `m0-hardware-free-baseline`.

Before writing the mission runtime, a reproducible headless WSL baseline was established:

- ROS 2 Jazzy sourced;
- `nav2_bringup` available;
- `nav2_loopback_sim` available;
- correct installed loopback launch file identified;
- Nav2 stack starts without Gazebo/physical hardware;
- a Python/ROS client sends one navigation goal;
- the goal reaches a terminal result;
- commands and observed versions are recorded.

M0 is green; Phase 1 has begun.

## 23. References used to define the current plan

- Nav2 Jazzy loopback simulator:
  https://docs.nav2.org/jazzy/configuration_and_development/configuration_guide/others/configuring_loopback_sim/
- Nav2 Jazzy quickstart:
  https://docs.nav2.org/jazzy/getting_started/quickstart/quickstart/
- ROS 2 actions design:
  https://design.ros2.org/articles/actions.html
- ROS 2 developer guide:
  https://docs.ros.org/en/rolling/The-ROS2-Project/Contributing/Developer-Guide.html
- ROS distribution review guidelines:
  https://github.com/ros/rosdistro/blob/master/REVIEW_GUIDELINES.md
- ROS 2 launch testing:
  https://docs.ros.org/en/rolling/p/launch_testing/
