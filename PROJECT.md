# ROS 2 Task Resilience

> **Working title:** ROS 2 Task Resilience
> **Working repository name:** `ros2-task-resilience`
> **Initial ROS packages:** `task_resilience_msgs`, `task_resilience`, `task_resilience_demos`
> **Status:** Pre-implementation / MVP design
> **Last updated:** 2026-09-23
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

## 12. Proposed persisted data

SQLite is the initial persistence mechanism.

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

Candidate mission states:

```text
ACCEPTED
RUNNING
CANCELLING
COMPLETED
FAILED
CANCELLED
NEEDS_RECONCILIATION
```

Candidate step states:

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

The distinction between a locally recorded dispatch intention and evidence that the downstream server accepted/executed it is important.

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

### Phase 0 — prove environment and Nav2 loopback baseline

No Task Resilience logic yet.

Deliverable:

- documented commands that work in WSL;
- Nav2 loopback stack starts;
- robot can accept a programmatically submitted navigation goal;
- goal can complete headlessly;
- environment/version information captured.

### Phase 1 — pure mission core

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

Apache-2.0 is the current preferred license because it is common in ROS 2, but the final license decision should be made before public release.

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

1. Exact mission/action schema.
2. Exact transaction boundaries around Nav2 dispatch.
3. How much downstream ROS action state can be reconciled after different crash timings.
4. Whether to expose attempt budgets in the public action interface in v0.1.
5. Whether mission events should be append-only from the beginning.
6. Whether the supervisor should be a managed lifecycle node in v0.1.
7. Final project/repository/package names.
8. Minimum supported ROS 2 distributions beyond Jazzy.
9. Final licensing choice.
10. Which failure semantics belong in the generic core versus the Nav2 adapter.

## 22. Immediate next milestone

**Milestone M0: Hardware-Free Nav2 Baseline**

Before writing the mission runtime, establish a reproducible headless WSL baseline:

- ROS 2 Jazzy sourced;
- `nav2_bringup` available;
- `nav2_loopback_sim` available;
- correct installed loopback launch file identified;
- Nav2 stack starts without Gazebo/physical hardware;
- a Python/ROS client sends one navigation goal;
- the goal reaches a terminal result;
- commands and observed versions are recorded.

Once M0 is green, begin Phase 1.

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
