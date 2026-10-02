# ROS 2 Task Resilience

**Experimental developer preview.** Independently maintained, not a ROS-endorsed package, not
released through any ROS distribution, and not a production supervisor. See
[Limits](#limits) before relying on anything here.

Experimental ROS 2 mission persistence and restart-aware action recovery, demonstrated with a
two-step Nav2 loopback mission.

The problem this explores: a long-running ROS 2 task should not silently lose unfinished work,
reset execution limits, duplicate a request, or convert an uncertain action outcome into a
successful result merely because a client, supervisor, or process restarted. The project targets
ordinary ROS 2 applications; it is usable from an AI/LLM agent but is **not** an AI/LLM framework
and does not require one. See `PROJECT.md` for the full design history and task-by-task record.

## Architecture

Two ROS packages and one source-checkout demos directory:

- **`task_resilience`** — the ROS-independent durable core. A SQLite-backed `MissionStore`
  (missions, steps, dispatch identity/context/goal binding, send authorization, recovery
  evidence, mission-level reconciliation) and the pure state machine/decision logic above it.
  Imports nothing from ROS, `rclpy`, or any `task_resilience_ros` module.
- **`task_resilience_ros`** — everything ROS-facing: `ExecutionOwner` (one-host execution
  ownership via an advisory file lock), `Nav2Adapter` (a thin, synchronous facade that submits
  exactly the stored, authorized `NavigateToPose` goal and separately answers read-only
  "what happened?" queries), and the experimental two-step preview `supervisor`/CLI described
  below.
- **`task_resilience_demos`** — source-checkout example scripts, not an installed ROS package.
  Read them as runnable examples, not as a stable interface.

Each layer's own doc covers its contract in detail: `docs/execution_ownership_model.md`,
`docs/nav2_adapter.md`, `docs/task016_supervisor.md`.

## Quickstart

### Prerequisites (as actually tested)

- Linux (tested on WSL2, Ubuntu 24.04) with ROS 2 **Jazzy** installed and sourceable at
  `/opt/ros/jazzy/setup.bash`.
- For the fake-server example below: `rclpy`, `nav2_msgs`, `action_msgs`, and
  `unique_identifier_msgs`, with their dependencies, must be available in the sourced Jazzy
  environment. The example uses the `NavigateToPose` interface but does not launch Nav2
  navigation nodes or the loopback simulator.
- For the real Nav2 example (linked below): `ros-jazzy-nav2-bringup` and
  `ros-jazzy-nav2-loopback-sim` additionally installed — see `docs/hardware_free_demo.md` for
  the exact tested package versions and the one `apt-get install` line that was run.
- Python 3.12 (whatever ships with the sourced ROS environment).
- All commands below are run from a clone of this repository, **from the repository root**,
  with that ROS environment sourced first in the same shell. No prior `colcon build` is
  required for either example — both scripts add `task_resilience`/`task_resilience_ros` to
  `sys.path` themselves from the source checkout.

### The fastest example: no Nav2 stack to launch, no second terminal

This exercises the same durable-recovery idea end to end — prepare, authorize, send, survive a
**genuine OS process exit**, recover the retained result without resending — against an
in-process fake `rclpy.action.ActionServer`, in a few seconds:

```bash
source /opt/ros/jazzy/setup.bash
python3 task_resilience_demos/scripts/nav2_adapter_smoke.py
```

### The real Nav2 example: a two-step mission, with restart

For the full `checkpoint_a` → `return_to_base` mission against the real (hardware-free) Nav2
loopback simulator, including starting the simulator, running the mission to completion, and
the interrupt/resume demonstration — see **[`docs/task016_supervisor.md`](docs/task016_supervisor.md)**.
That is the one maintained copy of those instructions; nothing here duplicates it.

## Example output (checkpoint/return progress and recovery)

*Illustrative — captured from an earlier verified run against the real Nav2 loopback stack
(`docs/task016_supervisor.md`'s own runbook), not run live for this README. Field values
(mission/dispatch IDs, timestamps) will differ on every real run.*

A normal run, both steps completing:

```text
OUTCOME COMPLETED
DETAIL every step SUCCEEDED
MISSION mission_id=990a3cdd-490d-46cd-b36c-253166dfb7de state=COMPLETED attempts_used=2/2
STEP checkpoint_a state=SUCCEEDED index=0
STEP return_to_base state=SUCCEEDED index=1
```

An interrupted run — `return_to_base` was sent, then the supervisor process exited before
checking the result — followed by a **separate process**, `resume`, recovering that result
without resending it:

```text
# first process (exits after send, before polling for the result):
OUTCOME PAUSED_FOR_DEMO
DETAIL ...paused for step 'return_to_base' after adapter.send returned accepted=True...
MISSION mission_id=36d9dcf7-... state=RUNNING attempts_used=2/2
STEP checkpoint_a state=SUCCEEDED index=0
STEP return_to_base state=DISPATCHED index=1

# second, separate process, `resume`, against the same database:
OUTCOME COMPLETED
DETAIL every step SUCCEEDED
MISSION mission_id=36d9dcf7-... state=COMPLETED attempts_used=2/2
STEP checkpoint_a state=SUCCEEDED index=0
STEP return_to_base state=SUCCEEDED index=1
```

## Limits

This is an experimental developer preview. In particular:

- **Experimental Linux/WSL2 + ROS 2 Jazzy support only**, as actually tested — see
  `docs/hardware_free_demo.md` for the exact tested environment. Nothing here has been tried
  on any other OS, ROS distribution, or against a physical robot.
- **One preview mission per dedicated database.** The supervisor supports exactly one native
  preview mission per database file; pointing it at a database already holding unrelated data
  is refused, not merged or repaired.
- **No cancellation integration and no automatic resubmission/retries.** Interrupting the
  supervisor is not a cancellation request and is never reported as proof that navigation
  stopped; nothing here ever resends an already-dispatched goal.
- **Recovery depends on the action server still having correlated evidence to give back.** If
  a result was never retained, or can no longer be retrieved, recovery cannot invent one.
- **An `UNKNOWN` result or a local timeout may leave an invocation `UNRESOLVED`** rather than
  resolved either way.
- **`UNRESOLVED` is a local invocation outcome only, never a newly persisted core state** — it
  is not written to the database and is not the same thing as the core's own durable
  `OUTCOME_UNKNOWN`.
- **Stopping the supervisor does not cancel remote navigation.** The robot (or simulator) keeps
  doing whatever it was already doing.
- **No exactly-once guarantee and no physical-safety guarantee.** "Succeeded" means the action
  server reported success within its own goal tolerance, not an independently verified
  physical outcome.

## Feedback

This is a developer preview and genuinely wants feedback: usability of the CLI/API shape,
anything that felt surprising, and especially any **reproducible deployment failure** — the
exact commands and output that didn't work as documented. Task 017 (cancellation integration)
remains identified as future work and is not implemented here.

## License

Apache License 2.0 — see [`LICENSE`](LICENSE).
