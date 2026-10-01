# Thin Nav2 adapter (Task 015)

This note defines `Nav2Adapter`, the thin ROS/Nav2 execution adapter in `task_resilience_ros`,
and the one goal format it implements. Code: `task_resilience_ros/task_resilience_ros/
nav2_adapter.py` (the class and its contract) and `nav2_goal_v1.py` (the one goal format and
the UUID/ROS bridge). Tests: `task_resilience_ros/test/test_nav2_adapter.py`, against the
test-only `fake_nav2_server.py`. Demo: `task_resilience_demos/scripts/nav2_adapter_smoke.py`.

The contract in one sentence: **`Nav2Adapter` submits exactly the stored, authorized goal for
one dispatch, and separately answers read-only "what happened?" queries against the stored
dispatch identity -- it never decides what to prepare, whether to authorize, or what a mission
should do next.**

## What this is not

This is an adapter, not a mission supervisor. It never creates or prepares a mission or step,
never calls `authorize_step_send` itself, never mutates `MissionStore` (including recording
recovery evidence, consuming an attempt, or transitioning a step or mission), and never acquires
or releases an `ExecutionOwner`. All of that remains the caller's job in Task 015, and will be
the future supervisor's job afterward. The test/demo fixtures in this task explicitly prepare and
authorize one dispatch through the core's own existing public API to have something to send --
that fixture code is not a supervisor and must not grow into one.

## Executor/spin model (the one this class supports)

The caller constructs and owns one `rclpy.Node` (and, if it wants one, an executor) and passes
it to `Nav2Adapter(node)`. Every method (`send`, `query_result`) is a bounded, **synchronous**
call: internally it calls `rclpy.spin_until_future_complete` on that same node, up to an
explicit timeout, then returns. There is no async variant and no background executor here, and
this module never calls `rclpy.init()`/`rclpy.shutdown()` -- that stays entirely the caller's
responsibility.

Two things this model does **not** support, and callers must not do:
- Calling `send`/`query_result` from inside a callback already being served by a spin of this
  same node (`spin_until_future_complete` does not support nested/re-entrant spinning of the
  same node).
- Spinning this node concurrently from another thread while a call here is in progress.

## The new-send caller contract

```text
acquire owner (ExecutionOwner)
  -> construct MissionStore(owner.canonical_db_path)
  -> complete startup recovery
  -> obtain a fresh SendAuthorization (store.authorize_step_send(...))
  -> invoke Nav2Adapter.send(owner=..., store=..., authorization=..., mission_id=..., step_key=...)
```

`send` requires, and checks, all three pieces named in Task 014/008's own contracts:
- **A currently held `ExecutionOwner`** (`owner.acquired`). Not acquired or released here.
- **The actual fresh `SendAuthorization` object** `authorize_step_send` returned to *this*
  caller for *this* mission/step -- never a `DispatchAuthorizationRecord` read back from
  storage. A `DispatchAuthorizationRecord` is historical provenance that a commit happened; it
  is not a reusable permit, and passing one where a `SendAuthorization` is expected raises
  `TypeError` before anything is sent (see `docs/execution_ownership_model.md` and
  `docs/send_authorization_model.md` for why these are not interchangeable).
- **`mission_id`/`step_key`** identifying the stored dispatch. Dispatch UUID, action context,
  and goal binding are always **reloaded from `store`** via `get_step_dispatch`/
  `get_dispatch_context`/`get_dispatch_goal`; there is no way to pass an independent UUID,
  endpoint, or goal value that could disagree with those durable records.

`authorization.mission_id`/`step_key`/`dispatch_id` are checked against the reloaded dispatch;
any mismatch raises `AuthorizationMismatch` before anything is sent.

This class does not inspect `MissionStore`'s private SQLite connection, and does not otherwise
try to enforce that `owner` and `store` were actually opened against the same database file --
that is the caller contract above, not something this class can or does verify mechanically.

## Send guard: local, not durable

`Nav2Adapter` refuses a second local `send()` call for a dispatch ID it has already attempted,
even after that first attempt raised or timed out (the dispatch is marked attempted *before* the
ROS call, specifically so a failure or timeout still counts). This is a **local, in-process
courtesy guard** against an accidental repeat at one call site, tracked in a plain `set` on one
`Nav2Adapter` instance. It is **not** durable exactly-once enforcement and **not** an unforgeable
capability: a new `Nav2Adapter`, a new process, or a crash and restart has no memory of it
whatsoever. Durable exactly-once-send enforcement is exactly the problem execution ownership
(Task 014) and durable send authorization (Task 008) already exist to bound, in the specific
ways their own docs describe -- this guard does not add to, replace, or strengthen that.

**Document one `Nav2Adapter` instance per owner lifetime for sending.** Construct one adapter
when you acquire the owner, call `send` at most once per dispatch through it, and `close()` it
(destroying its ROS clients) no later than when you release the owner.

No automatic retry, no fresh replacement UUID, and no attempt refund follow from any outcome
`send` returns -- that remains entirely a caller/supervisor policy decision.

## `SendOutcome`: unresolved is not rejection

```python
@dataclass(frozen=True)
class SendOutcome:
    dispatch_id: uuid.UUID
    accepted: bool | None   # True/False = an actual server response; None = UNRESOLVED
    reason: str             # 'ACCEPTED', 'REJECTED', 'ACCEPT_TIMEOUT', 'SERVER_UNAVAILABLE'
```

`accepted is None` covers both "the action server was never discovered" and "a local timeout
waiting for the accept/reject response." Neither is a rejection and neither is proof the request
was never received by the server -- a timeout **after** `send_goal_async` has already been
called means the request may or may not have reached or been accepted by the server. Treat it as
genuinely unresolved, the same way Task 011's existing recovery-query limitation treats an
inconclusive local timeout: it needs a later read-only query, never an automatic resend.

## Goal format: `nav2/navigate_to_pose/v1`

The one `goal_format` this adapter implements (`nav2_goal_v1.GOAL_FORMAT`). The exact JSON
payload schema, validated exactly (extra/missing keys, wrong types, non-finite numbers, or a
non-unit quaternion are all rejected before anything is built or sent -- never repaired or
normalized):

```json
{
  "frame_id": "map",
  "position": {"x": 0.0, "y": 0.0, "z": 0.0},
  "orientation": {"x": 0.0, "y": 0.0, "z": 0.0, "w": 1.0},
  "behavior_tree": ""
}
```

All four top-level keys are required explicitly, including `behavior_tree`: an empty string is
itself a valid, meaningful value (Nav2's own default behavior tree), not a field callers may
omit. `position`/`orientation` each require exactly their own keys (`x`/`y`/`z`, or
`x`/`y`/`z`/`w`); every numeric value must be finite; the orientation's squared norm must be
`1.0` within `1e-6` (a real unit-quaternion check, not merely four independent finite numbers).

`header.stamp` is always set to zero -- Nav2 commonly runs on sim time, unrelated to this
process's wall clock, matching `task_resilience_demos/scripts/send_nav2_goal_with_uuid.py`'s own
existing working convention ("Stamp stays 0 on purpose"). This is this format's explicit v1
convention, not an oversight; the current time is never substituted.

The ROS request is always built from the **stored** `DispatchGoalRecord.payload`, never
regenerated from the mission's original request payload or from current configuration.

The goal UUID sent to the action server is derived, byte for byte, from the durable dispatch
UUID (`nav2_goal_v1.uuid_to_ros_uuid`, matching `task_resilience_demos/scripts/ros_uuid.py`'s own
`uuid_to_msg` exactly), via `ActionClient.send_goal_async(goal, goal_uuid=...)` -- the same
explicit-UUID mechanism the Task 006 spike already established, never a server-generated UUID.

## Endpoint resolution: stored `action_name`/`robot_namespace` -> one ROS name

`DispatchContextRecord.action_type` must equal exactly `'nav2_msgs/action/NavigateToPose'`
(`nav2_adapter.SUPPORTED_ACTION_TYPE`); anything else raises `ActionEndpointMismatch` before
resolution is even attempted -- this adapter implements exactly one action type, never a family
of them, and never silently ignores a type mismatch.

This adapter composes the final action name itself, from the two stored strings, under one
narrow, explicit, FAIL-FAST convention (not general ROS remapping) -- it never resolves a
relative name against whatever namespace the caller's node happens to have, since durable
stored identity has no way to know that unstored, caller-local configuration:

| `robot_namespace` | `action_name` | Resolved name |
| --- | --- | --- |
| `''` (empty) | absolute (starts with `/`) | `action_name`, unchanged |
| non-empty, must start with `/` | must be relative (must NOT start with `/`) | `robot_namespace` with any trailing `/` stripped, then `/`, then `action_name` |
| `''` (empty) | relative (does not start with `/`) | **refused**: `ActionEndpointMismatch` -- would otherwise silently inherit the caller node's own namespace |
| non-empty | absolute (starts with `/`) | **refused**: `ActionEndpointMismatch` -- this convention does not resolve that combination |
| relative (does not start with `/`) | anything | **refused**: `ActionEndpointMismatch` |

Nothing here is guessed at or silently ignored: any combination not in the first two rows raises
before any ROS call is made, and the result of the first two rows is always an absolute name.

**Remapping is detected and refused, not silently followed.** Before `send` calls the send_goal
service, and before `query_result` calls the get_result service, this adapter confirms the
service it is ABOUT to use is still the one it INTENDED: it resolves its own composed name
against the caller's node with ROS remap rules deliberately excluded
(`Node.resolve_service_name(name, only_expand=True)`), separately creates a throwaway client for
the exact same (type, name) pair and reads back its `.service_name` (which DOES reflect any
`--ros-args -r` remap rule actually applied at creation), and compares the two. A mismatch means
some remap rule redirected that specific endpoint out from under the stored identity; this
adapter implements no general remapping support, so that is refused with
`ActionEndpointMismatch` before the real call is ever made -- the original stored context is
never attached to a response that was knowingly obtained from a different endpoint, because no
such response is ever obtained in the first place. An ordinary, unremapped binding (including an
explicitly namespaced one, row two above) is unaffected and resolves exactly as the table says.

**A remap on the BASE action name itself is a separate check, not covered by the above.**
Upstream Jazzy `rcl_action` (`rcl_action/action_client.c`) resolves/remaps the base action
name first, via `rcl_node_resolve_name`, and only then constructs every one of `ActionClient`'s
generated service/topic names from whatever that resolved to. A remap rule written against the
base name (e.g. `/robot_a/navigate_to_pose:=/robot_b/navigate_to_pose`) therefore redirects
every one of those services at once -- including, invisibly, the specific send_goal/get_result
probe described above, since that probe is a throwaway client created directly for one
already-generated service name and is never itself given the base action name to remap. Before
either `_verify_unremapped` check runs, this adapter additionally resolves the base action name
the same way `rcl_node_resolve_name` does, with and without remap rules
(`Node.resolve_topic_name(resolved_name, only_expand=True)` vs. `only_expand=False`); a mismatch
is refused with `ActionEndpointMismatch` before any `ActionClient`/service client for that name
is even created. Both checks are retained because they catch different remap targets: one a
remap on a specific generated service, the other a remap on the shared base name every one of
those services is derived from.

## Read-only recovery query

`query_result(store=..., mission_id=..., step_key=...)` asks the resolved action's generated
`GetResult` service (`NavigateToPose.Impl.GetResultService`, at
`{resolved_action_name}/_action/get_result`) what it knows about the stored dispatch UUID.
It:
- **Never sends a goal.** It only calls the `GetResult` service directly, the same protocol-
  level mechanism `task_resilience_demos/scripts/probe_nav2_goal.py`'s own Section C already
  established as the supported way to ask about a UUID without resending.
- **Never fabricates a `ClientGoalHandle` or an acceptance response** (unlike that same probe
  script's experimental, private-internals Section D, which this adapter does not use).
- **Does not require a fresh send receipt or a bound `DispatchGoalRecord`** --
  `get_dispatch_goal` is never called here. Historical result inspection needs only the
  dispatch identity and its durable action context (`get_step_dispatch`/`get_dispatch_context`),
  which is all that is available or needed to correlate a result, even for a dispatch prepared
  under a schema version that predates goal binding.
- **Does not require execution ownership.** A read-only diagnostic query is not an act of
  execution.
- **Never calls `store.apply_recovery_observation`.** The returned `RecoveryObservation` is
  handed back to the caller (or, later, a supervisor) to apply through the core's own API if and
  when it chooses to; this adapter only observes and reports.

It returns a `task_resilience.reconciliation.RecoveryObservation` directly -- reused as-is, not
wrapped in a new type, since its existing vocabulary already covers every distinction this task
requires:

| Situation | `RecoveryObservation.kind` | `.status` |
| --- | --- | --- |
| Actual terminal result: succeeded | `RESULT_RESPONSE` | `ActionStatus.SUCCEEDED` |
| Actual terminal result: aborted | `RESULT_RESPONSE` | `ActionStatus.ABORTED` |
| Actual terminal result: canceled | `RESULT_RESPONSE` | `ActionStatus.CANCELED` |
| Server has no record of this goal ID | `RESULT_RESPONSE` | `ActionStatus.UNKNOWN` |
| Local timeout waiting for the service response | `LOCAL_TIMEOUT` | (none) |
| The `GetResult` service was never discovered | `SERVICE_UNAVAILABLE` | (none) |
| An exception during the ROS call itself | `TRANSPORT_ERROR` | (none) |

`result_code`/`result_message` (Nav2's raw `error_code`/`error_msg`) are always included
whenever `kind` is `RESULT_RESPONSE`, **including when `status` is `UNKNOWN`** -- `error_code`
happens to default to `0` in that case, and `0` is never treated as success anywhere in this
adapter or in `task_resilience.reconciliation.decide_reconciliation`, which judges outcomes by
`status`, never by `result_code`. Recognizing `CANCELED` here is a status-mapping fact, not
cancellation integration: no cancel request is ever sent by this adapter.

Every wait in both `send` and `query_result` is finite and bounded by an explicit,
caller-overridable timeout. A timeout never issues a ROS cancellation and never implies the
underlying request is or was active. This adapter never writes a durable `OUTCOME_UNKNOWN`;
Task 011's existing limitation -- an inconclusive restart probe against a `DISPATCHED` step
cannot currently become one -- is unchanged.

**Every timeout parameter is validated before anything is created or submitted.**
`server_wait_timeout_s`/`accept_timeout_s` (checked at the very start of `send`) and
`service_wait_timeout_s`/`result_timeout_s` (checked at the very start of `query_result`) must
each be a finite, positive, non-boolean number; `None`, a bool, a wrong type, zero, a negative
value, `NaN`, or infinity all raise `TypeError`/`ValueError` immediately -- before any ROS client
is created, before a local send attempt is marked, and before `send_goal_async`/`call_async` is
ever reached. An invalid `accept_timeout_s` is specifically never discovered only after a goal
has already been sent.

**A ROS-level failure during `query_result` always yields a correlated `TRANSPORT_ERROR`,
including one discovered only once the future has already completed.** The GetResult future's
own `.result()` call sits inside the same narrow exception boundary as `call_async`/
`spin_until_future_complete`: a future can complete with an exception set on it rather than a
value (not only "still pending"), and that exception only actually raises at `.result()` --
catching it there, not only around the earlier calls, is what keeps it from escaping as an
uncaught exception instead of the typed, correlated outcome this method promises for every
ROS-level failure. A still-pending request is cleaned up (`remove_pending_request`) on this path
exactly as it already is on an ordinary timeout; nothing here ever issues a ROS goal
cancellation, since there is no goal in this call at all, only a GetResult service request.
Contract violations -- an invalid timeout, an unresolvable/remapped endpoint, a missing dispatch,
an unsupported goal format -- are all raised from outside this boundary, before it is ever
entered, and are never turned into a `TRANSPORT_ERROR` observation.

## Fake server (test/demo only)

`task_resilience_ros/test/fake_nav2_server.py` is a real `rclpy.action.ActionServer` for
`NavigateToPose`, spun continuously on a dedicated background thread (`SpinningNode`) so a
test's own bounded, synchronous client-side calls can be answered concurrently -- the standard
rclpy pattern for an in-process fake server, not a rebuild of Task 014's multi-process harness.
It is controllable (accept/reject, delayed accept, delayed result, a planned terminal status per
goal UUID) and never imported by any file under `task_resilience_ros/task_resilience_ros/`.
Every test using it picks a fresh, unique action name (`unique_action_name()`), so tests never
share a server or a ROS graph namespace.

## Running the smoke example

```bash
source /opt/ros/jazzy/setup.bash
cd task_resilience_demos/scripts
python3 nav2_adapter_smoke.py
```

The fake `ActionServer` runs in this controller process and stays alive throughout. The send and
the recovery query each run in their OWN, genuinely separate OS process (ordinary
`subprocess.Popen`, reusing `task_resilience_ros/test/_nav2_adapter_subprocess.py`'s small
sender/query roles) -- not same-process object recreation. The script prints each process's own
PID, confirming the sender has fully exited before the query process is even started, and prints
each step's outcome plainly (`--reject` makes the fake server reject the goal instead; see
`--help`). No physical robot or ROS launch file is required.

The same thing, as a plain command sequence (useful for seeing exactly what "genuine process
restart" means here, without reading the script):

```bash
source /opt/ros/jazzy/setup.bash
cd task_resilience_ros/test
DB=/tmp/nav2-adapter-manual-demo.db; rm -f "$DB" "$DB.lock"

# Sender: a real, separate process. Prepares, authorizes, sends, and exits.
python3 _nav2_adapter_subprocess.py sender "$DB" /my_demo/navigate_to_pose req-1 navigate
# -> prints MISSION-ID <uuid>, DISPATCH-ID <uuid>, SEND-OUTCOME <accepted> <reason>, then exits

# Query: a DIFFERENT real process, started only after the sender above has already exited.
# No SendAuthorization, no goal handle -- just the database path, mission ID, and step key.
python3 _nav2_adapter_subprocess.py query "$DB" <MISSION-ID from above> navigate
# -> prints RESULT <kind> <status> <code> <message>
```

(This manual sequence needs a real action server already listening at `/my_demo/navigate_to_pose`
-- `nav2_adapter_smoke.py` itself starts one for you automatically; the sequence above is shown
only to make the two-process structure concrete.)

## Known limitations (backlog, not expanded here)

- No cancellation integration (out of Task 015's scope by design).
- The endpoint-resolution convention is narrow by design; a real deployment with more elaborate
  ROS remapping needs are not addressed here.
- The local send-attempt guard has no cross-process or cross-restart memory, as documented
  above; do not mistake it for exactly-once enforcement in any design built on top of this.
- Running `test_execution_owner_processes.py` (which uses real `os.fork()`) and
  `test_nav2_adapter.py` (which uses real rclpy threads) in the *same* pytest process emits
  Python's own "process is multi-threaded, use of fork() may lead to deadlocks" warning; every
  test still passes (observed repeatedly), but a reviewer who wants to avoid the warning
  entirely can run the two files as separate `pytest` invocations.
