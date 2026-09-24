# ROS action recovery spike (Task 006)

An experiment, not production code. Question:

> Task Resilience persists a dispatch UUID before an external action is sent. If
> the original action client disappears, what can a **fresh ROS 2 process that
> knows only that UUID** learn about a Nav2 `NavigateToPose` goal?

Nothing in `task_resilience/` was changed and no ROS code was added to it.

Four classes of evidence appear below. Each claim rests on one of them, and the text says which:

| Evidence | What it is |
|---|---|
| **Installed inspection** | Read from the packages installed on the test machine: versions, `rclpy` source, generated action types, the stock `nav2_params.yaml`, strings in installed libraries. |
| **Reported experiment** | Observed in the original runs of 2026-09-23 on the hardware-free Nav2 loopback baseline (`docs/hardware_free_demo.md`) and recorded in this document. The client-side raw outputs were not found later, so the tables here are the record. Only some items are corroborated by retained server logs (see "Evidence availability and closeout checks"). |
| **Upstream source reading** | The version-tagged `rclcpp_action` source, checked online by an external reviewer on 2026-09-24. Source inspection, not a trace of the installed binary. |
| **Closeout checks** | Checks actually performed when this document was closed out, listed in "Evidence availability and closeout checks". |

"Measured" below means "reported from the original experiments" unless a sentence says that the
retained logs corroborate it.

Three kinds of mechanism are kept apart throughout:

| Label | Meaning |
|---|---|
| **PUBLIC API** | Documented, high-level `rclpy.action.ActionClient` / `ClientGoalHandle` methods (no leading underscore). |
| **PROTOCOL** | The generated action services/topics under `/navigate_to_pose/_action/...`, used directly with ordinary ROS clients. |
| **PRIVATE** | rclpy names with a leading `_`, hand-built internals, or anything users are not expected to depend on. |

## Headline results

Limited to the tested versions and setup (see Environment). Unless marked otherwise, the numbers
are as reported from the original experiments.

1. A fresh process with only the UUID **can** ask the action server about it, but only at the
   **PROTOCOL** level. In the inspected `rclpy` (7.1.11) the **PUBLIC API** has no way to turn a
   UUID into a goal handle or fetch a result by UUID (installed inspection).
2. For a goal that was executing, a `get_result` request by UUID received no response until the
   goal finished, then returned the final status and result. In those runs (experiments 1 and 9a)
   the goal was also seen `EXECUTING` on the status topic, and the `SUCCEEDED` response was
   received later. Those observations, not the wait, showed the goal was running. **A pending
   request or a local timeout establishes only that no response was received within the local
   wait.** It does not establish that the query was delivered, that the server tracks the UUID,
   that the goal is executing, or that the server is alive. For a goal the server had no record
   of, the answer was an **immediate `UNKNOWN`**, which is a different thing from a local timeout.
3. A finished goal's result stayed retrievable for **the server's configured
   `action_server_result_timeout`**: 900 s in the stock `nav2_params.yaml` (reported: retrievable
   at 882.5 s, gone at 902.2 s). The retained server log corroborates only the success time that
   the offsets are measured from.
4. The **status topic was not a reliable witness** in these runs. It kept showing a finished goal
   as `SUCCEEDED` long after the server had forgotten it, and a fresh subscriber sometimes
   received nothing at all. The cause of the missed samples was not investigated.
5. **In the tested restarts, nothing was recoverable from the `NavigateToPose` result and status
   behavior.** After a clean restart and after a SIGKILL of the Nav2 stack (one host, Fast DDS,
   Nav2 1.3.13), the server answered `UNKNOWN` for a goal that had been retained or active before
   the restart, the status topic was silent, and the same UUID was accepted again as a new goal.
6. **A stable UUID did not prevent duplicate execution.** Resubmitting a UUID the server still
   tracked **aborted Nav2's whole container** (7 of 7 planned trials, as reported; the retained
   launch logs hold eight such death records, which is not a per-trial mapping). Resubmitting after
   retention expired, or after a restart, was accepted and started a second navigation (n=1 each).

## Environment

| Item | Value |
|---|---|
| Host | Windows + WSL2 (`Ubuntu-24.04`), kernel `6.18.33.2-microsoft-standard-WSL2`, 16 CPUs |
| OS / Python | Ubuntu 24.04.4 LTS, Python 3.12.3 |
| ROS | Jazzy, `ros-jazzy-ros-base` 0.11.0 |
| rclpy | 7.1.11 |
| rcl_action / rclcpp_action | 9.2.11 / 28.1.21 |
| action_msgs / unique_identifier_msgs | 2.0.4 / 2.5.1 |
| Nav2 | `nav2_bringup`, `nav2_bt_navigator`, `nav2_util`, `nav2_msgs`, `nav2_loopback_sim` all 1.3.13 |
| RMW | `rmw_fastrtps_cpp` 8.4.4 (default, `RMW_IMPLEMENTATION` unset), Fast DDS 2.14.6 |
| Network | one host, `ROS_LOCALHOST_ONLY` unset. Domain 0 for the main runs; domains 5 and 6 for parallel stacks. |
| Nav2 config | stock `nav2_params.yaml`, unmodified, except the one clearly labeled supplementary run |

Runs were done on 2026-09-23. Goals took 13 to 60 s in loopback (M0 documents the spread). The
package versions, OS release, Python, kernel and CPU count in this table were compared with the
installed system during the closeout and matched (see "Evidence availability and closeout
checks"). The environment-variable and network rows are as reported.

## Installed inspection

Read from the installed packages on the test machine, not from memory or online docs. Items that
came from the running stack instead are marked as reported.

**`send_goal_async` accepts an explicit UUID (PUBLIC API).** From
`/opt/ros/jazzy/lib/python3.12/site-packages/rclpy/action/client.py`:

```text
ActionClient.send_goal_async(self, goal, feedback_callback=None, goal_uuid=None)
  :param goal_uuid: Universally unique identifier for the goal.
                    If None, then a random UUID is generated.
  :type: unique_identifier_msgs.UUID
```

rclpy's own default is `UUID(uuid=list(uuid.uuid4().bytes))` (`_generate_random_uuid`), so
`list(uuid.UUID.bytes)` is the matching conversion. The only public `ActionClient` method with a UUID
parameter is `send_goal_async`, and it **sends a goal request**.

**No public way to recover a goal handle from a UUID.**
`ClientGoalHandle` is not exported from `rclpy.action` (which exports `ActionClient`, `ActionServer`,
`CancelResponse`, `GoalResponse` and three `get_*_names_and_types*` helpers); it lives in
`rclpy.action.client`. Its constructor is `(action_client, goal_id, goal_response)` and its class
docstring is only `'Goal handle for working with Action Clients.'`. Its public members are
`accepted, cancel_goal, cancel_goal_async, get_result, get_result_async, goal_id, stamp, status`.
`get_result_async()` requires an existing handle and calls
`ActionClient._get_result_async(goal_handle)`, which is private and type-checks its argument.
No documented public route from an arbitrary existing UUID to a goal handle was identified. The only public `ActionClient` method found taking a goal UUID is `send_goal_async`, and it sends a goal request.

**The protocol service and its type (PROTOCOL).** Taken from the generated
`NavigateToPose.Impl`, not guessed:

| Endpoint | Type |
|---|---|
| `/navigate_to_pose/_action/get_result` | `nav2_msgs/action/NavigateToPose_GetResult` |
| `/navigate_to_pose/_action/send_goal` | `nav2_msgs/action/NavigateToPose_SendGoal` |
| `/navigate_to_pose/_action/cancel_goal` | `action_msgs/srv/CancelGoal` |
| `/navigate_to_pose/_action/status` | `action_msgs/msg/GoalStatusArray` |
| `/navigate_to_pose/_action/feedback` | `nav2_msgs/action/NavigateToPose_FeedbackMessage` |

`GetResult.Request` is `{goal_id: unique_identifier_msgs/UUID}`. `GetResult.Response` is
`{status: int8, result: NavigateToPose_Result{error_code: uint16, error_msg: string}}`.
`SendGoal.Response` is `{accepted: bool, stamp: builtin_interfaces/Time}`.
A plain `node.create_client(NavigateToPose.Impl.GetResultService, '/navigate_to_pose/_action/get_result')`
works with it. (The task text mentioned the rclpy test suite using this type; the tests are not
installed here, so that specific claim was not checked.)

**Status topic QoS.** `rclpy.qos.qos_profile_action_status_default` is KEEP_LAST depth 1,
RELIABLE, TRANSIENT_LOCAL (installed inspection). The publisher on the running stack
(`/bt_navigator`) was reported to be RELIABLE + TRANSIENT_LOCAL as well.

**Result timeout.** rcl's generic default is 10 s (`rcl_action/action_server.h`, "result_timeout =
RCUTILS_S_TO_NS(10)"). The installed stock `nav2_params.yaml` sets
`bt_navigator.action_server_result_timeout: 900.0` (line 49) and the same for `waypoint_follower`
(line 329); both were re-read during the closeout. The parameter name is present in
`libnav2_navigate_to_pose_navigator.so`. What the running stack used is a separate question:
`ros2 param get /bt_navigator action_server_result_timeout` on the live stack was reported to
return `900.0` (reported experiment; that output was not found later, and the live value cannot be
re-checked now). Which value applies was **measured**, not assumed (experiment 3).

**Abort message string.** `Failed to accept new goal` is present in the installed
`librclcpp_action.so` and absent from `librcl_action.so` (re-checked during the closeout). The
Nav2 libraries checked when the runs were made were not listed, so that part is as reported. The
installed packages ship no `rclcpp_action` sources. What produces the string, and why a duplicate
UUID leads to it, is covered in the next section and is not an installed-source finding.

## Duplicate-UUID abort: what each source shows

The four kinds of evidence about the abort are kept apart here.

- **Reported experiment.** Resubmitting a tracked goal UUID made `nav2_container` die with
  `std::runtime_error` `Failed to accept new goal` (exit code -6) in 7 of 7 planned trials
  (experiments 5 and 6). The second sender was told `ACCEPTED` in 6 of the 7. The retained launch
  logs corroborate part of this: eight `component_container_isolated` death records with exit code
  -6 exist for 2026-09-23 (`docs/evidence/task006_server_log_excerpts.md`, E1). That equals the
  reported 7 planned trials plus 1 unplanned instance in number only. The records carry no UUID or
  trial label, so they are not a per-trial mapping. The exception text and the `ACCEPTED`
  responses are in the original report and not in the retained files.
- **Installed inspection.** The string is present in `librclcpp_action.so` (see above). No trace of
  the installed binary was made.
- **Upstream source reading (not measured here).** The goal-request handling in
  `rclcpp_action/src/server.cpp` at tag `28.1.21`, the installed `rclcpp_action` version
  (https://github.com/ros2/rclcpp/blob/28.1.21/rclcpp_action/src/server.cpp), was checked online
  by an external reviewer on 2026-09-24. The ordering is: the handle-goal callback runs, the goal
  response is sent, the accepted goal is registered, and a null handle from that registration
  throws `std::runtime_error` with the message `Failed to accept new goal`. This is source
  inspection, not an installed-binary trace, and it was not fetched again during the closeout.
  That ordering is consistent with the reported observation that a duplicate client may be told
  `ACCEPTED` just before the container aborts. It does not prove that this path was the one taken
  in the reported runs. In the original run the exception was reported unhandled, with the line
  `terminate called after throwing an instance of 'std::runtime_error'` (from the report; that
  line is not in the retained raw files).
- **Issue #3120 and PR #2953 (related references).** ros2/rclcpp issue #3120 ("Backport #2953 to
  jazzy") describes a crash with the same message on Jazzy for a duplicate of a *recently
  completed* goal, and asks for PR #2953 to be backported. The reported experiments also saw the
  abort for a goal that was still active, which is broader than the trigger the issue describes.
  PR #2953 ("try aborting before canceling 1st on dtor of ServerGoalHandle") is a related
  reference only. This document does not establish that it addresses duplicate goals in general
  or the active-goal case, and it makes no claim about the current status of the issue or the PR.
  Neither was fetched again during the closeout.

## Tools

All in `task_resilience_demos/scripts/`. They import ROS and Nav2; the production core does not.

| File | Purpose |
|---|---|
| `ros_uuid.py` | `uuid.UUID` <-> `unique_identifier_msgs/msg/UUID`. The 16 bytes keep `uuid.UUID.bytes` order; nothing is reversed or reinterpreted. Only canonical text is accepted. `python3 ros_uuid.py --self-test` runs the round trip on five fixed vectors, including `00010203-...-0e0f`, where any reordering shows. |
| `send_nav2_goal_with_uuid.py` | Sends `NavigateToPose` under `--uuid` via public `send_goal_async(goal, goal_uuid=...)`. Prints the UUID (text and ROS byte order), submission, ACCEPTED/REJECTED, the response's server stamp, and whether the handle's UUID matches. Modes: default (wait for result), `--exit-after-accept`, `--exit-before-response`, `--exit-immediately`. The caller supplies the UUID and the script does not guard against one that was used before (see "Reproduce"). In the early-exit modes exit code 0 does not confirm acceptance or success. With a result timeout it reports that the current goal state is unknown, not that the goal is running. |
| `probe_nav2_goal.py` | Fresh process that knows only `--uuid` and never sends a goal. A: status topic. B: public-API inspection. C: protocol `get_result`. D (only with `--private-handle`): a labeled private-internals comparison. Every wait is bounded. |
| `task_resilience_demos/test/test_ros_uuid.py` | pytest version of the round-trip check (skipped when ROS is not sourced). |
| `task_resilience_demos/test/test_sender_reporting.py` | Two offline pytest checks of what the sender says: fake client and futures, no `rclpy.init()`, node, server or waiting (skipped when ROS is not sourced). |

`--exit-before-response` is the controlled version of "the client dies before it records the
answer": it hands the goal to `send_goal_async`, then watches the status topic from a **separate
node** and never spins the `ActionClient`'s own node, so the accept/reject response is never taken.
It exits with `os._exit` once the status topic lists the UUID, and reports only that observation (a
status entry, not an acceptance response). The experiment needs a never-before-used UUID: the
status topic is TRANSIENT_LOCAL, so a status entry left by an earlier goal with the same UUID would
end the wait at once and invalidate the control.

The probe reports a **local timeout** ("no response arrived; not a server answer") separately from
a server reply of `UNKNOWN`.

## Experiments

The tables record what was reported from the original 2026-09-23 runs. The client-side outputs
(probe and sender output) were not found in the locations checked during the closeout, so they
cannot be re-read. Nav2's own launch and container logs from those runs were found under
`~/.ros/log`, and a small sanitized selection is in `docs/evidence/task006_server_log_excerpts.md`.
"Evidence availability and closeout checks" says what those logs do and do not corroborate.
Unless a note says the retained logs corroborate a cell, treat it as reported.

### Summary table

"Status sees UUID?" is the probe's status section (window 1 to 8 s). "Result by UUID?" is the
protocol `get_result` call (section C). n = number of runs. In that column, "no response within
the wait" is a local timeout or a call still pending: it says only that nothing arrived in the
wait, not that the server received the request or is executing the goal.

| # | Experiment | Status sees UUID? | Result by UUID? | Same UUID accepted? | Observation |
|---|---|---|---|---|---|
| 1 | Active goal, client A exited after accept; fresh probes | Yes, `EXECUTING` (first sample at 0.38 s and 0.41 s) | No response within the 5.0 s wait (local timeout; the goal was independently seen `EXECUTING`). With a 150 s timeout: response after 45.8 s, at the instant Nav2 logged `Goal succeeded`, `SUCCEEDED`, `error_code=0`, `error_msg=''` | not tested here | With the goal independently seen `EXECUTING`, the request was answered only when the goal finished. The wait itself was not the evidence. n=1 |
| 2 | Finished goal, sender exited, probe soon after | Yes, `SUCCEEDED` at +2.45 s | Immediate (1 ms) `SUCCEEDED`, `error_code=0` | not tested here | Same run as 3 (its first checkpoints). n=1 |
| 3 | Retention, stock 900 s config | Yes at 37 of 41 probes through +1443.5 s; 4 got no message. Gone from +1463 s, after an unrelated goal was accepted | Retrievable through **+882.5 s**; `UNKNOWN` from **+902.2 s** | n/a | Status outlived the result. n=1 |
| 3b | Retention, supplementary 10 s config | Yes at 15 of 19 probes; 4 got no message | Retrievable through +8.6 s; `UNKNOWN` from +11.2 s | n/a | Bracket matches the configured value. n=1 |
| 4 | Unknown UUID | No (fresh server: topic silent, no message; busy server: latest array lacked it) | `UNKNOWN (0)` in 1 ms, `error_code=0`, empty message | not tested here | n=2 (fresh and busy server) |
| 5a | Same UUID + same payload while **active** | Original was `EXECUTING` just before (run 1) | not obtained: the server had aborted | Second request: `ACCEPTED` in 2 runs, no response within 10 s in 1 | Container aborted in all 3. n=3 |
| 5b | Same UUID + **different** payload while active | Original was `EXECUTING` before | server gone | Second request told `ACCEPTED` (2 ms) | Container aborted. n=2 |
| 6 | Same UUID after **completion**, result still retained | `SUCCEEDED` before | `SUCCEEDED` before resubmitting | Second request told `ACCEPTED` (1 ms) | Container aborted; no `Begin navigating`. n=2 |
| 7 | Same UUID after retention **expired** (1482 s) | Stale `SUCCEEDED` before; then new `EXECUTING` | `UNKNOWN` before; after resubmission `SUCCEEDED` | `ACCEPTED` | New navigation began, no abort. n=1 |
| 8a | Goal finished, clean Nav2 restart, fresh probe | No (topic silent, 5 s and 8 s windows) | `UNKNOWN` in 1 ms | `ACCEPTED` | New navigation began. n=1 |
| 8b | Goal active, Nav2 SIGKILLed and relaunched | No (topic silent) | `UNKNOWN` in 1 ms | `ACCEPTED` | New navigation began. n=1 |
| 9a | Sender dies without taking the accept response (controlled) | Yes, `EXECUTING` (second probe; the first probe's 2 s window got nothing) | No response during the wait; `SUCCEEDED` received later | not tested here | Goal was received and ran, shown by the `EXECUTING` status entry and the later `SUCCEEDED` response, not by the wait. n=1 |
| 9b | Sender dies immediately after `send_goal_async`, 6 trials | Present in 5 of 6 (1 empty 3 s window) | As originally reported in aggregate: pending or returned a result in all 6. The per-trial breakdown was not found. | not tested here | Originally read as "all 6 requests were received". The retained material does not establish per-trial delivery for all six (see Experiment 9). n=6 |
| 9c | UUID of a **rejected** goal (server inactive during bringup) | not tested | `UNKNOWN` in 2 ms, right after the first rejection | Retry with the same UUID: `ACCEPTED` on attempt 22, normal navigation | 21 consecutive rejections. n=1 |

Across the seven planned duplicate trials of a tracked UUID (5a x3, 5b x2, 6 x2), as reported, the
second sender saw `ACCEPTED` in 6 and no response within its wait in 1 (the first 5a run), and the
container aborted in all 7.

### Experiment 3 detail: retention window (stock config)

Fresh probe process per checkpoint, offsets from the server's own `Goal succeeded` log time
(`21:08:41.139`). "Result request at" is when the probe's `get_result` was actually sent.

| Result request at | Status topic | `get_result` |
|---|---|---|
| +2.45 s | present `SUCCEEDED` | `SUCCEEDED` |
| +4.84, +7.48, +12.69, +18.62 s | present | `SUCCEEDED` |
| +33.25, +63.31, +122.5, +304.7 s | present | `SUCCEEDED` |
| +606.9, +847.1, +862.1, +882.5 s | present | `SUCCEEDED` |
| **+902.2 s** | present `SUCCEEDED` | **`UNKNOWN`** |
| +922.9, +942.2, +961.8 s | present | `UNKNOWN` |
| +983.4 s | **no message in the 1 s window** | `UNKNOWN` |
| +1001.8 to +1422.3 s (20 s steps) | present, except no message at +1084.1 and +1262.1 s | `UNKNOWN` |
| +1443.5 s | **no message in the 1 s window** | `UNKNOWN` |
| +1463.2, +1482.6 s | X3 **absent**: an unrelated goal (U7) was accepted at +1456 s and the array it republished listed only U7 | `UNKNOWN` |

So the result was reported retrievable for between 882.5 s and 902.2 s, consistent with the
configured 900 s. Not 1800 s: there was no sign of a second timer period. The reported bracket is
the evidence here. It agrees with the 900.0 s value in the installed stock `nav2_params.yaml`.

What the retained logs add: the offsets above are measured from the server's own `Goal succeeded`
log time, 21:08:41.139, and the retained container log has an entry at that time (corroborated).
It also has `Begin navigating` entries at +1456.16 s and +1484.61 s after that time; the entries
carry no UUID, so tying them to goal U7 and to the post-expiry resubmission rests on the report
and on timing. The client-side checkpoints (the responses at 882.5 s and 902.2 s, the status
samples) were reported from client output that was not found, and stay as reported.

The supplementary run (`action_server_result_timeout: 10.0` in a copy of `nav2_params.yaml`, a
one-line change verified with `diff` and at runtime) gave a retrievable result at +8.64 s and
`UNKNOWN` at +11.17 s, so retention follows that parameter.

**Status topic does not expire with the result.** At +1450 s a probe still saw
`X3=SUCCEEDED` on the status topic while `get_result` said `UNKNOWN`. A different goal (U7) was then
submitted at +1456 s; the status array republished by that submission contained only U7, and X3
was gone. Three separate probe processes saw that (the driver's probes at +1463 s and +1483 s, and a manual
one at +1463 s). The retention driver's stop rule ("status absent and `UNKNOWN` three times in a
row") only triggered because of that update. So the earlier "presence" was a stale TRANSIENT_LOCAL sample, not a live view of
what the server tracks. It disappears only when the server next publishes (for example when
another goal changes state) or when the publisher itself goes away.

### Experiment 5 and 6: resubmitting a tracked UUID aborts Nav2

The same sequence in all seven planned trials: first submission accepted (or finished), second
submission of the same UUID (same or different payload, active or retained-completed), then, as
reported from the terminal output (these lines are not in the retained raw files), in
`nav2_container`:

```text
terminate called after throwing an instance of 'std::runtime_error'
  what():  Failed to accept new goal
[ERROR] process has died [... exit code -6, ... __node:=nav2_container ...]
```

What was observed:

- The process that dies is `nav2_container` (`component_container_isolated`), which hosts
  `bt_navigator`, the controller, planner and the other Nav2 servers. Exit code -6 is SIGABRT.
  `robot_state_publisher`, `map_server`, `lifecycle_manager` and the loopback simulator survived,
  but nothing serving `/navigate_to_pose` remained.
- The original goal was lost with the server. No second navigation began (`Begin navigating` did
  not appear a second time).
- The second submitter was told `ACCEPTED` in 6 of 7 trials (1, 2 and 87 ms in the three runs where
  the time was printed), and got no response within 10 s in 1. A client can therefore be told a
  duplicate was accepted by a server that then dies.
- It did not depend on the payload (5b) or on whether the first goal was still running (6).
- One further, unplanned instance happened when I mistakenly resent a tracked UUID during 9c setup;
  same message, same exit code.
- As originally reported, after the abort the graph kept showing the dead server's status
  publisher and `get_result` service for approximately 20 s (fresh rclpy poller; abort anchor
  ~21:14:06.23, publisher and service gone at 21:14:26.246). The retained launch log for that run
  records the container's death (exit -6) at 21:14:13.367, which is 7.1 s after the report's abort
  anchor, and the last retained container-log entry is at 21:13:55.606. The poller output was not
  found, so the precise interval cannot be independently re-established from the retained
  material; the approximately 20 s is kept as originally reported. If the reported 21:14:26.246 is
  right, it is later than both anchors. Inside such a window, graph presence alone cannot tell a
  dead server from a live one.
- The trigger was observed only for UUIDs the server was still tracking (active or retained).
  Expired UUIDs (7), restarted servers (8) and previously rejected UUIDs (9c) were accepted normally.

### Experiment 9: the acceptance ambiguity

The sender can vanish before it learns the outcome. What was reported:

- **9a (controlled).** The goal was submitted through public `send_goal_async`. The status topic
  (read on a separate node) listed the UUID as `EXECUTING` 26 ms later, then the sender died
  with `os._exit` **without ever taking the send_goal response**. A fresh probe's status window
  then showed the goal `EXECUTING` (on the second probe; the first probe's window got nothing).
  The `get_result` request went unanswered during the wait, and `SUCCEEDED` was received later.
  The client never recorded accept/reject. What shows that the server had the goal and ran it is
  the status entry, seen independently of any wait, and the later `SUCCEEDED` response. The
  unanswered wait is not part of that evidence.
- **9b (uncontrolled).** Six trials where the sender died immediately after `send_goal_async`
  returned, before waiting for anything. As originally reported in aggregate: the status topic
  listed the UUID in 5 of the 6 (one 3 s window was empty), and the probes' `get_result` requests
  were pending or returned a result in all 6. The original reading was that the server had received
  the goal all six times. **The retained material does not establish per-trial delivery for all
  six**: the client outputs and the per-trial result breakdown were not found, and a pending call
  does not by itself show delivery. That is a limit on the evidence, not a finding that any request
  was lost. The retained server logs cannot fill the gap, because their goal entries carry no UUID
  and so cannot be assigned to these trials (see `docs/evidence/task006_server_log_excerpts.md`).
  The trials also cannot show that a request is always delivered if the process dies at an earlier
  instant.
- **9c.** The UUID of a rejected goal is indistinguishable from an unknown UUID (`UNKNOWN`).
- **What a fresh probe can and cannot conclude.**
  - A **terminal result** from `get_result` is evidence that the server held the UUID when it
    answered.
  - An immediate **`UNKNOWN`** says only that the server has no record now. That single answer
    covers four different histories: never delivered, rejected, expired after the retention
    window, and server restarted. The probe cannot tell them apart.
  - A **pending request or local timeout** establishes only that no response was received within
    the local wait. It does not establish that the query was delivered, that the server tracks the
    UUID, that the goal is executing, or that the server is alive. A live server holding an active
    goal, a dead or hung server, and a lost request or response all look the same.
  - A **status entry** is advisory: it can be stale, and its absence proves nothing.
  - Neither `UNKNOWN` nor a local timeout authorizes sending again under the same UUID or under a
    replacement UUID. A replacement UUID is a new request and does not resolve what happened to
    the old one.

### Status topic reliability

The status probe listens for a bounded window and then reports; an empty window is reported as
"no message" and is not read as absence.

- 8 of 62 scheduled probes (1.0 s window, goal's status sample known to be retained) received no
  status message (4 of 43 and 4 of 19). Three more ad hoc probes with 2 to 3 s windows also got
  nothing: experiment 9a's first probe (goal `EXECUTING`), 9b trial 2 (goal active) and 8a's
  pre-restart probe (goal finished, result retrievable).
- When a message arrived, the delay after subscribing ranged from 0.31 s to 2.58 s in the saved
  outputs (median 0.47 s).
- Right after a server restart, or before any goal, the topic is silent (no sample to deliver).

These are counts from the original probes. The cause of the missed samples was not investigated;
they are observations in these runs, not a diagnosis.

## API classification

| Technique | Class | Used for |
|---|---|---|
| `ActionClient.send_goal_async(goal, goal_uuid=...)` | PUBLIC API | Submit under a chosen UUID. Sends a goal; it is not a query. |
| `ClientGoalHandle.get_result_async()` on a handle from an accepted goal | PUBLIC API | Result, but only for a handle obtained by sending a goal in this process. |
| A public method that turns a UUID into a handle or result | none exists | The probe's section B inspects `rclpy.action.client`; nothing found in rclpy 7.1.11. |
| `/navigate_to_pose/_action/get_result` with a generated `GetResult` request | PROTOCOL | Result, status and error fields by UUID (experiments 1 to 9). |
| `/navigate_to_pose/_action/status` (`GoalStatusArray`, TRANSIENT_LOCAL) | PROTOCOL | Advisory view of goals (unreliable, see above). |
| `/navigate_to_pose/_action/cancel_goal` (`CancelGoal`) | PROTOCOL | Used only to clear goals between trials, never as a result. |
| `ClientGoalHandle(action_client, goal_id, fabricated_response)` then `get_result_async()` | PRIVATE | Probe `--private-handle`, comparison only. Reaches the same server endpoint; returned exactly the same as the protocol call (`SUCCEEDED` for a retained goal, `UNKNOWN` for an unknown one). Depends on constructing a handle with a fabricated acceptance and on `_get_result_async`. |
| `ActionClient._client_handle`, `_get_result_async` | PRIVATE | Inspected and documented; not used as a primary mechanism. |

## Conclusions

1. **Can a fresh process recover an active goal's state with only the UUID?** Only in part, at the
   PROTOCOL level. The status topic showed `EXECUTING` in most probes (a sample was missing in some),
   and a `get_result` request sent during an active goal received its response only when the goal
   finished. A request with no response yet is not a state observation: it cannot be told apart from
   an unresponsive server or a lost request without a separate liveness signal (graph presence was
   reported to lag a dead server by some seconds; see experiments 5 and 6). No PUBLIC API does this.
2. **Can it recover the final result?** Yes, at the PROTOCOL level: `get_result` returned `SUCCEEDED`,
   `error_code=0`, `error_msg=''` for a finished goal, and it returned at the instant the goal
   finished when called during the goal.
3. **For how long?** For the server's `action_server_result_timeout`. Reported: retrievable through
   882.5 s and gone at 902.2 s for the stock 900 s value; 8.6 to 11.2 s when configured to 10 s.
4. **What evidence disappears first?** The result (at about the timeout). The status topic keeps a
   stale `SUCCEEDED` entry afterwards, until the server republishes for some other reason.
5. **What survives an action-server restart?** In the tested restarts (one clean restart and one
   SIGKILL of the Nav2 stack; `NavigateToPose` on Nav2 1.3.13, one host, Fast DDS), nothing was
   observed to survive at the result and status level: status was silent, `get_result` returned
   `UNKNOWN`, and the UUID was accepted again. This says nothing about other servers, other
   actions, other RMWs or servers with persistence.
6. **Does resending the same UUID prevent duplicate execution?** No. While the server tracks the
   UUID, resending it aborted the Nav2 container in 7 of 7 planned trials (as reported). After the retention
   window or a restart, it was accepted and a second navigation started. After a rejection it was
   accepted normally.
7. **What ambiguities remain?** `UNKNOWN` is one answer for four histories (never delivered, rejected,
   expired, restarted). A pending or timed-out call cannot separate "active" from "server
   unresponsive" or from a lost request or response, and says nothing about delivery. The accept
   response cannot be trusted for a duplicate. An absent status entry proves nothing. Whether a
   request sent by a process that dies at any earlier instant is always delivered was not
   established, and the retained material does not establish per-trial delivery for all six
   experiment 9b trials (client outputs not found).
8. **Which mechanism is suitable as input to future reconciliation?** The PROTOCOL `get_result` call,
   with bounded timeouts and the local-timeout / server-`UNKNOWN` distinction kept explicit, is
   the only UUID-keyed query found. The status topic is at best advisory. The private route adds
   nothing over the protocol call.

## What this suggests for reconciliation design

These follow directly from the reported measurements. They are inputs for a later reviewed task, not
decisions.

- A `DISPATCH_PREPARED` step cannot be resolved from the UUID alone. A terminal `get_result` (within
  the retention window) is positive evidence; an immediate `UNKNOWN` is not evidence the goal never ran.
- Resubmitting a stored dispatch UUID must not be used as a probe or a retry. It can abort the
  server that hosts the whole navigation stack, and it starts a second execution once the server has
  forgotten the UUID. A newly generated UUID is no way around that: it is a new request, not a
  retry, and it does not resolve what happened to the old one.
- A pending or timed-out `get_result` is not evidence about the goal. Treat it as no answer, not as
  "active" and not as "alive". Neither it nor an `UNKNOWN` authorizes another send under the same
  UUID or a replacement UUID.
- Anything that interprets `UNKNOWN` needs to know how long ago the goal could have finished, against
  the server's configured retention, and whether the server has restarted since. Neither is
  observable from the UUID.
- The configured `action_server_result_timeout` is deployment state, not a constant.
- Recovery stays conservative, as `PROJECT.md` already requires.

## Evidence availability and closeout checks

**Where the raw material is.**

- *Client-side outputs* (probe and sender output for experiments 1 to 9) were **not found** in the
  locations checked during the closeout: the session scratch directories that existed then, and
  `/tmp`. They are not stated to have been deleted. Every client-side observation in this document
  is therefore as originally reported.
- *Nav2 server logs* from the 2026-09-23 runs were found under `~/.ros/log` (launch logs and
  container logs). A small sanitized selection, with source file names, line ranges and SHA-256
  values, is in `docs/evidence/task006_server_log_excerpts.md`. The logs themselves are not
  committed.

**What the retained logs corroborate.**

- The server's `Goal succeeded` time (21:08:41.139 local) that the retention offsets are measured
  from.
- Eight `component_container_isolated` death records with exit code -6 on 2026-09-23. The number
  equals the reported 7 planned plus 1 unplanned. It is not a per-trial mapping: the records carry
  no UUID or trial label.
- Two later `Begin navigating` entries at about +1456 s and +1484 s after that time, consistent
  with the report's description but not correlated to any UUID.
- The launch-log death time of the run behind the graph-timing observation (21:14:13.367), which
  differs from the report's abort anchor (see experiments 5 and 6).

**What they cannot corroborate.** Acceptance responses, `UNKNOWN` replies, status listings, probe
timings, the retention checkpoints at 882.5 s and 902.2 s, per-trial results, and the exception
text. The `terminate called ... Failed to accept new goal` lines are in the original report, not in
the retained raw files from the evening runs. Goal entries in the server logs carry no UUID, so
nothing in them can be tied to a dispatch.

**Checks performed during the closeout (2026-09-24), all read-only.**

- Installed package versions (`dpkg-query`), OS release, Python, kernel and CPU count, compared
  with the Environment table.
- The installed stock `nav2_params.yaml`: `action_server_result_timeout: 900.0` for
  `bt_navigator` and `waypoint_follower`.
- The string `Failed to accept new goal` in `librclcpp_action.so` (present) and `librcl_action.so`
  (absent).
- The retained server logs described above, and a search of every file under `~/.ros/log` modified
  on 2026-09-23 for the exception text.

Not done: no experiment was rerun, Nav2 was not launched, no goal was sent or cancelled, and no
upstream page was fetched again.

## Limitations

- One host and one RMW (Fast DDS default). Discovery and liveliness timing on a real network, or with
  another RMW (`rmw_zenoh_cpp` is installed but was not tried), may differ.
- One action (`NavigateToPose`) on the loopback simulator. Goal timing varied from 13 to 60 s.
- The client-side raw outputs were not found (see "Evidence availability and closeout checks"), so
  every client-side number here is as originally reported.
- Small n: most experiments ran once. The duplicate-UUID abort was repeated (7 planned trials); the
  retention window was measured once at 900 s and once at 10 s, with checkpoints 20 s and 1 to 3 s
  apart, so the boundaries are brackets, not exact values. The restart results rest on two restarts
  of the Nav2 stack (one clean, one SIGKILL) on one host.
- The duplicate-UUID abort was measured through Nav2 only. This spike did not trace the installed
  binary and did not test the abort with other actions or another RMW. The explanation of the internal
  path comes from a reviewer's reading of the version-tagged upstream source and from issue #3120
  (see "Duplicate-UUID abort: what each source shows"), not from this spike's measurements.
- Experiment 9b's original reading (all six requests arrived) is not established per trial by the
  retained material, because the client outputs were not found. That limits the evidence and is not
  a finding that a request was lost. It also cannot show that a request would always arrive.
- I did not test `ros2 action send_goal` or other clients, other Nav2 actions, multiple servers, or a
  goal that finished by cancel or abort (only success was retained and expired).
- A few harness mistakes were made and corrected during the runs: a control script that declared Nav2
  active too early (one 5b run, discarded and redone), a malformed UUID template (9b, discarded and
  redone), a self-matching `pkill`, and an accidental resend of a tracked UUID. That last one is
  mentioned above as an unplanned instance and is not counted in the seven planned trials.

## Reproduce (selected examples)

These are selected reproductions of some reported observations, not a harness for every
experiment. Scenarios that are not shown (3b, 4 on a busy server, 7, 8a, 8b, 9b, 9c) have no
command here. The commands below were not run during the closeout.

Rules for every example:

- Each scenario is an independent, controlled run with its **own newly generated UUID**. Never use
  one scenario's UUID in another scenario's send command.
- Use a scenario's UUID unchanged for that scenario's read-only probes. The probe never sends a
  goal. Print the UUID when it is generated, so it can be reused for the probes if the terminal is
  closed.
- Start a scenario only when the previous one has finished. Independent scenarios are not
  permission to submit new goals while a preceding scenario remains unresolved.
- A new UUID is not authorization to retry unresolved work. It is a new request, and it does not
  resolve what happened to the old one. Neither an `UNKNOWN` reply nor a local timeout authorizes
  another send under the same UUID or a replacement UUID.

Run from the repository root with ROS sourced. Start Nav2 as in M0:

```bash
./task_resilience_demos/scripts/run_loopback_baseline.sh          # terminal 1
ros2 topic pub --once /initialpose geometry_msgs/msg/PoseWithCovarianceStamped \
  '{header: {frame_id: map}, pose: {pose: {position: {x: -2.0, y: -0.5, z: 0.0}, orientation: {x: 0.0, y: 0.0, z: 0.0, w: 1.0}}}}'
# wait for: [lifecycle_manager_navigation]: Managed nodes are active
```

Experiment 1 (active goal, then a fresh probe that only knows the UUID):

```bash
EXP1_UUID="$(python3 -c 'import uuid; print(uuid.uuid4())')"; echo "EXP1_UUID=$EXP1_UUID"
python3 task_resilience_demos/scripts/send_nav2_goal_with_uuid.py --uuid "$EXP1_UUID" --exit-after-accept
python3 task_resilience_demos/scripts/probe_nav2_goal.py --uuid "$EXP1_UUID" --result-timeout 150
```

In the reported run the probe's request was answered when the goal finished, after 45.8 s. If the
probe reports a local timeout, that means only that no response arrived within the wait.

Experiments 2 and 3 (finished goal, then repeated fresh probes). This is a new scenario with a new
UUID. Start it after experiment 1 has ended:

```bash
RETENTION_UUID="$(python3 -c 'import uuid; print(uuid.uuid4())')"; echo "RETENTION_UUID=$RETENTION_UUID"
python3 task_resilience_demos/scripts/send_nav2_goal_with_uuid.py --uuid "$RETENTION_UUID"   # waits for the result
python3 task_resilience_demos/scripts/probe_nav2_goal.py --uuid "$RETENTION_UUID" --status-window 1 --result-timeout 10
# repeat only the probe line, with the same RETENTION_UUID, at +2, +5, +10, +15, +30, +60 s ...
# and every 20 s around 900 s. Never run the send line again for this UUID.
```

Experiment 4 (unknown UUID). `UNKNOWN_UUID` is the never-submitted control: **it must not be
submitted.** Never pass it to the sender.

```bash
UNKNOWN_UUID="$(python3 -c 'import uuid; print(uuid.uuid4())')"; echo "UNKNOWN_UUID=$UNKNOWN_UUID"
python3 task_resilience_demos/scripts/probe_nav2_goal.py --uuid "$UNKNOWN_UUID"
```

Experiment 9a (acceptance ambiguity, controlled). Another new scenario with its own UUID. The probe
settings are illustrative; the settings of the reported run were not recorded:

```bash
AMBIGUITY_UUID="$(python3 -c 'import uuid; print(uuid.uuid4())')"; echo "AMBIGUITY_UUID=$AMBIGUITY_UUID"
python3 task_resilience_demos/scripts/send_nav2_goal_with_uuid.py --uuid "$AMBIGUITY_UUID" --exit-before-response
python3 task_resilience_demos/scripts/probe_nav2_goal.py --uuid "$AMBIGUITY_UUID" --status-window 3 --result-timeout 150
```

`--exit-before-response` requires a never-before-used UUID for this controlled experiment. The
sender exits as soon as the status topic lists the UUID, and status retained for an old UUID (the
topic is TRANSIENT_LOCAL) would satisfy that at once and invalidate the control. The sender reports
only that observation. It is not an acceptance response and does not show the goal is executing.

### Destructive duplicate-UUID experiments (5 and 6): not reproduced here

These sent a UUID the server still tracked and aborted the Nav2 container in all planned trials.
They are deliberately not given as runnable commands, and none of the examples above may be
combined with them. Do not run them against a Nav2 you care about. If they are repeated, use a
throwaway stack with its own `ROS_DOMAIN_ID` and a UUID that belongs only to that trial. Sending a
tracked UUID is not a probe or a retry (see the reconciliation notes above).

UUID check: `python3 task_resilience_demos/scripts/ros_uuid.py --self-test`, or
`python3 -m pytest task_resilience_demos/test -q`.

## References

Upstream references are for context only. They were not fetched again during the closeout, and this
document makes no claim about the current status of the issue or the PR.

- `rclcpp_action/src/server.cpp` at tag `28.1.21` (the installed `rclcpp_action` version), the
  primary source-code reference for the goal-request path; checked online by an external reviewer
  on 2026-09-24. https://github.com/ros2/rclcpp/blob/28.1.21/rclcpp_action/src/server.cpp
- ros2/rclcpp issue #3120, "Backport #2953 to jazzy": a crash on Jazzy for a duplicate of a
  recently completed goal (the trigger the issue describes). https://github.com/ros2/rclcpp/issues/3120
- ros2/rclcpp PR #2953, "try aborting before canceling 1st on dtor of ServerGoalHandle": a related
  reference that issue #3120 asks to backport. Not established here as a fix for duplicate goals in
  general or for the active-goal case. https://github.com/ros2/rclcpp/pull/2953
