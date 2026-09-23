# Hardware-free Nav2 baseline

Milestone M0 from `PROJECT.md`: stock ROS 2 Jazzy + Nav2 executes a
programmatically submitted `NavigateToPose` goal against the Nav2 loopback
simulator, headless, with no physical robot. No Task Resilience logic is
involved yet; this exists so later failures can be attributed to our code and
not to an unverified ROS/Nav2 environment.

## Tested environment

Tested on 2026-09-23.

| Item | Value |
|---|---|
| Host | Windows + WSL2 (`WSL_DISTRO_NAME=Ubuntu-24.04`), systemd enabled |
| Kernel | `6.18.33.2-microsoft-standard-WSL2`, x86_64, 16 CPUs, 15 GiB RAM |
| OS | Ubuntu 24.04.4 LTS (noble) |
| ROS | Jazzy (`ros-jazzy-ros-base` 0.11.0), `rclpy` 7.1.11, Python 3.12.3 |
| RMW | `rmw_fastrtps_cpp` (default), `ROS_DOMAIN_ID` unset |
| `nav2_bringup` | 1.3.13 (`ros-jazzy-nav2-bringup 1.3.13-1noble.20260907.023821`) |
| `nav2_loopback_sim` | 1.3.13 (`ros-jazzy-nav2-loopback-sim 1.3.13-1noble.20260903.071341`) |
| Other | `ros-jazzy-navigation2` / `nav2-msgs` / `nav2-bt-navigator` 1.3.13, `nav2-minimal-tb3-sim` 1.0.1 |

## Install

Neither Nav2 package was present on a fresh ROS Jazzy `ros-base` install.
This was the install that was run:

```bash
sudo apt-get install ros-jazzy-nav2-bringup ros-jazzy-nav2-loopback-sim
```

That pulled in 188 packages (59 ROS, 129 Ubuntu system libraries such as
OpenCV and VTK), about 142 MB to download and about 871 MB installed. It
upgraded and removed nothing.

## Installed loopback entry points

Found by inspecting the installed packages, not from online docs:

```text
/opt/ros/jazzy/share/nav2_bringup/launch/tb3_loopback_simulation.launch.py
/opt/ros/jazzy/share/nav2_bringup/launch/tb4_loopback_simulation.launch.py
```

`ros2 pkg executables nav2_loopback_sim` reports `nav2_loopback_sim
loopback_simulator`. The TB3 launch file includes
`/opt/ros/jazzy/share/nav2_loopback_sim/loopback_simulation.launch.py`, which
sits at the package share root rather than in a `launch/` directory.

This baseline uses the TB3 file. There is no Gazebo in it. `use_rviz` defaults
to `True`, so it is turned off here.

## Run it

Run from the repository root. Use two terminals.

**Terminal 1: start Nav2 and the loopback sim**

```bash
./task_resilience_demos/scripts/run_loopback_baseline.sh
```

It checks for Jazzy and the two packages, prints the command, and `exec`s:

```bash
source /opt/ros/jazzy/setup.bash
ros2 launch nav2_bringup tb3_loopback_simulation.launch.py use_rviz:=False
```

**Terminal 2: set the initial pose, wait, then send the goal**

The initial pose must be published within about 60 s of launch (see
[Problems encountered](#problems-encountered)).

```bash
source /opt/ros/jazzy/setup.bash
ros2 topic pub --once /initialpose geometry_msgs/msg/PoseWithCovarianceStamped \
  '{header: {frame_id: map}, pose: {pose: {position: {x: -2.0, y: -0.5, z: 0.0}, orientation: {x: 0.0, y: 0.0, z: 0.0, w: 1.0}}}}'
```

Wait until terminal 1 prints `[lifecycle_manager_navigation]: Managed nodes are
active` (about 2 s after the pose in the tested runs), then:

```bash
python3 task_resilience_demos/scripts/send_nav2_goal.py
```

The defaults send a goal to `x=0.5 y=0.5 yaw=0.0` in the `map` frame. Both the
start pose `(-2.0, -0.5)` and the goal `(0.5, 0.5)` were checked against
`tb3_sandbox.pgm`: free cells with 0.5 m clearance, connected at a 0.3 m robot
radius. Options: `--x --y --yaw --server-timeout (10 s) --timeout (120 s)`.

## Expected success indicators

- Terminal 1: `[loopback_simulator]: Received initial pose!`, then
  `[lifecycle_manager_navigation]: Managed nodes are active`.
- `ros2 action list -t` shows
  `/navigate_to_pose [nav2_msgs/action/NavigateToPose]`, and
  `ros2 action info /navigate_to_pose` shows one server, `/bt_navigator`.
  Being listed is not enough; see problem 2 below.
- `ros2 lifecycle get <node>` reports `active [3]` for `/controller_server`,
  `/smoother_server`, `/planner_server`, `/behavior_server`,
  `/bt_navigator`, `/waypoint_follower`, `/velocity_smoother` and
  `/collision_monitor`.
- Client output, exit code 0:

  ```text
  goal submitted: x=0.5 y=0.5 yaw=0.0 frame=map
  goal ACCEPTED
  terminal status: SUCCEEDED (error_code=0 error_msg='')
  ```

- Terminal 1 also shows `Begin navigating from current location (-2.00,
  -0.50) to (0.50, 0.50)`, `Reached the goal!` and `Goal succeeded`.

`SUCCEEDED` means within the goal checker tolerance, not exactly on the pose.
The stock `nav2_params.yaml` sets `xy_goal_tolerance` and `yaw_goal_tolerance`
to 0.25. In one run the robot came to rest at `(0.277, 0.542)` per
`tf2_echo map base_link`, about 0.23 m from the goal.

## Client exit codes

`send_nav2_goal.py` returns 0 on `SUCCEEDED`, 1 on rejection or any other
terminal status, and 2 on a timeout. Each case in this table was exercised
against the live stack:

| Case | How it was triggered | Output | Exit |
|---|---|---|---|
| Success | default goal | `SUCCEEDED` | 0 |
| Rejected | goal sent while `bt_navigator` was `inactive` (no initial pose) | `goal REJECTED` | 1 |
| Aborted | goal `(50, 50)`, outside the map | `ABORTED (error_code=204)`; 204 is `GOAL_OUTSIDE_MAP` in `ComputePathToPose` | 1 |
| Result timeout | `--timeout 3` on a goal that takes over 10 s | `no terminal result within 3.0s, requesting cancel`, then `cancel request ACCEPTED (goal CANCELING; CANCELED not confirmed)`; see below | 2 |
| No server | Nav2 not running, `--server-timeout 3` | `/navigate_to_pose not available after 3.0s` | 2 |

A timeout is not a cancellation result. The client returns 2 either way and
reports only what it saw:

- **Acceptance timeout** (no accept/reject before `--timeout`): it prints that
  the acceptance outcome is UNKNOWN and that no cancel was sent. The goal may or
  may not have been received.
- **Result timeout** (accepted, but no terminal result in time): it requests a
  cancel, waits up to 5 s for the cancel response, and reports that response as
  ACCEPTED, NOT ACCEPTED, or outcome UNKNOWN. ACCEPTED means the server put the
  goal in CANCELING. The client does not wait for or observe a terminal
  `CANCELED` status.

In the tested Nav2 run (`--timeout 3`, goal `(0.5, 0.5)` from `(-2.0, -0.5)`)
the cancel request was ACCEPTED. Nav2's own log then showed
`[navigate_to_pose] [ActionServer] Client requested to cancel the goal.
Cancelling.`, `Goal canceled` (`bt_navigator`) and `Cancellation was
successful. Stopping the robot.` (`controller_server`), and `tf2_echo map
base_link` gave the same position, `(-1.937, -0.461)`, in two samples 3 s
apart. Those log lines are Nav2's; the client itself never received a
`CANCELED` result. The acceptance-timeout message and the NOT ACCEPTED and
UNKNOWN cancel outcomes were exercised only against a throwaway fake action
server (not in the repo), not against Nav2.

## Observed timing

Across six real goals in three launches, Nav2 reported goal times of 13.3, 13.6,
13.6, 18.1, 18.1 and 60.9 s (client wall time is about 1-2 s longer). Three
of the six ran clean. The other three each logged at least one
`Failed to make progress` and recovered on their own; the 60.9 s goal logged
five of them and went through costmap clears and one spin before it reached the
goal. All six ended `SUCCEEDED`. Set test timeouts with that spread in mind;
the client default is 120 s.

## Stop it

Press Ctrl-C in terminal 1. This was tested by sending SIGINT to
`ros2 launch`, which is what Ctrl-C does, and every Nav2 process was gone
within about 5-11 s. See problem 5 for the exit codes it prints.

## Known limitations of loopback simulation

- It is a "frictionless, inertialess, and collisionless" replacement for a
  physics sim, per the upstream docstring in `loopback_simulator.py`. It
  integrates `cmd_vel` into odometry and publishes `/odom`, `/tf`, `/clock`
  and a scan. The source comments that scan as "bogus", made for the collision
  monitor.
- There is no AMCL or SLAM here (`use_localization: False`). The robot's pose
  is whatever `/initialpose` last said, plus integrated velocity.
- It says nothing about physical robot safety, braking, sensor behavior or
  collision safety (`PROJECT.md` section 4.5). It validates software
  execution and recovery semantics only.
- The stock `nav2_params.yaml` is used unmodified, so goal tolerances and
  recovery behavior are Nav2's defaults.

## Problems encountered

Only things seen in the runs above. Each of these was observed, not assumed.

1. **Nav2 bringup aborts if no initial pose is published.** The loopback sim
   ignores `cmd_vel` and publishes no `map`->`odom` transform until it gets
   `/initialpose` (`loopback_simulator.py`, `initialPoseCallback`). Without it
   the global costmap waits about 60 s for `base_link`->`map`, then the
   lifecycle manager logs `Failed to activate global_costmap because transform
   from base_link to map did not become available before timeout`,
   `Failed to change state for node: planner_server` and `Failed to bring up
   all requested nodes. Aborting bringup.` Checked right after the abort,
   `planner_server` and `bt_navigator` were `inactive`, and the log shows no
   further bringup attempt over the ~100 s until I stopped it. Verified fix:
   publish `/initialpose` within the window, as above. I did not test whether
   publishing after the abort recovers the stack; relaunching is the tested
   path.
2. **The action is advertised even when it can't serve goals.** In the aborted
   state above, `/navigate_to_pose` was still in `ros2 action list` with type
   `nav2_msgs/action/NavigateToPose` while `bt_navigator` was `inactive`. In a
   separate launch with no initial pose, `bt_navigator` was likewise
   `inactive` and the client's goal was rejected. Check
   `ros2 lifecycle get /bt_navigator`, or wait for `Managed nodes are active`.
3. **RViz defaults on.** `--show-args` lists `use_rviz` with default `True`.
   The launcher passes `use_rviz:=False`. I did not launch with RViz.
4. **Intermittent `Failed to make progress`.** See Observed timing. Cause not
   determined. It was not tied to how soon after activation the goal was sent:
   it appeared on goals sent right after activation and on goals sent on a
   settled stack, and did not appear on others.
5. **Crash of `component_container_isolated` at shutdown.** After SIGINT,
   `launch` reported `process has died ... exit code -11` (SIGSEGV) for
   `component_container_isolated` in 4 of 5 shutdowns, always after Nav2 had
   logged its preshutdown/cleanup steps. In the fifth, the launch that never
   finished bringup, it reported exit code -9 (SIGKILL). The
   `loopback_simulator` `exit code -2` is just the SIGINT itself. Cause not
   investigated, and it did not affect any goal result.
6. **One-off log errors.**
   - The `loopback_simulator` logs `Transform lookup failed: "base_footprint"
     ... does not exist` once or twice at startup, in every run. It did not
     recur and did not stop bringup.
   - On a client cancel, `bt_navigator` logged `[ERROR] Failed to get result
     for follow_path in node halt!` (seen in both timeout-test runs).
