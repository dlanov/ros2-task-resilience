# Claude Task 001 — Establish the Hardware-Free Nav2 Baseline

## Context

We are starting an independent ROS 2 project currently called **ROS 2 Task Resilience**.

Read `PROJECT.md` first. Treat it as the project source of truth.

The eventual project will provide durable mission state and restart-aware recovery above ROS 2/Nav2 actions.

**Do not implement the mission supervisor yet.**

This first task exists only to establish a clean, reproducible, hardware-free Nav2 baseline in WSL so later failures can be attributed to our code rather than to an unverified ROS/Nav2 environment.

## Environment expectation

Expected primary environment:

- WSL2
- Ubuntu 24.04
- ROS 2 Jazzy
- Nav2
- Nav2 loopback simulator
- no physical robot
- headless execution preferred

Do not assume these components are installed. Inspect first.

## Task objective

Establish and document a reproducible baseline that proves all of the following:

1. ROS 2 Jazzy is available and sourceable.
2. `nav2_bringup` is installed.
3. `nav2_loopback_sim` is installed.
4. Identify the **actual installed launch filename** for the TurtleBot3/Nav2 loopback demo rather than guessing it from online documentation.
5. Launch Nav2 with the loopback simulator and no physical robot.
6. Confirm core Nav2 nodes/actions are available.
7. Programmatically submit one `NavigateToPose` goal.
8. Observe a terminal action result.
9. Capture enough diagnostic information that another person can reproduce the run.

## Important constraints

- Do not use Gazebo unless the installed Nav2 version makes loopback unusable and you can explain why.
- Do not require RViz.
- Do not modify Nav2 source.
- Do not create the Task Resilience supervisor yet.
- Do not introduce an LLM dependency.
- Do not use Docker unless the native WSL environment is unusable.
- Do not commit or push anything.
- Do not use `sudo` to install packages until after you show what is currently installed/missing.
- Do not make broad refactors.
- Do not claim a command worked unless you actually ran it and captured the result.
- If the environment differs from the expectation, report the difference before adapting.
- Keep comments human, short, and useful.

## Step 1 — Inspect environment

Run and report the output of commands equivalent to:

```bash
uname -a
cat /etc/os-release
printf 'ROS_DISTRO=%s\n' "${ROS_DISTRO:-}"
command -v ros2 || true
ls -ld /opt/ros/* 2>/dev/null || true
```

If Jazzy exists, source it:

```bash
source /opt/ros/jazzy/setup.bash
```

Then capture:

```bash
printf 'ROS_DISTRO=%s\n' "$ROS_DISTRO"
ros2 doctor --report
ros2 pkg prefix nav2_bringup || true
ros2 pkg prefix nav2_loopback_sim || true
```

Do not dump irrelevant pages of output into source files. Summarize environment facts in documentation and keep raw terminal output available in your response.

## Step 2 — Discover the installed loopback launch entry point

Do not guess whether this installation uses a filename such as:

- `tb3_loopback_simulation.launch.py`
- `tb3_loopback_sim_launch.py`
- another name

Inspect the installed package:

```bash
NAV2_BRINGUP_PREFIX="$(ros2 pkg prefix nav2_bringup)"
find "$NAV2_BRINGUP_PREFIX/share/nav2_bringup/launch" \
  -maxdepth 1 -type f -iname '*loopback*' -print | sort
```

Also inspect:

```bash
ros2 pkg executables nav2_loopback_sim
```

Record the actual result.

## Step 3 — Install only missing binary packages if necessary

If Jazzy is present but required Nav2 packages are missing, stop and report the missing packages and the exact proposed apt command before running it.

Likely package names may include:

```text
ros-jazzy-navigation2
ros-jazzy-nav2-bringup
ros-jazzy-nav2-loopback-sim
```

But verify availability with `apt-cache policy` before installation.

If ROS 2 Jazzy itself is absent, do not perform a full ROS installation in this task without first reporting that fact.

## Step 4 — Launch the loopback baseline

Use the installed launch file discovered in Step 2.

Prefer:

- headless operation;
- autostart;
- no Gazebo;
- no RViz.

If the installed launch file accepts different arguments, inspect it or use `ros2 launch ... --show-args`.

Capture the exact launch command.

Verify the graph in a second shell with Jazzy sourced:

```bash
ros2 node list
ros2 action list -t
ros2 topic list -t
```

At minimum, determine whether `/navigate_to_pose` is available and identify its exact action type.

## Step 5 — Create the smallest programmatic navigation client

Only after the baseline stack is proven alive, create a tiny demo client under the repository's demo area.

Preferred location:

```text
task_resilience_demos/scripts/send_nav2_goal.py
```

Requirements:

- Python / `rclpy`;
- use the standard installed Nav2 `NavigateToPose` action type;
- wait for the action server with a bounded timeout;
- send one simple reachable map-frame pose;
- print:
  - goal submitted;
  - accepted/rejected;
  - terminal action status/result;
- use a bounded overall timeout so the script cannot wait forever;
- return non-zero on rejection, timeout, or unsuccessful terminal status;
- keep it small;
- no project-specific mission abstractions yet.

If a simpler standard Nav2 example can prove the same baseline with less code, explain the tradeoff. We still want one small script in our repository eventually so CI/system tests have a controlled client.

## Step 6 — Add baseline documentation

Create:

```text
docs/hardware_free_demo.md
```

Document:

- tested date;
- WSL/Ubuntu version;
- ROS distribution;
- installed Nav2 package/version information;
- exact loopback launch filename;
- exact launch command;
- exact goal-client command;
- expected success indicators;
- how to stop the demo;
- known limitations of loopback simulation;
- troubleshooting only for problems actually encountered.

Do not present untested workarounds as verified fixes.

## Step 7 — Optional convenience launcher

Only if it materially simplifies repetition, add a small script such as:

```text
task_resilience_demos/scripts/run_loopback_baseline.sh
```

It should:

- use `set -euo pipefail`;
- verify `/opt/ros/jazzy/setup.bash`;
- source Jazzy;
- verify required ROS packages;
- print the launch command;
- `exec` the discovered/selected ROS launch command.

Do not build a large orchestration script.

## Step 8 — Validation

Run the baseline end to end.

Minimum validation evidence:

```bash
python3 -m py_compile task_resilience_demos/scripts/send_nav2_goal.py
```

If a ROS workspace/package already exists, run the relevant build/test commands.

For the live demo, verify:

- Nav2 loopback starts;
- `/navigate_to_pose` exists;
- client connects;
- goal is accepted;
- goal reaches a terminal result.

If the goal cannot complete, diagnose the actual failure and stop before adding unrelated code.

## Deliverables

At the end, report:

### Environment

- Ubuntu version
- ROS distribution
- Nav2 package versions
- WSL details relevant to the run

### Baseline result

- PASS or BLOCKED
- actual loopback launch file
- exact launch command
- action name/type
- whether goal was accepted
- terminal result

### Files created/modified

For each file, one sentence explaining why it exists.

### Tests/commands run

List the meaningful commands and whether each passed.

### Problems

List observed problems only.

### Next recommendation

If the baseline is green, recommend the smallest next task for Phase 1. Do not implement Phase 1 yet.

## Definition of done

Task 001 is complete only when we can truthfully state:

> On this WSL environment, stock ROS 2 Jazzy + Nav2 can execute a programmatically submitted navigation goal against the loopback simulator without a physical robot, and the exact reproducible procedure is documented.

If this cannot be established, the task is still useful: report the specific blocker with evidence rather than masking it.
