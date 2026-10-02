#!/usr/bin/env python3
# Copyright 2026 Dennis Lanov
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Task 016 demo runner: the minimal supervisor against the REAL Nav2 loopback baseline.

EXPERIMENTAL DEVELOPER PREVIEW. This is a thin wrapper around
task_resilience_ros.supervisor_cli.main -- it adds no supervisor logic of its own. Its only
job is to (a) make task_resilience/task_resilience_ros importable from a source checkout
without a prior colcon build, exactly like nav2_adapter_smoke.py already does, and (b) default
every argument to the committed hardware-free Nav2 loopback baseline's own verified-free poses
(docs/hardware_free_demo.md), so the ordinary demo invocation needs no coordinates at all.

Prerequisites, in order:

1. A sourced ROS 2 Jazzy environment with nav2_bringup/nav2_loopback_sim installed (see
   docs/hardware_free_demo.md).
2. The loopback baseline running and activated in ANOTHER terminal:

       ./task_resilience_demos/scripts/run_loopback_baseline.sh
       # then, in a second terminal, within ~60s:
       ros2 topic pub --once /initialpose geometry_msgs/msg/PoseWithCovarianceStamped \\
         '{header: {frame_id: map}, pose: {pose: {position: {x: -2.0, y: -0.5, z: 0.0}, \\
         orientation: {x: 0.0, y: 0.0, z: 0.0, w: 1.0}}}}'
       # wait for "[lifecycle_manager_navigation]: Managed nodes are active"

3. Then, in a third terminal, with a unique, persistent trial directory (never a shared or
   reused database path, and never `rm -f`'d mid-trial -- see docs/task016_supervisor.md):

       source /opt/ros/jazzy/setup.bash
       TRIAL_DIR=".local-artifacts/task016/runs/manual-$(date +%Y%m%dT%H%M%S)"
       mkdir -p "$TRIAL_DIR"
       python3 task_resilience_demos/scripts/task016_loopback_demo.py \
         run --db "$TRIAL_DIR/mission.db"

See docs/task016_supervisor.md for the full runbook, including the interrupt/resume
demonstration, the exit codes, and what each reported OUTCOME means -- in particular, that
`resume` is not guaranteed to reach COMPLETED (an UNRESOLVED result can legitimately persist
if the action server no longer retains the goal's result), and that the total polling time
bound includes service-discovery waits, not just `--poll-attempts` x `--poll-timeout`.
"""

import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_HERE, '..', '..'))
for _pkg in ('task_resilience', 'task_resilience_ros'):
    _path = os.path.join(_REPO_ROOT, _pkg)
    if _path not in sys.path:
        sys.path.insert(0, _path)

try:
    import rclpy  # noqa: F401
except ImportError:
    print('ERROR: rclpy is not importable. Source a ROS 2 Jazzy environment first, e.g.:\n'
          '  source /opt/ros/jazzy/setup.bash', file=sys.stderr)
    raise SystemExit(2)

from task_resilience_ros.supervisor_cli import main  # noqa: E402

if __name__ == '__main__':
    sys.exit(main())
