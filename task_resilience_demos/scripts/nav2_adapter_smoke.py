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
Task 015 smoke example: prepare, authorize, send, and recover across a GENUINE process exit.

The fake rclpy.action.ActionServer (task_resilience_ros/test/fake_nav2_server.py) runs in
THIS controller process and stays alive throughout -- no physical robot, no Nav2 launch
file, no shared server. The send and the recovery query, however, each run in their OWN
separate OS process (ordinary subprocess execution, reusing task_resilience_ros/test/
_nav2_adapter_subprocess.py's small sender/query roles): the sender process acquires
ownership, prepares, authorizes, sends, and EXITS; only afterward does a genuinely separate
query process start, reopen the database on its own, and recover the result. This is an
actual process restart, not same-process object recreation -- see docs/nav2_adapter.md's
"Running the smoke example" section for the plain command-by-command equivalent.

This script is TEST/DEMO fixture code, exactly like Task 015's own pytest fixtures: the
subprocess roles it reuses explicitly prepare and authorize one dispatch through
task_resilience's real public API, because Nav2Adapter itself is never allowed to do that.
Neither this script nor the roles it reuses are a supervisor, and must not grow into one.

Prerequisites: a sourced ROS 2 Jazzy environment, and task_resilience/task_resilience_ros
importable -- either source a colcon workspace that has built both packages, or run this
from a source checkout with both package directories added to PYTHONPATH (this script adds
its own repository's copies automatically, so a plain

    source /opt/ros/jazzy/setup.bash
    python3 task_resilience_demos/scripts/nav2_adapter_smoke.py

from a repository checkout works without a prior colcon build).
"""

import argparse
import os
import subprocess
import sys
import tempfile

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_HERE, '..', '..'))
_PACKAGE_DIRS = {
    'task_resilience': os.path.join(_REPO_ROOT, 'task_resilience'),
    'task_resilience_ros': os.path.join(_REPO_ROOT, 'task_resilience_ros'),
    'task_resilience_ros/test': os.path.join(_REPO_ROOT, 'task_resilience_ros', 'test'),
}
for _path in _PACKAGE_DIRS.values():
    if _path not in sys.path:
        sys.path.insert(0, _path)

try:
    import rclpy
except ImportError:
    print('ERROR: rclpy is not importable. Source a ROS 2 Jazzy environment first, e.g.:\n'
          '  source /opt/ros/jazzy/setup.bash', file=sys.stderr)
    raise SystemExit(2)

from fake_nav2_server import FakeNav2Server, SpinningNode, unique_action_name  # noqa: E402

from task_resilience.model import ExpectedGoalSpecification  # noqa: E402
from task_resilience.reconciliation import ExpectedActionContext  # noqa: E402
from task_resilience.storage import MissionStore  # noqa: E402

from task_resilience_ros.execution_owner import ExecutionOwner  # noqa: E402
from task_resilience_ros.nav2_adapter import SUPPORTED_ACTION_TYPE  # noqa: E402
from task_resilience_ros.nav2_goal_v1 import GOAL_FORMAT  # noqa: E402

SUBPROCESS_HELPER = os.path.join(
    _REPO_ROOT, 'task_resilience_ros', 'test', '_nav2_adapter_subprocess.py')

GOAL_PAYLOAD = {
    'frame_id': 'map',
    'position': {'x': 1.0, 'y': 0.5, 'z': 0.0},
    'orientation': {'x': 0.0, 'y': 0.0, 'z': 0.0, 'w': 1.0},
    'behavior_tree': '',
}


def parse_args(argv):
    p = argparse.ArgumentParser(description=__doc__.strip().split('\n')[0])
    p.add_argument('--reject', action='store_true',
                    help='make the fake server reject the goal instead of accepting it')
    return p.parse_args(argv)


def say(text):
    print(text, flush=True)


def _subprocess_env():
    env = dict(os.environ)
    parts = [_PACKAGE_DIRS['task_resilience_ros'], _PACKAGE_DIRS['task_resilience']]
    existing = env.get('PYTHONPATH', '')
    if existing:
        parts.append(existing)
    env['PYTHONPATH'] = os.pathsep.join(parts)
    return env


def run_subprocess_role(role_args, timeout_s=30.0):
    proc = subprocess.Popen(
        [sys.executable, SUBPROCESS_HELPER, *role_args],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=_subprocess_env())
    try:
        stdout, stderr = proc.communicate(timeout=timeout_s)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.communicate(timeout=5)
        raise SystemExit(f'subprocess {role_args} did not complete within {timeout_s}s')
    if proc.returncode != 0:
        raise SystemExit(
            f'subprocess {role_args} exited {proc.returncode}:\n{stdout}\n{stderr}')
    return proc.pid, stdout


def parse_lines(stdout):
    parsed = {}
    for line in stdout.strip().splitlines():
        parts = line.split(' ', 1)
        parsed[parts[0]] = parts[1] if len(parts) > 1 else ''
    return parsed


def main(argv=None):
    args = parse_args(argv)
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, 'mission.db')

        rclpy.init()
        try:
            server_node = SpinningNode('nav2_adapter_smoke_fake_server')
            action_name = unique_action_name('nav2_adapter_smoke')
            server = FakeNav2Server(server_node.node, action_name)
            server.set_accept(not args.reject)
            try:
                say('--- 1/2. SENDER subprocess: prepare, authorize, and send -- then exit ---')
                say(f'(this controller process is pid {os.getpid()})')
                sender_pid, sender_out = run_subprocess_role(
                    ['sender', db_path, action_name, 'smoke-req-1', 'navigate'])
                sender_lines = parse_lines(sender_out)
                mission_id = sender_lines['MISSION-ID']
                say(f'sender subprocess pid {sender_pid} (a REAL, separate OS process) '
                    f'has already exited')
                say(f'  MISSION-ID {mission_id}')
                say(f'  DISPATCH-ID {sender_lines["DISPATCH-ID"]}')
                say(f'  SEND-OUTCOME {sender_lines["SEND-OUTCOME"]}')
                say(f'fake server goal_request_count={server.goal_request_count} '
                    f'send_count={server.send_count}')

                say('\n--- 3/4. QUERY subprocess: a genuinely separate process recovers the '
                    'result ---')
                query_pid, query_out = run_subprocess_role(['query', db_path, mission_id, 'navigate'])
                query_lines = parse_lines(query_out)
                say(f'query subprocess pid {query_pid} (distinct from both the sender pid '
                    f'{sender_pid} and this controller pid {os.getpid()}) -- '
                    'it reopened the database on its own, with NO SendAuthorization and NO '
                    'goal handle, and never sent anything')
                say(f'  {query_lines["RESULT"]}')
                say(f'fake server send_count is still {server.send_count} (no resend occurred)')

                say('\n--- 5. an unrecognized dispatch is reported distinctly, never a resend ---')
                say('(prepared in this controller process, briefly re-acquiring ownership '
                    'now that the sender subprocess has already released its own)')
                owner = ExecutionOwner(db_path)
                owner.acquire()
                try:
                    store = MissionStore(owner.canonical_db_path)
                    try:
                        mission2 = store.create_or_get('smoke-req-2', {'demo': True}, 3)
                        store.initialize_steps(mission2.mission_id, ['never-sent'])
                        store.prepare_step_dispatch(
                            mission2.mission_id, 'never-sent',
                            ExpectedActionContext(
                                action_name=action_name, action_type=SUPPORTED_ACTION_TYPE,
                                robot_namespace=''),
                            ExpectedGoalSpecification(GOAL_FORMAT, GOAL_PAYLOAD))
                        never_sent_mission_id = mission2.mission_id
                    finally:
                        store.close()
                finally:
                    owner.close()
                _, unknown_out = run_subprocess_role(
                    ['query', db_path, never_sent_mission_id, 'never-sent'])
                say(f'  {parse_lines(unknown_out)["RESULT"]}')
                say(f'fake server send_count is still {server.send_count} (no resend occurred)')
            finally:
                server.destroy()
                server_node.stop()
        finally:
            rclpy.try_shutdown()
    return 0


if __name__ == '__main__':
    sys.exit(main())
