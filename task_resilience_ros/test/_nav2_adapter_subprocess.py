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
Private, test-only subprocess roles for the genuine fresh-process recovery scenario.

Not a test module itself (the leading underscore keeps pytest from collecting it) and not
installed runtime code: invoked only as `python3 _nav2_adapter_subprocess.py <role> ...` via
plain subprocess.Popen -- ordinary process execution, never os.fork(), never any of Task
014's process harness. Each role runs to completion and exits; there is no long-lived
command protocol here (unlike Task 014's own helper), since both roles are one-shot.

Two roles:
  sender: acquires execution ownership, opens MissionStore through owner.canonical_db_path,
    prepares and authorizes one dispatch through the core's real public API, sends it once
    through Nav2Adapter, and exits. Prints MISSION-ID/DISPATCH-ID/SEND-OUTCOME lines.
  query: reopens the SAME database file directly (no ExecutionOwner -- a read-only recovery
    query needs none) with a fresh MissionStore, and asks a fresh Nav2Adapter for the result
    via query_result(). Receives no SendAuthorization and no goal handle: only db_path,
    mission_id and step_key. Prints a RESULT-* line.

Every message is one complete, flushed, newline-terminated line, in the same spirit as
_execution_owner_process.py's own protocol, so the calling test can read them with a
straightforward bounded readline loop.
"""

import sys

import rclpy

from task_resilience.model import ExpectedGoalSpecification
from task_resilience.reconciliation import ExpectedActionContext
from task_resilience.state_machine import MissionState
from task_resilience.storage import MissionStore

from task_resilience_ros.execution_owner import ExecutionOwner
from task_resilience_ros.nav2_adapter import Nav2Adapter, SUPPORTED_ACTION_TYPE
from task_resilience_ros.nav2_goal_v1 import GOAL_FORMAT

GOAL_PAYLOAD = {
    'frame_id': 'map',
    'position': {'x': 1.5, 'y': -2.5, 'z': 0.0},
    'orientation': {'x': 0.0, 'y': 0.0, 'z': 0.0, 'w': 1.0},
    'behavior_tree': '',
}


def _say(*parts: object) -> None:
    print(*parts, flush=True)


def role_sender(
    db_path: str, action_name: str, request_id: str, step_key: str,
    robot_namespace: str = '',
) -> int:
    owner = ExecutionOwner(db_path)
    owner.acquire()
    try:
        store = MissionStore(owner.canonical_db_path)
        try:
            mission = store.create_or_get(request_id, {'demo': True}, 3)
            store.initialize_steps(mission.mission_id, [step_key])
            action = ExpectedActionContext(
                action_name=action_name, action_type=SUPPORTED_ACTION_TYPE,
                robot_namespace=robot_namespace)
            goal = ExpectedGoalSpecification(GOAL_FORMAT, GOAL_PAYLOAD)
            store.prepare_step_dispatch(mission.mission_id, step_key, action, goal)
            store.transition_mission_state(mission.mission_id, MissionState.RUNNING)
            authorization = store.authorize_step_send(mission.mission_id, step_key)
            dispatch = store.get_step_dispatch(mission.mission_id, step_key)
            _say('MISSION-ID', mission.mission_id)
            _say('DISPATCH-ID', dispatch.dispatch_id)

            rclpy.init()
            try:
                node = rclpy.create_node('nav2_adapter_subprocess_sender')
                adapter = Nav2Adapter(node)
                try:
                    outcome = adapter.send(
                        owner=owner, store=store, authorization=authorization,
                        mission_id=mission.mission_id, step_key=step_key)
                    _say('SEND-OUTCOME', outcome.accepted, outcome.reason)
                finally:
                    adapter.close()
                    node.destroy_node()
            finally:
                rclpy.try_shutdown()
        finally:
            store.close()
    finally:
        owner.close()
    return 0


def role_query(db_path: str, mission_id: str, step_key: str) -> int:
    # Deliberately NOT an ExecutionOwner: a read-only recovery query needs none, and this
    # role receives no SendAuthorization and no goal handle at all -- only the identity
    # needed to reload the persisted dispatch context from storage.
    store = MissionStore(db_path)
    try:
        rclpy.init()
        try:
            node = rclpy.create_node('nav2_adapter_subprocess_query')
            adapter = Nav2Adapter(node)
            try:
                observation = adapter.query_result(
                    store=store, mission_id=mission_id, step_key=step_key)
                _say(
                    'RESULT', observation.kind.name,
                    observation.status.name if observation.status else 'NONE',
                    observation.result_code, repr(observation.result_message))
            finally:
                adapter.close()
                node.destroy_node()
        finally:
            rclpy.try_shutdown()
    finally:
        store.close()
    return 0


def main() -> int:
    role = sys.argv[1]
    args = sys.argv[2:]
    roles = {'sender': role_sender, 'query': role_query}
    if role not in roles:
        raise SystemExit(f'unknown role: {role!r}')
    return roles[role](*args)


if __name__ == '__main__':
    raise SystemExit(main())
