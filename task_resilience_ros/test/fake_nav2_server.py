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
A small, deterministic, test-owned NavigateToPose action server.

Test/demo-only: never a production adapter dependency, and never imported by
nav2_adapter.py or anything under task_resilience_ros/task_resilience_ros/. Runs a real
rclpy.action.ActionServer, spun continuously by a dedicated background thread on its own
node, so a test's client-side calls (bounded, synchronous, on a SEPARATE node) can be
answered concurrently without any custom process/signal machinery -- this is the standard
rclpy pattern for an in-process fake server, not a rebuild of Task 014's process harness.

Every test using this fixture must give it a unique action_name (a fresh uuid4 suffix is
enough) so tests never share a server or collide against a previous test's still-draining
ROS graph state.
"""

import threading
import time
import uuid

from action_msgs.msg import GoalStatus
from nav2_msgs.action import NavigateToPose
import rclpy
from rclpy.action import ActionServer
from rclpy.action.server import CancelResponse, GoalResponse
from rclpy.executors import SingleThreadedExecutor


class FakeNav2Server:
    """
    A controllable fake for exactly one NavigateToPose action.

    All the knobs (accept policy, delays, canned per-goal terminal results) are plain
    Python attributes/methods set BEFORE the relevant request arrives; there is no
    thread-safety attempt beyond a single lock guarding the small bits of shared state,
    which is enough for the sequential, one-request-at-a-time scenarios this fixture is
    built for.
    """

    def __init__(self, node, action_name: str) -> None:
        self.node = node
        self.action_name = action_name
        self.received: list[tuple[uuid.UUID, object]] = []  # (goal uuid, Goal request)
        # goal_request_count: every SendGoal request the goal_callback ever saw, accepted or
        # rejected. send_count (below): only goals that reached _execute -- i.e. were
        # accepted. A rejected request bumps goal_request_count but never send_count, so
        # send_count alone cannot tell "no request arrived" apart from "one arrived and was
        # rejected" -- goal_request_count is what a test needs for that distinction.
        self.goal_request_count = 0
        self.send_count = 0
        self._lock = threading.Lock()
        self._accept = True
        self._accept_delay_s = 0.0
        self._result_delay_s = 0.0
        # dispatch uuid (str) -> (status, error_code, error_msg); default: SUCCEEDED, 0, ''
        self._planned_results: dict[str, tuple[int, int, str]] = {}
        self._default_result = (GoalStatus.STATUS_SUCCEEDED, 0, '')
        self._server = ActionServer(
            node, NavigateToPose, action_name,
            execute_callback=self._execute,
            goal_callback=self._goal_callback,
            cancel_callback=lambda goal_handle: CancelResponse.ACCEPT,
        )

    # --- test-side configuration -------------------------------------------------

    def set_accept(self, accept: bool) -> None:
        with self._lock:
            self._accept = accept

    def set_accept_delay(self, seconds: float) -> None:
        with self._lock:
            self._accept_delay_s = seconds

    def set_result_delay(self, seconds: float) -> None:
        with self._lock:
            self._result_delay_s = seconds

    def plan_result(
        self, dispatch_uuid: uuid.UUID, status: int, error_code: int = 0, error_msg: str = '',
    ) -> None:
        """Plan the terminal status/result this server gives for one specific goal UUID."""
        with self._lock:
            self._planned_results[str(dispatch_uuid)] = (status, error_code, error_msg)

    # --- ActionServer callbacks ----------------------------------------------------

    def _goal_callback(self, goal_request):
        with self._lock:
            self.goal_request_count += 1
            delay = self._accept_delay_s
            accept = self._accept
        if delay:
            time.sleep(delay)
        return GoalResponse.ACCEPT if accept else GoalResponse.REJECT

    def _execute(self, goal_handle):
        goal_uuid = uuid.UUID(bytes=bytes(goal_handle.goal_id.uuid))
        with self._lock:
            self.received.append((goal_uuid, goal_handle.request))
            self.send_count += 1
            delay = self._result_delay_s
            status, error_code, error_msg = self._planned_results.get(
                str(goal_uuid), self._default_result)
        if delay:
            time.sleep(delay)

        result = NavigateToPose.Result()
        result.error_code = error_code
        result.error_msg = error_msg
        if status == GoalStatus.STATUS_SUCCEEDED:
            goal_handle.succeed()
        elif status == GoalStatus.STATUS_ABORTED:
            goal_handle.abort()
        elif status == GoalStatus.STATUS_CANCELED:
            # A mapping-level fixture, per Task 015's own scope: the underlying goal state
            # machine requires CANCEL_GOAL (EXECUTING -> CANCELING) before CANCELED is a
            # valid transition, so that internal event is injected directly here -- no real
            # cancel request is sent or received, and no cancel-request plumbing is
            # implemented; this only reaches the same terminal server-side call
            # (goal_handle.canceled()) a real cancel flow would also end in, so the
            # ADAPTER's own status mapping can be tested.
            from rclpy._rclpy_pybind11 import GoalEvent
            goal_handle._update_state(GoalEvent.CANCEL_GOAL)
            goal_handle.canceled()
        else:
            raise ValueError(f'unsupported planned status for the fake server: {status}')
        return result

    def destroy(self) -> None:
        self._server.destroy()


class SpinningNode:
    """
    A real rclpy.Node, continuously spun on a dedicated background thread.

    Used for BOTH the fake server's own node (so it can answer requests while a test's
    client-side calls block the main thread) and, where a test wants one, a second,
    independent client node -- e.g. the fresh-client recovery test, which must prove a
    NEW process-like client (here: a new node with no relation to the original) can reach
    the still-live fake server. Bounded, deterministic teardown: stop() joins the thread
    with a timeout and raises loudly if it does not end in time, rather than leaking it.
    """

    def __init__(self, node_name: str) -> None:
        self.node = rclpy.create_node(node_name)
        self._executor = SingleThreadedExecutor()
        self._executor.add_node(self.node)
        self._thread = threading.Thread(target=self._executor.spin, daemon=True)
        self._thread.start()

    def stop(self, timeout_s: float = 5.0) -> None:
        self._executor.shutdown(timeout_sec=timeout_s)
        self._thread.join(timeout=timeout_s)
        if self._thread.is_alive():
            raise AssertionError('SpinningNode executor thread did not stop within timeout')
        self._executor.remove_node(self.node)
        self.node.destroy_node()


def unique_action_name(prefix: str = 'fake_nav2') -> str:
    return f'/{prefix}_{uuid.uuid4().hex}/navigate_to_pose'
