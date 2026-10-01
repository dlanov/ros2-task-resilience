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


class ExecutionOwnerAlreadyHeld(Exception):
    """Another process or instance already holds this execution-owner lock."""

    def __init__(self, canonical_db_path: str, lock_path: str) -> None:
        self.canonical_db_path = canonical_db_path
        self.lock_path = lock_path
        super().__init__(
            f'execution owner lock already held for {canonical_db_path!r} '
            f'(lock file {lock_path!r})')


class OwnerNotHeld(Exception):
    """The adapter's new-send contract requires a currently held ExecutionOwner."""

    def __init__(self, mission_id: str, step_key: str) -> None:
        super().__init__(
            f'nav2_adapter send for {mission_id!r}/{step_key!r} requires a currently '
            'held ExecutionOwner; owner.acquired is False')
        self.mission_id = mission_id
        self.step_key = step_key


class AuthorizationMismatch(Exception):
    """The supplied SendAuthorization does not identify the requested dispatch."""

    def __init__(self, mission_id: str, step_key: str, dispatch_id: object, reason: str) -> None:
        super().__init__(
            f'authorization does not match dispatch of {mission_id!r}/{step_key!r} '
            f'(dispatch_id={dispatch_id!r}): {reason}')
        self.mission_id = mission_id
        self.step_key = step_key
        self.dispatch_id = dispatch_id
        self.reason = reason


class UnsupportedGoalFormat(Exception):
    """The stored dispatch goal's goal_format is not one this adapter implements."""

    def __init__(self, goal_format: str, supported: tuple[str, ...]) -> None:
        super().__init__(
            f'goal_format {goal_format!r} is not supported by this adapter; '
            f'supported: {list(supported)}')
        self.goal_format = goal_format
        self.supported = supported


class InvalidGoalPayload(Exception):
    """The stored goal payload does not satisfy its declared format's exact schema."""

    def __init__(self, goal_format: str, reason: str) -> None:
        super().__init__(f'goal payload for format {goal_format!r} is invalid: {reason}')
        self.goal_format = goal_format
        self.reason = reason


class ActionEndpointMismatch(Exception):
    """The stored action_name/robot_namespace/action_type cannot be resolved or supported."""

    def __init__(
        self, action_name: str, action_type: str, robot_namespace: str, reason: str,
    ) -> None:
        super().__init__(
            f'cannot resolve action endpoint (action_name={action_name!r}, '
            f'action_type={action_type!r}, robot_namespace={robot_namespace!r}): {reason}')
        self.action_name = action_name
        self.action_type = action_type
        self.robot_namespace = robot_namespace
        self.reason = reason


class AdapterSendAlreadyAttempted(Exception):
    """
    This adapter instance already attempted a send for this dispatch.

    A local, in-process courtesy guard only -- see nav2_adapter.py's module docstring and
    docs/nav2_adapter.md. It is not durable exactly-once enforcement and not an unforgeable
    capability: a new adapter instance, or a new process, has no memory of this.
    """

    def __init__(self, dispatch_id: object) -> None:
        super().__init__(
            f'dispatch {dispatch_id!r} was already attempted through this adapter instance; '
            'a repeat local submission is refused, not retried')
        self.dispatch_id = dispatch_id
