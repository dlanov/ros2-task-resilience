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

import enum
import uuid


class InvalidRequest(ValueError):
    """The request cannot be accepted as submitted."""


class IdempotencyConflict(Exception):
    """A request ID was replayed with different contents."""

    def __init__(self, request_id: str) -> None:
        super().__init__(
            f'request_id {request_id!r} was already accepted with different contents')
        self.request_id = request_id


class MissionNotFound(Exception):
    """No mission exists for a mission ID or request ID."""

    def __init__(self, identifier: str) -> None:
        super().__init__(f'no mission for {identifier!r}')
        self.identifier = identifier


class AttemptLimitReached(Exception):
    """Every accepted attempt for the mission has already been used."""

    def __init__(self, mission_id: str, max_attempts: int) -> None:
        super().__init__(f'mission {mission_id!r} has used all {max_attempts} attempts')
        self.mission_id = mission_id
        self.max_attempts = max_attempts


class UnsupportedSchemaVersion(Exception):
    """The database was written by a schema version this code does not understand."""

    def __init__(self, version: int) -> None:
        super().__init__(f'unsupported database schema version {version}')
        self.version = version


class InvalidStateTransition(Exception):
    """A state change is not one of the allowed ordinary transitions."""

    def __init__(self, entity: str, current: enum.Enum, target: enum.Enum) -> None:
        super().__init__(f'{entity} cannot go from {current.value} to {target.value}')
        self.entity = entity
        self.current = current
        self.target = target


class MissionCompletionInvariantError(Exception):
    """A mission cannot complete unless it has steps and every one has succeeded."""

    def __init__(self, current: enum.Enum, step_states: tuple[enum.Enum, ...]) -> None:
        seen = ', '.join(state.value for state in step_states) or 'none'
        super().__init__(f'mission cannot complete from {current.value}; step states: {seen}')
        self.current = current
        self.step_states = step_states


class StepDefinitionConflict(Exception):
    """A mission's steps were already defined, and the requested keys differ."""

    def __init__(
        self,
        mission_id: str,
        existing_keys: tuple[str, ...],
        requested_keys: tuple[str, ...],
    ) -> None:
        super().__init__(
            f'mission {mission_id!r} already has steps {list(existing_keys)}, '
            f'not {list(requested_keys)}')
        self.mission_id = mission_id
        self.existing_keys = existing_keys
        self.requested_keys = requested_keys


class StepNotFound(Exception):
    """The mission exists but has no step with this key."""

    def __init__(self, mission_id: str, step_key: str) -> None:
        super().__init__(f'mission {mission_id!r} has no step {step_key!r}')
        self.mission_id = mission_id
        self.step_key = step_key


class CorruptStoredState(Exception):
    """The database holds a state value that is not a known state."""

    def __init__(self, entity: str, mission_id: str, value: object) -> None:
        super().__init__(f'{entity} of mission {mission_id!r} has unknown stored state {value!r}')
        self.entity = entity
        self.mission_id = mission_id
        self.value = value


class DispatchNotFound(Exception):
    """A step has no dispatch, or no dispatch has this ID."""

    # A lookup by step sets mission_id and step_key; a lookup by ID sets dispatch_id.
    def __init__(
        self,
        *,
        mission_id: str | None = None,
        step_key: str | None = None,
        dispatch_id: uuid.UUID | None = None,
    ) -> None:
        if dispatch_id is not None:
            super().__init__(f'no dispatch {dispatch_id}')
        else:
            super().__init__(f'step {step_key!r} of mission {mission_id!r} has no dispatch')
        self.mission_id = mission_id
        self.step_key = step_key
        self.dispatch_id = dispatch_id


class CorruptDispatchRecord(Exception):
    """A step's dispatch record is missing, duplicated, or unreadable for its state."""

    def __init__(
        self, mission_id: str, step_key: str, reason: str, value: object = None,
    ) -> None:
        super().__init__(f'dispatch of step {step_key!r} of mission {mission_id!r}: {reason}')
        self.mission_id = mission_id
        self.step_key = step_key
        self.reason = reason
        self.value = value


class DispatchPreparationRequired(Exception):
    """A step can enter DISPATCH_PREPARED only through prepare_step_dispatch."""

    def __init__(self, mission_id: str, step_key: str) -> None:
        super().__init__(
            f'step {step_key!r} of mission {mission_id!r} must be prepared with '
            'prepare_step_dispatch, not a plain state transition')
        self.mission_id = mission_id
        self.step_key = step_key


class SendAuthorizationRequired(Exception):
    """A step can enter DISPATCHED only through authorize_step_send."""

    def __init__(self, mission_id: str, step_key: str) -> None:
        super().__init__(
            f'step {step_key!r} of mission {mission_id!r} must be authorized with '
            'authorize_step_send, not a plain state transition')
        self.mission_id = mission_id
        self.step_key = step_key


class MissionNotRunning(Exception):
    """A send can be authorized only while the mission is RUNNING."""

    def __init__(self, mission_id: str, mission_state: enum.Enum) -> None:
        super().__init__(f'mission {mission_id!r} is {mission_state.value}, not RUNNING')
        self.mission_id = mission_id
        self.mission_state = mission_state


class MissionReconciliationRequired(Exception):
    """A step of the mission has an unknown outcome, so no new send is authorized."""

    def __init__(self, mission_id: str, step_keys: tuple[str, ...]) -> None:
        super().__init__(
            f'mission {mission_id!r} has steps with an unknown outcome: {list(step_keys)}')
        self.mission_id = mission_id
        self.step_keys = step_keys


class StepNotAwaitingSend(Exception):
    """Only a step that is DISPATCH_PREPARED can be authorized to send."""

    def __init__(self, mission_id: str, step_key: str, step_state: enum.Enum) -> None:
        super().__init__(
            f'step {step_key!r} of mission {mission_id!r} is {step_state.value}, '
            'not DISPATCH_PREPARED')
        self.mission_id = mission_id
        self.step_key = step_key
        self.step_state = step_state


class DispatchContextMissing(Exception):
    """A dispatch exists, but no durable action context is bound to it."""

    def __init__(self, mission_id: str, step_key: str) -> None:
        super().__init__(
            f'dispatch of step {step_key!r} of mission {mission_id!r} has no bound action context')
        self.mission_id = mission_id
        self.step_key = step_key


class DispatchContextConflict(Exception):
    """A dispatch is already bound to an action context, and the requested one differs."""

    def __init__(self, mission_id: str, step_key: str, fields: tuple[str, ...]) -> None:
        super().__init__(
            f'dispatch of step {step_key!r} of mission {mission_id!r} is already bound to a '
            f'different action context: {", ".join(fields)}')
        self.mission_id = mission_id
        self.step_key = step_key
        # The names of the fields that differ, never the values.
        self.fields = fields


class CorruptDispatchContext(Exception):
    """A dispatch's stored action context cannot be read as a valid context."""

    def __init__(
        self, mission_id: str, step_key: str, reason: str, value: object = None,
    ) -> None:
        super().__init__(
            f'action context of step {step_key!r} of mission {mission_id!r}: {reason}')
        self.mission_id = mission_id
        self.step_key = step_key
        self.reason = reason
        self.value = value
