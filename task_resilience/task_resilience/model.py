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

from collections.abc import Iterable
from dataclasses import dataclass, field, InitVar
from datetime import datetime, timedelta
import hashlib
import json
import math
from typing import Any
import uuid

from .errors import InvalidRequest
from .state_machine import MissionState, StepState


def _check_json(value: Any, path: str, active: set[int]) -> None:
    # Exact types on purpose: a str/int/dict subclass would be silently
    # flattened to a plain JSON value on its way to the database.
    kind = type(value)
    if value is None or kind in (bool, int, str):
        return
    if kind is float:
        if not math.isfinite(value):
            raise InvalidRequest(f'{path} is {value!r}, which JSON cannot represent')
        return
    if kind not in (list, dict):
        raise InvalidRequest(f'{path} has type {kind.__name__}, which is not a JSON type')

    # A container that contains itself never terminates; one that merely
    # appears twice is fine. So track only the containers on the current path.
    if id(value) in active:
        raise InvalidRequest(f'{path} contains itself')
    active.add(id(value))
    if kind is list:
        for index, item in enumerate(value):
            _check_json(item, f'{path}[{index}]', active)
    else:
        for key, item in value.items():
            if type(key) is not str:
                raise InvalidRequest(f'{path} has non-string key {key!r}')
            _check_json(item, f'{path}[{key!r}]', active)
    active.remove(id(value))


def _dumps(value: Any) -> str:
    # Sorted keys, no insignificant whitespace, ASCII-only escapes: the text
    # depends only on the data.
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


@dataclass(frozen=True)
class MissionRequest:
    """A validated request, reduced to its canonical form. The request ID is the lookup key."""

    request_id: str
    # The payload is not kept: a caller-owned object could change after
    # validation and stop matching payload_json and fingerprint.
    payload: InitVar[Any]
    max_attempts: int
    payload_json: str = field(init=False, repr=False)
    fingerprint: str = field(init=False)

    def __post_init__(self, payload: Any) -> None:
        if not isinstance(self.request_id, str) or not self.request_id:
            raise InvalidRequest('request_id must be a non-empty string')
        # bool is an int subclass, but True is not a meaningful attempt limit.
        if (isinstance(self.max_attempts, bool) or not isinstance(self.max_attempts, int)
                or self.max_attempts <= 0):
            raise InvalidRequest(
                f'max_attempts must be a positive integer, got {self.max_attempts!r}')

        try:
            _check_json(payload, 'payload', set())
            payload_json = _dumps(payload)
            # max_attempts is part of what was accepted: replaying with a larger
            # budget is a different request, not a way to top the budget up.
            accepted = _dumps({'max_attempts': self.max_attempts, 'payload': payload})
        except InvalidRequest:
            raise
        except (RecursionError, ValueError) as exc:
            # Nesting past the interpreter's limit, or an int with too many digits.
            raise InvalidRequest(f'payload cannot be serialised: {exc}') from exc

        object.__setattr__(self, 'payload_json', payload_json)
        object.__setattr__(
            self, 'fingerprint', hashlib.sha256(accepted.encode('utf-8')).hexdigest())


@dataclass(frozen=True)
class MissionRecord:
    """A mission as stored. A snapshot: it does not track later changes in the store."""

    mission_id: str
    request_id: str
    request_fingerprint: str
    payload: Any
    max_attempts: int
    attempts_used: int
    state: MissionState
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class StepRecord:
    """A step as stored. A snapshot, like MissionRecord."""

    mission_id: str
    step_key: str
    step_index: int
    state: StepState
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class DispatchRecord:
    """The durable identity of a step's external dispatch."""

    dispatch_id: uuid.UUID
    mission_id: str
    step_key: str
    created_at: datetime


@dataclass(frozen=True)
class DispatchContextRecord:
    """
    The action context bound to a dispatch, as stored.

    It is what was declared when the dispatch was prepared: the action its request was
    meant to reach. It is neither an authorization nor proof that a request was ever sent
    there. The three strings are exact and never normalized.
    """

    dispatch_id: uuid.UUID
    mission_id: str
    step_key: str
    action_name: str
    action_type: str
    robot_namespace: str

    def __post_init__(self) -> None:
        if not isinstance(self.dispatch_id, uuid.UUID):
            raise TypeError(f'dispatch_id must be a uuid.UUID, got {self.dispatch_id!r}')
        for name in ('mission_id', 'step_key', 'action_name', 'action_type', 'robot_namespace'):
            value = getattr(self, name)
            if not isinstance(value, str):
                raise TypeError(f'{name} must be a str, got {value!r}')
        if not self.mission_id or not self.step_key:
            raise ValueError('mission_id and step_key must not be empty')
        # The namespace is different: an empty string is an exact value.
        if not self.action_name or not self.action_type:
            raise ValueError('action_name and action_type must be non-empty')


@dataclass(frozen=True)
class SendAuthorization:
    """
    The receipt of a send authorization that has been committed.

    Returned once, to the one caller whose authorization committed. It is not stored, and
    no API accepts it: holding one, or building one, gives no permission. It tells that
    caller which dispatch_id to use and which attempt this was. The external request may
    or may not be sent afterwards, and nothing here says it was.
    """

    mission_id: str
    step_key: str
    dispatch_id: uuid.UUID
    # The mission's attempts_used after this authorization, so counted from 1.
    attempt_number: int
    max_attempts: int
    authorized_at: datetime

    def __post_init__(self) -> None:
        if not isinstance(self.mission_id, str) or not isinstance(self.step_key, str):
            raise TypeError('mission_id and step_key must be strings')
        if not self.mission_id or not self.step_key:
            raise ValueError('mission_id and step_key must not be empty')
        if not isinstance(self.dispatch_id, uuid.UUID):
            raise TypeError(f'dispatch_id must be a uuid.UUID, got {self.dispatch_id!r}')
        for name in ('attempt_number', 'max_attempts'):
            value = getattr(self, name)
            # bool is an int subclass, but True is not a count.
            if isinstance(value, bool) or not isinstance(value, int):
                raise TypeError(f'{name} must be an int, got {value!r}')
        if not 1 <= self.attempt_number <= self.max_attempts:
            raise ValueError(
                f'attempt_number {self.attempt_number} is not within 1..{self.max_attempts}')
        if not isinstance(self.authorized_at, datetime):
            raise TypeError(f'authorized_at must be a datetime, got {self.authorized_at!r}')
        if self.authorized_at.utcoffset() != timedelta(0):
            raise ValueError('authorized_at must be a timezone-aware UTC datetime')


def validate_step_keys(step_keys: Iterable[str]) -> tuple[str, ...]:
    # A bare string is iterable and would silently become one step per character.
    if isinstance(step_keys, (str, bytes)):
        raise InvalidRequest('step_keys must be a sequence of keys, not a single string')
    try:
        keys = tuple(step_keys)
    except TypeError as exc:
        raise InvalidRequest('step_keys must be an iterable of keys') from exc

    if not keys:
        raise InvalidRequest('a mission needs at least one step')
    for key in keys:
        if type(key) is not str or not key:
            raise InvalidRequest(f'step keys must be non-empty strings, got {key!r}')
    if len(set(keys)) != len(keys):
        raise InvalidRequest('step keys must be unique within a mission')
    return keys
