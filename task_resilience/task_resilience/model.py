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

from dataclasses import dataclass, field, InitVar
from datetime import datetime
import hashlib
import json
import math
from typing import Any

from .errors import InvalidRequest


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
    created_at: datetime
    updated_at: datetime
