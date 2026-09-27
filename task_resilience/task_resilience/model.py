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
import enum
import hashlib
import json
import math
from typing import Any
import uuid

from .errors import InvalidRequest
from .reconciliation import ActionStatus
from .state_machine import is_step_terminal, MissionState, StepState

# The most of a result message that is kept. The rest of it is represented by its length and its
# SHA-256, a compact content identity that is not a proof of uniqueness.
MESSAGE_PREFIX_MAX_BYTES = 1024

# The only result statuses that are a terminal outcome, and so the only ones recorded as evidence.
# This is a list of statuses, not a mapping: which step state a status means is Task 007's to say.
TERMINAL_RESULT_STATUSES = frozenset({
    ActionStatus.SUCCEEDED,
    ActionStatus.CANCELED,
    ActionStatus.ABORTED,
})


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
class ExpectedGoalSpecification:
    """
    The exact external goal a dispatch is declared to carry, reduced to its canonical form.

    Mirrors MissionRequest's own snapshot pattern: the caller's payload is not kept, only its
    canonical JSON text and a fingerprint over the COMPLETE specification (goal_format together
    with the payload, not the payload alone), so a later mutation of the caller's own dict or
    list cannot change what this object, or what gets durably bound from it, means. goal_format
    is opaque to the store: it names which adapter format/version the payload is meant for, and
    is never interpreted here.
    """

    goal_format: str
    # The payload is not kept: a caller-owned object could change after validation and stop
    # matching goal_json and goal_fingerprint.
    payload: InitVar[Any]
    goal_json: str = field(init=False, repr=False)
    goal_fingerprint: str = field(init=False)

    def __post_init__(self, payload: Any) -> None:
        if not isinstance(self.goal_format, str):
            raise TypeError(f'goal_format must be a str, got {self.goal_format!r}')
        if not self.goal_format:
            raise ValueError('goal_format must not be empty')

        try:
            _check_json(payload, 'payload', set())
            goal_json = _dumps(payload)
            # goal_format is part of what is bound: the same payload under a different format
            # is a different specification, not a way to relabel an existing one.
            complete = _dumps({'goal_format': self.goal_format, 'payload': payload})
        except InvalidRequest:
            raise
        except (RecursionError, ValueError) as exc:
            # Nesting past the interpreter's limit, or an int with too many digits.
            raise InvalidRequest(f'payload cannot be serialised: {exc}') from exc

        object.__setattr__(self, 'goal_json', goal_json)
        object.__setattr__(
            self, 'goal_fingerprint', hashlib.sha256(complete.encode('utf-8')).hexdigest())


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
class DispatchGoalRecord:
    """
    The goal specification bound to a dispatch, as stored.

    It is what was declared when the dispatch was prepared: the exact external request its
    action context is meant to carry. It is neither an authorization nor proof that anything
    was sent or executed. goal_format is opaque and never interpreted here. payload is a
    decoded snapshot of what is durably stored, in the same limited sense MissionRecord.payload
    is: a normal, mutable dict/list locally, but mutating it never writes through to SQLite, and
    it is never re-read from a caller's own object once this record exists.
    """

    dispatch_id: uuid.UUID
    mission_id: str
    step_key: str
    goal_format: str
    payload: Any
    goal_fingerprint: str

    def __post_init__(self) -> None:
        if not isinstance(self.dispatch_id, uuid.UUID):
            raise TypeError(f'dispatch_id must be a uuid.UUID, got {self.dispatch_id!r}')
        for name in ('mission_id', 'step_key', 'goal_format', 'goal_fingerprint'):
            value = getattr(self, name)
            if not isinstance(value, str):
                raise TypeError(f'{name} must be a str, got {value!r}')
        if not self.mission_id or not self.step_key:
            raise ValueError('mission_id and step_key must not be empty')
        if not self.goal_format:
            raise ValueError('goal_format must not be empty')


@dataclass(frozen=True)
class DispatchAuthorizationRecord:
    """
    Durable provenance, under the supported-store contract, of a committed send authorization.

    Under that contract a valid row is provenance that authorize_step_send committed
    authorization for this dispatch, and nothing but that operation creates one. It is not
    tamper-evident: a writer that bypasses MissionStore can forge or alter even a canonically
    formatted row, and such writes are outside the supported contract.

    That is all it says. It is not proof that send_goal was called, that a request left the
    process, that anything received or accepted it, or that a result exists, and it is not
    a permission: no API accepts it. authorized_at is the store's own wall-clock time for
    the authorization transaction, not a send, server or result time, and it says nothing
    about whether a server still retains a result. The attempt number is not kept.
    """

    dispatch_id: uuid.UUID
    mission_id: str
    step_key: str
    authorized_at: datetime

    def __post_init__(self) -> None:
        if not isinstance(self.dispatch_id, uuid.UUID):
            raise TypeError(f'dispatch_id must be a uuid.UUID, got {self.dispatch_id!r}')
        if not isinstance(self.mission_id, str) or not isinstance(self.step_key, str):
            raise TypeError('mission_id and step_key must be strings')
        if not self.mission_id or not self.step_key:
            raise ValueError('mission_id and step_key must not be empty')
        if not isinstance(self.authorized_at, datetime):
            raise TypeError(f'authorized_at must be a datetime, got {self.authorized_at!r}')
        if self.authorized_at.utcoffset() != timedelta(0):
            raise ValueError('authorized_at must be a timezone-aware UTC datetime')


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


@enum.unique
class EvidenceOutcome(enum.Enum):
    """What recording one piece of evidence did. The values are persisted: do not change them."""

    # The step was moved to the terminal state the result maps to.
    APPLIED = 'APPLIED'
    # The result matches the terminal state the step already had. Nothing changed.
    ALREADY_SETTLED = 'ALREADY_SETTLED'
    # The result contradicts the terminal state the step already had. Nothing changed.
    CONFLICT = 'CONFLICT'
    # The result could have settled the step, but the dispatch has no durable authorization
    # provenance, so it was withheld. That is not proof that no authorization happened.
    UNPROVEN_AUTHORIZATION = 'UNPROVEN_AUTHORIZATION'


# The step states a result can settle, and so the only ones a result can be APPLIED to. A prepared
# step has no authorization, and a step that is already terminal is not settled again.
_APPLICABLE_FROM = frozenset({
    StepState.DISPATCHED,
    StepState.ACCEPTED,
    StepState.EXECUTING,
    StepState.CANCEL_REQUESTED,
    StepState.OUTCOME_UNKNOWN,
})


@dataclass(frozen=True)
class RecoveryEvidenceRecord:
    """
    A terminal result response that was supplied to the store, as it was recorded.

    A row durably records a terminal RESULT_RESPONSE supplied to apply_recovery_observation and
    successfully correlated with this dispatch's durable action context. Under the future adapter
    contract, that observation is expected to come from an action-server result response.
    MissionStore itself does not authenticate or independently verify the observation's external
    origin. A row is not proof that the physical task happened, and it is not by itself a change
    to the step: outcome says what recording it did. The table is not tamper-evident: a writer
    that bypasses MissionStore can forge or alter even a well-formed row, and such writes are
    outside the supported contract.

    result_code is opaque and is never interpreted; 0 is not success. Only a bounded prefix of
    the message is kept, with its full length in bytes and the SHA-256 of the whole message. The
    digest is a compact content identity, used to distinguish message contents and to detect
    changed replay content. It is not authentication, tamper evidence, a proof of uniqueness or
    authorization provenance. recorded_at is this store's own wall-clock time, not a server or
    observation time.
    """

    evidence_id: uuid.UUID
    dispatch_id: uuid.UUID
    mission_id: str
    step_key: str
    observed_status: ActionStatus
    result_code: int | None
    result_message_prefix: str | None
    result_message_bytes: int | None
    result_message_sha256: bytes | None
    step_state_before: StepState
    outcome: EvidenceOutcome
    recorded_at: datetime

    def __post_init__(self) -> None:
        for name in ('evidence_id', 'dispatch_id'):
            if not isinstance(getattr(self, name), uuid.UUID):
                raise TypeError(f'{name} must be a uuid.UUID, got {getattr(self, name)!r}')
        if not isinstance(self.mission_id, str) or not isinstance(self.step_key, str):
            raise TypeError('mission_id and step_key must be strings')
        if not self.mission_id or not self.step_key:
            raise ValueError('mission_id and step_key must not be empty')
        if not isinstance(self.observed_status, ActionStatus):
            raise TypeError(
                f'observed_status must be an ActionStatus, got {self.observed_status!r}')
        if self.observed_status not in TERMINAL_RESULT_STATUSES:
            raise ValueError(f'{self.observed_status.name} is not a terminal result status')
        # bool is an int subclass, but True is not a result code.
        if self.result_code is not None and (
                isinstance(self.result_code, bool) or not isinstance(self.result_code, int)):
            raise TypeError(f'result_code must be an int or None, got {self.result_code!r}')
        self._check_message()
        if not isinstance(self.step_state_before, StepState):
            raise TypeError(
                f'step_state_before must be a StepState, got {self.step_state_before!r}')
        if self.step_state_before is StepState.PENDING:
            raise ValueError('a pending step has no dispatch, so there is nothing to record')
        if not isinstance(self.outcome, EvidenceOutcome):
            raise TypeError(f'outcome must be an EvidenceOutcome, got {self.outcome!r}')
        self._check_outcome()
        if not isinstance(self.recorded_at, datetime):
            raise TypeError(f'recorded_at must be a datetime, got {self.recorded_at!r}')
        if self.recorded_at.utcoffset() != timedelta(0):
            raise ValueError('recorded_at must be a timezone-aware UTC datetime')

    def _check_message(self) -> None:
        fields = (
            self.result_message_prefix, self.result_message_bytes, self.result_message_sha256)
        if all(field is None for field in fields):
            return
        if any(field is None for field in fields):
            raise ValueError('the result message prefix, size and SHA-256 are all set or all None')
        prefix, size, digest = fields
        if not isinstance(prefix, str):
            raise TypeError(f'result_message_prefix must be a str, got {prefix!r}')
        if isinstance(size, bool) or not isinstance(size, int):
            raise TypeError(f'result_message_bytes must be an int, got {size!r}')
        if not isinstance(digest, bytes):
            raise TypeError(f'result_message_sha256 must be bytes, got {digest!r}')
        if len(digest) != 32:
            raise ValueError('result_message_sha256 must be 32 bytes')
        kept = prefix.encode('utf-8', 'surrogatepass')
        if len(kept) > MESSAGE_PREFIX_MAX_BYTES:
            raise ValueError(f'the message prefix is more than {MESSAGE_PREFIX_MAX_BYTES} bytes')
        if size < len(kept):
            raise ValueError('the message is shorter than its own prefix')
        if size == len(kept):
            # Nothing was cut, so the prefix is the whole message and must hash to the digest.
            if hashlib.sha256(kept).digest() != digest:
                raise ValueError('the message SHA-256 does not match a message that was not cut')
        elif len(kept) <= MESSAGE_PREFIX_MAX_BYTES - 4:
            # A character is at most four bytes, so a cut prefix stops within three bytes of the
            # bound.
            raise ValueError('a prefix that was cut is shorter than the bound allows')

    def _check_outcome(self) -> None:
        before, outcome = self.step_state_before, self.outcome
        if outcome in (EvidenceOutcome.ALREADY_SETTLED, EvidenceOutcome.CONFLICT):
            if not is_step_terminal(before):
                raise ValueError(f'{outcome.name} needs a step that was already terminal')
        elif is_step_terminal(before):
            raise ValueError(f'{outcome.name} needs a step that was not terminal')
        if outcome is EvidenceOutcome.APPLIED and before not in _APPLICABLE_FROM:
            raise ValueError(f'a result cannot be applied to a step that was {before.value}')

    @property
    def result_message_truncated(self) -> bool:
        """Return whether the kept prefix is shorter than the message that was observed."""
        if self.result_message_prefix is None:
            return False
        return self.result_message_bytes > len(
            self.result_message_prefix.encode('utf-8', 'surrogatepass'))


@dataclass(frozen=True)
class MissionReconciliationContextRecord:
    """
    The mission state to restore once a NEEDS_RECONCILIATION mission can leave reconciliation.

    Durable active intent, not history, evidence, authorization or a permission. It exists while
    a mission that entered reconciliation through enter_mission_reconciliation is reconciling,
    and is removed when the reconciliation is resolved. It may be raised from RUNNING to
    CANCELLING while the mission stays blocked, and never the other way. It says nothing about
    the mission's current state, and CANCELLING here does not mean anything was cancelled. It has
    no timestamp: the mission's own updated_at is the time it entered. The table it comes from
    is not tamper-evident, and a writer that bypasses MissionStore can alter it.
    """

    mission_id: str
    resume_state: MissionState

    def __post_init__(self) -> None:
        if not isinstance(self.mission_id, str):
            raise TypeError(f'mission_id must be a str, got {self.mission_id!r}')
        if not self.mission_id:
            raise ValueError('mission_id must not be empty')
        if not isinstance(self.resume_state, MissionState):
            raise TypeError(f'resume_state must be a MissionState, got {self.resume_state!r}')
        if self.resume_state not in (MissionState.RUNNING, MissionState.CANCELLING):
            raise ValueError(
                f'resume_state must be RUNNING or CANCELLING, got {self.resume_state.name}')


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
