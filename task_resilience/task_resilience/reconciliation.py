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
Decide what one recovery observation can establish about a step's dispatch.

The decision is pure and deterministic: no I/O, no clock, no ROS, no storage. It is an
assessment only. It changes no durable state and authorizes nothing, a resend included.
See docs/reconciliation_decision_model.md.
"""

from dataclasses import dataclass
import enum
import uuid

from .state_machine import is_step_terminal, MissionState, StepState


# The values are strings so that nothing here depends on a ROS message's numbering; an
# adapter maps whatever it receives onto these names.
@enum.unique
class ActionStatus(enum.Enum):
    UNKNOWN = 'UNKNOWN'
    ACCEPTED = 'ACCEPTED'
    EXECUTING = 'EXECUTING'
    CANCELING = 'CANCELING'
    SUCCEEDED = 'SUCCEEDED'
    CANCELED = 'CANCELED'
    ABORTED = 'ABORTED'


@enum.unique
class ObservationKind(enum.Enum):
    RESULT_RESPONSE = 'RESULT_RESPONSE'
    STATUS_SAMPLE = 'STATUS_SAMPLE'
    LOCAL_TIMEOUT = 'LOCAL_TIMEOUT'
    SERVICE_UNAVAILABLE = 'SERVICE_UNAVAILABLE'
    TRANSPORT_ERROR = 'TRANSPORT_ERROR'


@enum.unique
class DecisionKind(enum.Enum):
    TERMINAL_RESULT_CANDIDATE = 'TERMINAL_RESULT_CANDIDATE'
    INCONCLUSIVE = 'INCONCLUSIVE'
    SERVER_UNKNOWN = 'SERVER_UNKNOWN'
    STATUS_ONLY = 'STATUS_ONLY'
    EVIDENCE_CONFLICT = 'EVIDENCE_CONFLICT'
    MISSING_DISPATCH_IDENTITY = 'MISSING_DISPATCH_IDENTITY'
    ALREADY_SETTLED = 'ALREADY_SETTLED'
    UNCORRELATED = 'UNCORRELATED'


# Why a decision came out as it did. Several reasons share the kind INCONCLUSIVE; the
# rest map one to one, so a log line can say more than the kind alone.
@enum.unique
class DecisionReason(enum.Enum):
    NO_DURABLE_DISPATCH_ID = 'NO_DURABLE_DISPATCH_ID'
    CORRELATION_MISMATCH = 'CORRELATION_MISMATCH'
    LOCAL_TIMEOUT = 'LOCAL_TIMEOUT'
    SERVICE_UNAVAILABLE = 'SERVICE_UNAVAILABLE'
    TRANSPORT_ERROR = 'TRANSPORT_ERROR'
    NONTERMINAL_RESULT_STATUS = 'NONTERMINAL_RESULT_STATUS'
    STATUS_IS_ADVISORY = 'STATUS_IS_ADVISORY'
    RESULT_UNKNOWN = 'RESULT_UNKNOWN'
    DURABLE_STATE_NOT_TERMINAL = 'DURABLE_STATE_NOT_TERMINAL'
    RESULT_MATCHES_DURABLE_STATE = 'RESULT_MATCHES_DURABLE_STATE'
    RESULT_CONTRADICTS_DURABLE_STATE = 'RESULT_CONTRADICTS_DURABLE_STATE'


# Which reasons a decision of each kind may carry. A decision is public and can be built
# directly, so a kind must not be paired with another kind's reason.
_REASONS_FOR_KIND: dict[DecisionKind, frozenset[DecisionReason]] = {
    DecisionKind.TERMINAL_RESULT_CANDIDATE: frozenset({
        DecisionReason.DURABLE_STATE_NOT_TERMINAL,
    }),
    DecisionKind.INCONCLUSIVE: frozenset({
        DecisionReason.LOCAL_TIMEOUT,
        DecisionReason.SERVICE_UNAVAILABLE,
        DecisionReason.TRANSPORT_ERROR,
        DecisionReason.NONTERMINAL_RESULT_STATUS,
    }),
    DecisionKind.SERVER_UNKNOWN: frozenset({DecisionReason.RESULT_UNKNOWN}),
    DecisionKind.STATUS_ONLY: frozenset({DecisionReason.STATUS_IS_ADVISORY}),
    DecisionKind.EVIDENCE_CONFLICT: frozenset({
        DecisionReason.RESULT_CONTRADICTS_DURABLE_STATE,
    }),
    DecisionKind.MISSING_DISPATCH_IDENTITY: frozenset({DecisionReason.NO_DURABLE_DISPATCH_ID}),
    DecisionKind.ALREADY_SETTLED: frozenset({DecisionReason.RESULT_MATCHES_DURABLE_STATE}),
    DecisionKind.UNCORRELATED: frozenset({DecisionReason.CORRELATION_MISMATCH}),
}


def _require(value: object, kind: type, name: str, *, optional: bool = False) -> None:
    if optional and value is None:
        return
    # bool is an int subclass, but True is not a meaningful result code.
    if not isinstance(value, kind) or (kind is int and isinstance(value, bool)):
        wanted = f'a {kind.__name__}' + (' or None' if optional else '')
        raise TypeError(f'{name} must be {wanted}, got {value!r}')


@dataclass(frozen=True, kw_only=True)
class DurableSnapshot:
    """
    The durable facts a decision rests on, as they were read.

    A snapshot: it does not track later changes in the store, and it is taken as given.
    Whether the step state and the dispatch agree is the store's invariant, not checked
    here. dispatch_id has no default, so "no durable dispatch UUID" must be said
    explicitly. mission_id and step_key are for diagnostics and never used by a decision.
    """

    mission_state: MissionState
    step_state: StepState
    dispatch_id: uuid.UUID | None
    mission_id: str | None = None
    step_key: str | None = None

    def __post_init__(self) -> None:
        _require(self.mission_state, MissionState, 'mission_state')
        _require(self.step_state, StepState, 'step_state')
        _require(self.dispatch_id, uuid.UUID, 'dispatch_id', optional=True)
        _require(self.mission_id, str, 'mission_id', optional=True)
        _require(self.step_key, str, 'step_key', optional=True)


@dataclass(frozen=True, kw_only=True)
class ExpectedActionContext:
    """
    The exact action a dispatch was expected to reach.

    Schema version 3 does not persist these, so the caller supplies them. Nothing is
    normalized or inferred: '/robot1' and 'robot1' differ, and so do '' and '/'.
    """

    action_name: str
    action_type: str
    robot_namespace: str

    def __post_init__(self) -> None:
        _require(self.action_name, str, 'action_name')
        _require(self.action_type, str, 'action_type')
        _require(self.robot_namespace, str, 'robot_namespace')
        # An action cannot be identified without both. The namespace is different: an
        # empty string is an exact value, and whether it means the root namespace is
        # the caller's convention.
        if not self.action_name or not self.action_type:
            raise ValueError('action_name and action_type must be non-empty')


@dataclass(frozen=True, kw_only=True)
class RecoveryObservation:
    """
    One thing a recovery attempt observed, or failed to observe, about a dispatch.

    The correlation fields say which dispatch and action the observation is about. For a
    timeout, an unavailable service or a transport error they describe the query that
    was attempted, for which no usable result response was obtained. An absent (None)
    field is not the same as a wrong one, but neither correlates. result_code and
    result_message are opaque evidence; no decision reads them.
    """

    kind: ObservationKind
    dispatch_id: uuid.UUID | None = None
    action_name: str | None = None
    action_type: str | None = None
    robot_namespace: str | None = None
    status: ActionStatus | None = None
    result_code: int | None = None
    result_message: str | None = None

    def __post_init__(self) -> None:
        _require(self.kind, ObservationKind, 'kind')
        _require(self.dispatch_id, uuid.UUID, 'dispatch_id', optional=True)
        _require(self.action_name, str, 'action_name', optional=True)
        _require(self.action_type, str, 'action_type', optional=True)
        _require(self.robot_namespace, str, 'robot_namespace', optional=True)
        _require(self.status, ActionStatus, 'status', optional=True)
        _require(self.result_code, int, 'result_code', optional=True)
        _require(self.result_message, str, 'result_message', optional=True)

        reports_status = self.kind in (ObservationKind.RESULT_RESPONSE,
                                       ObservationKind.STATUS_SAMPLE)
        if reports_status and self.status is None:
            raise ValueError(f'{self.kind.name} needs a status')
        if not reports_status and self.status is not None:
            raise ValueError(f'{self.kind.name} carries no status')
        if self.kind is not ObservationKind.RESULT_RESPONSE and (
                self.result_code is not None or self.result_message is not None):
            raise ValueError(f'{self.kind.name} carries no result')


@dataclass(frozen=True, kw_only=True)
class ReconciliationDecision:
    """
    What an observation establishes. An assessment: it applies nothing and permits nothing.

    The reason must be one of those defined for the kind. candidate_step_state is set only
    for TERMINAL_RESULT_CANDIDATE. ALREADY_SETTLED and EVIDENCE_CONFLICT leave it None on
    purpose, so a conflicting outcome cannot be mistaken for one to apply.
    """

    kind: DecisionKind
    reason: DecisionReason
    candidate_step_state: StepState | None = None

    def __post_init__(self) -> None:
        _require(self.kind, DecisionKind, 'kind')
        _require(self.reason, DecisionReason, 'reason')
        _require(self.candidate_step_state, StepState, 'candidate_step_state', optional=True)
        if self.reason not in _REASONS_FOR_KIND[self.kind]:
            raise ValueError(f'{self.reason.name} is not a reason for {self.kind.name}')
        if (self.candidate_step_state is not None) != (
                self.kind is DecisionKind.TERMINAL_RESULT_CANDIDATE):
            raise ValueError(
                'candidate_step_state is set exactly for TERMINAL_RESULT_CANDIDATE')
        if self.candidate_step_state is not None and not is_step_terminal(
                self.candidate_step_state):
            raise ValueError('candidate_step_state must be a terminal step state')

    @property
    def authorizes_send(self) -> bool:
        # A constant, not a field: it cannot be passed to the constructor or changed with
        # dataclasses.replace(). No observation handled here permits sending the same
        # dispatch again, or creating a replacement execution.
        return False


_INCONCLUSIVE_REASON = {
    ObservationKind.LOCAL_TIMEOUT: DecisionReason.LOCAL_TIMEOUT,
    ObservationKind.SERVICE_UNAVAILABLE: DecisionReason.SERVICE_UNAVAILABLE,
    ObservationKind.TRANSPORT_ERROR: DecisionReason.TRANSPORT_ERROR,
}

# ROS spells CANCELED with one L and StepState CANCELLED with two.
_TERMINAL_RESULT = {
    ActionStatus.SUCCEEDED: StepState.SUCCEEDED,
    ActionStatus.CANCELED: StepState.CANCELLED,
    ActionStatus.ABORTED: StepState.FAILED,
}

_NONTERMINAL_RESULT = frozenset({
    ActionStatus.ACCEPTED,
    ActionStatus.EXECUTING,
    ActionStatus.CANCELING,
})


def _decision(
    kind: DecisionKind, reason: DecisionReason, candidate: StepState | None = None,
) -> ReconciliationDecision:
    return ReconciliationDecision(kind=kind, reason=reason, candidate_step_state=candidate)


def _correlated(
    snapshot: DurableSnapshot, expected: ExpectedActionContext, observation: RecoveryObservation,
) -> bool:
    # Plain equality is enough: an absent field is None, which equals no UUID or string.
    return (observation.dispatch_id == snapshot.dispatch_id
            and observation.action_name == expected.action_name
            and observation.action_type == expected.action_type
            and observation.robot_namespace == expected.robot_namespace)


def _decide_result(snapshot: DurableSnapshot, status: ActionStatus) -> ReconciliationDecision:
    if status is ActionStatus.UNKNOWN:
        # The server has no record now. That covers a request it never received and one
        # it has forgotten, so it is not evidence that nothing ran.
        return _decision(DecisionKind.SERVER_UNKNOWN, DecisionReason.RESULT_UNKNOWN)
    if status in _NONTERMINAL_RESULT:
        return _decision(DecisionKind.INCONCLUSIVE, DecisionReason.NONTERMINAL_RESULT_STATUS)

    candidate = _TERMINAL_RESULT[status]
    # OUTCOME_UNKNOWN is not terminal, so a result can settle it. That is the point.
    if snapshot.step_state is candidate:
        return _decision(DecisionKind.ALREADY_SETTLED, DecisionReason.RESULT_MATCHES_DURABLE_STATE)
    if is_step_terminal(snapshot.step_state):
        return _decision(
            DecisionKind.EVIDENCE_CONFLICT, DecisionReason.RESULT_CONTRADICTS_DURABLE_STATE)
    return _decision(
        DecisionKind.TERMINAL_RESULT_CANDIDATE, DecisionReason.DURABLE_STATE_NOT_TERMINAL,
        candidate)


def decide_reconciliation(
    snapshot: DurableSnapshot,
    expected: ExpectedActionContext,
    observation: RecoveryObservation,
) -> ReconciliationDecision:
    """
    Assess one observation against a durable snapshot.

    In order: no durable dispatch UUID gives MISSING_DISPATCH_IDENTITY; an observation
    that does not match the dispatch UUID, action name, action type and namespace exactly
    gives UNCORRELATED; then a timeout, an unavailable service or a transport error gives
    INCONCLUSIVE; any status sample gives STATUS_ONLY; and a result response is judged by
    its status, against the durable step state only when it is terminal.
    """
    for value, kind, name in ((snapshot, DurableSnapshot, 'snapshot'),
                              (expected, ExpectedActionContext, 'expected'),
                              (observation, RecoveryObservation, 'observation')):
        _require(value, kind, name)

    if snapshot.dispatch_id is None:
        return _decision(
            DecisionKind.MISSING_DISPATCH_IDENTITY, DecisionReason.NO_DURABLE_DISPATCH_ID)
    # Before any outcome is compared: an observation about another dispatch or action
    # must never settle, or contradict, this step.
    if not _correlated(snapshot, expected, observation):
        return _decision(DecisionKind.UNCORRELATED, DecisionReason.CORRELATION_MISMATCH)

    if observation.kind in _INCONCLUSIVE_REASON:
        return _decision(DecisionKind.INCONCLUSIVE, _INCONCLUSIVE_REASON[observation.kind])
    if observation.kind is ObservationKind.STATUS_SAMPLE:
        return _decision(DecisionKind.STATUS_ONLY, DecisionReason.STATUS_IS_ADVISORY)
    return _decide_result(snapshot, observation.status)
