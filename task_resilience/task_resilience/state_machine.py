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
import enum

from .errors import InvalidStateTransition, MissionCompletionInvariantError


# Plain Enums rather than str mixins, so a StepState never compares equal to a
# MissionState or to a bare string. The values will be persisted: do not
# change them.
@enum.unique
class MissionState(enum.Enum):
    ACCEPTED = 'ACCEPTED'
    RUNNING = 'RUNNING'
    CANCELLING = 'CANCELLING'
    COMPLETED = 'COMPLETED'
    FAILED = 'FAILED'
    CANCELLED = 'CANCELLED'
    NEEDS_RECONCILIATION = 'NEEDS_RECONCILIATION'


@enum.unique
class StepState(enum.Enum):
    PENDING = 'PENDING'
    DISPATCH_PREPARED = 'DISPATCH_PREPARED'
    DISPATCHED = 'DISPATCHED'
    ACCEPTED = 'ACCEPTED'
    EXECUTING = 'EXECUTING'
    SUCCEEDED = 'SUCCEEDED'
    FAILED = 'FAILED'
    CANCEL_REQUESTED = 'CANCEL_REQUESTED'
    CANCELLED = 'CANCELLED'
    OUTCOME_UNKNOWN = 'OUTCOME_UNKNOWN'


# Some edges skip states (DISPATCHED -> SUCCEEDED). Durable state records what
# was observed, so ACCEPTED or EXECUTING is not invented just because they
# usually happen in between.
#
# States with no way out are blocked for ordinary transitions. That is not the
# same as terminal: OUTCOME_UNKNOWN and NEEDS_RECONCILIATION are blocked but
# still unresolved, and leaving them takes an explicit reconciliation with
# evidence, not an ordinary transition.
_STEP_TRANSITIONS: dict[StepState, frozenset[StepState]] = {
    StepState.PENDING: frozenset({
        StepState.DISPATCH_PREPARED,
        StepState.CANCELLED,
    }),
    StepState.DISPATCH_PREPARED: frozenset({
        StepState.DISPATCHED,
        StepState.FAILED,
        StepState.CANCELLED,
        StepState.OUTCOME_UNKNOWN,
    }),
    StepState.DISPATCHED: frozenset({
        StepState.ACCEPTED,
        StepState.SUCCEEDED,
        StepState.FAILED,
        StepState.CANCELLED,
        StepState.OUTCOME_UNKNOWN,
    }),
    StepState.ACCEPTED: frozenset({
        StepState.EXECUTING,
        StepState.SUCCEEDED,
        StepState.FAILED,
        StepState.CANCEL_REQUESTED,
        StepState.CANCELLED,
        StepState.OUTCOME_UNKNOWN,
    }),
    StepState.EXECUTING: frozenset({
        StepState.SUCCEEDED,
        StepState.FAILED,
        StepState.CANCEL_REQUESTED,
        StepState.CANCELLED,
        StepState.OUTCOME_UNKNOWN,
    }),
    StepState.CANCEL_REQUESTED: frozenset({
        StepState.CANCELLED,
        StepState.SUCCEEDED,
        StepState.FAILED,
        StepState.OUTCOME_UNKNOWN,
    }),
    StepState.SUCCEEDED: frozenset(),
    StepState.FAILED: frozenset(),
    StepState.CANCELLED: frozenset(),
    StepState.OUTCOME_UNKNOWN: frozenset(),
}

_MISSION_TRANSITIONS: dict[MissionState, frozenset[MissionState]] = {
    MissionState.ACCEPTED: frozenset({
        MissionState.RUNNING,
        MissionState.CANCELLED,
    }),
    MissionState.RUNNING: frozenset({
        MissionState.CANCELLING,
        MissionState.COMPLETED,
        MissionState.FAILED,
        MissionState.NEEDS_RECONCILIATION,
    }),
    MissionState.CANCELLING: frozenset({
        MissionState.CANCELLED,
        MissionState.COMPLETED,
        MissionState.FAILED,
        MissionState.NEEDS_RECONCILIATION,
    }),
    MissionState.COMPLETED: frozenset(),
    MissionState.FAILED: frozenset(),
    MissionState.CANCELLED: frozenset(),
    MissionState.NEEDS_RECONCILIATION: frozenset(),
}

_STEP_TERMINAL = frozenset({
    StepState.SUCCEEDED,
    StepState.FAILED,
    StepState.CANCELLED,
})

_MISSION_TERMINAL = frozenset({
    MissionState.COMPLETED,
    MissionState.FAILED,
    MissionState.CANCELLED,
})


def _require(value: object, kind: type[enum.Enum], name: str) -> None:
    if not isinstance(value, kind):
        raise TypeError(f'{name} must be a {kind.__name__}, got {value!r}')


def _step_tuple(step_states: Iterable[StepState]) -> tuple[StepState, ...]:
    # Read the iterable once; a generator can only be consumed one time.
    steps = tuple(step_states)
    for step in steps:
        _require(step, StepState, 'step state')
    return steps


def transition_step(current: StepState, target: StepState) -> StepState:
    _require(current, StepState, 'current')
    _require(target, StepState, 'target')
    # A replayed or duplicated observation is harmless, not a transition.
    if current is target:
        return target
    if target not in _STEP_TRANSITIONS[current]:
        raise InvalidStateTransition('step', current, target)
    return target


def transition_mission(
    current: MissionState,
    target: MissionState,
    step_states: Iterable[StepState] | None = None,
) -> MissionState:
    _require(current, MissionState, 'current')
    _require(target, MissionState, 'target')
    steps = () if step_states is None else _step_tuple(step_states)

    # Ahead of the completion check on purpose: a same-state call changes
    # nothing, and the invariant was enforced when the mission became COMPLETED.
    if current is target:
        return target
    if target not in _MISSION_TRANSITIONS[current]:
        raise InvalidStateTransition('mission', current, target)

    # I-1. Every supplied step counts as required for now.
    if target is MissionState.COMPLETED and (
            not steps or any(step is not StepState.SUCCEEDED for step in steps)):
        raise MissionCompletionInvariantError(current, steps)
    return target


def is_step_terminal(state: StepState) -> bool:
    """Return whether the step's outcome is settled. OUTCOME_UNKNOWN is not."""
    _require(state, StepState, 'state')
    return state in _STEP_TERMINAL


def is_mission_terminal(state: MissionState) -> bool:
    """Return whether the mission is finished. NEEDS_RECONCILIATION is not."""
    _require(state, MissionState, 'state')
    return state in _MISSION_TERMINAL


def requires_reconciliation(step_states: Iterable[StepState]) -> bool:
    return any(step is StepState.OUTCOME_UNKNOWN for step in _step_tuple(step_states))
