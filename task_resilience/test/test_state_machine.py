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

import pytest

from task_resilience.errors import InvalidStateTransition, MissionCompletionInvariantError
from task_resilience.state_machine import (
    is_mission_terminal,
    is_step_terminal,
    MissionState,
    requires_reconciliation,
    StepState,
    transition_mission,
    transition_step,
)

M = MissionState
S = StepState

# Written out by hand rather than imported, so a slip in the module's own
# tables cannot also slip into the expectation.
LEGAL_STEP = {
    S.PENDING: {S.DISPATCH_PREPARED, S.CANCELLED},
    S.DISPATCH_PREPARED: {S.DISPATCHED, S.FAILED, S.CANCELLED, S.OUTCOME_UNKNOWN},
    S.DISPATCHED: {S.ACCEPTED, S.SUCCEEDED, S.FAILED, S.CANCELLED, S.OUTCOME_UNKNOWN},
    S.ACCEPTED: {S.EXECUTING, S.SUCCEEDED, S.FAILED, S.CANCEL_REQUESTED, S.CANCELLED,
                 S.OUTCOME_UNKNOWN},
    S.EXECUTING: {S.SUCCEEDED, S.FAILED, S.CANCEL_REQUESTED, S.CANCELLED, S.OUTCOME_UNKNOWN},
    S.CANCEL_REQUESTED: {S.CANCELLED, S.SUCCEEDED, S.FAILED, S.OUTCOME_UNKNOWN},
    S.SUCCEEDED: set(),
    S.FAILED: set(),
    S.CANCELLED: set(),
    S.OUTCOME_UNKNOWN: set(),
}

LEGAL_MISSION = {
    M.ACCEPTED: {M.RUNNING, M.CANCELLED},
    M.RUNNING: {M.CANCELLING, M.COMPLETED, M.FAILED, M.NEEDS_RECONCILIATION},
    M.CANCELLING: {M.CANCELLED, M.COMPLETED, M.FAILED, M.NEEDS_RECONCILIATION},
    M.COMPLETED: set(),
    M.FAILED: set(),
    M.CANCELLED: set(),
    M.NEEDS_RECONCILIATION: set(),
}

LEGAL_STEP_PAIRS = [(c, t) for c, targets in LEGAL_STEP.items() for t in targets]
LEGAL_MISSION_PAIRS = [(c, t) for c, targets in LEGAL_MISSION.items() for t in targets]

ALL_SUCCEEDED = [S.SUCCEEDED, S.SUCCEEDED]
NOT_SUCCEEDED = [state for state in S if state is not S.SUCCEEDED]


def steps_for(target):
    # Only a transition into COMPLETED needs step evidence.
    return [S.SUCCEEDED] if target is M.COMPLETED else None


def is_allowed(transition, *args):
    try:
        transition(*args)
    except InvalidStateTransition:
        return False
    return True


def assert_step_rejected(current, target):
    with pytest.raises(InvalidStateTransition) as excinfo:
        transition_step(current, target)
    assert excinfo.value.entity == 'step'
    assert excinfo.value.current is current
    assert excinfo.value.target is target


def assert_mission_rejected(current, target, step_states=None):
    with pytest.raises(InvalidStateTransition) as excinfo:
        transition_mission(current, target, step_states)
    assert excinfo.value.entity == 'mission'
    assert excinfo.value.current is current
    assert excinfo.value.target is target


# Persisted values

def test_mission_state_values_are_pinned():
    assert {state.name: state.value for state in M} == {
        'ACCEPTED': 'ACCEPTED',
        'RUNNING': 'RUNNING',
        'CANCELLING': 'CANCELLING',
        'COMPLETED': 'COMPLETED',
        'FAILED': 'FAILED',
        'CANCELLED': 'CANCELLED',
        'NEEDS_RECONCILIATION': 'NEEDS_RECONCILIATION',
    }


def test_step_state_values_are_pinned():
    assert {state.name: state.value for state in S} == {
        'PENDING': 'PENDING',
        'DISPATCH_PREPARED': 'DISPATCH_PREPARED',
        'DISPATCHED': 'DISPATCHED',
        'ACCEPTED': 'ACCEPTED',
        'EXECUTING': 'EXECUTING',
        'SUCCEEDED': 'SUCCEEDED',
        'FAILED': 'FAILED',
        'CANCEL_REQUESTED': 'CANCEL_REQUESTED',
        'CANCELLED': 'CANCELLED',
        'OUTCOME_UNKNOWN': 'OUTCOME_UNKNOWN',
    }


def test_persisted_values_load_back_to_the_same_member():
    assert all(M(state.value) is state for state in M)
    assert all(S(state.value) is state for state in S)


def test_states_never_compare_equal_across_types_or_to_strings():
    # ACCEPTED exists in both enums; conflating them would corrupt durable state.
    assert S.ACCEPTED != M.ACCEPTED
    assert S.CANCELLED != M.CANCELLED
    assert S.FAILED != M.FAILED
    assert M.RUNNING != 'RUNNING'
    assert S.EXECUTING != 'EXECUTING'


# Step transitions

@pytest.mark.parametrize(('current', 'target'), LEGAL_STEP_PAIRS)
def test_listed_step_transitions_succeed(current, target):
    assert transition_step(current, target) is target


@pytest.mark.parametrize(('current', 'target'), [
    (S.PENDING, S.SUCCEEDED),
    (S.ACCEPTED, S.DISPATCH_PREPARED),
    (S.EXECUTING, S.ACCEPTED),
    (S.SUCCEEDED, S.EXECUTING),
    (S.FAILED, S.DISPATCH_PREPARED),
    (S.CANCELLED, S.EXECUTING),
    (S.OUTCOME_UNKNOWN, S.SUCCEEDED),
])
def test_representative_illegal_step_transitions_fail(current, target):
    assert_step_rejected(current, target)


def test_no_unlisted_step_transition_is_accepted():
    for current in S:
        for target in S:
            if current is not target and target not in LEGAL_STEP[current]:
                assert_step_rejected(current, target)


@pytest.mark.parametrize('state', list(S))
def test_step_same_state_is_an_idempotent_no_op(state):
    assert transition_step(state, state) is state


def test_step_can_go_straight_to_an_observed_result():
    assert transition_step(S.DISPATCHED, S.SUCCEEDED) is S.SUCCEEDED
    assert transition_step(S.ACCEPTED, S.SUCCEEDED) is S.SUCCEEDED


@pytest.mark.parametrize('outcome', [S.CANCELLED, S.SUCCEEDED, S.FAILED])
def test_cancel_request_can_lose_the_race_to_any_result(outcome):
    assert transition_step(S.CANCEL_REQUESTED, outcome) is outcome


def test_cancel_can_only_be_requested_once_the_goal_is_accepted():
    for current in (S.PENDING, S.DISPATCH_PREPARED, S.DISPATCHED):
        assert_step_rejected(current, S.CANCEL_REQUESTED)
    for current in (S.ACCEPTED, S.EXECUTING):
        assert transition_step(current, S.CANCEL_REQUESTED) is S.CANCEL_REQUESTED


# Mission transitions

@pytest.mark.parametrize(('current', 'target'), LEGAL_MISSION_PAIRS)
def test_listed_mission_transitions_succeed(current, target):
    assert transition_mission(current, target, steps_for(target)) is target


@pytest.mark.parametrize(('current', 'target'), [
    (M.ACCEPTED, M.COMPLETED),
    (M.RUNNING, M.ACCEPTED),
    (M.COMPLETED, M.RUNNING),
    (M.FAILED, M.RUNNING),
    (M.CANCELLED, M.RUNNING),
    (M.NEEDS_RECONCILIATION, M.RUNNING),
    (M.NEEDS_RECONCILIATION, M.COMPLETED),
])
def test_representative_illegal_mission_transitions_fail(current, target):
    assert_mission_rejected(current, target)


@pytest.mark.parametrize(('current', 'target'), [
    (M.ACCEPTED, M.COMPLETED),
    (M.NEEDS_RECONCILIATION, M.COMPLETED),
])
def test_illegal_transition_to_completed_stays_illegal_with_every_step_succeeded(
        current, target):
    # The transition table decides first; finished steps cannot make it legal.
    assert_mission_rejected(current, target, ALL_SUCCEEDED)


def test_no_unlisted_mission_transition_is_accepted():
    for current in M:
        for target in M:
            if current is not target and target not in LEGAL_MISSION[current]:
                assert_mission_rejected(current, target)
                assert_mission_rejected(current, target, ALL_SUCCEEDED)


@pytest.mark.parametrize('state', list(M))
def test_mission_same_state_is_an_idempotent_no_op(state):
    assert transition_mission(state, state) is state


# Completion invariant (I-1)

def test_running_completes_when_the_only_step_succeeded():
    assert transition_mission(M.RUNNING, M.COMPLETED, [S.SUCCEEDED]) is M.COMPLETED


def test_running_completes_when_every_step_succeeded():
    steps = [S.SUCCEEDED, S.SUCCEEDED, S.SUCCEEDED]
    assert transition_mission(M.RUNNING, M.COMPLETED, steps) is M.COMPLETED


@pytest.mark.parametrize('current', [M.RUNNING, M.CANCELLING])
def test_completion_needs_at_least_one_step(current):
    for missing in (None, [], ()):
        with pytest.raises(MissionCompletionInvariantError) as excinfo:
            transition_mission(current, M.COMPLETED, missing)
        assert excinfo.value.current is current
        assert excinfo.value.step_states == ()


@pytest.mark.parametrize('current', [M.RUNNING, M.CANCELLING])
@pytest.mark.parametrize('unfinished', NOT_SUCCEEDED)
def test_completion_is_rejected_by_any_step_that_has_not_succeeded(current, unfinished):
    with pytest.raises(MissionCompletionInvariantError) as excinfo:
        transition_mission(current, M.COMPLETED, [unfinished])
    assert excinfo.value.step_states == (unfinished,)

    mixed = [S.SUCCEEDED, unfinished, S.SUCCEEDED]
    with pytest.raises(MissionCompletionInvariantError) as excinfo:
        transition_mission(current, M.COMPLETED, mixed)
    assert excinfo.value.current is current
    assert excinfo.value.step_states == tuple(mixed)


def test_cancelling_mission_can_still_complete_if_every_step_succeeded():
    # The mission finished while a cancellation request was in flight.
    assert transition_mission(M.CANCELLING, M.COMPLETED, ALL_SUCCEEDED) is M.COMPLETED


@pytest.mark.parametrize('steps', [
    [S.SUCCEEDED, S.CANCEL_REQUESTED],
    [S.SUCCEEDED, S.CANCELLED],
    [S.FAILED],
    [S.OUTCOME_UNKNOWN, S.SUCCEEDED],
])
def test_cancelling_mission_cannot_complete_with_an_unfinished_step(steps):
    with pytest.raises(MissionCompletionInvariantError):
        transition_mission(M.CANCELLING, M.COMPLETED, steps)


@pytest.mark.parametrize(
    ('current', 'target'), [pair for pair in LEGAL_MISSION_PAIRS if pair[1] is not M.COMPLETED])
def test_other_targets_do_not_need_step_states(current, target):
    assert transition_mission(current, target) is target
    assert transition_mission(current, target, [S.EXECUTING, S.PENDING]) is target


def test_unmet_invariant_is_not_reported_as_an_illegal_transition():
    with pytest.raises(MissionCompletionInvariantError) as excinfo:
        transition_mission(M.RUNNING, M.COMPLETED, [S.EXECUTING])

    assert not isinstance(excinfo.value, InvalidStateTransition)


# Terminal versus blocked

@pytest.mark.parametrize('state', list(S))
def test_is_step_terminal(state):
    assert is_step_terminal(state) is (state in {S.SUCCEEDED, S.FAILED, S.CANCELLED})


@pytest.mark.parametrize('state', list(M))
def test_is_mission_terminal(state):
    assert is_mission_terminal(state) is (state in {M.COMPLETED, M.FAILED, M.CANCELLED})


def test_step_blocked_states_are_terminal_plus_outcome_unknown():
    blocked = {
        state for state in S
        if all(not is_allowed(transition_step, state, other) for other in S if other is not state)
    }

    assert blocked == {S.SUCCEEDED, S.FAILED, S.CANCELLED, S.OUTCOME_UNKNOWN}
    assert not is_step_terminal(S.OUTCOME_UNKNOWN)


def test_mission_blocked_states_are_terminal_plus_needs_reconciliation():
    blocked = {
        state for state in M
        if all(not is_allowed(transition_mission, state, other, ALL_SUCCEEDED)
               for other in M if other is not state)
    }

    assert blocked == {M.COMPLETED, M.FAILED, M.CANCELLED, M.NEEDS_RECONCILIATION}
    assert not is_mission_terminal(M.NEEDS_RECONCILIATION)


def test_an_unknown_outcome_cannot_be_resolved_by_an_ordinary_transition():
    for other in S:
        if other is not S.OUTCOME_UNKNOWN:
            assert_step_rejected(S.OUTCOME_UNKNOWN, other)

    for other in M:
        if other is not M.NEEDS_RECONCILIATION:
            assert_mission_rejected(M.NEEDS_RECONCILIATION, other, ALL_SUCCEEDED)
            assert_mission_rejected(M.NEEDS_RECONCILIATION, other)


# Reconciliation

def test_requires_reconciliation_when_any_step_outcome_is_unknown():
    assert requires_reconciliation([S.OUTCOME_UNKNOWN])
    assert requires_reconciliation([S.SUCCEEDED, S.OUTCOME_UNKNOWN, S.FAILED])
    assert requires_reconciliation((S.OUTCOME_UNKNOWN, S.OUTCOME_UNKNOWN))


@pytest.mark.parametrize('state', [state for state in S if state is not S.OUTCOME_UNKNOWN])
def test_no_reconciliation_for_any_other_step_state(state):
    assert requires_reconciliation([state]) is False


def test_no_reconciliation_for_empty_or_settled_steps():
    assert requires_reconciliation([]) is False
    assert requires_reconciliation(()) is False
    assert requires_reconciliation(ALL_SUCCEEDED) is False


def test_requires_reconciliation_does_not_modify_its_input():
    steps = [S.SUCCEEDED, S.OUTCOME_UNKNOWN]

    requires_reconciliation(steps)

    assert steps == [S.SUCCEEDED, S.OUTCOME_UNKNOWN]


def test_completion_check_does_not_modify_its_input():
    steps = [S.SUCCEEDED, S.SUCCEEDED]

    transition_mission(M.RUNNING, M.COMPLETED, steps)

    assert steps == [S.SUCCEEDED, S.SUCCEEDED]


# Wrong types

@pytest.mark.parametrize(('current', 'target'), [
    ('EXECUTING', S.SUCCEEDED),
    (S.EXECUTING, 'SUCCEEDED'),
    ('EXECUTING', 'EXECUTING'),
    (None, S.PENDING),
    (S.PENDING, None),
    (M.RUNNING, M.RUNNING),
    (S.PENDING, M.RUNNING),
    (M.ACCEPTED, S.ACCEPTED),
])
def test_transition_step_requires_step_states(current, target):
    with pytest.raises(TypeError):
        transition_step(current, target)


@pytest.mark.parametrize(('current', 'target'), [
    ('ACCEPTED', M.RUNNING),
    (M.ACCEPTED, 'RUNNING'),
    ('RUNNING', 'RUNNING'),
    (None, M.RUNNING),
    (M.RUNNING, None),
    (S.ACCEPTED, S.ACCEPTED),
    (S.PENDING, M.RUNNING),
    (M.ACCEPTED, S.ACCEPTED),
])
def test_transition_mission_requires_mission_states(current, target):
    with pytest.raises(TypeError):
        transition_mission(current, target)


@pytest.mark.parametrize('steps', [
    ['SUCCEEDED'],
    'SUCCEEDED',
    [M.COMPLETED],
    [S.SUCCEEDED, 'SUCCEEDED'],
    [S.SUCCEEDED, None],
    S.SUCCEEDED,
])
def test_step_states_must_be_step_states(steps):
    with pytest.raises(TypeError):
        transition_mission(M.RUNNING, M.COMPLETED, steps)
    with pytest.raises(TypeError):
        requires_reconciliation(steps)


def test_step_states_are_checked_even_when_they_are_not_needed():
    with pytest.raises(TypeError):
        transition_mission(M.RUNNING, M.CANCELLING, ['EXECUTING'])
    with pytest.raises(TypeError):
        transition_mission(M.RUNNING, M.RUNNING, ['EXECUTING'])


@pytest.mark.parametrize('state', ['SUCCEEDED', None, M.COMPLETED, 1])
def test_is_step_terminal_requires_a_step_state(state):
    with pytest.raises(TypeError):
        is_step_terminal(state)


@pytest.mark.parametrize('state', ['COMPLETED', None, S.SUCCEEDED, 1])
def test_is_mission_terminal_requires_a_mission_state(state):
    with pytest.raises(TypeError):
        is_mission_terminal(state)


def test_requires_reconciliation_needs_an_iterable():
    with pytest.raises(TypeError):
        requires_reconciliation(None)


# One-shot iterables

class CountingSteps:
    """Yield the given steps once, counting how many were pulled."""

    def __init__(self, steps):
        self.pulled = 0
        self._iterator = self._generate(steps)

    def _generate(self, steps):
        for step in steps:
            self.pulled += 1
            yield step

    def __iter__(self):
        return self._iterator


def test_generator_of_succeeded_steps_completes_the_mission():
    steps = (state for state in [S.SUCCEEDED, S.SUCCEEDED, S.SUCCEEDED])

    assert transition_mission(M.RUNNING, M.COMPLETED, steps) is M.COMPLETED


def test_generator_with_an_unfinished_step_is_rejected():
    steps = (state for state in [S.SUCCEEDED, S.EXECUTING, S.SUCCEEDED])

    with pytest.raises(MissionCompletionInvariantError) as excinfo:
        transition_mission(M.RUNNING, M.COMPLETED, steps)

    # The error reports every step that was supplied, not just those read
    # before the answer was known.
    assert excinfo.value.step_states == (S.SUCCEEDED, S.EXECUTING, S.SUCCEEDED)


def test_iterator_is_read_exactly_once():
    counted = CountingSteps([S.SUCCEEDED, S.SUCCEEDED, S.SUCCEEDED])

    assert transition_mission(M.RUNNING, M.COMPLETED, counted) is M.COMPLETED
    assert counted.pulled == 3

    counted = CountingSteps([S.FAILED, S.SUCCEEDED, S.SUCCEEDED])
    with pytest.raises(MissionCompletionInvariantError):
        transition_mission(M.RUNNING, M.COMPLETED, counted)
    assert counted.pulled == 3


def test_reconciliation_sees_a_late_unknown_step_in_a_generator():
    steps = (state for state in [S.SUCCEEDED, S.SUCCEEDED, S.OUTCOME_UNKNOWN])

    assert requires_reconciliation(steps) is True
    assert requires_reconciliation(state for state in ALL_SUCCEEDED) is False


def test_a_bad_element_after_a_disqualifying_one_still_fails_as_a_type_error():
    steps = (state for state in [S.FAILED, 'SUCCEEDED'])

    with pytest.raises(TypeError):
        transition_mission(M.RUNNING, M.COMPLETED, steps)
