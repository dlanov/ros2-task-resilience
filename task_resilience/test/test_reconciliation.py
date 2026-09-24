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

import ast
import dataclasses
import os
from pathlib import Path
import subprocess
import sys
import uuid

import pytest

import task_resilience
from task_resilience import reconciliation, state_machine
from task_resilience.errors import InvalidStateTransition
from task_resilience.reconciliation import (
    ActionStatus,
    decide_reconciliation,
    DecisionKind,
    DecisionReason,
    DurableSnapshot,
    ExpectedActionContext,
    ObservationKind,
    ReconciliationDecision,
    RecoveryObservation,
)
from task_resilience.state_machine import MissionState, StepState, transition_step

M = MissionState
S = StepState
A = ActionStatus
K = ObservationKind
D = DecisionKind
R = DecisionReason

DISPATCH = uuid.UUID('6f1f4b8e-2c1d-4e57-9a3b-5d0c8e7f1a24')
OTHER_DISPATCH = uuid.UUID('0a9e7c34-8b5d-4f16-b2e1-3c4d5a6b7e80')
ACTION_NAME = '/navigate_to_pose'
ACTION_TYPE = 'nav2_msgs/action/NavigateToPose'
NAMESPACE = '/robot1'
EXPECTED = ExpectedActionContext(
    action_name=ACTION_NAME, action_type=ACTION_TYPE, robot_namespace=NAMESPACE)

# The expectations below are written out by hand rather than derived from the module,
# so a slip in the module's own tables cannot also slip into the test.

ALL_STATUSES = [A.UNKNOWN, A.ACCEPTED, A.EXECUTING, A.CANCELING, A.SUCCEEDED, A.CANCELED,
                A.ABORTED]

# Every observation the module accepts, as (kind, status).
ALL_SPECS = (
    [(K.RESULT_RESPONSE, status) for status in ALL_STATUSES]
    + [(K.STATUS_SAMPLE, status) for status in ALL_STATUSES]
    + [(K.LOCAL_TIMEOUT, None), (K.SERVICE_UNAVAILABLE, None), (K.TRANSPORT_ERROR, None)]
)

# Result statuses that never depend on the durable step state.
UNSETTLING_RESULTS = {
    A.UNKNOWN: (D.SERVER_UNKNOWN, R.RESULT_UNKNOWN),
    A.ACCEPTED: (D.INCONCLUSIVE, R.NONTERMINAL_RESULT_STATUS),
    A.EXECUTING: (D.INCONCLUSIVE, R.NONTERMINAL_RESULT_STATUS),
    A.CANCELING: (D.INCONCLUSIVE, R.NONTERMINAL_RESULT_STATUS),
}

NON_RESULT_REASONS = {
    K.LOCAL_TIMEOUT: R.LOCAL_TIMEOUT,
    K.SERVICE_UNAVAILABLE: R.SERVICE_UNAVAILABLE,
    K.TRANSPORT_ERROR: R.TRANSPORT_ERROR,
}

# The step state each terminal result implies.
IMPLIED_STEP_STATE = {A.SUCCEEDED: S.SUCCEEDED, A.CANCELED: S.CANCELLED, A.ABORTED: S.FAILED}

SETTLED_STEPS = [S.SUCCEEDED, S.FAILED, S.CANCELLED]
UNSETTLED_STEPS = [S.PENDING, S.DISPATCH_PREPARED, S.DISPATCHED, S.ACCEPTED, S.EXECUTING,
                   S.CANCEL_REQUESTED, S.OUTCOME_UNKNOWN]

# (durable step state, reported terminal status) -> decision kind
SETTLED_VS_RESULT = {
    (S.SUCCEEDED, A.SUCCEEDED): D.ALREADY_SETTLED,
    (S.SUCCEEDED, A.CANCELED): D.EVIDENCE_CONFLICT,
    (S.SUCCEEDED, A.ABORTED): D.EVIDENCE_CONFLICT,
    (S.FAILED, A.SUCCEEDED): D.EVIDENCE_CONFLICT,
    (S.FAILED, A.CANCELED): D.EVIDENCE_CONFLICT,
    (S.FAILED, A.ABORTED): D.ALREADY_SETTLED,
    (S.CANCELLED, A.SUCCEEDED): D.EVIDENCE_CONFLICT,
    (S.CANCELLED, A.CANCELED): D.ALREADY_SETTLED,
    (S.CANCELLED, A.ABORTED): D.EVIDENCE_CONFLICT,
}

# The reasons each kind of decision may carry. Every reason belongs to exactly one kind.
# Tuples, not sets, so parametrized cases come out in the same order on every run.
ALLOWED_REASONS = {
    D.TERMINAL_RESULT_CANDIDATE: (R.DURABLE_STATE_NOT_TERMINAL,),
    D.INCONCLUSIVE: (R.LOCAL_TIMEOUT, R.SERVICE_UNAVAILABLE, R.TRANSPORT_ERROR,
                     R.NONTERMINAL_RESULT_STATUS),
    D.SERVER_UNKNOWN: (R.RESULT_UNKNOWN,),
    D.STATUS_ONLY: (R.STATUS_IS_ADVISORY,),
    D.EVIDENCE_CONFLICT: (R.RESULT_CONTRADICTS_DURABLE_STATE,),
    D.MISSING_DISPATCH_IDENTITY: (R.NO_DURABLE_DISPATCH_ID,),
    D.ALREADY_SETTLED: (R.RESULT_MATCHES_DURABLE_STATE,),
    D.UNCORRELATED: (R.CORRELATION_MISMATCH,),
}
ALLOWED_PAIRS = [(kind, reason) for kind, reasons in ALLOWED_REASONS.items()
                 for reason in reasons]
DISALLOWED_PAIRS = [(kind, reason) for kind in D for reason in R
                    if reason not in ALLOWED_REASONS[kind]]

# Ways an observation can fail to match the dispatch and the expected action.
MISMATCHES = {
    'wrong-uuid': {'dispatch_id': OTHER_DISPATCH},
    'wrong-action-name': {'action_name': '/follow_path'},
    'wrong-action-type': {'action_type': 'nav2_msgs/action/FollowPath'},
    'wrong-namespace': {'robot_namespace': '/robot2'},
    'absent-uuid': {'dispatch_id': None},
    'absent-action-name': {'action_name': None},
    'absent-action-type': {'action_type': None},
    'absent-namespace': {'robot_namespace': None},
}


def snapshot(step_state=S.DISPATCH_PREPARED, mission_state=M.NEEDS_RECONCILIATION,
             dispatch_id=DISPATCH):
    return DurableSnapshot(
        mission_state=mission_state, step_state=step_state, dispatch_id=dispatch_id)


def observe(kind, status=None, **fields):
    """Build an observation that correlates, unless a field is overridden."""
    values = {'dispatch_id': DISPATCH, 'action_name': ACTION_NAME, 'action_type': ACTION_TYPE,
              'robot_namespace': NAMESPACE}
    values.update(fields)
    return RecoveryObservation(kind=kind, status=status, **values)


def decide(step_state=S.DISPATCH_PREPARED, spec=(K.LOCAL_TIMEOUT, None), *,
           mission_state=M.NEEDS_RECONCILIATION, durable_dispatch_id=DISPATCH,
           expected=EXPECTED, **fields):
    """Decide for a correlated observation; extra keywords override observation fields."""
    kind, status = spec
    return decide_reconciliation(
        snapshot(step_state, mission_state, durable_dispatch_id), expected,
        observe(kind, status, **fields))


def spec_id(spec):
    kind, status = spec
    return f'{kind.name}-{status.name if status else "none"}'


def pair_id(pair):
    kind, reason = pair
    return f'{kind.name}-{reason.name}'


def build(kind, reason):
    """Build a decision, giving a terminal candidate exactly when the kind needs one."""
    candidate = S.SUCCEEDED if kind is D.TERMINAL_RESULT_CANDIDATE else None
    return ReconciliationDecision(kind=kind, reason=reason, candidate_step_state=candidate)


def every_decision():
    """Yield a decision for each step state and observation, correlated or not."""
    for step_state in S:
        for spec in ALL_SPECS:
            yield decide(step_state, spec)
            yield decide(step_state, spec, durable_dispatch_id=None)
            for fields in MISMATCHES.values():
                yield decide(step_state, spec, **fields)


# Persisted-looking values: they may end up in logs, so they do not change silently

def test_public_enum_values_are_pinned():
    assert {kind.name: kind.value for kind in D} == {
        'TERMINAL_RESULT_CANDIDATE': 'TERMINAL_RESULT_CANDIDATE',
        'INCONCLUSIVE': 'INCONCLUSIVE',
        'SERVER_UNKNOWN': 'SERVER_UNKNOWN',
        'STATUS_ONLY': 'STATUS_ONLY',
        'EVIDENCE_CONFLICT': 'EVIDENCE_CONFLICT',
        'MISSING_DISPATCH_IDENTITY': 'MISSING_DISPATCH_IDENTITY',
        'ALREADY_SETTLED': 'ALREADY_SETTLED',
        'UNCORRELATED': 'UNCORRELATED',
    }
    assert {reason.name: reason.value for reason in R} == {
        name: name for name in (
            'NO_DURABLE_DISPATCH_ID', 'CORRELATION_MISMATCH', 'LOCAL_TIMEOUT',
            'SERVICE_UNAVAILABLE', 'TRANSPORT_ERROR', 'NONTERMINAL_RESULT_STATUS',
            'STATUS_IS_ADVISORY', 'RESULT_UNKNOWN', 'DURABLE_STATE_NOT_TERMINAL',
            'RESULT_MATCHES_DURABLE_STATE', 'RESULT_CONTRADICTS_DURABLE_STATE')
    }
    assert {kind.name for kind in K} == {
        'RESULT_RESPONSE', 'STATUS_SAMPLE', 'LOCAL_TIMEOUT', 'SERVICE_UNAVAILABLE',
        'TRANSPORT_ERROR'}
    assert {status.name for status in A} == {
        'UNKNOWN', 'ACCEPTED', 'EXECUTING', 'CANCELING', 'SUCCEEDED', 'CANCELED', 'ABORTED'}


def test_the_test_matrix_covers_every_status_and_observation_kind():
    assert set(ALL_STATUSES) == set(A)
    assert {kind for kind, _ in ALL_SPECS} == set(K)
    assert set(SETTLED_STEPS + UNSETTLED_STEPS) == set(S)
    assert set(UNSETTLING_RESULTS) | set(IMPLIED_STEP_STATE) == set(A)


def test_every_decision_kind_and_reason_is_reached():
    decisions = list(every_decision())

    assert {decision.kind for decision in decisions} == set(D)
    assert {decision.reason for decision in decisions} == set(R)


# Correlation

@pytest.mark.parametrize('mismatch', list(MISMATCHES))
@pytest.mark.parametrize('spec', ALL_SPECS, ids=spec_id)
def test_an_observation_that_does_not_match_is_uncorrelated(spec, mismatch):
    decision = decide(S.OUTCOME_UNKNOWN, spec, **MISMATCHES[mismatch])

    assert decision == ReconciliationDecision(
        kind=D.UNCORRELATED, reason=R.CORRELATION_MISMATCH)


def test_an_observation_that_matches_nothing_is_uncorrelated():
    decision = decide(
        S.OUTCOME_UNKNOWN, (K.RESULT_RESPONSE, A.SUCCEEDED), dispatch_id=OTHER_DISPATCH,
        action_name='x', action_type='y', robot_namespace='z')

    assert decision.kind is D.UNCORRELATED


@pytest.mark.parametrize('settled', SETTLED_STEPS)
@pytest.mark.parametrize('reported', [A.SUCCEEDED, A.CANCELED, A.ABORTED])
def test_a_wrong_dispatch_never_reaches_the_terminal_comparison(settled, reported):
    decision = decide(settled, (K.RESULT_RESPONSE, reported), dispatch_id=OTHER_DISPATCH)

    assert decision.kind is D.UNCORRELATED
    assert decision.candidate_step_state is None


@pytest.mark.parametrize('reported', [
    'robot1', '/robot1/', '/Robot1', ' /robot1', '/robot1 ', '',
])
def test_the_namespace_is_compared_exactly(reported):
    decision = decide(spec=(K.RESULT_RESPONSE, A.SUCCEEDED), robot_namespace=reported)

    assert decision.kind is D.UNCORRELATED


def test_the_empty_and_root_namespaces_are_different_exact_values():
    empty = ExpectedActionContext(action_name=ACTION_NAME, action_type=ACTION_TYPE,
                                  robot_namespace='')
    root = ExpectedActionContext(action_name=ACTION_NAME, action_type=ACTION_TYPE,
                                 robot_namespace='/')
    spec = (K.RESULT_RESPONSE, A.SUCCEEDED)

    assert decide(spec=spec, expected=empty, robot_namespace='').kind is (
        D.TERMINAL_RESULT_CANDIDATE)
    assert decide(spec=spec, expected=empty, robot_namespace='/').kind is D.UNCORRELATED
    assert decide(spec=spec, expected=root, robot_namespace='').kind is D.UNCORRELATED


@pytest.mark.parametrize('step_state', list(S))
@pytest.mark.parametrize('spec', ALL_SPECS, ids=spec_id)
def test_without_a_durable_dispatch_id_nothing_can_be_correlated(spec, step_state):
    decision = decide(step_state, spec, durable_dispatch_id=None)

    assert decision == ReconciliationDecision(
        kind=D.MISSING_DISPATCH_IDENTITY, reason=R.NO_DURABLE_DISPATCH_ID)


def test_two_absent_dispatch_ids_do_not_match_each_other():
    # None == None is true, and must not pass for a correlation.
    observation = observe(K.RESULT_RESPONSE, A.SUCCEEDED, dispatch_id=None)

    decision = decide_reconciliation(snapshot(dispatch_id=None), EXPECTED, observation)

    assert decision.kind is D.MISSING_DISPATCH_IDENTITY
    assert decision.candidate_step_state is None


def test_a_missing_dispatch_id_is_reported_before_a_correlation_mismatch():
    decision = decide(spec=(K.STATUS_SAMPLE, A.SUCCEEDED), durable_dispatch_id=None,
                      dispatch_id=OTHER_DISPATCH, action_name='x', action_type='y',
                      robot_namespace='z')

    assert decision.kind is D.MISSING_DISPATCH_IDENTITY


# Result responses

@pytest.mark.parametrize('step_state', list(S))
@pytest.mark.parametrize('reported', list(UNSETTLING_RESULTS))
def test_unknown_and_nonterminal_results_do_not_depend_on_the_step_state(reported, step_state):
    kind, reason = UNSETTLING_RESULTS[reported]

    decision = decide(step_state, (K.RESULT_RESPONSE, reported))

    assert decision == ReconciliationDecision(kind=kind, reason=reason)


@pytest.mark.parametrize('step_state', UNSETTLED_STEPS)
@pytest.mark.parametrize('reported', list(IMPLIED_STEP_STATE))
def test_a_terminal_result_is_a_candidate_while_the_step_is_unsettled(reported, step_state):
    decision = decide(step_state, (K.RESULT_RESPONSE, reported))

    assert decision == ReconciliationDecision(
        kind=D.TERMINAL_RESULT_CANDIDATE, reason=R.DURABLE_STATE_NOT_TERMINAL,
        candidate_step_state=IMPLIED_STEP_STATE[reported])


@pytest.mark.parametrize(('step_state', 'reported'), list(SETTLED_VS_RESULT))
def test_a_terminal_result_is_compared_with_a_settled_step(step_state, reported):
    kind = SETTLED_VS_RESULT[(step_state, reported)]
    reason = (R.RESULT_MATCHES_DURABLE_STATE if kind is D.ALREADY_SETTLED
              else R.RESULT_CONTRADICTS_DURABLE_STATE)

    decision = decide(step_state, (K.RESULT_RESPONSE, reported))

    assert decision == ReconciliationDecision(kind=kind, reason=reason)
    assert decision.candidate_step_state is None


@pytest.mark.parametrize('status', ALL_STATUSES)
@pytest.mark.parametrize('code', [None, 0, 1, 65535, -1])
@pytest.mark.parametrize('message', [None, '', 'goal reached'])
def test_the_result_code_and_message_are_opaque(message, code, status):
    for step_state in (S.OUTCOME_UNKNOWN, S.SUCCEEDED):
        plain = decide(step_state, (K.RESULT_RESPONSE, status))

        described = decide(step_state, (K.RESULT_RESPONSE, status),
                           result_code=code, result_message=message)

        assert described == plain


def test_an_unknown_result_with_code_zero_is_still_unknown():
    decision = decide(S.DISPATCH_PREPARED, (K.RESULT_RESPONSE, A.UNKNOWN), result_code=0,
                      result_message='')

    assert decision.kind is D.SERVER_UNKNOWN
    assert decision.candidate_step_state is None


# Observations that are not results

@pytest.mark.parametrize('step_state', list(S))
@pytest.mark.parametrize('kind', list(NON_RESULT_REASONS))
def test_a_timeout_an_unavailable_service_and_a_transport_error_are_inconclusive(
        kind, step_state):
    decision = decide(step_state, (kind, None))

    assert decision == ReconciliationDecision(
        kind=D.INCONCLUSIVE, reason=NON_RESULT_REASONS[kind])


@pytest.mark.parametrize('step_state', list(S))
@pytest.mark.parametrize('status', ALL_STATUSES)
def test_any_status_sample_is_status_only(status, step_state):
    decision = decide(step_state, (K.STATUS_SAMPLE, status))

    assert decision == ReconciliationDecision(
        kind=D.STATUS_ONLY, reason=R.STATUS_IS_ADVISORY)


@pytest.mark.parametrize('settled', SETTLED_STEPS)
@pytest.mark.parametrize('sampled', ALL_STATUSES)
def test_a_status_sample_that_contradicts_a_settled_step_is_not_a_conflict(sampled, settled):
    decision = decide(settled, (K.STATUS_SAMPLE, sampled))

    assert decision.kind is D.STATUS_ONLY


# The Task 006 patterns

def test_a_prepared_step_with_a_local_timeout_is_inconclusive():
    assert decide(S.DISPATCH_PREPARED, (K.LOCAL_TIMEOUT, None)).kind is D.INCONCLUSIVE


def test_a_prepared_step_with_an_unknown_result_and_error_code_zero_is_server_unknown():
    decision = decide(S.DISPATCH_PREPARED, (K.RESULT_RESPONSE, A.UNKNOWN), result_code=0,
                      result_message='')

    assert decision.kind is D.SERVER_UNKNOWN


def test_a_prepared_step_with_a_succeeded_status_sample_is_status_only():
    assert decide(S.DISPATCH_PREPARED, (K.STATUS_SAMPLE, A.SUCCEEDED)).kind is D.STATUS_ONLY


def test_a_succeeded_result_for_a_prepared_step_is_a_candidate_without_using_the_edge(
        monkeypatch):
    # The ordinary graph has no DISPATCH_PREPARED -> SUCCEEDED edge, and this decision
    # must not depend on one.
    with pytest.raises(InvalidStateTransition):
        transition_step(S.DISPATCH_PREPARED, S.SUCCEEDED)

    def refuse(*args):
        raise AssertionError('the ordinary transition was consulted')

    monkeypatch.setattr(state_machine, 'transition_step', refuse)
    monkeypatch.setattr(state_machine, 'transition_mission', refuse)

    decision = decide(S.DISPATCH_PREPARED, (K.RESULT_RESPONSE, A.SUCCEEDED))

    assert decision.kind is D.TERMINAL_RESULT_CANDIDATE
    assert decision.candidate_step_state is S.SUCCEEDED


def test_an_unresolved_step_can_receive_a_terminal_candidate():
    decision = decide(S.OUTCOME_UNKNOWN, (K.RESULT_RESPONSE, A.CANCELED))

    assert decision.kind is D.TERMINAL_RESULT_CANDIDATE
    assert decision.candidate_step_state is S.CANCELLED


# The mission state is carried, not consulted

@pytest.mark.parametrize('mission_state', list(M))
def test_the_decision_does_not_depend_on_the_mission_state(mission_state):
    for step_state in S:
        for spec in ALL_SPECS:
            assert decide(step_state, spec, mission_state=mission_state) == decide(
                step_state, spec, mission_state=M.NEEDS_RECONCILIATION)


# Type boundaries

@pytest.mark.parametrize('bad', ['RUNNING', None, 3, S.PENDING])
def test_a_snapshot_needs_a_mission_state(bad):
    with pytest.raises(TypeError):
        DurableSnapshot(mission_state=bad, step_state=S.PENDING, dispatch_id=DISPATCH)


@pytest.mark.parametrize('bad', ['PENDING', None, 3, M.RUNNING])
def test_a_snapshot_needs_a_step_state(bad):
    with pytest.raises(TypeError):
        DurableSnapshot(mission_state=M.RUNNING, step_state=bad, dispatch_id=DISPATCH)


@pytest.mark.parametrize('bad', [str(DISPATCH), DISPATCH.hex, DISPATCH.bytes, DISPATCH.int])
def test_a_snapshot_does_not_coerce_a_dispatch_id(bad):
    with pytest.raises(TypeError):
        DurableSnapshot(mission_state=M.RUNNING, step_state=S.PENDING, dispatch_id=bad)


def test_a_snapshot_must_say_whether_it_has_a_dispatch_id():
    with pytest.raises(TypeError):
        DurableSnapshot(mission_state=M.RUNNING, step_state=S.PENDING)


@pytest.mark.parametrize('field', ['mission_id', 'step_key'])
def test_the_diagnostic_fields_are_strings_or_absent(field):
    good = {'mission_state': M.RUNNING, 'step_state': S.PENDING, 'dispatch_id': None}

    assert DurableSnapshot(**good, **{field: 'x'}).dispatch_id is None
    with pytest.raises(TypeError):
        DurableSnapshot(**good, **{field: 5})


def test_the_inputs_are_keyword_only():
    with pytest.raises(TypeError):
        DurableSnapshot(M.RUNNING, S.PENDING, None)
    with pytest.raises(TypeError):
        ExpectedActionContext('/a', 'b/C', '/r')
    with pytest.raises(TypeError):
        RecoveryObservation(K.LOCAL_TIMEOUT)


@pytest.mark.parametrize('field', ['action_name', 'action_type', 'robot_namespace'])
@pytest.mark.parametrize('bad', [None, b'/x', 1, ['/x']])
def test_the_expected_context_takes_strings_only(field, bad):
    values = {'action_name': '/a', 'action_type': 'b/C', 'robot_namespace': '/r'}
    values[field] = bad

    with pytest.raises(TypeError):
        ExpectedActionContext(**values)


@pytest.mark.parametrize('field', ['action_name', 'action_type'])
def test_the_expected_action_needs_a_name_and_a_type(field):
    values = {'action_name': '/a', 'action_type': 'b/C', 'robot_namespace': '/r'}
    values[field] = ''

    with pytest.raises(ValueError):
        ExpectedActionContext(**values)


@pytest.mark.parametrize('bad', ['RESULT_RESPONSE', None, 1, A.UNKNOWN])
def test_an_observation_needs_an_observation_kind(bad):
    with pytest.raises(TypeError):
        RecoveryObservation(kind=bad)


@pytest.mark.parametrize('bad', ['SUCCEEDED', 4, S.SUCCEEDED])
def test_an_observation_does_not_coerce_a_status(bad):
    with pytest.raises(TypeError):
        RecoveryObservation(kind=K.RESULT_RESPONSE, status=bad)


@pytest.mark.parametrize('bad', [str(DISPATCH), DISPATCH.hex, DISPATCH.bytes])
def test_an_observation_does_not_coerce_a_dispatch_id(bad):
    with pytest.raises(TypeError):
        RecoveryObservation(kind=K.LOCAL_TIMEOUT, dispatch_id=bad)


@pytest.mark.parametrize('field', ['action_name', 'action_type', 'robot_namespace'])
@pytest.mark.parametrize('bad', [b'/x', 1, ['/x']])
def test_an_observation_takes_strings_or_absence_for_correlation(field, bad):
    with pytest.raises(TypeError):
        RecoveryObservation(kind=K.LOCAL_TIMEOUT, **{field: bad})


@pytest.mark.parametrize('bad', [True, False, '0', 0.0, b'0'])
def test_a_result_code_is_an_integer(bad):
    with pytest.raises(TypeError):
        RecoveryObservation(kind=K.RESULT_RESPONSE, status=A.UNKNOWN, result_code=bad)


def test_a_result_message_is_a_string():
    with pytest.raises(TypeError):
        RecoveryObservation(kind=K.RESULT_RESPONSE, status=A.UNKNOWN, result_message=5)


@pytest.mark.parametrize('kind', [K.RESULT_RESPONSE, K.STATUS_SAMPLE])
def test_result_and_status_observations_need_a_status(kind):
    with pytest.raises(ValueError):
        RecoveryObservation(kind=kind)


@pytest.mark.parametrize('kind', [K.LOCAL_TIMEOUT, K.SERVICE_UNAVAILABLE, K.TRANSPORT_ERROR])
def test_observations_that_heard_nothing_carry_no_status_or_result(kind):
    with pytest.raises(ValueError):
        RecoveryObservation(kind=kind, status=A.UNKNOWN)
    with pytest.raises(ValueError):
        RecoveryObservation(kind=kind, result_code=0)
    with pytest.raises(ValueError):
        RecoveryObservation(kind=kind, result_message='')


def test_a_status_sample_carries_no_result():
    with pytest.raises(ValueError):
        RecoveryObservation(kind=K.STATUS_SAMPLE, status=A.SUCCEEDED, result_code=0)
    with pytest.raises(ValueError):
        RecoveryObservation(kind=K.STATUS_SAMPLE, status=A.SUCCEEDED, result_message='')


@pytest.mark.parametrize('arguments', [
    (None, EXPECTED, observe(K.LOCAL_TIMEOUT)),
    (snapshot(), None, observe(K.LOCAL_TIMEOUT)),
    (snapshot(), EXPECTED, None),
    (EXPECTED, snapshot(), observe(K.LOCAL_TIMEOUT)),
    (snapshot(), EXPECTED, snapshot()),
    ({'step_state': S.PENDING}, EXPECTED, observe(K.LOCAL_TIMEOUT)),
    (snapshot(), 'context', observe(K.LOCAL_TIMEOUT)),
], ids=['no-snapshot', 'no-expected', 'no-observation', 'swapped', 'wrong-third', 'dict',
        'string'])
def test_the_decision_rejects_arguments_of_the_wrong_type(arguments):
    with pytest.raises(TypeError):
        decide_reconciliation(*arguments)


def test_a_decision_needs_enum_values():
    with pytest.raises(TypeError):
        ReconciliationDecision(kind='INCONCLUSIVE', reason=R.LOCAL_TIMEOUT)
    with pytest.raises(TypeError):
        ReconciliationDecision(kind=D.INCONCLUSIVE, reason='LOCAL_TIMEOUT')


# The candidate is tested with valid kind and reason pairs, so it is the candidate rule that
# fails and not the reason rule.

@pytest.mark.parametrize(('kind', 'reason'), ALLOWED_PAIRS,
                         ids=[pair_id(pair) for pair in ALLOWED_PAIRS])
def test_a_candidate_belongs_to_a_terminal_result_candidate_only(kind, reason):
    if kind is D.TERMINAL_RESULT_CANDIDATE:
        with pytest.raises(ValueError, match='candidate_step_state'):
            ReconciliationDecision(kind=kind, reason=reason)
    else:
        with pytest.raises(ValueError, match='candidate_step_state'):
            ReconciliationDecision(kind=kind, reason=reason, candidate_step_state=S.SUCCEEDED)


@pytest.mark.parametrize('candidate', UNSETTLED_STEPS)
def test_a_candidate_must_be_a_terminal_step_state(candidate):
    with pytest.raises(ValueError, match='terminal'):
        ReconciliationDecision(kind=D.TERMINAL_RESULT_CANDIDATE,
                               reason=R.DURABLE_STATE_NOT_TERMINAL,
                               candidate_step_state=candidate)


# Kind and reason

def test_the_allowed_relation_gives_every_reason_to_exactly_one_kind():
    assert set(ALLOWED_REASONS) == set(D)
    reasons = [reason for kind in D for reason in ALLOWED_REASONS[kind]]
    assert sorted(reason.name for reason in reasons) == sorted(reason.name for reason in R)
    assert len(ALLOWED_PAIRS) == 11
    assert len(DISALLOWED_PAIRS) == len(D) * len(R) - 11


@pytest.mark.parametrize(('kind', 'reason'), ALLOWED_PAIRS,
                         ids=[pair_id(pair) for pair in ALLOWED_PAIRS])
def test_an_allowed_kind_and_reason_can_be_built(kind, reason):
    decision = build(kind, reason)

    assert decision.kind is kind
    assert decision.reason is reason
    assert decision.authorizes_send is False


@pytest.mark.parametrize(('kind', 'reason'), DISALLOWED_PAIRS,
                         ids=[pair_id(pair) for pair in DISALLOWED_PAIRS])
def test_a_reason_that_does_not_belong_to_the_kind_is_rejected(kind, reason):
    # The candidate is right for the kind, so only the reason can be at fault.
    with pytest.raises(ValueError, match=reason.name):
        build(kind, reason)


@pytest.mark.parametrize(('kind', 'reason', 'candidate'), [
    (D.SERVER_UNKNOWN, R.LOCAL_TIMEOUT, None),
    (D.INCONCLUSIVE, R.RESULT_UNKNOWN, None),
    (D.TERMINAL_RESULT_CANDIDATE, R.LOCAL_TIMEOUT, S.SUCCEEDED),
    (D.EVIDENCE_CONFLICT, R.RESULT_MATCHES_DURABLE_STATE, None),
], ids=['unknown-with-timeout', 'inconclusive-with-unknown', 'candidate-with-timeout',
        'conflict-with-match'])
def test_reasons_that_describe_another_decision_are_rejected(kind, reason, candidate):
    with pytest.raises(ValueError):
        ReconciliationDecision(kind=kind, reason=reason, candidate_step_state=candidate)


def test_replacing_the_reason_cannot_break_the_relation():
    decision = decide(spec=(K.LOCAL_TIMEOUT, None), dispatch_id=OTHER_DISPATCH)
    assert decision.kind is D.UNCORRELATED

    with pytest.raises(ValueError):
        dataclasses.replace(decision, reason=R.LOCAL_TIMEOUT)
    with pytest.raises(ValueError):
        dataclasses.replace(decision, kind=D.INCONCLUSIVE)


def test_every_decision_that_is_returned_satisfies_the_relation():
    decisions = list(every_decision())

    assert len(decisions) > 500
    for decision in decisions:
        assert decision.reason in ALLOWED_REASONS[decision.kind]
        assert (decision.candidate_step_state is not None) == (
            decision.kind is D.TERMINAL_RESULT_CANDIDATE)


# Immutability

def test_the_inputs_and_the_decision_are_frozen():
    frozen = [
        (snapshot(), 'step_state', S.SUCCEEDED),
        (EXPECTED, 'action_name', '/other'),
        (observe(K.LOCAL_TIMEOUT), 'dispatch_id', OTHER_DISPATCH),
        (decide(), 'kind', D.ALREADY_SETTLED),
    ]
    for instance, name, value in frozen:
        with pytest.raises(dataclasses.FrozenInstanceError):
            setattr(instance, name, value)
        with pytest.raises(dataclasses.FrozenInstanceError):
            setattr(instance, 'something_new', value)
        assert hash(instance) == hash(dataclasses.replace(instance))


def test_deciding_does_not_change_its_inputs():
    inputs = (snapshot(S.OUTCOME_UNKNOWN), EXPECTED,
              observe(K.RESULT_RESPONSE, A.SUCCEEDED, result_code=0, result_message='ok'))
    before = [(repr(value), hash(value)) for value in inputs]

    first = decide_reconciliation(*inputs)
    second = decide_reconciliation(*inputs)

    assert [(repr(value), hash(value)) for value in inputs] == before
    assert first == second
    assert inputs[0].step_state is S.OUTCOME_UNKNOWN


# No send is ever authorized

def test_no_decision_authorizes_a_send():
    decisions = list(every_decision())

    assert len(decisions) > 500
    assert all(decision.authorizes_send is False for decision in decisions)


def test_authorizes_send_cannot_be_supplied_or_changed():
    decision = decide(spec=(K.RESULT_RESPONSE, A.SUCCEEDED))

    with pytest.raises(TypeError):
        ReconciliationDecision(kind=D.INCONCLUSIVE, reason=R.LOCAL_TIMEOUT,
                               authorizes_send=True)
    with pytest.raises(TypeError):
        dataclasses.replace(decision, authorizes_send=True)
    with pytest.raises(dataclasses.FrozenInstanceError):
        decision.authorizes_send = True
    with pytest.raises(AttributeError):
        object.__setattr__(decision, 'authorizes_send', True)
    assert 'authorizes_send' not in {field.name for field in dataclasses.fields(decision)}
    assert decision.authorizes_send is False


# Boundaries

def test_the_module_imports_only_the_standard_library_and_the_state_definitions():
    tree = ast.parse(Path(reconciliation.__file__).read_text())
    plain = {alias.name for node in ast.walk(tree) if isinstance(node, ast.Import)
             for alias in node.names}
    from_imports = {(node.level, node.module): {alias.name for alias in node.names}
                    for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)}

    assert plain == {'enum', 'uuid'}
    # Only the state definitions and the terminal test: not the transition functions,
    # which would make legality of an ordinary edge part of this decision.
    assert from_imports == {
        (0, 'dataclasses'): {'dataclass'},
        (1, 'state_machine'): {'is_step_terminal', 'MissionState', 'StepState'},
    }


def package_env():
    package_root = Path(task_resilience.__file__).resolve().parent.parent
    return {**os.environ, 'PYTHONPATH': str(package_root)}


def test_importing_the_module_loads_no_ros_storage_network_or_llm_package():
    code = (
        'import sys\n'
        'import task_resilience.reconciliation\n'
        "banned = {'rclpy', 'rcl_interfaces', 'rosidl_runtime_py', 'ament_index_python',\n"
        "          'action_msgs', 'unique_identifier_msgs', 'nav2_msgs', 'geometry_msgs',\n"
        "          'builtin_interfaces', 'sqlite3', 'http', 'urllib', 'ssl', 'requests',\n"
        "          'httpx', 'anthropic', 'openai'}\n"
        "loaded = sorted(m for m in sys.modules if m.split('.')[0] in banned)\n"
        "loaded += [m for m in ('task_resilience.storage', 'task_resilience.model')\n"
        '           if m in sys.modules]\n'
        'print(loaded)\n')

    child = subprocess.run(
        [sys.executable, '-c', code], env=package_env(), capture_output=True, text=True,
        check=True, timeout=30)

    assert child.stdout.strip() == '[]'
