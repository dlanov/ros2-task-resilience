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

import contextlib
import os
from pathlib import Path
import sqlite3
import subprocess
import sys

import pytest

import task_resilience
from task_resilience import storage
from task_resilience.errors import (
    AttemptLimitReached,
    CorruptStoredState,
    DispatchPreparationRequired,
    InvalidStateTransition,
    MissionCompletionInvariantError,
    MissionNotFound,
    SendAuthorizationRequired,
    StepNotFound,
)
from task_resilience.reconciliation import ExpectedActionContext
from task_resilience.state_machine import MissionState, StepState
from task_resilience.storage import MissionStore

# The action every dispatch in these tests is declared to reach.
ACTION = ExpectedActionContext(
    action_name='/navigate_to_pose', action_type='nav2_msgs/action/NavigateToPose',
    robot_namespace='/robot1')

M = MissionState
S = StepState

KEYS = ['checkpoint_a', 'return_to_base']

# An ordinary route from PENDING to each step state.
ROUTE_TO = {
    S.PENDING: [],
    S.DISPATCH_PREPARED: [S.DISPATCH_PREPARED],
    S.DISPATCHED: [S.DISPATCH_PREPARED, S.DISPATCHED],
    S.ACCEPTED: [S.DISPATCH_PREPARED, S.DISPATCHED, S.ACCEPTED],
    S.EXECUTING: [S.DISPATCH_PREPARED, S.DISPATCHED, S.ACCEPTED, S.EXECUTING],
    S.SUCCEEDED: [S.DISPATCH_PREPARED, S.DISPATCHED, S.ACCEPTED, S.EXECUTING, S.SUCCEEDED],
    S.FAILED: [S.DISPATCH_PREPARED, S.FAILED],
    S.CANCEL_REQUESTED: [S.DISPATCH_PREPARED, S.DISPATCHED, S.ACCEPTED, S.CANCEL_REQUESTED],
    S.CANCELLED: [S.CANCELLED],
    S.OUTCOME_UNKNOWN: [S.DISPATCH_PREPARED, S.OUTCOME_UNKNOWN],
}


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / 'missions.db'


@pytest.fixture
def store(db_path):
    with MissionStore(db_path) as opened:
        yield opened


@pytest.fixture
def mission(store):
    """Create an accepted mission with two pending steps."""
    created = store.create_or_get('req-001', {'goal': 'demo'}, 3)
    store.initialize_steps(created.mission_id, KEYS)
    return created


@pytest.fixture
def running(store, mission):
    return store.transition_mission_state(mission.mission_id, M.RUNNING)


def drive(store, mission_id, key, state):
    for step in ROUTE_TO[state]:
        if step is S.DISPATCH_PREPARED:
            # A plain transition cannot enter this state...
            store.prepare_step_dispatch(mission_id, key, ACTION)
        elif step is S.DISPATCHED:
            # ...nor this one. It takes authorizing the send, which needs a RUNNING mission
            # and an attempt.
            store.authorize_step_send(mission_id, key)
        else:
            store.transition_step_state(mission_id, key, step)


def corrupt(db_path, sql):
    with contextlib.closing(sqlite3.connect(db_path, isolation_level=None)) as conn:
        conn.execute('PRAGMA ignore_check_constraints = ON')
        conn.execute(sql)


def test_every_step_state_has_a_route():
    assert set(ROUTE_TO) == set(S)


# Missions

def test_new_mission_starts_accepted(store):
    assert store.create_or_get('req-001', {}, 1).state is M.ACCEPTED


def test_replaying_a_request_returns_the_current_state_not_accepted(store, mission, running):
    replay = store.create_or_get('req-001', {'goal': 'demo'}, 3)

    assert replay.mission_id == mission.mission_id
    assert replay.state is M.RUNNING


def test_mission_state_change_is_persisted_and_stamped(store, mission):
    updated = store.transition_mission_state(mission.mission_id, M.RUNNING)

    assert updated.state is M.RUNNING
    assert updated.updated_at > mission.updated_at
    assert updated.created_at == mission.created_at
    assert updated.request_fingerprint == mission.request_fingerprint
    assert updated.attempts_used == mission.attempts_used
    assert store.get_by_mission_id(mission.mission_id) == updated
    assert store.get_by_request_id('req-001') == updated


def test_mission_same_state_is_a_no_op_that_keeps_updated_at(store, running):
    replay = store.transition_mission_state(running.mission_id, M.RUNNING)

    assert replay == running
    assert store.get_by_mission_id(running.mission_id) == running


def test_illegal_mission_transitions_are_rejected_and_write_nothing(store, mission):
    for target in (M.COMPLETED, M.FAILED, M.CANCELLING, M.NEEDS_RECONCILIATION):
        with pytest.raises(InvalidStateTransition) as excinfo:
            store.transition_mission_state(mission.mission_id, target)
        assert excinfo.value.entity == 'mission'
        assert excinfo.value.current is M.ACCEPTED
        assert excinfo.value.target is target

    assert store.get_by_mission_id(mission.mission_id) == mission


def test_a_finished_mission_cannot_be_restarted(store, mission):
    store.transition_mission_state(mission.mission_id, M.CANCELLED)

    with pytest.raises(InvalidStateTransition):
        store.transition_mission_state(mission.mission_id, M.RUNNING)
    assert store.get_by_mission_id(mission.mission_id).state is M.CANCELLED


@pytest.mark.parametrize('target', ['RUNNING', None, S.PENDING, 1])
def test_mission_target_must_be_a_mission_state(store, mission, target):
    with pytest.raises(TypeError):
        store.transition_mission_state(mission.mission_id, target)

    assert store.get_by_mission_id(mission.mission_id) == mission


def test_wrong_target_types_fail_before_the_database_is_touched(db_path):
    with MissionStore(db_path, busy_timeout=0.05) as store:
        blocker = sqlite3.connect(db_path, isolation_level=None)
        blocker.execute('BEGIN IMMEDIATE')
        try:
            # Neither the mission nor the lock matters: this is a caller bug.
            with pytest.raises(TypeError):
                store.transition_mission_state('no-such-mission', 'RUNNING')
            with pytest.raises(TypeError):
                store.transition_step_state('no-such-mission', 'checkpoint_a', 'PENDING')
        finally:
            blocker.execute('ROLLBACK')
            blocker.close()


def test_unknown_mission_cannot_change_state(store):
    with pytest.raises(MissionNotFound):
        store.transition_mission_state('no-such-mission', M.RUNNING)


# Steps

def test_step_state_change_is_persisted_and_stamped(store, running):
    drive(store, running.mission_id, 'checkpoint_a', S.DISPATCHED)
    before = store.get_step(running.mission_id, 'checkpoint_a')

    updated = store.transition_step_state(running.mission_id, 'checkpoint_a', S.ACCEPTED)

    assert updated.state is S.ACCEPTED
    assert updated.updated_at > before.updated_at
    assert updated.created_at == before.created_at
    assert updated.step_index == before.step_index
    assert store.get_step(running.mission_id, 'checkpoint_a') == updated
    assert store.get_step(running.mission_id, 'return_to_base').state is S.PENDING


def test_step_same_state_is_a_no_op_that_keeps_updated_at(store, running):
    drive(store, running.mission_id, 'checkpoint_a', S.ACCEPTED)
    moved = store.get_step(running.mission_id, 'checkpoint_a')

    replay = store.transition_step_state(running.mission_id, 'checkpoint_a', S.ACCEPTED)

    assert replay == moved
    assert store.get_step(running.mission_id, 'checkpoint_a') == moved


def test_illegal_step_transitions_are_rejected_and_write_nothing(store, mission):
    before = store.get_steps(mission.mission_id)

    for target in (S.SUCCEEDED, S.EXECUTING, S.OUTCOME_UNKNOWN, S.CANCEL_REQUESTED):
        with pytest.raises(InvalidStateTransition) as excinfo:
            store.transition_step_state(mission.mission_id, 'checkpoint_a', target)
        assert excinfo.value.entity == 'step'
        assert excinfo.value.current is S.PENDING
        assert excinfo.value.target is target

    assert store.get_steps(mission.mission_id) == before


def test_a_step_can_record_an_observed_result_without_intermediate_states(store, running):
    drive(store, running.mission_id, 'checkpoint_a', S.DISPATCHED)

    done = store.transition_step_state(running.mission_id, 'checkpoint_a', S.SUCCEEDED)

    assert done.state is S.SUCCEEDED


@pytest.mark.parametrize('target', ['EXECUTING', None, M.RUNNING, 1])
def test_step_target_must_be_a_step_state(store, mission, target):
    before = store.get_steps(mission.mission_id)

    with pytest.raises(TypeError):
        store.transition_step_state(mission.mission_id, 'checkpoint_a', target)

    assert store.get_steps(mission.mission_id) == before


def test_unknown_mission_or_step_cannot_change_state(store, mission):
    with pytest.raises(MissionNotFound):
        store.transition_step_state('no-such-mission', 'checkpoint_a', S.CANCELLED)
    with pytest.raises(StepNotFound) as excinfo:
        store.transition_step_state(mission.mission_id, 'no_such_step', S.CANCELLED)

    assert excinfo.value.mission_id == mission.mission_id
    assert excinfo.value.step_key == 'no_such_step'


def test_step_changes_leave_the_mission_row_alone(store, running):
    store.transition_step_state(running.mission_id, 'checkpoint_a', S.CANCELLED)

    assert store.get_by_mission_id(running.mission_id) == running


# The state machine stays the only authority

def test_storage_asks_the_state_machine_and_holds_no_transition_table(store, mission, monkeypatch):
    # If storage had its own rules, changing the state machine would not matter.
    with monkeypatch.context() as patch:
        patch.setattr(storage, 'transition_step', lambda current, target: target)
        stubbed = store.transition_step_state(mission.mission_id, 'checkpoint_a', S.SUCCEEDED)
    assert stubbed.state is S.SUCCEEDED

    with monkeypatch.context() as patch:
        patch.setattr(storage, 'transition_mission', lambda current, target, steps: target)
        assert store.transition_mission_state(mission.mission_id, M.COMPLETED).state is M.COMPLETED


def test_a_state_machine_refusal_is_propagated_and_nothing_is_written(
        store, mission, monkeypatch):
    def refuse(current, target):
        raise InvalidStateTransition('step', current, target)

    with monkeypatch.context() as patch:
        patch.setattr(storage, 'transition_step', refuse)
        with pytest.raises(InvalidStateTransition):
            store.transition_step_state(mission.mission_id, 'checkpoint_a', S.CANCELLED)

    assert store.get_step(mission.mission_id, 'checkpoint_a').state is S.PENDING


# Restart

def test_mission_and_step_state_survive_reopening_the_store(db_path):
    with MissionStore(db_path) as store:
        mission = store.create_or_get('req-001', {'goal': 'demo'}, 3)
        assert mission.state is M.ACCEPTED
        store.initialize_steps(mission.mission_id, KEYS)
        store.transition_mission_state(mission.mission_id, M.RUNNING)
        drive(store, mission.mission_id, 'checkpoint_a', S.SUCCEEDED)
        before = (store.get_by_mission_id(mission.mission_id), store.get_steps(mission.mission_id))

    with MissionStore(db_path) as reopened:
        after_mission = reopened.get_by_mission_id(mission.mission_id)
        after_steps = reopened.get_steps(mission.mission_id)

    assert after_mission.state is M.RUNNING
    assert [(step.step_key, step.step_index, step.state) for step in after_steps] == [
        ('checkpoint_a', 0, S.SUCCEEDED), ('return_to_base', 1, S.PENDING)]
    assert after_mission.attempts_used == 1
    assert (after_mission, after_steps) == before


def test_unknown_outcome_survives_reopening_the_store(db_path):
    with MissionStore(db_path) as store:
        mission = store.create_or_get('req-001', {}, 1)
        store.initialize_steps(mission.mission_id, KEYS)
        store.transition_mission_state(mission.mission_id, M.RUNNING)
        drive(store, mission.mission_id, 'checkpoint_a', S.OUTCOME_UNKNOWN)
        store.transition_mission_state(mission.mission_id, M.NEEDS_RECONCILIATION)

    with MissionStore(db_path) as reopened:
        assert reopened.get_step(mission.mission_id, 'checkpoint_a').state is S.OUTCOME_UNKNOWN
        assert reopened.get_step(mission.mission_id, 'return_to_base').state is S.PENDING
        assert reopened.get_by_mission_id(mission.mission_id).state is M.NEEDS_RECONCILIATION


_CHILD = """
import os
import sys

from task_resilience.state_machine import StepState
from task_resilience.storage import MissionStore

store = MissionStore(sys.argv[1])
store.transition_step_state(sys.argv[2], 'checkpoint_a', StepState.CANCELLED)
# Die without closing the store or running any cleanup.
os._exit(0)
"""


def test_a_state_change_survives_a_process_that_exits_abruptly(db_path):
    with MissionStore(db_path) as store:
        mission = store.create_or_get('req-001', {}, 1)
        store.initialize_steps(mission.mission_id, KEYS)

    package_root = Path(task_resilience.__file__).resolve().parent.parent
    subprocess.run(
        [sys.executable, '-c', _CHILD, str(db_path), mission.mission_id],
        env={**os.environ, 'PYTHONPATH': str(package_root)},
        check=True,
        timeout=30,
    )

    with MissionStore(db_path) as reopened:
        assert reopened.get_step(mission.mission_id, 'checkpoint_a').state is S.CANCELLED


# Completion invariant through storage

def test_mission_cannot_complete_while_a_step_is_pending(store, running):
    drive(store, running.mission_id, 'checkpoint_a', S.SUCCEEDED)
    # Authorizing the send updated the mission row, so compare with it as it is now.
    before = store.get_by_mission_id(running.mission_id)

    with pytest.raises(MissionCompletionInvariantError) as excinfo:
        store.transition_mission_state(running.mission_id, M.COMPLETED)

    assert excinfo.value.current is M.RUNNING
    assert excinfo.value.step_states == (S.SUCCEEDED, S.PENDING)
    assert store.get_by_mission_id(running.mission_id) == before


def test_mission_completes_once_every_step_has_succeeded_and_stays_completed(
        store, running, db_path):
    drive(store, running.mission_id, 'checkpoint_a', S.SUCCEEDED)
    with pytest.raises(MissionCompletionInvariantError):
        store.transition_mission_state(running.mission_id, M.COMPLETED)

    drive(store, running.mission_id, 'return_to_base', S.SUCCEEDED)
    completed = store.transition_mission_state(running.mission_id, M.COMPLETED)

    assert completed.state is M.COMPLETED
    with MissionStore(db_path) as reopened:
        assert reopened.get_by_mission_id(running.mission_id).state is M.COMPLETED


def test_mission_without_initialized_steps_cannot_complete(store):
    bare = store.create_or_get('req-bare', {}, 1)
    store.transition_mission_state(bare.mission_id, M.RUNNING)

    with pytest.raises(MissionCompletionInvariantError) as excinfo:
        store.transition_mission_state(bare.mission_id, M.COMPLETED)

    assert excinfo.value.step_states == ()
    assert store.get_by_mission_id(bare.mission_id).state is M.RUNNING


@pytest.mark.parametrize('unfinished', [state for state in S if state is not S.SUCCEEDED])
def test_any_persisted_step_that_is_not_succeeded_blocks_completion(
        store, running, unfinished):
    drive(store, running.mission_id, 'checkpoint_a', S.SUCCEEDED)
    drive(store, running.mission_id, 'return_to_base', unfinished)

    with pytest.raises(MissionCompletionInvariantError):
        store.transition_mission_state(running.mission_id, M.COMPLETED)

    assert store.get_by_mission_id(running.mission_id).state is M.RUNNING


def test_cancelling_mission_can_complete_only_if_every_step_succeeded(store, running):
    # The steps get their sends while the mission is RUNNING: a CANCELLING mission
    # authorizes none.
    drive(store, running.mission_id, 'checkpoint_a', S.SUCCEEDED)
    drive(store, running.mission_id, 'return_to_base', S.CANCEL_REQUESTED)
    store.transition_mission_state(running.mission_id, M.CANCELLING)
    with pytest.raises(MissionCompletionInvariantError):
        store.transition_mission_state(running.mission_id, M.COMPLETED)

    store.transition_step_state(running.mission_id, 'return_to_base', S.SUCCEEDED)

    assert store.transition_mission_state(running.mission_id, M.COMPLETED).state is M.COMPLETED


def test_completing_an_already_completed_mission_is_a_no_op(store, running):
    drive(store, running.mission_id, 'checkpoint_a', S.SUCCEEDED)
    drive(store, running.mission_id, 'return_to_base', S.SUCCEEDED)
    completed = store.transition_mission_state(running.mission_id, M.COMPLETED)

    assert store.transition_mission_state(running.mission_id, M.COMPLETED) == completed


# Reconciliation states are stored, and are not ways out

def test_an_unknown_step_outcome_does_not_force_the_mission_state(store, running):
    drive(store, running.mission_id, 'checkpoint_a', S.OUTCOME_UNKNOWN)

    assert store.get_by_mission_id(running.mission_id) == running

    # Choosing NEEDS_RECONCILIATION is left to the caller.
    reconciling = store.transition_mission_state(running.mission_id, M.NEEDS_RECONCILIATION)
    assert reconciling.state is M.NEEDS_RECONCILIATION


def test_no_ordinary_transition_leaves_an_unknown_step_outcome(store, running):
    drive(store, running.mission_id, 'checkpoint_a', S.OUTCOME_UNKNOWN)

    for target in S:
        if target is S.OUTCOME_UNKNOWN:
            continue
        # DISPATCH_PREPARED and DISPATCHED have their own refusals, ahead of the state machine.
        refusal = {
            S.DISPATCH_PREPARED: DispatchPreparationRequired,
            S.DISPATCHED: SendAuthorizationRequired,
        }.get(target, InvalidStateTransition)
        with pytest.raises(refusal):
            store.transition_step_state(running.mission_id, 'checkpoint_a', target)

    assert store.get_step(running.mission_id, 'checkpoint_a').state is S.OUTCOME_UNKNOWN


def test_no_ordinary_transition_leaves_needs_reconciliation(store, running):
    # Even with every step finished, the mission cannot simply be completed.
    drive(store, running.mission_id, 'checkpoint_a', S.SUCCEEDED)
    drive(store, running.mission_id, 'return_to_base', S.SUCCEEDED)
    store.transition_mission_state(running.mission_id, M.NEEDS_RECONCILIATION)

    for target in M:
        if target is not M.NEEDS_RECONCILIATION:
            with pytest.raises(InvalidStateTransition):
                store.transition_mission_state(running.mission_id, target)

    assert store.get_by_mission_id(running.mission_id).state is M.NEEDS_RECONCILIATION


def test_the_store_offers_no_way_to_bypass_the_state_machine():
    public = [name for name in dir(MissionStore) if not name.startswith('_')]

    assert not [name for name in public if any(
        word in name for word in ('reconcil', 'resolve', 'force', 'override', 'set_state'))]


# Stored values are validated on read

@pytest.mark.parametrize('bad', ['BOGUS', 'running', 'PENDING', ''])
def test_an_unknown_stored_mission_state_is_an_error_not_a_guess(store, mission, db_path, bad):
    corrupt(db_path, f"UPDATE missions SET mission_state = '{bad}'")

    with pytest.raises(CorruptStoredState) as excinfo:
        store.get_by_mission_id(mission.mission_id)

    assert excinfo.value.entity == 'mission'
    assert excinfo.value.mission_id == mission.mission_id
    assert excinfo.value.value == bad
    with pytest.raises(CorruptStoredState):
        store.get_by_request_id('req-001')
    with pytest.raises(CorruptStoredState):
        store.transition_mission_state(mission.mission_id, M.RUNNING)


def test_corruption_found_while_authorizing_a_send_leaves_nothing_behind(
        store, running, db_path):
    store.prepare_step_dispatch(running.mission_id, 'checkpoint_a', ACTION)

    corrupt(db_path, "UPDATE missions SET mission_state = 'BOGUS'")
    with pytest.raises(CorruptStoredState):
        store.authorize_step_send(running.mission_id, 'checkpoint_a')
    corrupt(db_path, "UPDATE missions SET mission_state = 'RUNNING'")

    # A different step is damaged: the check that no step is unknown has to read it.
    corrupt(db_path, "UPDATE mission_steps SET step_state = 'BOGUS'"
                     " WHERE step_key = 'return_to_base'")
    with pytest.raises(CorruptStoredState):
        store.authorize_step_send(running.mission_id, 'checkpoint_a')
    corrupt(db_path, "UPDATE mission_steps SET step_state = 'PENDING'"
                     " WHERE step_key = 'return_to_base'")

    assert store.get_by_mission_id(running.mission_id).attempts_used == 0
    assert store.get_step(running.mission_id, 'checkpoint_a').state is S.DISPATCH_PREPARED


@pytest.mark.parametrize('bad', ['BOGUS', 'executing', 'RUNNING', ''])
def test_an_unknown_stored_step_state_is_an_error_not_a_guess(store, mission, db_path, bad):
    corrupt(db_path, f"UPDATE mission_steps SET step_state = '{bad}'"
                     " WHERE step_key = 'checkpoint_a'")

    with pytest.raises(CorruptStoredState) as excinfo:
        store.get_step(mission.mission_id, 'checkpoint_a')

    assert excinfo.value.entity == 'step'
    assert excinfo.value.mission_id == mission.mission_id
    assert excinfo.value.value == bad
    with pytest.raises(CorruptStoredState):
        store.get_steps(mission.mission_id)
    with pytest.raises(CorruptStoredState):
        store.transition_step_state(mission.mission_id, 'checkpoint_a', S.CANCELLED)
    # The mission decision reads every step, so it refuses to guess as well.
    with pytest.raises(CorruptStoredState):
        store.transition_mission_state(mission.mission_id, M.RUNNING)
    assert store.get_by_mission_id(mission.mission_id).state is M.ACCEPTED


# Attempt accounting is independent

def test_only_authorizing_a_send_consumes_an_attempt(store, running):
    store.prepare_step_dispatch(running.mission_id, 'checkpoint_a', ACTION)
    assert store.get_by_mission_id(running.mission_id).attempts_used == 0

    store.authorize_step_send(running.mission_id, 'checkpoint_a')
    for step in (S.ACCEPTED, S.EXECUTING, S.SUCCEEDED):
        store.transition_step_state(running.mission_id, 'checkpoint_a', step)

    # Preparing took none, authorizing took one, and the later transitions took none.
    assert store.get_by_mission_id(running.mission_id).attempts_used == 1


def test_authorizing_a_send_does_not_change_the_mission_state(store, running):
    store.prepare_step_dispatch(running.mission_id, 'checkpoint_a', ACTION)

    receipt = store.authorize_step_send(running.mission_id, 'checkpoint_a')

    after = store.get_by_mission_id(running.mission_id)
    assert receipt.attempt_number == 1
    assert after.state is M.RUNNING
    assert after.attempts_used == 1
    assert store.get_step(running.mission_id, 'return_to_base').state is S.PENDING


def test_the_attempt_limit_still_holds_alongside_state(store):
    one_shot = store.create_or_get('req-one', {}, 1)
    store.initialize_steps(one_shot.mission_id, KEYS)
    store.transition_mission_state(one_shot.mission_id, M.RUNNING)
    for key in KEYS:
        store.prepare_step_dispatch(one_shot.mission_id, key, ACTION)
    store.authorize_step_send(one_shot.mission_id, KEYS[0])

    with pytest.raises(AttemptLimitReached):
        store.authorize_step_send(one_shot.mission_id, KEYS[1])

    assert store.get_by_mission_id(one_shot.mission_id).state is M.RUNNING
