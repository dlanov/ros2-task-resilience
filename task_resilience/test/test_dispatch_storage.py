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
from datetime import timedelta
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import uuid

from fault_injection import open_faulty_store
import pytest

import task_resilience
from task_resilience import storage
from task_resilience.errors import (
    CorruptDispatchRecord,
    DispatchNotFound,
    DispatchPreparationRequired,
    InvalidStateTransition,
    MissionNotFound,
    StepNotFound,
)
from task_resilience.model import DispatchRecord
from task_resilience.state_machine import MissionState, StepState
from task_resilience.storage import MissionStore

M = MissionState
S = StepState

KEYS = ['checkpoint_a', 'return_to_base']

# From DISPATCH_PREPARED onward.
ROUTE_AFTER_PREPARE = {
    S.DISPATCHED: [S.DISPATCHED],
    S.ACCEPTED: [S.DISPATCHED, S.ACCEPTED],
    S.EXECUTING: [S.DISPATCHED, S.ACCEPTED, S.EXECUTING],
    S.SUCCEEDED: [S.DISPATCHED, S.ACCEPTED, S.EXECUTING, S.SUCCEEDED],
    S.FAILED: [S.FAILED],
    S.CANCEL_REQUESTED: [S.DISPATCHED, S.ACCEPTED, S.CANCEL_REQUESTED],
    S.CANCELLED: [S.CANCELLED],
    S.OUTCOME_UNKNOWN: [S.OUTCOME_UNKNOWN],
}

# A way out of each in-flight state that a plain transition allows. DISPATCHED is entered
# only by authorize_step_send, so the plain way out of DISPATCH_PREPARED is cancellation.
IN_FLIGHT_EXITS = [
    (S.DISPATCH_PREPARED, S.CANCELLED),
    (S.DISPATCHED, S.ACCEPTED),
    (S.ACCEPTED, S.EXECUTING),
    (S.EXECUTING, S.SUCCEEDED),
    (S.CANCEL_REQUESTED, S.CANCELLED),
]
IN_FLIGHT = [state for state, _ in IN_FLIGHT_EXITS]

# A mission in each state that can still have a pending step.
MISSION_ROUTE = {
    M.ACCEPTED: [],
    M.RUNNING: [M.RUNNING],
    M.CANCELLING: [M.RUNNING, M.CANCELLING],
    M.CANCELLED: [M.CANCELLED],
    M.FAILED: [M.RUNNING, M.FAILED],
    M.NEEDS_RECONCILIATION: [M.RUNNING, M.NEEDS_RECONCILIATION],
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
    """Move the same mission to RUNNING, the state in which a send can be authorized."""
    return store.transition_mission_state(mission.mission_id, M.RUNNING)


def raw(db_path, sql, *params):
    with contextlib.closing(sqlite3.connect(db_path, isolation_level=None)) as conn:
        return conn.execute(sql, params).fetchall()


def advance(store, mission_id, key, state):
    """Walk a prepared step to a state along ROUTE_AFTER_PREPARE."""
    for step in ROUTE_AFTER_PREPARE.get(state, []):
        if step is S.DISPATCHED:
            # A plain transition cannot enter DISPATCHED; the mission must be RUNNING.
            store.authorize_step_send(mission_id, key)
        else:
            store.transition_step_state(mission_id, key, step)


def dispatch_count(db_path):
    return raw(db_path, 'SELECT COUNT(*) FROM step_dispatches')[0][0]


def leave_without_dispatch(db_path, mission, key, state):
    """Put a step in a state with no dispatch, as a version 2 database could."""
    raw(db_path, 'UPDATE mission_steps SET step_state = ? WHERE mission_id = ? AND step_key = ?',
        state.value, mission.mission_id, key)


# Preparation

def test_preparing_a_pending_step_reserves_an_identity_and_moves_the_step(
        store, mission, db_path):
    assert store.get_step(mission.mission_id, 'checkpoint_a').state is S.PENDING
    assert dispatch_count(db_path) == 0

    record = store.prepare_step_dispatch(mission.mission_id, 'checkpoint_a')

    assert isinstance(record, DispatchRecord)
    assert isinstance(record.dispatch_id, uuid.UUID)
    assert record.dispatch_id.version == 4
    assert record.mission_id == mission.mission_id
    assert record.step_key == 'checkpoint_a'
    assert record.created_at.utcoffset() == timedelta(0)
    assert store.get_step(mission.mission_id, 'checkpoint_a').state is S.DISPATCH_PREPARED
    assert dispatch_count(db_path) == 1


def test_only_the_prepared_step_is_touched(store, mission, db_path):
    other_before = store.get_step(mission.mission_id, 'return_to_base')

    store.prepare_step_dispatch(mission.mission_id, 'checkpoint_a')

    assert store.get_step(mission.mission_id, 'return_to_base') == other_before
    with pytest.raises(DispatchNotFound):
        store.get_step_dispatch(mission.mission_id, 'return_to_base')


def test_preparation_stamps_the_step_like_any_state_change(store, mission):
    before = store.get_step(mission.mission_id, 'checkpoint_a')

    store.prepare_step_dispatch(mission.mission_id, 'checkpoint_a')

    after = store.get_step(mission.mission_id, 'checkpoint_a')
    assert after.updated_at > before.updated_at
    assert after.created_at == before.created_at
    assert after.step_index == before.step_index


def test_the_id_is_stored_as_canonical_uuid_text(store, mission, db_path):
    record = store.prepare_step_dispatch(mission.mission_id, 'checkpoint_a')

    assert raw(db_path, 'SELECT dispatch_id FROM step_dispatches') == [(str(record.dispatch_id),)]


def test_every_step_gets_its_own_identity(store, mission):
    other = store.create_or_get('req-002', {}, 1)
    store.initialize_steps(other.mission_id, KEYS)

    ids = {
        store.prepare_step_dispatch(mission.mission_id, 'checkpoint_a').dispatch_id,
        store.prepare_step_dispatch(mission.mission_id, 'return_to_base').dispatch_id,
        store.prepare_step_dispatch(other.mission_id, 'checkpoint_a').dispatch_id,
    }

    assert len(ids) == 3


def test_dispatch_record_is_read_only(store, mission):
    record = store.prepare_step_dispatch(mission.mission_id, 'checkpoint_a')

    with pytest.raises(AttributeError):
        record.dispatch_id = uuid.uuid4()


def test_preparing_an_unknown_mission_or_step_fails(store, mission):
    with pytest.raises(MissionNotFound):
        store.prepare_step_dispatch('no-such-mission', 'checkpoint_a')
    with pytest.raises(StepNotFound) as excinfo:
        store.prepare_step_dispatch(mission.mission_id, 'no_such_step')

    assert excinfo.value.step_key == 'no_such_step'


# Replay

def test_preparing_again_returns_the_same_identity_and_changes_nothing(
        store, mission, db_path):
    first = store.prepare_step_dispatch(mission.mission_id, 'checkpoint_a')
    step_after_first = store.get_step(mission.mission_id, 'checkpoint_a')

    second = store.prepare_step_dispatch(mission.mission_id, 'checkpoint_a')

    assert second == first
    assert second.dispatch_id == first.dispatch_id
    assert second.created_at == first.created_at
    assert store.get_step(mission.mission_id, 'checkpoint_a') == step_after_first
    assert dispatch_count(db_path) == 1


def test_the_identity_survives_reopening_the_store(db_path):
    with MissionStore(db_path) as store:
        created = store.create_or_get('req-001', {}, 1)
        store.initialize_steps(created.mission_id, KEYS)
        first = store.prepare_step_dispatch(created.mission_id, 'checkpoint_a')

    with MissionStore(db_path) as reopened:
        assert reopened.get_step_dispatch(created.mission_id, 'checkpoint_a') == first
        assert reopened.get_dispatch(first.dispatch_id) == first
        assert reopened.prepare_step_dispatch(created.mission_id, 'checkpoint_a') == first
        assert reopened.get_step(created.mission_id, 'checkpoint_a').state is (
            S.DISPATCH_PREPARED)
    assert dispatch_count(db_path) == 1


def test_a_prepared_step_can_advance_and_keeps_its_identity(store, mission, running, db_path):
    prepared = store.prepare_step_dispatch(mission.mission_id, 'checkpoint_a')

    advance(store, mission.mission_id, 'checkpoint_a', S.SUCCEEDED)

    assert store.get_step_dispatch(mission.mission_id, 'checkpoint_a') == prepared
    assert dispatch_count(db_path) == 1


# Steps that cannot be prepared

@pytest.mark.parametrize('state', list(ROUTE_AFTER_PREPARE))
def test_a_step_past_preparation_cannot_be_prepared_again(
        store, mission, running, db_path, state):
    prepared = store.prepare_step_dispatch(mission.mission_id, 'checkpoint_a')
    advance(store, mission.mission_id, 'checkpoint_a', state)

    with pytest.raises(InvalidStateTransition) as excinfo:
        store.prepare_step_dispatch(mission.mission_id, 'checkpoint_a')

    assert excinfo.value.entity == 'step'
    assert excinfo.value.current is state
    assert excinfo.value.target is S.DISPATCH_PREPARED
    assert store.get_step_dispatch(mission.mission_id, 'checkpoint_a') == prepared
    assert dispatch_count(db_path) == 1


# Cancellation. A dispatch row is historical evidence that preparation happened,
# so it outlives a later cancellation; before preparation there is none to keep.

def test_cancelling_before_preparation_leaves_no_dispatch_and_blocks_preparation(
        store, mission, db_path):
    cancelled = store.transition_step_state(mission.mission_id, 'checkpoint_a', S.CANCELLED)

    assert cancelled.state is S.CANCELLED
    with pytest.raises(DispatchNotFound):
        store.get_step_dispatch(mission.mission_id, 'checkpoint_a')
    with pytest.raises(InvalidStateTransition) as excinfo:
        store.prepare_step_dispatch(mission.mission_id, 'checkpoint_a')

    assert excinfo.value.current is S.CANCELLED
    assert dispatch_count(db_path) == 0
    assert store.get_step(mission.mission_id, 'checkpoint_a').state is S.CANCELLED


def test_cancelling_after_preparation_keeps_the_original_dispatch(store, mission, db_path):
    prepared = store.prepare_step_dispatch(mission.mission_id, 'checkpoint_a')

    cancelled = store.transition_step_state(mission.mission_id, 'checkpoint_a', S.CANCELLED)

    assert cancelled.state is S.CANCELLED
    assert store.get_step_dispatch(mission.mission_id, 'checkpoint_a') == prepared
    assert store.get_dispatch(prepared.dispatch_id) == prepared
    assert dispatch_count(db_path) == 1
    with pytest.raises(InvalidStateTransition):
        store.prepare_step_dispatch(mission.mission_id, 'checkpoint_a')
    assert store.get_step_dispatch(mission.mission_id, 'checkpoint_a') == prepared


def test_the_dispatch_of_a_cancelled_step_survives_reopening_the_store(db_path):
    with MissionStore(db_path) as store:
        created = store.create_or_get('req-001', {}, 1)
        store.initialize_steps(created.mission_id, KEYS)
        prepared = store.prepare_step_dispatch(created.mission_id, 'checkpoint_a')
        store.transition_step_state(created.mission_id, 'checkpoint_a', S.CANCELLED)

    with MissionStore(db_path) as reopened:
        assert reopened.get_step(created.mission_id, 'checkpoint_a').state is S.CANCELLED
        assert reopened.get_step_dispatch(created.mission_id, 'checkpoint_a') == prepared


# Entering DISPATCH_PREPARED takes prepare_step_dispatch

def test_a_plain_transition_cannot_enter_dispatch_prepared(store, mission, db_path):
    before = store.get_steps(mission.mission_id)

    with pytest.raises(DispatchPreparationRequired) as excinfo:
        store.transition_step_state(mission.mission_id, 'checkpoint_a', S.DISPATCH_PREPARED)

    assert excinfo.value.mission_id == mission.mission_id
    assert excinfo.value.step_key == 'checkpoint_a'
    # PENDING -> DISPATCH_PREPARED is legal in the state machine, so this is
    # not a refused transition: the caller used the wrong operation.
    assert not isinstance(excinfo.value, InvalidStateTransition)
    assert store.get_step(mission.mission_id, 'checkpoint_a').state is S.PENDING
    assert store.get_steps(mission.mission_id) == before
    assert dispatch_count(db_path) == 0


def test_prepare_step_dispatch_is_the_supported_way_to_enter_dispatch_prepared(
        store, mission, db_path):
    with pytest.raises(DispatchPreparationRequired):
        store.transition_step_state(mission.mission_id, 'checkpoint_a', S.DISPATCH_PREPARED)

    record = store.prepare_step_dispatch(mission.mission_id, 'checkpoint_a')

    assert store.get_step(mission.mission_id, 'checkpoint_a').state is S.DISPATCH_PREPARED
    assert store.get_step_dispatch(mission.mission_id, 'checkpoint_a') == record
    assert dispatch_count(db_path) == 1


@pytest.mark.parametrize('state', [S.PENDING, S.DISPATCH_PREPARED, *ROUTE_AFTER_PREPARE])
def test_the_refusal_does_not_depend_on_the_current_state(
        store, mission, running, db_path, state):
    if state is not S.PENDING:
        store.prepare_step_dispatch(mission.mission_id, 'checkpoint_a')
        advance(store, mission.mission_id, 'checkpoint_a', state)
    step_before = store.get_step(mission.mission_id, 'checkpoint_a')
    dispatches_before = raw(db_path, 'SELECT * FROM step_dispatches')

    # Not even a step that is already there, or one the state machine would
    # call illegal: misuse must not depend on timing.
    with pytest.raises(DispatchPreparationRequired):
        store.transition_step_state(mission.mission_id, 'checkpoint_a', S.DISPATCH_PREPARED)

    assert store.get_step(mission.mission_id, 'checkpoint_a') == step_before
    assert raw(db_path, 'SELECT * FROM step_dispatches') == dispatches_before


def test_the_refusal_is_raised_before_the_database_is_touched(db_path):
    with MissionStore(db_path, busy_timeout=0.05) as store:
        blocker = sqlite3.connect(db_path, isolation_level=None)
        blocker.execute('BEGIN IMMEDIATE')
        try:
            with pytest.raises(DispatchPreparationRequired) as excinfo:
                store.transition_step_state('no-such-mission', 'no-such-step', S.DISPATCH_PREPARED)
        finally:
            blocker.execute('ROLLBACK')
            blocker.close()

    assert excinfo.value.mission_id == 'no-such-mission'
    assert excinfo.value.step_key == 'no-such-step'


def test_other_targets_still_use_the_plain_transition(store, mission, running):
    store.prepare_step_dispatch(mission.mission_id, 'checkpoint_a')
    store.authorize_step_send(mission.mission_id, 'checkpoint_a')

    moved = store.transition_step_state(mission.mission_id, 'checkpoint_a', S.ACCEPTED)

    assert moved.state is S.ACCEPTED


def test_preparation_still_asks_the_state_machine_to_validate_the_move(
        store, mission, monkeypatch, db_path):
    calls = []
    real_transition_step = storage.transition_step

    def spy(current, target):
        calls.append((current, target))
        return real_transition_step(current, target)

    with monkeypatch.context() as patch:
        patch.setattr(storage, 'transition_step', spy)
        store.prepare_step_dispatch(mission.mission_id, 'checkpoint_a')

    assert calls == [(S.PENDING, S.DISPATCH_PREPARED)]


def test_a_state_machine_refusal_stops_preparation_and_writes_nothing(
        store, mission, monkeypatch, db_path):
    def refuse(current, target):
        raise InvalidStateTransition('step', current, target)

    with monkeypatch.context() as patch:
        patch.setattr(storage, 'transition_step', refuse)
        with pytest.raises(InvalidStateTransition):
            store.prepare_step_dispatch(mission.mission_id, 'checkpoint_a')

    assert store.get_step(mission.mission_id, 'checkpoint_a').state is S.PENDING
    assert dispatch_count(db_path) == 0


# Steps in flight without a dispatch. A version 2 database can hold them; they
# are kept as they were and nothing moves them on until a later reconciliation.

@pytest.mark.parametrize(('state', 'exit_to'), IN_FLIGHT_EXITS)
def test_a_step_in_flight_without_a_dispatch_cannot_progress(
        store, mission, db_path, state, exit_to):
    # A counter value seeded directly: this test is about the dispatch check, not about
    # how the attempt was consumed.
    raw(db_path, 'UPDATE missions SET attempts_used = 1 WHERE mission_id = ?',
        mission.mission_id)
    leave_without_dispatch(db_path, mission, 'checkpoint_a', state)
    mission_before = store.get_by_mission_id(mission.mission_id)
    steps_before = store.get_steps(mission.mission_id)

    with pytest.raises(CorruptDispatchRecord) as excinfo:
        store.transition_step_state(mission.mission_id, 'checkpoint_a', exit_to)

    assert excinfo.value.mission_id == mission.mission_id
    assert excinfo.value.step_key == 'checkpoint_a'
    assert state.value in excinfo.value.reason
    assert store.get_steps(mission.mission_id) == steps_before
    # Neither the mission's state nor its attempts moved, and no ID was made up.
    assert store.get_by_mission_id(mission.mission_id) == mission_before
    assert mission_before.attempts_used == 1
    assert dispatch_count(db_path) == 0


@pytest.mark.parametrize('state', IN_FLIGHT)
def test_every_target_is_refused_for_a_step_in_flight_without_a_dispatch(
        store, mission, db_path, state):
    leave_without_dispatch(db_path, mission, 'checkpoint_a', state)
    steps_before = store.get_steps(mission.mission_id)

    for target in S:
        if target in (S.DISPATCH_PREPARED, S.DISPATCHED):
            continue  # each is refused for every step, ahead of this check
        # Legal, illegal and same-state alike: the dispatch check comes first.
        with pytest.raises(CorruptDispatchRecord):
            store.transition_step_state(mission.mission_id, 'checkpoint_a', target)

    assert store.get_steps(mission.mission_id) == steps_before
    assert dispatch_count(db_path) == 0


def test_the_dispatch_check_comes_before_the_state_machine(
        store, mission, db_path, monkeypatch):
    leave_without_dispatch(db_path, mission, 'checkpoint_a', S.EXECUTING)

    def unreachable(current, target):
        raise AssertionError('the state machine was consulted')

    with monkeypatch.context() as patch:
        patch.setattr(storage, 'transition_step', unreachable)
        with pytest.raises(CorruptDispatchRecord):
            store.transition_step_state(mission.mission_id, 'checkpoint_a', S.SUCCEEDED)


@pytest.mark.parametrize('state', [S.SUCCEEDED, S.FAILED, S.CANCELLED, S.OUTCOME_UNKNOWN])
def test_finished_and_unknown_steps_need_no_dispatch(store, mission, db_path, state):
    leave_without_dispatch(db_path, mission, 'checkpoint_a', state)

    step = store.get_step(mission.mission_id, 'checkpoint_a')

    assert step.state is state
    assert store.get_steps(mission.mission_id)[0] == step
    with pytest.raises(DispatchNotFound):
        store.get_step_dispatch(mission.mission_id, 'checkpoint_a')
    # The state machine answers here, not the dispatch check.
    assert store.transition_step_state(mission.mission_id, 'checkpoint_a', state) == step
    with pytest.raises(InvalidStateTransition):
        store.transition_step_state(mission.mission_id, 'checkpoint_a', S.EXECUTING)
    assert dispatch_count(db_path) == 0


@pytest.mark.parametrize(('state', 'exit_to'), IN_FLIGHT_EXITS)
def test_the_same_transitions_work_when_the_step_was_prepared_through_the_store(
        store, mission, running, db_path, state, exit_to):
    prepared = store.prepare_step_dispatch(mission.mission_id, 'checkpoint_a')
    advance(store, mission.mission_id, 'checkpoint_a', state)
    assert store.get_step(mission.mission_id, 'checkpoint_a').state is state

    moved = store.transition_step_state(mission.mission_id, 'checkpoint_a', exit_to)

    assert moved.state is exit_to
    assert store.get_step_dispatch(mission.mission_id, 'checkpoint_a') == prepared
    assert dispatch_count(db_path) == 1


def test_an_in_flight_step_with_an_unreadable_dispatch_id_cannot_progress(
        store, mission, db_path):
    store.prepare_step_dispatch(mission.mission_id, 'checkpoint_a')
    raw(db_path, "UPDATE step_dispatches SET dispatch_id = 'not-a-uuid'")

    with pytest.raises(CorruptDispatchRecord) as excinfo:
        store.transition_step_state(mission.mission_id, 'checkpoint_a', S.CANCELLED)

    assert excinfo.value.value == 'not-a-uuid'
    assert store.get_step(mission.mission_id, 'checkpoint_a').state is S.DISPATCH_PREPARED


# Preparation is not bound to mission state or to attempts. Authorizing a send is: see
# test_send_authorization.py.

@pytest.mark.parametrize('mission_state', list(MISSION_ROUTE))
def test_preparation_is_not_gated_on_the_mission_state(store, mission_state):
    created = store.create_or_get('req-gate', {}, 1)
    store.initialize_steps(created.mission_id, KEYS)
    for step in MISSION_ROUTE[mission_state]:
        store.transition_mission_state(created.mission_id, step)
    before = store.get_by_mission_id(created.mission_id)

    record = store.prepare_step_dispatch(created.mission_id, 'checkpoint_a')

    assert record.step_key == 'checkpoint_a'
    assert store.get_by_mission_id(created.mission_id) == before
    assert before.state is mission_state


def test_preparation_does_not_consume_attempts(store, mission, db_path):
    # A counter value seeded directly, as a mission with one attempt already used would have.
    raw(db_path, 'UPDATE missions SET attempts_used = 1 WHERE mission_id = ?',
        mission.mission_id)
    before = store.get_by_mission_id(mission.mission_id)

    store.prepare_step_dispatch(mission.mission_id, 'checkpoint_a')
    store.prepare_step_dispatch(mission.mission_id, 'checkpoint_a')
    store.prepare_step_dispatch(mission.mission_id, 'return_to_base')

    assert store.get_by_mission_id(mission.mission_id) == before
    assert before.attempts_used == 1
    # Only authorizing the send takes the next attempt.
    store.transition_mission_state(mission.mission_id, M.RUNNING)
    assert store.authorize_step_send(mission.mission_id, 'checkpoint_a').attempt_number == 2


# Lookups

def test_a_step_can_be_found_by_step_or_by_id(store, mission):
    record = store.prepare_step_dispatch(mission.mission_id, 'checkpoint_a')

    assert store.get_step_dispatch(mission.mission_id, 'checkpoint_a') == record
    assert store.get_dispatch(record.dispatch_id) == record


def test_a_step_without_a_dispatch_is_reported_as_such(store, mission):
    with pytest.raises(DispatchNotFound) as excinfo:
        store.get_step_dispatch(mission.mission_id, 'checkpoint_a')

    assert excinfo.value.mission_id == mission.mission_id
    assert excinfo.value.step_key == 'checkpoint_a'
    assert excinfo.value.dispatch_id is None


def test_step_lookup_tells_missing_mission_missing_step_and_missing_dispatch_apart(
        store, mission):
    with pytest.raises(MissionNotFound):
        store.get_step_dispatch('no-such-mission', 'checkpoint_a')
    with pytest.raises(StepNotFound):
        store.get_step_dispatch(mission.mission_id, 'no_such_step')
    with pytest.raises(DispatchNotFound):
        store.get_step_dispatch(mission.mission_id, 'checkpoint_a')


def test_an_unknown_id_is_not_found(store, mission):
    unknown = uuid.uuid4()

    with pytest.raises(DispatchNotFound) as excinfo:
        store.get_dispatch(unknown)

    assert excinfo.value.dispatch_id == unknown
    assert excinfo.value.mission_id is None


@pytest.mark.parametrize('bad', ['0b3c5d9e-1f2a-4b6c-8d7e-9f0a1b2c3d4e', None, 5, b'0' * 16])
def test_lookup_by_id_needs_a_uuid_not_a_string(store, bad):
    with pytest.raises(TypeError):
        store.get_dispatch(bad)


# Atomicity. Failures are injected around the connection, not by any hook in
# the store.

def test_failure_after_the_dispatch_insert_leaves_neither_change(monkeypatch, db_path):
    store, connection = open_faulty_store(monkeypatch, db_path)
    with store:
        created = store.create_or_get('req-001', {}, 1)
        store.initialize_steps(created.mission_id, KEYS)
        connection.statements.clear()
        connection.fail_when = lambda sql: sql.lstrip().startswith('UPDATE mission_steps')

        with pytest.raises(sqlite3.OperationalError, match='injected failure'):
            store.prepare_step_dispatch(created.mission_id, 'checkpoint_a')

        # The row really was inserted first; the rollback is what removed it.
        assert any(sql.startswith('INSERT INTO step_dispatches') for sql in connection.statements)
        connection.fail_when = None
        assert not connection.in_transaction
        assert dispatch_count(db_path) == 0
        assert store.get_step(created.mission_id, 'checkpoint_a').state is S.PENDING
        with pytest.raises(DispatchNotFound):
            store.get_step_dispatch(created.mission_id, 'checkpoint_a')

        # And the store still works.
        record = store.prepare_step_dispatch(created.mission_id, 'checkpoint_a')
        assert store.get_step(created.mission_id, 'checkpoint_a').state is S.DISPATCH_PREPARED
        assert dispatch_count(db_path) == 1
        assert store.get_dispatch(record.dispatch_id) == record


def test_failure_at_commit_leaves_neither_change(monkeypatch, db_path):
    store, connection = open_faulty_store(monkeypatch, db_path)
    with store:
        created = store.create_or_get('req-001', {}, 1)
        store.initialize_steps(created.mission_id, KEYS)
        connection.statements.clear()
        connection.fail_when = lambda sql: sql.strip().upper() == 'COMMIT'

        with pytest.raises(sqlite3.OperationalError, match='injected failure'):
            store.prepare_step_dispatch(created.mission_id, 'checkpoint_a')

        # Both writes ran; only the commit failed.
        assert any(sql.startswith('INSERT INTO step_dispatches') for sql in connection.statements)
        assert any(sql.startswith('UPDATE mission_steps') for sql in connection.statements)
        connection.fail_when = None
        assert not connection.in_transaction
        assert dispatch_count(db_path) == 0
        assert store.get_step(created.mission_id, 'checkpoint_a').state is S.PENDING

        assert store.prepare_step_dispatch(created.mission_id, 'checkpoint_a').step_key == (
            'checkpoint_a')
        assert dispatch_count(db_path) == 1


# Crash

_CHILD = """
import os
import sys

from task_resilience.storage import MissionStore

store = MissionStore(sys.argv[1])
record = store.prepare_step_dispatch(sys.argv[2], sys.argv[3])
print(record.dispatch_id, flush=True)
# Die without closing the store or running any cleanup.
os._exit(0)
"""


def package_env():
    package_root = Path(task_resilience.__file__).resolve().parent.parent
    return {**os.environ, 'PYTHONPATH': str(package_root)}


def test_a_prepared_dispatch_survives_a_process_that_exits_abruptly(db_path):
    with MissionStore(db_path) as store:
        created = store.create_or_get('req-001', {}, 1)
        store.initialize_steps(created.mission_id, KEYS)

    child = subprocess.run(
        [sys.executable, '-c', _CHILD, str(db_path), created.mission_id, 'checkpoint_a'],
        env=package_env(), capture_output=True, text=True, check=True, timeout=30)
    child_id = uuid.UUID(child.stdout.strip())

    with MissionStore(db_path) as reopened:
        assert reopened.get_step(created.mission_id, 'checkpoint_a').state is (
            S.DISPATCH_PREPARED)
        found = reopened.get_step_dispatch(created.mission_id, 'checkpoint_a')
        assert found.dispatch_id == child_id
        # Preparing again must hand back the child's ID, not mint another.
        assert reopened.prepare_step_dispatch(created.mission_id, 'checkpoint_a') == found
    assert dispatch_count(db_path) == 1


# Inconsistent or unreadable stored dispatches are reported, never repaired

def test_a_pending_step_that_already_has_a_dispatch_is_inconsistent(store, mission, db_path):
    planted = str(uuid.uuid4())
    raw(db_path, 'INSERT INTO step_dispatches VALUES (?, ?, ?, ?)',
        planted, mission.mission_id, 'checkpoint_a', '2026-01-01T00:00:00.000000+00:00')

    with pytest.raises(CorruptDispatchRecord) as excinfo:
        store.prepare_step_dispatch(mission.mission_id, 'checkpoint_a')

    assert excinfo.value.mission_id == mission.mission_id
    assert excinfo.value.step_key == 'checkpoint_a'
    assert store.get_step(mission.mission_id, 'checkpoint_a').state is S.PENDING
    assert raw(db_path, 'SELECT dispatch_id FROM step_dispatches') == [(planted,)]


def test_a_prepared_step_without_a_dispatch_is_inconsistent_and_gets_no_new_identity(
        store, mission, db_path):
    raw(db_path, "UPDATE mission_steps SET step_state = 'DISPATCH_PREPARED'"
                 " WHERE step_key = 'checkpoint_a'")

    with pytest.raises(CorruptDispatchRecord) as excinfo:
        store.prepare_step_dispatch(mission.mission_id, 'checkpoint_a')

    assert excinfo.value.mission_id == mission.mission_id
    assert excinfo.value.step_key == 'checkpoint_a'
    assert dispatch_count(db_path) == 0
    with pytest.raises(DispatchNotFound):
        store.get_step_dispatch(mission.mission_id, 'checkpoint_a')


@pytest.mark.parametrize('step_state', ['PENDING', 'DISPATCH_PREPARED'])
def test_more_than_one_dispatch_for_a_step_is_inconsistent(store, mission, db_path, step_state):
    # The UNIQUE constraint makes this impossible, so rebuild the table without it.
    raw(db_path, 'DROP TABLE step_dispatches')
    raw(db_path, 'CREATE TABLE step_dispatches'
                 ' (dispatch_id TEXT, mission_id TEXT, step_key TEXT, created_at TEXT)')
    for _ in range(2):
        raw(db_path, 'INSERT INTO step_dispatches VALUES (?, ?, ?, ?)',
            str(uuid.uuid4()), mission.mission_id, 'checkpoint_a',
            '2026-01-01T00:00:00.000000+00:00')
    raw(db_path, f"UPDATE mission_steps SET step_state = '{step_state}'"
                 " WHERE step_key = 'checkpoint_a'")

    with pytest.raises(CorruptDispatchRecord):
        store.prepare_step_dispatch(mission.mission_id, 'checkpoint_a')
    with pytest.raises(CorruptDispatchRecord):
        store.get_step_dispatch(mission.mission_id, 'checkpoint_a')

    assert dispatch_count(db_path) == 2


@pytest.mark.parametrize('damage', [
    lambda text: 'not-a-uuid',
    lambda text: '',
    lambda text: text.upper(),
    lambda text: text.replace('-', ''),
    lambda text: '{' + text + '}',
    lambda text: 'urn:uuid:' + text,
    lambda text: text[:-1],
    lambda text: text.encode(),
], ids=['garbage', 'empty', 'upper case', 'no hyphens', 'braces', 'urn', 'truncated', 'blob'])
def test_an_unreadable_stored_id_is_an_error_and_is_never_replaced(
        store, mission, db_path, damage):
    original = store.prepare_step_dispatch(mission.mission_id, 'checkpoint_a').dispatch_id
    damaged = damage(str(original))
    raw(db_path, 'UPDATE step_dispatches SET dispatch_id = ?', damaged)

    with pytest.raises(CorruptDispatchRecord) as excinfo:
        store.get_step_dispatch(mission.mission_id, 'checkpoint_a')
    assert excinfo.value.mission_id == mission.mission_id
    assert excinfo.value.step_key == 'checkpoint_a'
    assert excinfo.value.value == damaged
    with pytest.raises(CorruptDispatchRecord):
        store.prepare_step_dispatch(mission.mission_id, 'checkpoint_a')

    # No replacement was minted, and the damaged row was left as found.
    assert raw(db_path, 'SELECT dispatch_id FROM step_dispatches') == [(damaged,)]
    with pytest.raises(DispatchNotFound):
        store.get_dispatch(original)


# Boundaries

def test_the_core_imports_no_ros_package():
    code = (
        'import sys\n'
        'import task_resilience.storage\n'
        "ros = {'rclpy', 'rcl_interfaces', 'rosidl_runtime_py', 'ament_index_python',\n"
        "       'action_msgs', 'unique_identifier_msgs', 'nav2_msgs', 'geometry_msgs'}\n"
        "print(sorted(m for m in sys.modules if m.split('.')[0] in ros))\n")

    child = subprocess.run(
        [sys.executable, '-c', code], env=package_env(), capture_output=True, text=True,
        check=True, timeout=30)

    assert child.stdout.strip() == '[]'
