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
from concurrent.futures import ThreadPoolExecutor
import contextlib
import dataclasses
from datetime import datetime, timedelta, timezone
import inspect
import os
from pathlib import Path
import signal
import socket
import sqlite3
import subprocess
import sys
import textwrap
import threading
import uuid

from fault_injection import open_faulty_store
import pytest

import task_resilience
from task_resilience import reconciliation, storage
from task_resilience.errors import (
    AttemptLimitReached,
    CorruptDispatchRecord,
    CorruptStoredState,
    InvalidStateTransition,
    MissionCompletionInvariantError,
    MissionNotFound,
    MissionNotRunning,
    MissionReconciliationRequired,
    SendAuthorizationRequired,
    StepNotAwaitingSend,
    StepNotFound,
)
from task_resilience.model import SendAuthorization
from task_resilience.state_machine import MissionState, StepState, transition_step
from task_resilience.storage import MissionStore

# The action every dispatch in these tests is declared to reach.
ACTION = reconciliation.ExpectedActionContext(
    action_name='/navigate_to_pose', action_type='nav2_msgs/action/NavigateToPose',
    robot_namespace='/robot1')

M = MissionState
S = StepState

KEYS = ['checkpoint_a', 'return_to_base']

# Each round is an independent race, repeated because a single race can be won "by luck".
ROUNDS = 25
TIMEOUT = 10

# The tables as version 6 defines them, written out here so a new column or table shows up.
COLUMNS = {
    'missions': [
        'mission_id', 'request_id', 'request_fingerprint', 'request_json', 'max_attempts',
        'attempts_used', 'created_at', 'updated_at', 'mission_state',
    ],
    'mission_steps': [
        'mission_id', 'step_key', 'step_index', 'step_state', 'created_at', 'updated_at',
    ],
    'step_dispatches': ['dispatch_id', 'mission_id', 'step_key', 'created_at'],
    'dispatch_contexts': ['dispatch_id', 'action_name', 'action_type', 'robot_namespace'],
    'dispatch_authorizations': ['dispatch_id', 'authorized_at'],
    'recovery_evidence': [
        'evidence_id', 'dispatch_id', 'observed_status', 'result_code', 'result_message_prefix',
        'result_message_bytes', 'result_message_sha256', 'step_state_before', 'outcome',
        'recorded_at',
    ],
    'mission_reconciliation_contexts': ['mission_id', 'resume_state'],
}

# The one table that a committed authorization adds a row to. No other table gains or loses one.
AUTHORIZATION_TABLE = 'dispatch_authorizations'

# The step states a step can reach after its send was authorized, by an ordinary route.
ROUTES_FROM_DISPATCHED = {
    S.ACCEPTED: [S.ACCEPTED],
    S.EXECUTING: [S.ACCEPTED, S.EXECUTING],
    S.CANCEL_REQUESTED: [S.ACCEPTED, S.CANCEL_REQUESTED],
    S.SUCCEEDED: [S.SUCCEEDED],
    S.FAILED: [S.FAILED],
    S.CANCELLED: [S.CANCELLED],
    S.OUTCOME_UNKNOWN: [S.OUTCOME_UNKNOWN],
}

# Only a RUNNING mission may authorize a send.
MAY_AUTHORIZE = {
    M.ACCEPTED: False,
    M.RUNNING: True,
    M.CANCELLING: False,
    M.COMPLETED: False,
    M.FAILED: False,
    M.CANCELLED: False,
    M.NEEDS_RECONCILIATION: False,
}

# An ordinary route to each mission state. COMPLETED has none while a step is still
# prepared, because completion needs every step to have succeeded.
MISSION_ROUTES = {
    M.ACCEPTED: (),
    M.RUNNING: (M.RUNNING,),
    M.CANCELLING: (M.RUNNING, M.CANCELLING),
    M.COMPLETED: None,
    M.FAILED: (M.RUNNING, M.FAILED),
    M.CANCELLED: (M.CANCELLED,),
    # Entered through enter_mission_reconciliation, once a step is unknown: see the gate test.
    M.NEEDS_RECONCILIATION: (M.RUNNING,),
}


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / 'missions.db'


@pytest.fixture
def store(db_path):
    with MissionStore(db_path) as opened:
        yield opened


def raw(db_path, sql, *params):
    with contextlib.closing(sqlite3.connect(db_path, isolation_level=None)) as conn:
        return conn.execute(sql, params).fetchall()


def corrupt(db_path, sql):
    with contextlib.closing(sqlite3.connect(db_path, isolation_level=None)) as conn:
        conn.execute('PRAGMA ignore_check_constraints = ON')
        conn.execute(sql)


def make_mission(store, request_id='req-001', max_attempts=3, keys=KEYS, prepared=None,
                 route=(M.RUNNING,)):
    """Create a mission, prepare some of its steps and walk it along a mission route."""
    prepared = keys if prepared is None else prepared
    mission = store.create_or_get(request_id, {'goal': 'demo'}, max_attempts)
    store.initialize_steps(mission.mission_id, keys)
    for key in prepared:
        store.prepare_step_dispatch(mission.mission_id, key, ACTION)
    for state in route:
        store.transition_mission_state(mission.mission_id, state)
    return mission.mission_id


def dump(db_path):
    """Return every row of every table, for comparing the database before and after."""
    return {table: raw(db_path, f'SELECT * FROM {table} ORDER BY 1, 2') for table in COLUMNS}


def changed_cells(before, after):
    """Return {(table, row key, column): (old, new)} for every cell that differs."""
    changes = {}
    for table, columns in COLUMNS.items():
        if table == AUTHORIZATION_TABLE:
            # It gains a row instead: the caller checks that row on its own.
            continue
        assert len(before[table]) == len(after[table]), f'{table} gained or lost a row'
        for old, new in zip(before[table], after[table]):
            row_key = old[1] if table == 'mission_steps' else old[0]
            for column, was, now in zip(columns, old, new):
                if was != now:
                    changes[(table, row_key, column)] = (was, now)
    return changes


def set_step_state(db_path, mission_id, key, state):
    raw(db_path, 'UPDATE mission_steps SET step_state = ? WHERE mission_id = ? AND step_key = ?',
        state.value, mission_id, key)


def package_env():
    package_root = Path(task_resilience.__file__).resolve().parent.parent
    return {**os.environ, 'PYTHONPATH': str(package_root)}


# The authorization

def test_a_prepared_step_of_a_running_mission_is_authorized(store):
    mission_id = make_mission(store, max_attempts=3)
    dispatch = store.get_step_dispatch(mission_id, 'checkpoint_a')

    receipt = store.authorize_step_send(mission_id, 'checkpoint_a')

    assert isinstance(receipt, SendAuthorization)
    assert receipt.mission_id == mission_id
    assert receipt.step_key == 'checkpoint_a'
    assert receipt.dispatch_id == dispatch.dispatch_id
    assert receipt.attempt_number == 1
    assert receipt.max_attempts == 3
    assert receipt.authorized_at.utcoffset() == timedelta(0)
    assert store.get_step(mission_id, 'checkpoint_a').state is S.DISPATCHED
    assert store.get_by_mission_id(mission_id).attempts_used == 1


def test_the_mission_and_the_step_share_the_authorized_at_timestamp(store, db_path):
    mission_id = make_mission(store)
    mission_before = store.get_by_mission_id(mission_id)
    step_before = store.get_step(mission_id, 'checkpoint_a')

    receipt = store.authorize_step_send(mission_id, 'checkpoint_a')

    mission = store.get_by_mission_id(mission_id)
    step = store.get_step(mission_id, 'checkpoint_a')
    assert mission.updated_at == receipt.authorized_at
    assert step.updated_at == receipt.authorized_at
    assert receipt.authorized_at > mission_before.updated_at
    assert receipt.authorized_at > step_before.updated_at
    # The very same text was written to both rows.
    stored_on_mission = raw(db_path, 'SELECT updated_at FROM missions')
    stored_on_step = raw(
        db_path, "SELECT updated_at FROM mission_steps WHERE step_key = 'checkpoint_a'")
    assert stored_on_mission == stored_on_step


def test_only_the_intended_cells_change(store, db_path):
    mission_id = make_mission(store, max_attempts=3)
    before = dump(db_path)

    receipt = store.authorize_step_send(mission_id, 'checkpoint_a')

    after = dump(db_path)
    changes = changed_cells(before, after)
    assert set(changes) == {
        ('missions', mission_id, 'attempts_used'),
        ('missions', mission_id, 'updated_at'),
        ('mission_steps', 'checkpoint_a', 'step_state'),
        ('mission_steps', 'checkpoint_a', 'updated_at'),
    }
    assert changes[('missions', mission_id, 'attempts_used')] == (0, 1)
    assert changes[('mission_steps', 'checkpoint_a', 'step_state')] == (
        'DISPATCH_PREPARED', 'DISPATCHED')
    stamp = changes[('missions', mission_id, 'updated_at')][1]
    assert stamp == changes[('mission_steps', 'checkpoint_a', 'updated_at')][1]
    assert datetime.fromisoformat(stamp) == receipt.authorized_at
    # The one row that is added: this dispatch and the same stamp, and nothing else in it.
    assert before[AUTHORIZATION_TABLE] == []
    assert after[AUTHORIZATION_TABLE] == [(str(receipt.dispatch_id), stamp)]
    # The dispatch row is untouched, and so is the other step.
    assert after['step_dispatches'] == before['step_dispatches']
    assert [row for row in after['mission_steps'] if row[1] == 'return_to_base'] == [
        row for row in before['mission_steps'] if row[1] == 'return_to_base']


def test_each_authorization_takes_the_next_attempt_number(store):
    mission_id = make_mission(store, max_attempts=3)

    first = store.authorize_step_send(mission_id, 'checkpoint_a')
    second = store.authorize_step_send(mission_id, 'return_to_base')

    assert (first.attempt_number, second.attempt_number) == (1, 2)
    assert first.max_attempts == second.max_attempts == 3
    assert first.dispatch_id != second.dispatch_id


def test_a_later_plain_transition_still_works_and_takes_no_attempt(store):
    mission_id = make_mission(store)
    store.authorize_step_send(mission_id, 'checkpoint_a')

    moved = store.transition_step_state(mission_id, 'checkpoint_a', S.ACCEPTED)

    assert moved.state is S.ACCEPTED
    assert store.get_by_mission_id(mission_id).attempts_used == 1


# Budget

def test_the_final_available_attempt_succeeds_exactly_once(store):
    mission_id = make_mission(store, max_attempts=1)

    first = store.authorize_step_send(mission_id, 'checkpoint_a')
    with pytest.raises(AttemptLimitReached):
        store.authorize_step_send(mission_id, 'return_to_base')

    assert first.attempt_number == first.max_attempts == 1
    assert store.get_by_mission_id(mission_id).attempts_used == 1
    assert store.get_step(mission_id, 'checkpoint_a').state is S.DISPATCHED
    assert store.get_step(mission_id, 'return_to_base').state is S.DISPATCH_PREPARED


def test_an_exhausted_budget_is_refused_without_any_write(store, db_path):
    mission_id = make_mission(store, max_attempts=1)
    store.authorize_step_send(mission_id, 'checkpoint_a')
    before = dump(db_path)

    with pytest.raises(AttemptLimitReached) as excinfo:
        store.authorize_step_send(mission_id, 'return_to_base')

    assert excinfo.value.mission_id == mission_id
    assert excinfo.value.max_attempts == 1
    assert dump(db_path) == before


@pytest.mark.parametrize(('max_attempts', 'sends', 'completes'), [
    (1, 1, False),      # one of the two normal sends: the mission cannot complete normally
    (2, 2, True),       # exactly enough for the two normal sends
    (3, 2, True),       # one unit over, which no step can use: one dispatch per step
])
def test_the_two_step_mission_under_each_budget(store, max_attempts, sends, completes):
    mission_id = make_mission(store, max_attempts=max_attempts)

    granted = []
    for key in KEYS:
        try:
            store.authorize_step_send(mission_id, key)
        except AttemptLimitReached:
            continue
        granted.append(key)

    assert granted == KEYS[:sends]
    assert store.get_by_mission_id(mission_id).attempts_used == sends
    # Budget left over is not a retry: an authorized step is never authorized again.
    for key in granted:
        with pytest.raises(StepNotAwaitingSend):
            store.authorize_step_send(mission_id, key)
    assert store.get_by_mission_id(mission_id).attempts_used == sends

    for key in granted:
        store.transition_step_state(mission_id, key, S.SUCCEEDED)
    if completes:
        assert store.transition_mission_state(mission_id, M.COMPLETED).state is M.COMPLETED
    else:
        with pytest.raises(MissionCompletionInvariantError):
            store.transition_mission_state(mission_id, M.COMPLETED)


def test_a_legacy_nonzero_counter_continues_from_that_value(store, db_path):
    mission_id = make_mission(store, max_attempts=3)
    # Seeded as a mission that used two attempts before this design would be.
    raw(db_path, 'UPDATE missions SET attempts_used = 2 WHERE mission_id = ?', mission_id)

    receipt = store.authorize_step_send(mission_id, 'checkpoint_a')

    assert (receipt.attempt_number, receipt.max_attempts) == (3, 3)
    with pytest.raises(AttemptLimitReached):
        store.authorize_step_send(mission_id, 'return_to_base')
    assert store.get_by_mission_id(mission_id).attempts_used == 3


# Replay

def test_a_second_authorization_of_the_same_step_is_refused(store, db_path):
    mission_id = make_mission(store)
    store.authorize_step_send(mission_id, 'checkpoint_a')
    before = dump(db_path)

    with pytest.raises(StepNotAwaitingSend) as excinfo:
        store.authorize_step_send(mission_id, 'checkpoint_a')

    assert excinfo.value.mission_id == mission_id
    assert excinfo.value.step_key == 'checkpoint_a'
    assert excinfo.value.step_state is S.DISPATCHED
    assert dump(db_path) == before


def test_the_refusal_survives_reopening_the_store(db_path):
    with MissionStore(db_path) as store:
        mission_id = make_mission(store)
        receipt = store.authorize_step_send(mission_id, 'checkpoint_a')

    for _ in range(3):
        with MissionStore(db_path) as reopened:
            with pytest.raises(StepNotAwaitingSend):
                reopened.authorize_step_send(mission_id, 'checkpoint_a')
            assert reopened.get_step_dispatch(mission_id, 'checkpoint_a').dispatch_id == (
                receipt.dispatch_id)
            assert reopened.get_by_mission_id(mission_id).attempts_used == 1


@pytest.mark.parametrize('state', list(ROUTES_FROM_DISPATCHED))
def test_a_step_that_has_progressed_is_still_refused(store, state):
    mission_id = make_mission(store)
    store.authorize_step_send(mission_id, 'checkpoint_a')
    for step in ROUTES_FROM_DISPATCHED[state]:
        store.transition_step_state(mission_id, 'checkpoint_a', step)

    with pytest.raises(StepNotAwaitingSend) as excinfo:
        store.authorize_step_send(mission_id, 'checkpoint_a')

    assert excinfo.value.step_state is state
    assert store.get_by_mission_id(mission_id).attempts_used == 1


def test_replaying_preparation_cannot_recreate_a_send_permission(store, db_path):
    mission_id = make_mission(store)
    prepared = store.get_step_dispatch(mission_id, 'checkpoint_a')
    store.authorize_step_send(mission_id, 'checkpoint_a')
    before = dump(db_path)

    with pytest.raises(InvalidStateTransition):
        store.prepare_step_dispatch(mission_id, 'checkpoint_a', ACTION)
    with pytest.raises(StepNotAwaitingSend):
        store.authorize_step_send(mission_id, 'checkpoint_a')

    assert store.get_step_dispatch(mission_id, 'checkpoint_a') == prepared
    assert dump(db_path) == before


def test_replaying_preparation_before_authorization_returns_the_same_dispatch(store):
    mission_id = make_mission(store)
    prepared = store.get_step_dispatch(mission_id, 'checkpoint_a')

    assert store.prepare_step_dispatch(mission_id, 'checkpoint_a', ACTION) == prepared
    receipt = store.authorize_step_send(mission_id, 'checkpoint_a')

    assert receipt.dispatch_id == prepared.dispatch_id
    assert receipt.attempt_number == 1


@pytest.mark.parametrize('state', [state for state in S if state is not S.DISPATCH_PREPARED])
def test_only_a_step_that_awaits_its_send_can_be_authorized(store, db_path, state):
    mission_id = make_mission(store, prepared=[])
    set_step_state(db_path, mission_id, 'checkpoint_a', state)
    before = dump(db_path)

    with pytest.raises(StepNotAwaitingSend) as excinfo:
        store.authorize_step_send(mission_id, 'checkpoint_a')

    assert excinfo.value.step_state is state
    assert dump(db_path) == before


# Mission gate

@pytest.mark.parametrize('state', list(MAY_AUTHORIZE))
def test_only_a_running_mission_authorizes_a_send(store, db_path, state):
    route = MISSION_ROUTES[state]
    mission_id = make_mission(store, route=route or ())
    if state is M.NEEDS_RECONCILIATION:
        # A mission reconciles because a step of it is unknown.
        store.transition_step_state(mission_id, 'return_to_base', S.OUTCOME_UNKNOWN)
        store.enter_mission_reconciliation(mission_id)
    if route is None:
        # No ordinary way to COMPLETED while a step is still prepared, so the row is set
        # directly. The gate must hold for a state that should not occur here as well.
        raw(db_path, "UPDATE missions SET mission_state = 'COMPLETED' WHERE mission_id = ?",
            mission_id)
    before = dump(db_path)

    if MAY_AUTHORIZE[state]:
        assert store.authorize_step_send(mission_id, 'checkpoint_a').attempt_number == 1
        return

    with pytest.raises(MissionNotRunning) as excinfo:
        store.authorize_step_send(mission_id, 'checkpoint_a')

    assert excinfo.value.mission_id == mission_id
    assert excinfo.value.mission_state is state
    assert dump(db_path) == before


def test_the_mission_state_is_checked_before_the_step_is_looked_up(store):
    mission_id = make_mission(store, route=())

    with pytest.raises(MissionNotRunning):
        store.authorize_step_send(mission_id, 'no_such_step')


def test_an_unknown_mission_or_step_is_reported_as_such(store):
    mission_id = make_mission(store)

    with pytest.raises(MissionNotFound) as missing_mission:
        store.authorize_step_send('no-such-mission', 'checkpoint_a')
    with pytest.raises(StepNotFound) as missing_step:
        store.authorize_step_send(mission_id, 'no_such_step')

    assert missing_mission.value.identifier == 'no-such-mission'
    assert missing_step.value.mission_id == mission_id
    assert missing_step.value.step_key == 'no_such_step'


def test_a_stored_mission_state_that_is_not_valid_is_reported_not_guessed(store, db_path):
    mission_id = make_mission(store)
    corrupt(db_path, "UPDATE missions SET mission_state = 'BOGUS'")
    before = dump(db_path)

    with pytest.raises(CorruptStoredState) as excinfo:
        store.authorize_step_send(mission_id, 'checkpoint_a')

    assert excinfo.value.entity == 'mission'
    assert dump(db_path) == before


# Unknown-outcome gate

def test_a_running_mission_with_an_unknown_step_authorizes_nothing(store, db_path):
    mission_id = make_mission(store)
    store.transition_step_state(mission_id, 'return_to_base', S.OUTCOME_UNKNOWN)
    # The parent mission has not been moved on: this is the crash window being closed.
    assert store.get_by_mission_id(mission_id).state is M.RUNNING
    before = dump(db_path)

    with pytest.raises(MissionReconciliationRequired) as excinfo:
        store.authorize_step_send(mission_id, 'checkpoint_a')

    assert excinfo.value.mission_id == mission_id
    assert excinfo.value.step_keys == ('return_to_base',)
    assert dump(db_path) == before
    assert store.get_by_mission_id(mission_id).attempts_used == 0
    assert store.get_step(mission_id, 'checkpoint_a').state is S.DISPATCH_PREPARED


def test_every_unknown_step_is_named_in_step_order(store):
    keys = ['c_step', 'b_step', 'a_step', 'd_target']
    mission_id = make_mission(store, keys=keys, prepared=keys)
    store.transition_step_state(mission_id, 'a_step', S.OUTCOME_UNKNOWN)
    store.transition_step_state(mission_id, 'b_step', S.OUTCOME_UNKNOWN)

    with pytest.raises(MissionReconciliationRequired) as excinfo:
        store.authorize_step_send(mission_id, 'd_target')

    # In step order, not alphabetical.
    assert excinfo.value.step_keys == ('b_step', 'a_step')


def test_authorization_proceeds_once_no_step_is_unknown(store, db_path):
    mission_id = make_mission(store)
    store.transition_step_state(mission_id, 'return_to_base', S.OUTCOME_UNKNOWN)
    with pytest.raises(MissionReconciliationRequired):
        store.authorize_step_send(mission_id, 'checkpoint_a')

    # Nothing ordinary leaves OUTCOME_UNKNOWN. A later reconciliation would; the row is
    # changed directly here to stand for that.
    set_step_state(db_path, mission_id, 'return_to_base', S.FAILED)

    assert store.authorize_step_send(mission_id, 'checkpoint_a').attempt_number == 1


def test_a_mission_that_is_not_running_is_reported_before_an_unknown_step(store):
    mission_id = make_mission(store, route=(M.RUNNING,))
    store.transition_step_state(mission_id, 'return_to_base', S.OUTCOME_UNKNOWN)
    store.enter_mission_reconciliation(mission_id)

    with pytest.raises(MissionNotRunning):
        store.authorize_step_send(mission_id, 'checkpoint_a')


def test_the_target_step_state_is_reported_before_an_unknown_step(store):
    mission_id = make_mission(store)
    store.transition_step_state(mission_id, 'checkpoint_a', S.OUTCOME_UNKNOWN)
    store.transition_step_state(mission_id, 'return_to_base', S.OUTCOME_UNKNOWN)

    with pytest.raises(StepNotAwaitingSend) as excinfo:
        store.authorize_step_send(mission_id, 'checkpoint_a')

    assert excinfo.value.step_state is S.OUTCOME_UNKNOWN


def test_an_unknown_step_is_reported_before_a_missing_dispatch_and_before_the_budget(
        store, db_path):
    mission_id = make_mission(store, max_attempts=1, keys=['a', 'b', 'c'])
    store.authorize_step_send(mission_id, 'a')                  # the budget is now spent
    store.transition_step_state(mission_id, 'b', S.OUTCOME_UNKNOWN)
    raw(db_path, "DELETE FROM step_dispatches WHERE step_key = 'c'")

    with pytest.raises(MissionReconciliationRequired):
        store.authorize_step_send(mission_id, 'c')


# Missing or corrupt dispatch

def test_a_prepared_step_without_a_dispatch_row_cannot_be_authorized(store, db_path):
    mission_id = make_mission(store)
    raw(db_path, "DELETE FROM step_dispatches WHERE step_key = 'checkpoint_a'")
    before = dump(db_path)

    with pytest.raises(CorruptDispatchRecord) as excinfo:
        store.authorize_step_send(mission_id, 'checkpoint_a')

    assert excinfo.value.mission_id == mission_id
    assert excinfo.value.step_key == 'checkpoint_a'
    assert 'DISPATCH_PREPARED' in excinfo.value.reason
    assert dump(db_path) == before
    assert store.get_by_mission_id(mission_id).attempts_used == 0


@pytest.mark.parametrize('stored', [
    'not-a-uuid',
    str(uuid.uuid4()).upper(),
    '{' + str(uuid.uuid4()) + '}',
    '',
])
def test_a_dispatch_id_that_is_not_canonical_uuid_text_cannot_be_authorized(
        store, db_path, stored):
    mission_id = make_mission(store)
    raw(db_path, "UPDATE step_dispatches SET dispatch_id = ? WHERE step_key = 'checkpoint_a'",
        stored)
    before = dump(db_path)

    with pytest.raises(CorruptDispatchRecord) as excinfo:
        store.authorize_step_send(mission_id, 'checkpoint_a')

    assert excinfo.value.value == stored
    assert dump(db_path) == before
    assert store.get_by_mission_id(mission_id).attempts_used == 0


def test_a_migrated_prepared_step_without_an_identity_cannot_be_authorized(db_path):
    with MissionStore(db_path) as store:
        mission_id = make_mission(store, prepared=[])
    # Rewind to what a version 2 database holds: a step already prepared, with no
    # dispatch or context table at all. Opening it migrates it for real.
    with contextlib.closing(sqlite3.connect(db_path, isolation_level=None)) as conn:
        conn.execute("UPDATE mission_steps SET step_state = 'DISPATCH_PREPARED'"
                     " WHERE step_key = 'checkpoint_a'")
        conn.execute('DROP TABLE mission_reconciliation_contexts')
        conn.execute('DROP TABLE recovery_evidence')
        conn.execute('DROP TABLE dispatch_authorizations')
        conn.execute('DROP TABLE dispatch_contexts')
        conn.execute('DROP TABLE step_dispatches')
        conn.execute('PRAGMA user_version = 2')

    with MissionStore(db_path) as store:
        before = dump(db_path)
        with pytest.raises(CorruptDispatchRecord) as excinfo:
            store.authorize_step_send(mission_id, 'checkpoint_a')

        assert 'DISPATCH_PREPARED' in excinfo.value.reason
        assert dump(db_path) == before
    assert raw(db_path, 'PRAGMA user_version') == [(7,)]
    assert raw(db_path, 'SELECT COUNT(*) FROM step_dispatches') == [(0,)]
    assert raw(db_path, 'SELECT COUNT(*) FROM dispatch_contexts') == [(0,)]
    assert raw(db_path, 'SELECT COUNT(*) FROM dispatch_authorizations') == [(0,)]
    assert raw(db_path, 'SELECT COUNT(*) FROM recovery_evidence') == [(0,)]
    assert raw(db_path, 'SELECT COUNT(*) FROM mission_reconciliation_contexts') == [(0,)]


# Direct entry to DISPATCHED

@pytest.mark.parametrize('state', list(S))
def test_a_plain_transition_cannot_enter_dispatched(store, db_path, state):
    mission_id = make_mission(store, prepared=[])
    # Whatever the current state is, seeded directly so that none has to be reachable.
    set_step_state(db_path, mission_id, 'checkpoint_a', state)
    before = dump(db_path)

    with pytest.raises(SendAuthorizationRequired) as excinfo:
        store.transition_step_state(mission_id, 'checkpoint_a', S.DISPATCHED)

    assert excinfo.value.mission_id == mission_id
    assert excinfo.value.step_key == 'checkpoint_a'
    # Not a refused transition: the caller used the wrong operation.
    assert not isinstance(excinfo.value, InvalidStateTransition)
    assert dump(db_path) == before


def test_the_guard_is_raised_before_the_database_is_touched(db_path):
    with MissionStore(db_path, busy_timeout=0.05) as store:
        blocker = sqlite3.connect(db_path, isolation_level=None)
        blocker.execute('BEGIN IMMEDIATE')
        try:
            with pytest.raises(SendAuthorizationRequired) as excinfo:
                store.transition_step_state('no-such-mission', 'no-such-step', S.DISPATCHED)
        finally:
            blocker.execute('ROLLBACK')
            blocker.close()

    assert excinfo.value.mission_id == 'no-such-mission'
    assert excinfo.value.step_key == 'no-such-step'


def test_the_pure_state_machine_edge_is_unchanged():
    assert transition_step(S.DISPATCH_PREPARED, S.DISPATCHED) is S.DISPATCHED


def test_only_the_guard_and_the_authorization_mention_the_dispatched_state():
    mentioning = {
        name for name, function in inspect.getmembers(MissionStore, inspect.isfunction)
        if 'StepState.DISPATCHED' in inspect.getsource(function)}

    assert mentioning == {'transition_step_state', 'authorize_step_send'}


# No refund

@pytest.mark.parametrize('state', list(ROUTES_FROM_DISPATCHED))
def test_a_committed_authorization_is_never_refunded(db_path, state):
    with MissionStore(db_path) as store:
        mission_id = make_mission(store)
        store.authorize_step_send(mission_id, 'checkpoint_a')
        for step in ROUTES_FROM_DISPATCHED[state]:
            store.transition_step_state(mission_id, 'checkpoint_a', step)
        assert store.get_by_mission_id(mission_id).attempts_used == 1

    for _ in range(2):
        with MissionStore(db_path) as reopened:
            assert reopened.get_by_mission_id(mission_id).attempts_used == 1


def test_a_cancelling_or_failed_mission_does_not_refund_an_authorization(store):
    mission_id = make_mission(store)
    store.authorize_step_send(mission_id, 'checkpoint_a')

    store.transition_mission_state(mission_id, M.CANCELLING)
    assert store.get_by_mission_id(mission_id).attempts_used == 1
    store.transition_mission_state(mission_id, M.CANCELLED)

    assert store.get_by_mission_id(mission_id).attempts_used == 1


# Cancellation, both serial orders

def test_a_step_cancelled_first_is_never_authorized_and_takes_no_attempt(store):
    mission_id = make_mission(store)
    prepared = store.get_step_dispatch(mission_id, 'checkpoint_a')
    store.transition_step_state(mission_id, 'checkpoint_a', S.CANCELLED)

    with pytest.raises(StepNotAwaitingSend) as excinfo:
        store.authorize_step_send(mission_id, 'checkpoint_a')

    assert excinfo.value.step_state is S.CANCELLED
    assert store.get_by_mission_id(mission_id).attempts_used == 0
    assert store.get_step_dispatch(mission_id, 'checkpoint_a') == prepared


def test_a_step_authorized_first_can_still_be_cancelled_without_a_refund(store):
    mission_id = make_mission(store)
    prepared = store.get_step_dispatch(mission_id, 'checkpoint_a')
    store.authorize_step_send(mission_id, 'checkpoint_a')

    # A legal move in the ordinary graph. It records a decision made here: it does not show
    # that an external request was cancelled or that a robot stopped.
    cancelled = store.transition_step_state(mission_id, 'checkpoint_a', S.CANCELLED)

    assert cancelled.state is S.CANCELLED
    assert store.get_by_mission_id(mission_id).attempts_used == 1
    assert store.get_step_dispatch(mission_id, 'checkpoint_a') == prepared


def test_a_mission_cancelling_first_authorizes_nothing_and_takes_no_attempt(store, db_path):
    mission_id = make_mission(store)
    store.transition_mission_state(mission_id, M.CANCELLING)
    before = dump(db_path)

    with pytest.raises(MissionNotRunning) as excinfo:
        store.authorize_step_send(mission_id, 'checkpoint_a')

    assert excinfo.value.mission_state is M.CANCELLING
    assert dump(db_path) == before
    assert store.get_step(mission_id, 'checkpoint_a').state is S.DISPATCH_PREPARED


def test_an_authorization_that_won_before_the_mission_cancelled_stays_consumed(store):
    mission_id = make_mission(store)
    receipt = store.authorize_step_send(mission_id, 'checkpoint_a')

    store.transition_mission_state(mission_id, M.CANCELLING)

    assert store.get_by_mission_id(mission_id).attempts_used == receipt.attempt_number == 1
    assert store.get_step(mission_id, 'checkpoint_a').state is S.DISPATCHED


# Concurrency. Each worker opens its own store, as separate callers would.

def run_concurrently(db_path, workers, rounds, operation):
    """
    Run operation(store, worker, round) in every worker, released together each round.

    Returns results[worker][round].
    """
    barrier = threading.Barrier(workers, timeout=TIMEOUT)

    def run_worker(worker):
        results = []
        with MissionStore(db_path) as store:
            for round_index in range(rounds):
                barrier.wait()
                results.append(operation(store, worker, round_index))
        return results

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(run_worker, worker) for worker in range(workers)]
        return [future.result(timeout=TIMEOUT) for future in futures]


def try_authorize(store, mission_id, key):
    try:
        return store.authorize_step_send(mission_id, key)
    except (StepNotAwaitingSend, AttemptLimitReached, MissionNotRunning) as refusal:
        return refusal


def missions(db_path, count, max_attempts, keys):
    with MissionStore(db_path) as store:
        return [make_mission(store, request_id=f'req-{n}', max_attempts=max_attempts, keys=keys)
                for n in range(count)]


def test_concurrent_callers_get_one_authorization_for_one_step(db_path):
    workers = 8
    mission_ids = missions(db_path, ROUNDS, 3, ['checkpoint'])

    results = run_concurrently(
        db_path, workers, ROUNDS,
        lambda store, worker, n: try_authorize(store, mission_ids[n], 'checkpoint'))

    with MissionStore(db_path) as store:
        for n, mission_id in enumerate(mission_ids):
            outcomes = [worker[n] for worker in results]
            receipts = [o for o in outcomes if isinstance(o, SendAuthorization)]
            refusals = [o for o in outcomes if isinstance(o, StepNotAwaitingSend)]
            assert len(receipts) == 1, f'round {n}: {outcomes}'
            assert len(refusals) == workers - 1, f'round {n}: {outcomes}'
            assert receipts[0].attempt_number == 1
            assert receipts[0].dispatch_id == (
                store.get_step_dispatch(mission_id, 'checkpoint').dispatch_id)
            # Everyone who lost saw the state the winner committed.
            assert {refusal.step_state for refusal in refusals} == {S.DISPATCHED}
            assert store.get_step(mission_id, 'checkpoint').state is S.DISPATCHED
            assert store.get_by_mission_id(mission_id).attempts_used == 1
    # The same, read straight from storage.
    assert raw(db_path, "SELECT COUNT(*) FROM mission_steps WHERE step_state = 'DISPATCHED'"
               ) == [(ROUNDS,)]
    assert raw(db_path, 'SELECT SUM(attempts_used) FROM missions') == [(ROUNDS,)]


def test_callers_on_different_steps_with_one_attempt_left_get_one_authorization(db_path):
    mission_ids = missions(db_path, ROUNDS, 1, KEYS)

    results = run_concurrently(
        db_path, 2, ROUNDS,
        lambda store, worker, n: try_authorize(store, mission_ids[n], KEYS[worker]))

    with MissionStore(db_path) as store:
        for n, mission_id in enumerate(mission_ids):
            outcomes = [worker[n] for worker in results]
            receipts = [o for o in outcomes if isinstance(o, SendAuthorization)]
            refusals = [o for o in outcomes if isinstance(o, AttemptLimitReached)]
            assert len(receipts) == 1 and len(refusals) == 1, f'round {n}: {outcomes}'
            assert store.get_by_mission_id(mission_id).attempts_used == 1
            states = sorted(step.state.value for step in store.get_steps(mission_id))
            assert states == ['DISPATCHED', 'DISPATCH_PREPARED']


def test_more_callers_than_attempts_never_exceed_the_budget(db_path):
    workers, max_attempts = 4, 3
    keys = [f'step_{n}' for n in range(workers)]
    mission_ids = missions(db_path, ROUNDS, max_attempts, keys)

    results = run_concurrently(
        db_path, workers, ROUNDS,
        lambda store, worker, n: try_authorize(store, mission_ids[n], keys[worker]))

    with MissionStore(db_path) as store:
        for n, mission_id in enumerate(mission_ids):
            outcomes = [worker[n] for worker in results]
            receipts = [o for o in outcomes if isinstance(o, SendAuthorization)]
            assert len(receipts) == max_attempts, f'round {n}: {outcomes}'
            assert sorted(r.attempt_number for r in receipts) == [1, 2, 3]
            assert store.get_by_mission_id(mission_id).attempts_used == max_attempts


def test_an_authorization_racing_a_step_cancellation_has_one_consistent_outcome(db_path):
    mission_ids = missions(db_path, ROUNDS, 3, ['checkpoint'])

    def act(store, worker, n):
        if worker == 0:
            return try_authorize(store, mission_ids[n], 'checkpoint')
        return store.transition_step_state(mission_ids[n], 'checkpoint', S.CANCELLED)

    results = run_concurrently(db_path, 2, ROUNDS, act)

    with MissionStore(db_path) as store:
        for n, mission_id in enumerate(mission_ids):
            authorization, cancellation = results[0][n], results[1][n]
            # CANCELLED is legal from PREPARED and from DISPATCHED: it always lands.
            assert cancellation.state is S.CANCELLED, f'round {n}'
            assert store.get_step(mission_id, 'checkpoint').state is S.CANCELLED
            used = store.get_by_mission_id(mission_id).attempts_used
            if isinstance(authorization, SendAuthorization):
                assert used == 1, f'round {n}'
            else:
                # The cancellation won: no permission, and no attempt taken.
                assert isinstance(authorization, StepNotAwaitingSend), f'round {n}'
                assert authorization.step_state is S.CANCELLED
                assert used == 0, f'round {n}'
            # The dispatch row is kept either way.
            assert store.get_step_dispatch(mission_id, 'checkpoint')


def test_an_authorization_racing_a_mission_cancellation_has_one_consistent_outcome(db_path):
    mission_ids = missions(db_path, ROUNDS, 3, ['checkpoint'])

    def act(store, worker, n):
        if worker == 0:
            return try_authorize(store, mission_ids[n], 'checkpoint')
        return store.transition_mission_state(mission_ids[n], M.CANCELLING)

    results = run_concurrently(db_path, 2, ROUNDS, act)

    with MissionStore(db_path) as store:
        for n, mission_id in enumerate(mission_ids):
            authorization = results[0][n]
            mission = store.get_by_mission_id(mission_id)
            step = store.get_step(mission_id, 'checkpoint')
            assert mission.state is M.CANCELLING, f'round {n}'
            if isinstance(authorization, SendAuthorization):
                assert (mission.attempts_used, step.state) == (1, S.DISPATCHED), f'round {n}'
            else:
                assert isinstance(authorization, MissionNotRunning), f'round {n}'
                assert authorization.mission_state is M.CANCELLING
                assert (mission.attempts_used, step.state) == (0, S.DISPATCH_PREPARED)


# Failures injected around the connection, not by any hook in the store.

def normalized(sql):
    return ' '.join(sql.split()).upper()


STAGES = {
    'begin': lambda sql: sql.startswith('BEGIN'),
    'read the mission': lambda sql: sql.startswith('SELECT') and 'FROM MISSIONS ' in sql,
    'read the target step': lambda sql: (
        sql.startswith('SELECT') and 'FROM MISSION_STEPS' in sql and 'STEP_KEY = ?' in sql),
    'read every step': lambda sql: (
        sql.startswith('SELECT') and 'FROM MISSION_STEPS' in sql and 'ORDER BY' in sql),
    'read the dispatch': lambda sql: sql.startswith('SELECT') and 'FROM STEP_DISPATCHES' in sql,
    'update the attempt counter': lambda sql: sql.startswith('UPDATE MISSIONS'),
    'update the step': lambda sql: sql.startswith('UPDATE MISSION_STEPS'),
    'commit': lambda sql: sql == 'COMMIT',
}

# Statements that must already have run when a later stage fails: the rollback, not an
# early exit, is what leaves the counter and the step as they were.
RAN_BEFORE = {
    'update the step': ['UPDATE MISSIONS'],
    'commit': ['UPDATE MISSIONS', 'UPDATE MISSION_STEPS'],
}


@pytest.mark.parametrize('stage', list(STAGES))
def test_a_failure_before_the_commit_leaves_everything_as_it_was(monkeypatch, db_path, stage):
    store, connection = open_faulty_store(monkeypatch, db_path)
    with store:
        mission_id = make_mission(store, max_attempts=2)
        before = dump(db_path)
        connection.statements.clear()
        connection.fail_when = lambda sql: STAGES[stage](normalized(sql))

        with pytest.raises(sqlite3.OperationalError, match='injected failure'):
            store.authorize_step_send(mission_id, 'checkpoint_a')

        connection.fail_when = None
        assert not connection.in_transaction
        logged = [normalized(sql) for sql in connection.statements]
        assert any(STAGES[stage](sql) for sql in logged), 'the stage was never reached'
        for prefix in RAN_BEFORE.get(stage, []):
            assert any(sql.startswith(prefix) for sql in logged)
        # Still PREPARED, with the attempt not taken, and not a cell different.
        assert dump(db_path) == before

        # And the store still works.
        receipt = store.authorize_step_send(mission_id, 'checkpoint_a')
        assert receipt.attempt_number == 1
        assert store.get_step(mission_id, 'checkpoint_a').state is S.DISPATCHED


def test_an_error_after_a_successful_commit_returns_no_receipt_and_allows_no_second(
        monkeypatch, db_path):
    store, connection = open_faulty_store(monkeypatch, db_path)
    with store:
        mission_id = make_mission(store, max_attempts=2)

        def commit_then_fail(sql):
            if normalized(sql) == 'COMMIT':
                connection._real.execute('COMMIT')      # the commit really happens
                return True
            return False

        connection.fail_when = commit_then_fail
        with pytest.raises(sqlite3.OperationalError, match='injected failure'):
            store.authorize_step_send(mission_id, 'checkpoint_a')
        connection.fail_when = None

        # The caller got an error and no receipt, and the authorization is durable.
        assert store.get_step(mission_id, 'checkpoint_a').state is S.DISPATCHED
        assert store.get_by_mission_id(mission_id).attempts_used == 1
        with pytest.raises(StepNotAwaitingSend):
            store.authorize_step_send(mission_id, 'checkpoint_a')
        assert store.get_by_mission_id(mission_id).attempts_used == 1


# Crashes. A real child process is killed at a chosen point in the real code.

_CHILD = """
import os
import signal
import sqlite3
import sys

from task_resilience import storage
from task_resilience.storage import MissionStore

db_path, mission_id, step_key, when, marker, how = sys.argv[1:7]
armed = False


def die():
    sys.stdout.flush()
    if how == 'sigkill':
        os.kill(os.getpid(), signal.SIGKILL)
    os._exit(0)


class Dying:
    def __init__(self, real):
        object.__setattr__(self, '_real', real)

    def execute(self, sql, *args):
        hit = armed and marker != '-' and ' '.join(sql.split()).upper().startswith(marker)
        if hit and when == 'before':
            die()
        result = self._real.execute(sql, *args)
        if hit and when == 'after':
            die()
        return result

    def __getattr__(self, name):
        return getattr(self._real, name)

    def __setattr__(self, name, value):
        setattr(self._real, name, value)


real_connect = sqlite3.connect
storage.sqlite3.connect = lambda *args, **kwargs: Dying(real_connect(*args, **kwargs))
store = MissionStore(db_path)
armed = True
receipt = store.authorize_step_send(mission_id, step_key)
print(receipt.dispatch_id, receipt.attempt_number, flush=True)
# Nothing is sent here, on purpose: the process dies with the receipt in hand.
die()
"""


def run_child(db_path, mission_id, when, marker, how):
    return subprocess.run(
        [sys.executable, '-c', _CHILD, str(db_path), mission_id, 'checkpoint_a', when, marker,
         how],
        env=package_env(), capture_output=True, text=True, timeout=30)


EXIT_CODE = {'exit': 0, 'sigkill': -signal.SIGKILL}


@pytest.mark.parametrize(('marker', 'how'), [
    ('UPDATE MISSIONS', 'exit'),            # before the attempt counter is touched
    ('UPDATE MISSION_STEPS', 'exit'),       # after the counter, before the step
    ('COMMIT', 'exit'),                     # after both writes, before the commit
    ('COMMIT', 'sigkill'),
])
def test_a_process_that_dies_before_the_commit_authorizes_nothing(db_path, marker, how):
    with MissionStore(db_path) as store:
        mission_id = make_mission(store, max_attempts=2)
        before = dump(db_path)

    child = run_child(db_path, mission_id, 'before', marker, how)

    assert child.returncode == EXIT_CODE[how]
    assert child.stdout == ''                   # it never got a receipt
    # SQLite rolled the unfinished transaction back on reopening.
    with MissionStore(db_path) as reopened:
        assert dump(db_path) == before
        assert reopened.get_step(mission_id, 'checkpoint_a').state is S.DISPATCH_PREPARED
        assert reopened.get_by_mission_id(mission_id).attempts_used == 0
        # Nobody ever held a permission, so a later caller may be granted one.
        assert reopened.authorize_step_send(mission_id, 'checkpoint_a').attempt_number == 1


@pytest.mark.parametrize('how', ['exit', 'sigkill'])
def test_a_process_that_dies_right_after_the_commit_leaves_the_attempt_consumed(db_path, how):
    with MissionStore(db_path) as store:
        mission_id = make_mission(store, max_attempts=2)

    child = run_child(db_path, mission_id, 'after', 'COMMIT', how)

    assert child.returncode == EXIT_CODE[how]
    assert child.stdout == ''                   # it died before it could return the receipt
    for _ in range(2):
        with MissionStore(db_path) as reopened:
            assert reopened.get_step(mission_id, 'checkpoint_a').state is S.DISPATCHED
            assert reopened.get_by_mission_id(mission_id).attempts_used == 1
            # No one holds a permission and nothing was sent, and no one may be granted a
            # second one: zero sends is the accepted outcome of this window.
            with pytest.raises(StepNotAwaitingSend) as excinfo:
                reopened.authorize_step_send(mission_id, 'checkpoint_a')
            assert excinfo.value.step_state is S.DISPATCHED
            assert reopened.get_by_mission_id(mission_id).attempts_used == 1


@pytest.mark.parametrize('how', ['exit', 'sigkill'])
def test_a_process_that_dies_holding_the_receipt_leaves_no_second_authorization(db_path, how):
    with MissionStore(db_path) as store:
        mission_id = make_mission(store, max_attempts=2)

    child = run_child(db_path, mission_id, 'before', '-', how)

    assert child.returncode == EXIT_CODE[how]
    printed_id, printed_attempt = child.stdout.split()
    with MissionStore(db_path) as reopened:
        # The receipt was for this very dispatch. It is not sent anywhere: the process
        # died before doing any external send, which nothing here can tell from a send.
        assert reopened.get_step_dispatch(mission_id, 'checkpoint_a').dispatch_id == (
            uuid.UUID(printed_id))
        assert printed_attempt == '1'
        assert reopened.get_step(mission_id, 'checkpoint_a').state is S.DISPATCHED
        assert reopened.get_by_mission_id(mission_id).attempts_used == 1
        with pytest.raises(StepNotAwaitingSend):
            reopened.authorize_step_send(mission_id, 'checkpoint_a')
        assert reopened.get_by_mission_id(mission_id).attempts_used == 1


# The receipt

def make_receipt(**overrides):
    values = {
        'mission_id': 'm-1', 'step_key': 'checkpoint_a', 'dispatch_id': uuid.uuid4(),
        'attempt_number': 1, 'max_attempts': 3,
        'authorized_at': datetime(2026, 9, 24, 12, 0, 0, tzinfo=timezone.utc),
    }
    values.update(overrides)
    return SendAuthorization(**values)


def test_a_receipt_is_frozen_and_carries_what_it_was_given():
    receipt = make_receipt()

    assert (receipt.mission_id, receipt.step_key, receipt.attempt_number) == (
        'm-1', 'checkpoint_a', 1)
    with pytest.raises(dataclasses.FrozenInstanceError):
        receipt.attempt_number = 2
    assert {field.name for field in dataclasses.fields(receipt)} == {
        'mission_id', 'step_key', 'dispatch_id', 'attempt_number', 'max_attempts',
        'authorized_at'}


@pytest.mark.parametrize('override', [
    {'mission_id': 5}, {'step_key': None}, {'dispatch_id': str(uuid.uuid4())},
    {'dispatch_id': uuid.uuid4().hex}, {'attempt_number': True}, {'attempt_number': '1'},
    {'max_attempts': 3.0}, {'authorized_at': '2026-09-24T12:00:00+00:00'},
])
def test_a_receipt_rejects_values_of_the_wrong_type(override):
    with pytest.raises(TypeError):
        make_receipt(**override)


@pytest.mark.parametrize('override', [
    {'mission_id': ''}, {'step_key': ''}, {'attempt_number': 0}, {'attempt_number': -1},
    {'attempt_number': 4}, {'max_attempts': 0},
    {'authorized_at': datetime(2026, 9, 24, 12, 0, 0)},                      # no time zone
    {'authorized_at': datetime(2026, 9, 24, 12, 0, 0, tzinfo=timezone(timedelta(hours=2)))},
])
def test_a_receipt_rejects_values_that_cannot_describe_an_authorization(override):
    with pytest.raises(ValueError):
        make_receipt(**override)


def test_no_public_operation_accepts_a_receipt_as_authority():
    for name, function in inspect.getmembers(MissionStore, inspect.isfunction):
        if name.startswith('_'):
            continue
        for parameter in inspect.signature(function).parameters.values():
            assert parameter.annotation is not SendAuthorization, name
            assert parameter.name not in ('authorization', 'receipt'), name


def test_the_new_errors_carry_their_facts():
    required = SendAuthorizationRequired('m-1', 'k')
    not_running = MissionNotRunning('m-1', M.CANCELLING)
    reconcile = MissionReconciliationRequired('m-1', ('a', 'b'))
    not_awaiting = StepNotAwaitingSend('m-1', 'k', S.DISPATCHED)

    assert (required.mission_id, required.step_key) == ('m-1', 'k')
    assert (not_running.mission_id, not_running.mission_state) == ('m-1', M.CANCELLING)
    assert (reconcile.mission_id, reconcile.step_keys) == ('m-1', ('a', 'b'))
    assert (not_awaiting.mission_id, not_awaiting.step_key, not_awaiting.step_state) == (
        'm-1', 'k', S.DISPATCHED)
    for error in (required, not_running, reconcile, not_awaiting):
        assert 'm-1' in str(error)
        assert not isinstance(error, InvalidStateTransition)


# The API and its boundaries

def test_the_store_no_longer_exposes_consume_attempt():
    public = [name for name in dir(MissionStore) if not name.startswith('_')]

    assert not hasattr(MissionStore, 'consume_attempt')
    assert not [name for name in public if any(
        word in name for word in ('consume', 'burn', 'refund'))]


def test_authorization_is_the_only_operation_that_raises_the_attempt_counter():
    increasing = {
        name for name, function in inspect.getmembers(MissionStore, inspect.isfunction)
        if '_CONSUME' in inspect.getsource(function)}

    assert increasing == {'authorize_step_send'}


def test_the_authorization_takes_no_callback_or_external_work():
    signature = inspect.signature(MissionStore.authorize_step_send)

    assert list(signature.parameters) == ['self', 'mission_id', 'step_key']
    assert signature.return_annotation is SendAuthorization


def test_the_authorization_code_does_no_external_work():
    source = textwrap.dedent(inspect.getsource(MissionStore.authorize_step_send))
    tree = ast.parse(source)
    names = {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)}
    imports = [node for node in ast.walk(tree) if isinstance(node, (ast.Import, ast.ImportFrom))]

    assert not imports
    assert not names & {'os', 'sys', 'time', 'socket', 'subprocess', 'urllib', 'http',
                        'requests', 'threading'}


def test_an_authorization_uses_no_network_or_subprocess(store, monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError('external work inside the authorization')

    mission_id = make_mission(store)
    with monkeypatch.context() as patch:
        patch.setattr(socket, 'socket', forbidden)
        patch.setattr(subprocess, 'Popen', forbidden)
        patch.setattr(os, 'system', forbidden)

        store.authorize_step_send(mission_id, 'checkpoint_a')
        with pytest.raises(StepNotAwaitingSend):
            store.authorize_step_send(mission_id, 'checkpoint_a')


def test_importing_the_store_loads_no_ros_network_or_llm_package():
    code = (
        'import sys\n'
        'import task_resilience.storage\n'
        "banned = {'rclpy', 'rcl_interfaces', 'rosidl_runtime_py', 'ament_index_python',\n"
        "          'action_msgs', 'unique_identifier_msgs', 'nav2_msgs', 'geometry_msgs',\n"
        "          'builtin_interfaces', 'socket', 'http', 'urllib', 'ssl', 'requests',\n"
        "          'httpx', 'anthropic', 'openai'}\n"
        "print(sorted(m for m in sys.modules if m.split('.')[0] in banned))\n")

    child = subprocess.run(
        [sys.executable, '-c', code], env=package_env(), capture_output=True, text=True,
        check=True, timeout=30)

    assert child.stdout.strip() == '[]'


def test_the_schema_is_version_7_with_exactly_the_expected_tables_and_columns(db_path):
    MissionStore(db_path).close()

    assert storage.SCHEMA_VERSION == 7
    assert raw(db_path, 'PRAGMA user_version') == [(7,)]
    tables = {name for (name,) in raw(
        db_path, "SELECT name FROM sqlite_master WHERE type = 'table'")}
    assert tables == set(COLUMNS)
    for table, columns in COLUMNS.items():
        assert [row[1] for row in raw(db_path, f'PRAGMA table_info({table})')] == columns


# Task 007 stays separate

def test_a_reconciliation_decision_cannot_reach_the_authorization():
    tree = ast.parse(Path(reconciliation.__file__).read_text())
    imported = {(node.level, node.module) for node in ast.walk(tree)
                if isinstance(node, ast.ImportFrom)}

    # It reads the state definitions and nothing that stores or authorizes.
    assert imported == {(0, 'dataclasses'), (1, 'state_machine')}
    assert 'authorize_step_send' not in Path(reconciliation.__file__).read_text()


def test_a_receipt_is_not_accepted_as_a_snapshot_and_decisions_still_authorize_no_send():
    receipt = make_receipt()
    expected = reconciliation.ExpectedActionContext(
        action_name='/navigate_to_pose', action_type='nav2_msgs/action/NavigateToPose',
        robot_namespace='/robot1')
    observation = reconciliation.RecoveryObservation(
        kind=reconciliation.ObservationKind.LOCAL_TIMEOUT)
    snapshot = reconciliation.DurableSnapshot(
        mission_state=M.RUNNING, step_state=S.DISPATCHED, dispatch_id=uuid.uuid4())

    with pytest.raises(TypeError):
        reconciliation.decide_reconciliation(receipt, expected, observation)

    decision = reconciliation.decide_reconciliation(snapshot, expected, observation)
    assert decision.authorizes_send is False
    assert not hasattr(decision, 'authorization')
