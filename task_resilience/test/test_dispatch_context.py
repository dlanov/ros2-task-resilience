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
import inspect
import os
from pathlib import Path
import re
import signal
import socket
import sqlite3
import subprocess
import sys
import textwrap
import threading
import types
import uuid

from fault_injection import open_faulty_store
import pytest

import task_resilience
from task_resilience import reconciliation, storage
from task_resilience.errors import (
    AttemptLimitReached,
    CorruptDispatchContext,
    CorruptDispatchRecord,
    DispatchContextConflict,
    DispatchContextMissing,
    DispatchNotFound,
    InvalidStateTransition,
    MissionNotFound,
    MissionNotRunning,
    MissionReconciliationRequired,
    SendAuthorizationRequired,
    StepNotAwaitingSend,
    StepNotFound,
)
from task_resilience.model import DispatchContextRecord, DispatchRecord, SendAuthorization
from task_resilience.reconciliation import ExpectedActionContext
from task_resilience.state_machine import MissionState, StepState, transition_step
from task_resilience.storage import MissionStore

M = MissionState
S = StepState

KEYS = ['checkpoint_a', 'return_to_base']

# Each round is an independent race, repeated because a single race can be won "by luck".
ROUNDS = 25
TIMEOUT = 10

ACTION = ExpectedActionContext(
    action_name='/navigate_to_pose', action_type='nav2_msgs/action/NavigateToPose',
    robot_namespace='/robot1')

# The tables as version 5 defines them, written out here so a new column or table shows up.
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
}
V3_TABLES = ['missions', 'mission_steps', 'step_dispatches']
V4_TABLES = [*V3_TABLES, 'dispatch_contexts']

WRITE_VERBS = ('INSERT', 'UPDATE', 'DELETE', 'REPLACE', 'CREATE', 'ALTER', 'DROP')


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


def corrupt(db_path, sql, *params):
    with contextlib.closing(sqlite3.connect(db_path, isolation_level=None)) as conn:
        conn.execute('PRAGMA ignore_check_constraints = ON')
        conn.execute(sql, params)


def dump(db_path, tables=tuple(COLUMNS)):
    """Return every row of the named tables, for comparing the database before and after."""
    return {table: raw(db_path, f'SELECT * FROM {table} ORDER BY 1, 2') for table in tables}


def table_diff(before, after):
    """Return {table: (rows only before, rows only after)} for every table that differs."""
    diff = {}
    for table in before:
        gone = [row for row in before[table] if row not in after[table]]
        new = [row for row in after[table] if row not in before[table]]
        if gone or new:
            diff[table] = (gone, new)
    return diff


def schema_of(db_path):
    return raw(db_path, 'SELECT type, name, sql FROM sqlite_master ORDER BY type, name')


def table_names(db_path):
    return {name for (name,) in raw(
        db_path, "SELECT name FROM sqlite_master WHERE type = 'table'")}


def make_mission(store, request_id='req-001', max_attempts=3, keys=KEYS, prepared=(),
                 route=(M.RUNNING,)):
    """Create a mission, prepare some of its steps with ACTION and walk it along a route."""
    mission = store.create_or_get(request_id, {'goal': 'demo'}, max_attempts)
    store.initialize_steps(mission.mission_id, keys)
    for key in prepared:
        store.prepare_step_dispatch(mission.mission_id, key, ACTION)
    for state in route:
        store.transition_mission_state(mission.mission_id, state)
    return mission.mission_id


def unbind(db_path):
    """Remove every context row, leaving dispatches that have an identity and no context."""
    raw(db_path, 'DELETE FROM dispatch_contexts')


def package_env():
    package_root = Path(task_resilience.__file__).resolve().parent.parent
    return {**os.environ, 'PYTHONPATH': str(package_root)}


def writes(statements):
    return [sql for sql in statements if sql.split()[0].upper() in WRITE_VERBS]


# A version 3 database, built from the schema exactly as Task 005 left it. The tables are
# written out here rather than taken from the store, because the store creates a newer version.

V3_MISSIONS_DDL = """
CREATE TABLE missions (
    mission_id          TEXT PRIMARY KEY,
    request_id          TEXT NOT NULL UNIQUE,
    request_fingerprint TEXT NOT NULL,
    request_json        TEXT NOT NULL,
    max_attempts        INTEGER NOT NULL CHECK (max_attempts > 0),
    attempts_used       INTEGER NOT NULL DEFAULT 0 CHECK (attempts_used >= 0),
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL,
    mission_state       TEXT NOT NULL CHECK (mission_state IN (
        'ACCEPTED', 'RUNNING', 'CANCELLING', 'COMPLETED', 'FAILED', 'CANCELLED',
        'NEEDS_RECONCILIATION'
    )),
    CHECK (attempts_used <= max_attempts)
)
"""

V3_MISSION_STEPS_DDL = """
CREATE TABLE mission_steps (
    mission_id  TEXT NOT NULL REFERENCES missions (mission_id) ON DELETE CASCADE,
    step_key    TEXT NOT NULL CHECK (step_key <> ''),
    step_index  INTEGER NOT NULL CHECK (step_index >= 0),
    step_state  TEXT NOT NULL CHECK (step_state IN (
        'PENDING', 'DISPATCH_PREPARED', 'DISPATCHED', 'ACCEPTED', 'EXECUTING',
        'SUCCEEDED', 'FAILED', 'CANCEL_REQUESTED', 'CANCELLED', 'OUTCOME_UNKNOWN'
    )),
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL,
    PRIMARY KEY (mission_id, step_key),
    UNIQUE (mission_id, step_index)
)
"""

V3_STEP_DISPATCHES_DDL = """
CREATE TABLE step_dispatches (
    dispatch_id TEXT NOT NULL PRIMARY KEY,
    mission_id  TEXT NOT NULL,
    step_key    TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    FOREIGN KEY (mission_id, step_key)
        REFERENCES mission_steps (mission_id, step_key)
        ON DELETE CASCADE,
    UNIQUE (mission_id, step_key)
)
"""

V3_MAIN_ID = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'
V3_UNKNOWN_ID = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb'
V3_CREATED = '2026-05-06T07:08:09.123456+00:00'
V3_UPDATED = '2026-05-06T10:11:12.654321+00:00'

# The steps of the main mission, one per state but OUTCOME_UNKNOWN, which has a mission of
# its own so that it does not gate the sends of this one. Every step but the pending one has
# the dispatch UUID that a version 3 store gave it, and none has a context.
V3_MAIN_STEPS = [
    ('pending', 'PENDING'),
    ('prepared', 'DISPATCH_PREPARED'),
    ('dispatched', 'DISPATCHED'),
    ('accepted', 'ACCEPTED'),
    ('executing', 'EXECUTING'),
    ('cancel_requested', 'CANCEL_REQUESTED'),
    ('succeeded', 'SUCCEEDED'),
    ('failed', 'FAILED'),
    ('cancelled', 'CANCELLED'),
]
V3_UNKNOWN_STEPS = [('unknown', 'OUTCOME_UNKNOWN')]

V3_DISPATCH_IDS = {
    (V3_MAIN_ID, key): f'{index:08x}-0000-4000-8000-{index:012x}'
    for index, (key, state) in enumerate(V3_MAIN_STEPS) if state != 'PENDING'
}
V3_DISPATCH_IDS[(V3_UNKNOWN_ID, 'unknown')] = 'f0000000-0000-4000-8000-000000000001'


def make_v3_database(path):
    with contextlib.closing(sqlite3.connect(path, isolation_level=None)) as conn:
        for ddl in (V3_MISSIONS_DDL, V3_MISSION_STEPS_DDL, V3_STEP_DISPATCHES_DDL):
            conn.execute(ddl)
        for mission_id, request_id, state, attempts, steps in (
                (V3_MAIN_ID, 'req-v3-main', 'RUNNING', 3, V3_MAIN_STEPS),
                (V3_UNKNOWN_ID, 'req-v3-unknown', 'NEEDS_RECONCILIATION', 1, V3_UNKNOWN_STEPS)):
            conn.execute(
                'INSERT INTO missions (mission_id, request_id, request_fingerprint, request_json,'
                ' max_attempts, attempts_used, created_at, updated_at, mission_state)'
                ' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                (mission_id, request_id, 'fingerprint', '{"go":"a"}', 5, attempts, V3_CREATED,
                 V3_UPDATED, state))
            for index, (key, step_state) in enumerate(steps):
                conn.execute(
                    'INSERT INTO mission_steps (mission_id, step_key, step_index, step_state,'
                    ' created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)',
                    (mission_id, key, index, step_state, V3_CREATED, V3_UPDATED))
                if (mission_id, key) in V3_DISPATCH_IDS:
                    conn.execute(
                        'INSERT INTO step_dispatches (dispatch_id, mission_id, step_key,'
                        ' created_at) VALUES (?, ?, ?, ?)',
                        (V3_DISPATCH_IDS[(mission_id, key)], mission_id, key, V3_CREATED))
        conn.execute('PRAGMA user_version = 3')


# The schema

def test_the_schema_is_exactly_version_5_with_the_context_table_and_no_evidence(db_path):
    MissionStore(db_path).close()

    assert storage.SCHEMA_VERSION == 5
    assert raw(db_path, 'PRAGMA user_version') == [(5,)]
    assert table_names(db_path) == set(COLUMNS)
    # Version 4 added the context table. Version 5 adds only the authorization table.
    assert set(COLUMNS) - set(V3_TABLES) == {'dispatch_contexts', 'dispatch_authorizations'}
    assert set(COLUMNS) - set(V4_TABLES) == {'dispatch_authorizations'}
    assert not [name for name in table_names(db_path)
                if any(word in name for word in ('evidence', 'observation', 'reconcil'))]
    for table, columns in COLUMNS.items():
        assert [row[1] for row in raw(db_path, f'PRAGMA table_info({table})')] == columns


def test_the_context_table_is_the_three_strings_keyed_by_the_dispatch(db_path):
    MissionStore(db_path).close()

    # (name, declared type, not null, default, primary key)
    assert [(row[1], row[2], row[3], row[4], row[5])
            for row in raw(db_path, 'PRAGMA table_info(dispatch_contexts)')] == [
        ('dispatch_id', 'TEXT', 1, None, 1),
        ('action_name', 'TEXT', 1, None, 0),
        ('action_type', 'TEXT', 1, None, 0),
        ('robot_namespace', 'TEXT', 1, None, 0),
    ]
    foreign_keys = raw(db_path, 'PRAGMA foreign_key_list(dispatch_contexts)')
    # (id, seq, table, from, to, on update, on delete, match)
    assert [(row[2], row[3], row[4], row[6]) for row in foreign_keys] == [
        ('step_dispatches', 'dispatch_id', 'dispatch_id', 'CASCADE')]


@pytest.mark.parametrize('word', [
    'goal', 'hash', 'body', 'epoch', 'incarnation', 'created', 'updated', 'time', 'evidence',
    'retry', 'attempt', 'lease', 'owner', 'fence', 'revision'])
def test_no_goal_time_evidence_or_ownership_field_is_stored(db_path, word):
    MissionStore(db_path).close()

    columns = [row[1] for row in raw(db_path, 'PRAGMA table_info(dispatch_contexts)')]
    assert not [column for column in columns if word in column]
    assert not [field.name for field in dataclasses.fields(DispatchContextRecord)
                if word in field.name]


@contextlib.contextmanager
def strict_connection(db_path):
    with contextlib.closing(sqlite3.connect(db_path, isolation_level=None)) as conn:
        conn.execute('PRAGMA foreign_keys = ON')
        yield conn


def test_the_table_rejects_rows_the_schema_can_reject(store, db_path):
    mission_id = make_mission(store, prepared=KEYS)
    unbind(db_path)
    first = store.get_step_dispatch(mission_id, 'checkpoint_a').dispatch_id
    second = store.get_step_dispatch(mission_id, 'return_to_base').dispatch_id
    insert = 'INSERT INTO dispatch_contexts VALUES (?, ?, ?, ?)'

    with strict_connection(db_path) as conn:
        for row in [
            (str(uuid.uuid4()), 'a', 't', ''),      # no such dispatch
            (str(first), '', 't', ''),              # empty action name
            (str(first), 'a', '', ''),              # empty action type
            (str(first), None, 't', ''),
            (str(first), 'a', None, ''),
            (str(first), 'a', 't', None),
            (None, 'a', 't', ''),
        ]:
            with pytest.raises(sqlite3.IntegrityError):
                conn.execute(insert, row)
        # An empty namespace is an exact value, not a missing one.
        conn.execute(insert, (str(first), 'a', 't', ''))
        # One row per dispatch: the primary key refuses a second.
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(insert, (str(first), 'b', 'u', '/other'))
        conn.execute(insert, (str(second), 'a', 't', '/r'))
    assert raw(db_path, 'SELECT COUNT(*) FROM dispatch_contexts') == [(2,)]


def test_deleting_a_mission_removes_its_contexts(store, db_path):
    make_mission(store, prepared=KEYS)
    assert raw(db_path, 'SELECT COUNT(*) FROM dispatch_contexts') == [(2,)]

    with strict_connection(db_path) as conn:
        conn.execute('DELETE FROM missions')

    assert raw(db_path, 'SELECT COUNT(*) FROM step_dispatches') == [(0,)]
    assert raw(db_path, 'SELECT COUNT(*) FROM dispatch_contexts') == [(0,)]


# A. Input semantics

EXACT_NAMESPACES = [
    '/robot1', 'robot1', '', '/', '//', ' /robot1', '/robot1 ', '/Robot1', '/robot1/',
    '/robot/1', '/ロボット', 'a\tb',
]


@pytest.mark.parametrize('namespace', EXACT_NAMESPACES)
def test_the_namespace_is_stored_and_read_back_exactly(store, db_path, namespace):
    mission_id = make_mission(store)
    action = ExpectedActionContext(
        action_name='/nav', action_type='pkg/action/Nav', robot_namespace=namespace)

    store.prepare_step_dispatch(mission_id, 'checkpoint_a', action)

    assert raw(db_path, 'SELECT robot_namespace FROM dispatch_contexts') == [(namespace,)]
    assert store.get_dispatch_context(mission_id, 'checkpoint_a').robot_namespace == namespace


def test_lookalike_namespaces_stay_different_values(store):
    keys = ['a', 'b', 'c', 'd']
    mission_id = make_mission(store, keys=keys)
    namespaces = ['/robot1', 'robot1', '', '/']

    for key, namespace in zip(keys, namespaces):
        store.prepare_step_dispatch(mission_id, key, ExpectedActionContext(
            action_name='/nav', action_type='pkg/action/Nav', robot_namespace=namespace))

    assert [store.get_dispatch_context(mission_id, key).robot_namespace
            for key in keys] == namespaces


@pytest.mark.parametrize(('action_name', 'action_type'), [
    ('/navigate_to_pose', 'nav2_msgs/action/NavigateToPose'),
    ('navigate_to_pose', 'nav2_msgs/NavigateToPose'),
    ('/Navigate_To_Pose', 'Nav2_Msgs/Action/NavigateToPose'),
    ('/navigate_to_pose/', 'nav2_msgs/action/NavigateToPose '),
    (' /navigate_to_pose', ' nav2_msgs/action/NavigateToPose'),
    ('/ナビ', 'pkg/action/ナビ'),
])
def test_the_action_name_and_type_are_stored_and_read_back_exactly(
        store, action_name, action_type):
    mission_id = make_mission(store)

    store.prepare_step_dispatch(mission_id, 'checkpoint_a', ExpectedActionContext(
        action_name=action_name, action_type=action_type, robot_namespace='/r'))

    bound = store.get_dispatch_context(mission_id, 'checkpoint_a')
    assert (bound.action_name, bound.action_type) == (action_name, action_type)


def test_an_empty_namespace_is_accepted(store):
    mission_id = make_mission(store)

    store.prepare_step_dispatch(mission_id, 'checkpoint_a', ExpectedActionContext(
        action_name='/nav', action_type='pkg/action/Nav', robot_namespace=''))

    assert store.get_dispatch_context(mission_id, 'checkpoint_a').robot_namespace == ''


@pytest.mark.parametrize('overrides', [
    {'action_name': ''}, {'action_type': ''}, {'action_name': '', 'action_type': ''}])
def test_an_empty_action_name_or_type_is_rejected_by_the_existing_context_type(overrides):
    values = {'action_name': '/nav', 'action_type': 'pkg/action/Nav', 'robot_namespace': '/r'}

    with pytest.raises(ValueError):
        ExpectedActionContext(**{**values, **overrides})


NOT_AN_ACTION = [
    pytest.param(None, id='none'),
    pytest.param('/navigate_to_pose', id='string'),
    pytest.param(('/navigate_to_pose', 'nav2_msgs/action/NavigateToPose', '/robot1'), id='tuple'),
    pytest.param({'action_name': '/a', 'action_type': 't', 'robot_namespace': '/r'}, id='dict'),
    pytest.param(types.SimpleNamespace(
        action_name='/a', action_type='t', robot_namespace='/r'), id='lookalike'),
    pytest.param(object(), id='object'),
    pytest.param(reconciliation.RecoveryObservation(
        kind=reconciliation.ObservationKind.LOCAL_TIMEOUT), id='observation'),
    pytest.param(DispatchContextRecord(
        dispatch_id=uuid.uuid4(), mission_id='m', step_key='k', action_name='/a',
        action_type='t', robot_namespace='/r'), id='stored-record'),
]


@pytest.mark.parametrize('bad', NOT_AN_ACTION)
def test_a_wrong_action_argument_is_rejected_before_the_database_is_touched(db_path, bad):
    with MissionStore(db_path, busy_timeout=0.05) as store:
        blocker = sqlite3.connect(db_path, isolation_level=None)
        blocker.execute('BEGIN IMMEDIATE')
        try:
            # A store that reached the database would fail on the lock instead.
            with pytest.raises(TypeError, match='ExpectedActionContext'):
                store.prepare_step_dispatch('no-such-mission', 'no-such-step', bad)
        finally:
            blocker.execute('ROLLBACK')
            blocker.close()


def test_the_action_is_a_required_argument(store):
    mission_id = make_mission(store)

    assert inspect.signature(MissionStore.prepare_step_dispatch).parameters[
        'action'].default is inspect.Parameter.empty
    with pytest.raises(TypeError):
        store.prepare_step_dispatch(mission_id, 'checkpoint_a')


def test_a_wrong_action_binds_nothing_for_a_real_step(store, db_path):
    mission_id = make_mission(store)
    before = dump(db_path)

    with pytest.raises(TypeError):
        store.prepare_step_dispatch(mission_id, 'checkpoint_a', 'not an action')

    assert dump(db_path) == before


# B. First preparation

def test_preparation_binds_the_uuid_the_context_and_the_state_together(store, db_path):
    mission_id = make_mission(store)

    record = store.prepare_step_dispatch(mission_id, 'checkpoint_a', ACTION)

    assert isinstance(record, DispatchRecord)
    assert (record.mission_id, record.step_key) == (mission_id, 'checkpoint_a')
    assert store.get_step(mission_id, 'checkpoint_a').state is S.DISPATCH_PREPARED
    # Written out in full, not read back through the same names.
    assert raw(
        db_path,
        'SELECT dispatch_id, action_name, action_type, robot_namespace FROM dispatch_contexts'
    ) == [(str(record.dispatch_id), '/navigate_to_pose', 'nav2_msgs/action/NavigateToPose',
           '/robot1')]
    assert store.get_dispatch_context(mission_id, 'checkpoint_a') == DispatchContextRecord(
        dispatch_id=record.dispatch_id, mission_id=mission_id, step_key='checkpoint_a',
        action_name='/navigate_to_pose', action_type='nav2_msgs/action/NavigateToPose',
        robot_namespace='/robot1')


def test_only_the_expected_rows_and_cells_change_on_first_preparation(store, db_path):
    mission_id = make_mission(store)
    before = dump(db_path)

    record = store.prepare_step_dispatch(mission_id, 'checkpoint_a', ACTION)

    diff = table_diff(before, dump(db_path))
    # No mission row, and not the other step.
    assert set(diff) == {'mission_steps', 'step_dispatches', 'dispatch_contexts'}
    (old_step,), (new_step,) = diff['mission_steps']
    changed = {column for column, was, now in zip(COLUMNS['mission_steps'], old_step, new_step)
               if was != now}
    assert changed == {'step_state', 'updated_at'}
    assert (old_step[3], new_step[3]) == ('PENDING', 'DISPATCH_PREPARED')
    # The dispatch was created in the same instant the step changed.
    assert diff['step_dispatches'] == (
        [], [(str(record.dispatch_id), mission_id, 'checkpoint_a', new_step[5])])
    assert diff['dispatch_contexts'] == (
        [], [(str(record.dispatch_id), '/navigate_to_pose', 'nav2_msgs/action/NavigateToPose',
              '/robot1')])


def test_preparation_consumes_no_attempt_and_leaves_the_mission_row_alone(store):
    mission_id = make_mission(store, max_attempts=1)
    before = store.get_by_mission_id(mission_id)

    for key in KEYS:
        store.prepare_step_dispatch(mission_id, key, ACTION)

    assert before.attempts_used == 0
    assert store.get_by_mission_id(mission_id) == before


def test_preparing_authorizes_nothing(store):
    mission_id = make_mission(store, prepared=['checkpoint_a'])

    assert store.get_step(mission_id, 'checkpoint_a').state is S.DISPATCH_PREPARED
    assert store.get_by_mission_id(mission_id).attempts_used == 0
    # A step is DISPATCHED only once authorize_step_send has committed.
    with pytest.raises(SendAuthorizationRequired):
        store.transition_step_state(mission_id, 'checkpoint_a', S.DISPATCHED)


def test_every_step_gets_its_own_dispatch_and_its_own_context(store):
    mission_id = make_mission(store)
    other = ExpectedActionContext(
        action_name='/compute_path', action_type='nav2_msgs/action/ComputePathToPose',
        robot_namespace='/robot2')

    first = store.prepare_step_dispatch(mission_id, 'checkpoint_a', ACTION)
    second = store.prepare_step_dispatch(mission_id, 'return_to_base', other)

    assert first.dispatch_id != second.dispatch_id
    assert store.get_dispatch_context(mission_id, 'checkpoint_a').action_name == (
        '/navigate_to_pose')
    assert store.get_dispatch_context(mission_id, 'return_to_base') == DispatchContextRecord(
        dispatch_id=second.dispatch_id, mission_id=mission_id, step_key='return_to_base',
        action_name='/compute_path', action_type='nav2_msgs/action/ComputePathToPose',
        robot_namespace='/robot2')


def test_steps_may_declare_the_same_action(store, db_path):
    make_mission(store, prepared=KEYS)

    assert raw(db_path, 'SELECT COUNT(*) FROM dispatch_contexts') == [(2,)]
    assert raw(db_path, 'SELECT COUNT(DISTINCT dispatch_id) FROM dispatch_contexts') == [(2,)]


def test_the_returned_dispatch_record_has_not_changed_shape():
    assert [field.name for field in dataclasses.fields(DispatchRecord)] == [
        'dispatch_id', 'mission_id', 'step_key', 'created_at']


@pytest.mark.parametrize(('error', 'mission', 'step'), [
    pytest.param(MissionNotFound, 'no-such-mission', 'checkpoint_a', id='no mission'),
    pytest.param(StepNotFound, None, 'no_such_step', id='no step'),
    pytest.param(InvalidStateTransition, None, 'return_to_base', id='cancelled step'),
])
def test_a_refused_preparation_binds_nothing(store, db_path, error, mission, step):
    mission_id = make_mission(store)
    store.transition_step_state(mission_id, 'return_to_base', S.CANCELLED)
    before = dump(db_path)

    with pytest.raises(error):
        store.prepare_step_dispatch(mission or mission_id, step, ACTION)

    assert dump(db_path) == before


def test_the_context_survives_reopening_the_store(db_path):
    with MissionStore(db_path) as store:
        mission_id = make_mission(store, prepared=['checkpoint_a'])
        bound = store.get_dispatch_context(mission_id, 'checkpoint_a')

    with MissionStore(db_path) as reopened:
        assert reopened.get_dispatch_context(mission_id, 'checkpoint_a') == bound
        assert reopened.prepare_step_dispatch(mission_id, 'checkpoint_a', ACTION) == (
            reopened.get_step_dispatch(mission_id, 'checkpoint_a'))


# C. Replay

# (id, what the replay changes, the fields the conflict must name)
DIFFERENT_CONTEXTS = [
    ('name', {'action_name': '/other_action'}, ('action_name',)),
    ('name case', {'action_name': '/Navigate_To_Pose'}, ('action_name',)),
    ('name slash', {'action_name': 'navigate_to_pose'}, ('action_name',)),
    ('type', {'action_type': 'nav2_msgs/action/ComputePathToPose'}, ('action_type',)),
    ('namespace', {'robot_namespace': '/robot2'}, ('robot_namespace',)),
    ('namespace slash', {'robot_namespace': 'robot1'}, ('robot_namespace',)),
    ('namespace empty', {'robot_namespace': ''}, ('robot_namespace',)),
    ('name and type', {'action_name': '/x', 'action_type': 'y/z'},
     ('action_name', 'action_type')),
    ('all three', {'action_name': '/x', 'action_type': 'y/z', 'robot_namespace': '/w'},
     ('action_name', 'action_type', 'robot_namespace')),
]


def replay_with(**overrides):
    values = {'action_name': '/navigate_to_pose', 'action_type': 'nav2_msgs/action/NavigateToPose',
              'robot_namespace': '/robot1'}
    return ExpectedActionContext(**{**values, **overrides})


def test_replaying_the_same_context_returns_the_same_record_and_writes_nothing(
        monkeypatch, db_path):
    store, connection = open_faulty_store(monkeypatch, db_path)
    with store:
        mission_id = make_mission(store)
        first = store.prepare_step_dispatch(mission_id, 'checkpoint_a', ACTION)
        before = dump(db_path)
        connection.statements.clear()

        # An equal context, not the same object.
        second = store.prepare_step_dispatch(mission_id, 'checkpoint_a', replay_with())

        assert second == first
        assert dump(db_path) == before
        assert writes(connection.statements) == []
        assert store.get_step(mission_id, 'checkpoint_a').updated_at == first.created_at


@pytest.mark.parametrize(('overrides', 'fields'), [c[1:] for c in DIFFERENT_CONTEXTS],
                         ids=[c[0] for c in DIFFERENT_CONTEXTS])
def test_replaying_with_a_different_context_conflicts_and_writes_nothing(
        store, db_path, overrides, fields):
    mission_id = make_mission(store)
    first = store.prepare_step_dispatch(mission_id, 'checkpoint_a', ACTION)
    before = dump(db_path)

    with pytest.raises(DispatchContextConflict) as excinfo:
        store.prepare_step_dispatch(mission_id, 'checkpoint_a', replay_with(**overrides))

    assert excinfo.value.fields == fields
    assert (excinfo.value.mission_id, excinfo.value.step_key) == (mission_id, 'checkpoint_a')
    assert dump(db_path) == before
    # The context that was bound first is still the one bound.
    bound = store.get_dispatch_context(mission_id, 'checkpoint_a')
    assert (bound.action_name, bound.action_type, bound.robot_namespace) == (
        '/navigate_to_pose', 'nav2_msgs/action/NavigateToPose', '/robot1')
    assert store.get_step_dispatch(mission_id, 'checkpoint_a') == first


def test_a_bound_context_is_never_replaced_by_repeated_conflicting_replays(store, db_path):
    mission_id = make_mission(store)
    store.prepare_step_dispatch(mission_id, 'checkpoint_a', ACTION)
    before = dump(db_path)

    for _, overrides, _ in DIFFERENT_CONTEXTS:
        with pytest.raises(DispatchContextConflict):
            store.prepare_step_dispatch(mission_id, 'checkpoint_a', replay_with(**overrides))
        # Not even after the same request comes back with the original context.
        store.prepare_step_dispatch(mission_id, 'checkpoint_a', ACTION)

    assert dump(db_path) == before


def test_a_replay_against_a_corrupt_context_reports_corruption_not_a_conflict(store, db_path):
    mission_id = make_mission(store, prepared=['checkpoint_a'])
    corrupt(db_path, "UPDATE dispatch_contexts SET action_name = ''")
    before = dump(db_path)

    for action in (ACTION, replay_with(action_name='/other')):
        with pytest.raises(CorruptDispatchContext) as excinfo:
            store.prepare_step_dispatch(mission_id, 'checkpoint_a', action)
        assert not isinstance(excinfo.value, DispatchContextConflict)

    assert dump(db_path) == before


def test_the_conflict_error_names_fields_and_is_not_another_error(store):
    mission_id = make_mission(store, prepared=['checkpoint_a'])

    with pytest.raises(DispatchContextConflict) as excinfo:
        store.prepare_step_dispatch(mission_id, 'checkpoint_a', replay_with(action_name='/x'))

    error = excinfo.value
    assert 'action_name' in str(error)
    assert '/x' not in str(error) and '/navigate_to_pose' not in str(error)
    for other in (CorruptDispatchContext, CorruptDispatchRecord, DispatchNotFound,
                  DispatchContextMissing, InvalidStateTransition):
        assert not isinstance(error, other)


# D. A prepared step with a UUID and no context

def test_migration_creates_no_context_for_any_existing_dispatch(db_path):
    make_v3_database(db_path)

    with MissionStore(db_path) as store:
        for (mission_id, key) in V3_DISPATCH_IDS:
            assert str(store.get_step_dispatch(mission_id, key).dispatch_id) == (
                V3_DISPATCH_IDS[(mission_id, key)])
            with pytest.raises(DispatchContextMissing) as excinfo:
                store.get_dispatch_context(mission_id, key)
            assert (excinfo.value.mission_id, excinfo.value.step_key) == (mission_id, key)

    assert raw(db_path, 'SELECT COUNT(*) FROM dispatch_contexts') == [(0,)]
    assert raw(db_path, 'SELECT COUNT(*) FROM step_dispatches') == [(len(V3_DISPATCH_IDS),)]


UNBOUND_ORIGINS = ['migrated version 3 row', 'native row with its context deleted']


def unbound_prepared_step(db_path, origin):
    """Return (mission_id, step_key) of a DISPATCH_PREPARED step with a UUID and no context."""
    if origin == 'migrated version 3 row':
        make_v3_database(db_path)
        MissionStore(db_path).close()           # migrates
        return V3_MAIN_ID, 'prepared'
    with MissionStore(db_path) as store:
        mission_id = make_mission(store, prepared=['checkpoint_a'])
    # Removed by something outside the supported code. Nothing in the file says so.
    unbind(db_path)
    return mission_id, 'checkpoint_a'


@pytest.mark.parametrize('origin', UNBOUND_ORIGINS)
def test_replaying_preparation_of_an_unbound_step_reports_it_missing_and_writes_nothing(
        monkeypatch, db_path, origin):
    mission_id, key = unbound_prepared_step(db_path, origin)
    store, connection = open_faulty_store(monkeypatch, db_path)
    with store:
        before = dump(db_path)
        mission_before = store.get_by_mission_id(mission_id)
        step_before = store.get_step(mission_id, key)
        dispatch_before = store.get_step_dispatch(mission_id, key)
        connection.statements.clear()

        # The same request three times, then every different context: absence is reported
        # first, so not even a different context is a conflict, and nothing is inserted.
        actions = [ACTION] * 3 + [replay_with(**overrides) for _, overrides, _ in
                                  DIFFERENT_CONTEXTS]
        for action in actions:
            with pytest.raises(DispatchContextMissing) as excinfo:
                store.prepare_step_dispatch(mission_id, key, action)
            assert (excinfo.value.mission_id, excinfo.value.step_key) == (mission_id, key)

        assert dump(db_path) == before
        assert writes(connection.statements) == []
        assert store.get_step_dispatch(mission_id, key) == dispatch_before
        assert store.get_step(mission_id, key) == step_before
        assert step_before.state is S.DISPATCH_PREPARED
        # No attempt, no mission timestamp.
        assert store.get_by_mission_id(mission_id) == mission_before
    assert raw(db_path, 'SELECT COUNT(*) FROM dispatch_contexts') == [(0,)]


@pytest.mark.parametrize('origin', UNBOUND_ORIGINS)
def test_an_unbound_prepared_step_cannot_be_authorized_and_nothing_changes(db_path, origin):
    mission_id, key = unbound_prepared_step(db_path, origin)
    with MissionStore(db_path) as store:
        before = dump(db_path)
        mission_before = store.get_by_mission_id(mission_id)

        for _ in range(3):
            with pytest.raises(DispatchContextMissing):
                store.authorize_step_send(mission_id, key)
            with pytest.raises(DispatchContextMissing):
                store.get_dispatch_context(mission_id, key)

        assert dump(db_path) == before
        assert store.get_by_mission_id(mission_id) == mission_before
        assert store.get_step(mission_id, key).state is S.DISPATCH_PREPARED


@pytest.mark.parametrize('origin', UNBOUND_ORIGINS)
def test_an_unbound_prepared_step_can_still_be_cancelled_and_stays_unbound(db_path, origin):
    mission_id, key = unbound_prepared_step(db_path, origin)
    with MissionStore(db_path) as store:
        assert store.transition_step_state(mission_id, key, S.CANCELLED).state is S.CANCELLED

    assert raw(db_path, 'SELECT COUNT(*) FROM dispatch_contexts') == [(0,)]


def test_a_migrated_row_and_a_row_whose_context_was_deleted_cannot_be_told_apart(tmp_path):
    shapes, outcomes = [], []
    for number, origin in enumerate(UNBOUND_ORIGINS):
        path = tmp_path / f'shape-{number}.db'
        mission_id, key = unbound_prepared_step(path, origin)
        shapes.append((
            raw(path, 'SELECT step_state FROM mission_steps WHERE mission_id = ?'
                ' AND step_key = ?', mission_id, key),
            raw(path, 'SELECT COUNT(*) FROM step_dispatches WHERE mission_id = ?'
                ' AND step_key = ?', mission_id, key),
            raw(path, 'SELECT COUNT(*) FROM dispatch_contexts'),
            raw(path, 'PRAGMA user_version')))
        with MissionStore(path) as store:
            found = []
            for operation in (lambda: store.prepare_step_dispatch(mission_id, key, ACTION),
                              lambda: store.authorize_step_send(mission_id, key),
                              lambda: store.get_dispatch_context(mission_id, key)):
                with pytest.raises(Exception) as excinfo:
                    operation()
                found.append(type(excinfo.value))
            outcomes.append(found)

    # Nothing in the schema distinguishes them, so the supported code must not guess.
    assert shapes[0] == shapes[1] == ([('DISPATCH_PREPARED',)], [(1,)], [(0,)], [(5,)])
    assert outcomes[0] == outcomes[1] == [DispatchContextMissing] * 3


def test_a_pending_step_of_a_migrated_mission_is_prepared_and_bound_normally(db_path):
    make_v3_database(db_path)
    with MissionStore(db_path) as store:
        record = store.prepare_step_dispatch(V3_MAIN_ID, 'pending', ACTION)

        assert store.get_step(V3_MAIN_ID, 'pending').state is S.DISPATCH_PREPARED
        assert store.get_dispatch_context(V3_MAIN_ID, 'pending').dispatch_id == record.dispatch_id
    assert raw(db_path, 'SELECT COUNT(*) FROM dispatch_contexts') == [(1,)]


# E. Migrated version 3 steps that have progressed

LEGACY_PROGRESSED = [
    (V3_MAIN_ID, 'dispatched'), (V3_MAIN_ID, 'accepted'), (V3_MAIN_ID, 'executing'),
    (V3_MAIN_ID, 'cancel_requested'), (V3_UNKNOWN_ID, 'unknown'), (V3_MAIN_ID, 'succeeded'),
    (V3_MAIN_ID, 'failed'), (V3_MAIN_ID, 'cancelled'),
]


@pytest.mark.parametrize(('mission_id', 'key'), LEGACY_PROGRESSED)
def test_preparation_never_binds_a_context_to_a_step_that_has_progressed(
        db_path, mission_id, key):
    make_v3_database(db_path)
    with MissionStore(db_path) as store:
        before = dump(db_path)

        # As before this task: the state machine refuses to prepare a step past preparation,
        # whatever context is offered.
        for action in (ACTION, replay_with(action_name='/other')):
            with pytest.raises(InvalidStateTransition):
                store.prepare_step_dispatch(mission_id, key, action)

        assert dump(db_path) == before
        with pytest.raises(DispatchContextMissing):
            store.get_dispatch_context(mission_id, key)
    assert raw(db_path, 'SELECT COUNT(*) FROM dispatch_contexts') == [(0,)]


@pytest.mark.parametrize(('mission_id', 'key', 'target'), [
    (V3_MAIN_ID, 'dispatched', S.ACCEPTED),
    (V3_MAIN_ID, 'accepted', S.EXECUTING),
    (V3_MAIN_ID, 'executing', S.SUCCEEDED),
    (V3_MAIN_ID, 'cancel_requested', S.CANCELLED),
])
def test_ordinary_transitions_of_unbound_legacy_steps_are_unchanged_and_bind_nothing(
        db_path, mission_id, key, target):
    make_v3_database(db_path)
    with MissionStore(db_path) as store:
        assert store.transition_step_state(mission_id, key, target).state is target
        with pytest.raises(DispatchContextMissing):
            store.get_dispatch_context(mission_id, key)
    assert raw(db_path, 'SELECT COUNT(*) FROM dispatch_contexts') == [(0,)]


def test_an_unbound_legacy_unknown_step_keeps_its_ordinary_behavior(db_path):
    make_v3_database(db_path)
    with MissionStore(db_path) as store:
        for target in (S.SUCCEEDED, S.FAILED, S.CANCELLED, S.ACCEPTED):
            with pytest.raises(InvalidStateTransition):
                store.transition_step_state(V3_UNKNOWN_ID, 'unknown', target)
        assert store.get_step(V3_UNKNOWN_ID, 'unknown').state is S.OUTCOME_UNKNOWN
    assert raw(db_path, 'SELECT COUNT(*) FROM dispatch_contexts') == [(0,)]


def test_only_a_pending_step_can_ever_receive_a_context():
    # The one place a context is inserted is the first preparation, which is the state
    # machine's PENDING to DISPATCH_PREPARED move. Same-state replays are not moves. Written
    # out by hand, and asked of the state machine rather than assumed.
    accepted, moves = set(), set()
    for state in S:
        try:
            result = transition_step(state, S.DISPATCH_PREPARED)
        except InvalidStateTransition:
            continue
        accepted.add(state)
        if result is not state:
            moves.add(state)

    assert accepted == {S.PENDING, S.DISPATCH_PREPARED}
    assert moves == {S.PENDING}


# F. The authorization gate

def test_a_bound_prepared_step_is_authorized_as_before(store):
    mission_id = make_mission(store, prepared=['checkpoint_a'])

    receipt = store.authorize_step_send(mission_id, 'checkpoint_a')

    assert isinstance(receipt, SendAuthorization)
    assert receipt.dispatch_id == store.get_step_dispatch(mission_id, 'checkpoint_a').dispatch_id
    assert (receipt.mission_id, receipt.step_key) == (mission_id, 'checkpoint_a')
    assert (receipt.attempt_number, receipt.max_attempts) == (1, 3)
    assert store.get_step(mission_id, 'checkpoint_a').state is S.DISPATCHED
    assert store.get_by_mission_id(mission_id).attempts_used == 1


def test_an_unbound_prepared_step_is_refused_and_nothing_changes(store, db_path):
    mission_id = make_mission(store, prepared=['checkpoint_a'])
    unbind(db_path)
    before = dump(db_path)

    with pytest.raises(DispatchContextMissing) as excinfo:
        store.authorize_step_send(mission_id, 'checkpoint_a')

    assert (excinfo.value.mission_id, excinfo.value.step_key) == (mission_id, 'checkpoint_a')
    # Not one cell, timestamps included: the counter, the mission, the step, the dispatch.
    assert dump(db_path) == before
    assert store.get_step(mission_id, 'checkpoint_a').state is S.DISPATCH_PREPARED
    assert store.get_by_mission_id(mission_id).attempts_used == 0


def test_a_refused_unbound_authorization_executes_no_write_statement(monkeypatch, db_path):
    store, connection = open_faulty_store(monkeypatch, db_path)
    with store:
        mission_id = make_mission(store, prepared=['checkpoint_a'])
        unbind(db_path)
        connection.statements.clear()

        with pytest.raises(DispatchContextMissing):
            store.authorize_step_send(mission_id, 'checkpoint_a')

        assert writes(connection.statements) == []


def test_replaying_preparation_does_not_make_an_unbound_step_authorizable(store, db_path):
    mission_id = make_mission(store, prepared=['checkpoint_a'])
    unbind(db_path)
    before = dump(db_path)

    # In either order and as often as you like: preparation never binds a context, so
    # authorization never opens.
    for _ in range(2):
        with pytest.raises(DispatchContextMissing):
            store.authorize_step_send(mission_id, 'checkpoint_a')
        with pytest.raises(DispatchContextMissing):
            store.prepare_step_dispatch(mission_id, 'checkpoint_a', ACTION)

    assert dump(db_path) == before
    assert store.get_by_mission_id(mission_id).attempts_used == 0
    assert store.get_step(mission_id, 'checkpoint_a').state is S.DISPATCH_PREPARED


def refuse_when(db_path, condition):
    """Return a mission whose prepared step is unbound and also fails the given condition."""
    with MissionStore(db_path) as store:
        route = (M.RUNNING,)
        if condition == 'mission not running':
            route = ()
        mission_id = make_mission(store, prepared=KEYS, route=route)
        if condition == 'step already authorized':
            store.authorize_step_send(mission_id, 'checkpoint_a')
        if condition == 'another step unknown':
            store.transition_step_state(mission_id, 'return_to_base', S.OUTCOME_UNKNOWN)
    unbind(db_path)
    if condition == 'no dispatch row':
        raw(db_path, "DELETE FROM step_dispatches WHERE step_key = 'checkpoint_a'")
    if condition == 'dispatch id not a uuid':
        raw(db_path, "UPDATE step_dispatches SET dispatch_id = 'not-a-uuid'"
                     " WHERE step_key = 'checkpoint_a'")
    if condition == 'attempts exhausted':
        raw(db_path, 'UPDATE missions SET attempts_used = max_attempts')
    return mission_id


# Every check that comes first still wins when the context is also missing.
PRECEDENCE = [
    ('mission not running', MissionNotRunning),
    ('step already authorized', StepNotAwaitingSend),
    ('another step unknown', MissionReconciliationRequired),
    ('no dispatch row', CorruptDispatchRecord),
    ('dispatch id not a uuid', CorruptDispatchRecord),
    # The context comes before the budget, so an exhausted mission is told what is missing.
    ('attempts exhausted', DispatchContextMissing),
]


@pytest.mark.parametrize(('condition', 'expected'), PRECEDENCE, ids=[p[0] for p in PRECEDENCE])
def test_the_context_is_checked_after_dispatch_validation_and_before_the_budget(
        db_path, condition, expected):
    mission_id = refuse_when(db_path, condition)
    with MissionStore(db_path) as store:
        before = dump(db_path)

        with pytest.raises(expected):
            store.authorize_step_send(mission_id, 'checkpoint_a')

        assert dump(db_path) == before


def test_a_bound_step_with_no_budget_is_still_refused_for_the_budget(store, db_path):
    mission_id = make_mission(store, prepared=['checkpoint_a'])
    raw(db_path, 'UPDATE missions SET attempts_used = max_attempts')
    before = dump(db_path)

    with pytest.raises(AttemptLimitReached):
        store.authorize_step_send(mission_id, 'checkpoint_a')

    assert dump(db_path) == before


CORRUPT_CONTEXTS = [
    pytest.param("UPDATE dispatch_contexts SET action_name = ''", (), id='empty name'),
    pytest.param("UPDATE dispatch_contexts SET action_type = ''", (), id='empty type'),
    pytest.param('UPDATE dispatch_contexts SET action_name = ?', (b'\x00\x01',), id='name blob'),
    pytest.param('UPDATE dispatch_contexts SET action_type = ?', (b'\xff',), id='type blob'),
    pytest.param(
        'UPDATE dispatch_contexts SET robot_namespace = ?', (b'ns',), id='namespace blob'),
]


@pytest.mark.parametrize(('sql', 'params'), CORRUPT_CONTEXTS)
def test_a_malformed_context_is_corruption_and_authorization_writes_nothing(
        store, db_path, sql, params):
    mission_id = make_mission(store, prepared=['checkpoint_a'])
    corrupt(db_path, sql, *params)
    before = dump(db_path)

    with pytest.raises(CorruptDispatchContext) as excinfo:
        store.authorize_step_send(mission_id, 'checkpoint_a')

    assert (excinfo.value.mission_id, excinfo.value.step_key) == (mission_id, 'checkpoint_a')
    assert excinfo.value.reason
    assert dump(db_path) == before
    assert store.get_by_mission_id(mission_id).attempts_used == 0
    assert store.get_step(mission_id, 'checkpoint_a').state is S.DISPATCH_PREPARED


def test_a_failure_reading_the_context_during_authorization_leaves_everything_as_it_was(
        monkeypatch, db_path):
    store, connection = open_faulty_store(monkeypatch, db_path)
    with store:
        mission_id = make_mission(store, prepared=['checkpoint_a'])
        before = dump(db_path)
        connection.fail_when = lambda sql: (
            sql.startswith('SELECT') and 'FROM dispatch_contexts' in sql)

        with pytest.raises(sqlite3.OperationalError, match='injected failure'):
            store.authorize_step_send(mission_id, 'checkpoint_a')

        connection.fail_when = None
        assert not connection.in_transaction
        assert dump(db_path) == before
        assert store.authorize_step_send(mission_id, 'checkpoint_a').attempt_number == 1


# G. The read API

def test_get_dispatch_context_returns_the_frozen_stored_record(store):
    mission_id = make_mission(store, prepared=['checkpoint_a'])

    bound = store.get_dispatch_context(mission_id, 'checkpoint_a')

    assert bound == DispatchContextRecord(
        dispatch_id=store.get_step_dispatch(mission_id, 'checkpoint_a').dispatch_id,
        mission_id=mission_id, step_key='checkpoint_a', action_name='/navigate_to_pose',
        action_type='nav2_msgs/action/NavigateToPose', robot_namespace='/robot1')
    with pytest.raises(dataclasses.FrozenInstanceError):
        bound.action_name = '/other'


def test_reading_a_context_for_something_that_does_not_exist_keeps_the_existing_errors(
        store):
    mission_id = make_mission(store)

    with pytest.raises(MissionNotFound):
        store.get_dispatch_context('no-such-mission', 'checkpoint_a')
    with pytest.raises(StepNotFound):
        store.get_dispatch_context(mission_id, 'no_such_step')
    # A step that was never prepared has no dispatch, which is not the same as no context.
    with pytest.raises(DispatchNotFound):
        store.get_dispatch_context(mission_id, 'checkpoint_a')


def test_a_dispatch_without_a_context_is_reported_as_missing_a_context(store, db_path):
    mission_id = make_mission(store, prepared=['checkpoint_a'])
    unbind(db_path)

    with pytest.raises(DispatchContextMissing):
        store.get_dispatch_context(mission_id, 'checkpoint_a')
    # The dispatch itself is fine.
    assert store.get_step_dispatch(mission_id, 'checkpoint_a').step_key == 'checkpoint_a'


@pytest.mark.parametrize(('sql', 'params'), CORRUPT_CONTEXTS)
def test_a_malformed_context_row_is_corruption_when_read(store, db_path, sql, params):
    mission_id = make_mission(store, prepared=['checkpoint_a'])
    corrupt(db_path, sql, *params)

    with pytest.raises(CorruptDispatchContext) as excinfo:
        store.get_dispatch_context(mission_id, 'checkpoint_a')

    assert not isinstance(excinfo.value, (CorruptDispatchRecord, DispatchContextMissing))


def test_a_corrupt_dispatch_is_still_reported_as_a_corrupt_dispatch(store, db_path):
    mission_id = make_mission(store, prepared=['checkpoint_a'])
    raw(db_path, "UPDATE step_dispatches SET dispatch_id = 'not-a-uuid'")

    with pytest.raises(CorruptDispatchRecord):
        store.get_dispatch_context(mission_id, 'checkpoint_a')


def test_reading_never_binds_or_repairs_anything(monkeypatch, db_path):
    store, connection = open_faulty_store(monkeypatch, db_path)
    with store:
        mission_id = make_mission(store, prepared=KEYS)
        unbind(db_path)
        corrupt(db_path, "INSERT INTO dispatch_contexts VALUES (?, '', 't', '')",
                str(store.get_step_dispatch(mission_id, 'return_to_base').dispatch_id))
        before = dump(db_path)
        connection.statements.clear()

        for _ in range(3):
            with pytest.raises(DispatchContextMissing):
                store.get_dispatch_context(mission_id, 'checkpoint_a')
            with pytest.raises(CorruptDispatchContext):
                store.get_dispatch_context(mission_id, 'return_to_base')

        assert dump(db_path) == before
        assert writes(connection.statements) == []
        assert not [sql for sql in connection.statements if sql.startswith('BEGIN')]


def test_repeated_reads_of_a_valid_context_write_nothing(monkeypatch, db_path):
    store, connection = open_faulty_store(monkeypatch, db_path)
    with store:
        mission_id = make_mission(store, prepared=['checkpoint_a'])
        before = dump(db_path)
        connection.statements.clear()

        reads = [store.get_dispatch_context(mission_id, 'checkpoint_a') for _ in range(3)]

        assert reads[0] == reads[1] == reads[2]
        assert dump(db_path) == before
        assert writes(connection.statements) == []


# The stored record and the new errors

@pytest.mark.parametrize('overrides', [
    {'dispatch_id': str(uuid.uuid4())}, {'dispatch_id': None}, {'mission_id': 5},
    {'step_key': None}, {'action_name': b'a'}, {'action_type': 3}, {'robot_namespace': None},
    {'robot_namespace': b'x'},
])
def test_the_stored_record_rejects_values_of_the_wrong_type(overrides):
    with pytest.raises(TypeError):
        make_record(**overrides)


@pytest.mark.parametrize('overrides', [
    {'mission_id': ''}, {'step_key': ''}, {'action_name': ''}, {'action_type': ''}])
def test_the_stored_record_rejects_values_that_cannot_describe_a_context(overrides):
    with pytest.raises(ValueError):
        make_record(**overrides)


def make_record(**overrides):
    values = {
        'dispatch_id': uuid.UUID('6f1f4b8e-2c1d-4e57-9a3b-5d0c8e7f1a24'), 'mission_id': 'm-1',
        'step_key': 'checkpoint_a', 'action_name': '/navigate_to_pose',
        'action_type': 'nav2_msgs/action/NavigateToPose', 'robot_namespace': '/robot1',
    }
    return DispatchContextRecord(**{**values, **overrides})


def test_the_stored_record_accepts_an_empty_namespace_like_the_context_type():
    assert make_record(robot_namespace='').robot_namespace == ''


def test_the_stored_record_and_the_context_type_agree_on_their_fields_and_rules():
    assert [f.name for f in dataclasses.fields(DispatchContextRecord)][3:] == [
        f.name for f in dataclasses.fields(ExpectedActionContext)]
    for name, value in (('action_name', ''), ('action_type', '')):
        with pytest.raises(ValueError):
            make_record(**{name: value})
        with pytest.raises(ValueError):
            replay_with(**{name: value})
    assert make_record(robot_namespace='') and replay_with(robot_namespace='')


def test_the_new_errors_carry_their_facts_and_are_distinct():
    missing = DispatchContextMissing('m-1', 'k')
    conflict = DispatchContextConflict('m-1', 'k', ('action_name', 'robot_namespace'))
    corrupt_context = CorruptDispatchContext('m-1', 'k', 'action_name is not text', b'\x01')

    assert (missing.mission_id, missing.step_key) == ('m-1', 'k')
    assert (conflict.mission_id, conflict.step_key, conflict.fields) == (
        'm-1', 'k', ('action_name', 'robot_namespace'))
    assert (corrupt_context.mission_id, corrupt_context.step_key, corrupt_context.reason,
            corrupt_context.value) == ('m-1', 'k', 'action_name is not text', b'\x01')
    errors = [missing, conflict, corrupt_context]
    for error in errors:
        assert 'm-1' in str(error)
        assert not isinstance(error, (CorruptDispatchRecord, DispatchNotFound))
    for one in errors:
        assert len([other for other in errors if isinstance(one, type(other))]) == 1


# H. Immutability is a property of the supported code, not of the table

def test_the_store_has_no_public_way_to_update_or_replace_a_context():
    public = [name for name in dir(MissionStore) if not name.startswith('_')]

    assert [name for name in public if 'context' in name] == ['get_dispatch_context']
    assert not [name for name in public if any(
        word in name for word in ('replace', 'update', 'rebind', 'overwrite', 'rewrite',
                                  'set_context', 'bind_'))]
    # The one operation that takes a context is preparation.
    takers = [name for name, function in inspect.getmembers(MissionStore, inspect.isfunction)
              if not name.startswith('_') and any(
                  parameter.annotation is ExpectedActionContext
                  or parameter.name in ('action', 'context')
                  for parameter in inspect.signature(function).parameters.values())]
    assert takers == ['prepare_step_dispatch']


def test_production_code_never_updates_deletes_or_replaces_a_context():
    package = Path(task_resilience.__file__).resolve().parent
    for path in sorted(package.glob('*.py')):
        source = path.read_text()
        assert not re.search(r'UPDATE\s+dispatch_contexts', source, re.I), path.name
        assert not re.search(r'DELETE\s+FROM\s+dispatch_contexts', source, re.I), path.name
        assert not re.search(r'REPLACE\s+INTO\s+dispatch_contexts', source, re.I), path.name
        assert not re.search(r'INSERT\s+OR\s+(REPLACE|IGNORE)', source, re.I), path.name
        assert not re.search(r'ON\s+CONFLICT', source, re.I), path.name


def names_in(function):
    tree = ast.parse(textwrap.dedent(inspect.getsource(function)))
    return tree, {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)}


def executed(tree):
    """Return the first argument of every .execute() call, in source order."""
    calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)
             and isinstance(node.func, ast.Attribute) and node.func.attr == 'execute']
    return [ast.unparse(call.args[0]) for call in sorted(calls, key=lambda call: call.lineno)]


def test_only_the_first_preparation_inserts_a_context():
    package = Path(task_resilience.__file__).resolve().parent
    source = (package / 'storage.py').read_text()

    # One statement, run from one method.
    assert len(re.findall(r'INSERT\s+INTO\s+dispatch_contexts', source, re.I)) == 1
    assert {name for name, function in inspect.getmembers(MissionStore, inspect.isfunction)
            if '_INSERT_CONTEXT' in inspect.getsource(function)} == {'prepare_step_dispatch'}

    # Inside it, only on the path that also inserts the dispatch and moves the step: three
    # statements, in that order, and none of them in the replay branch.
    tree, names = names_in(MissionStore.prepare_step_dispatch)
    assert executed(tree) == ['_INSERT_DISPATCH', '_INSERT_CONTEXT', '_SET_STEP_STATE']
    replay = [node for node in ast.walk(tree)
              if isinstance(node, ast.If) and ast.unparse(node.test) == 'new_state is step.state']
    assert len(replay) == 1
    branch = ast.Module(body=replay[0].body, type_ignores=[])
    assert executed(branch) == []
    assert '_INSERT_CONTEXT' not in {node.id for node in ast.walk(branch)
                                     if isinstance(node, ast.Name)}


def test_no_replay_read_or_helper_can_write_and_no_repair_path_exists():
    writers = {'_INSERT_CONTEXT', '_INSERT_DISPATCH', '_INSERT', '_INSERT_STEP', '_SET_STEP_STATE',
               '_SET_MISSION_STATE', '_CONSUME'}
    for name in ('get_dispatch_context', '_fetch_context', '_require_context',
                 '_require_matching_context'):
        tree, names = names_in(getattr(MissionStore, name))
        assert not names & writers, name
        assert set(executed(tree)) <= {'_CONTEXT_OF_DISPATCH'}, name

    # No method, public or private, is a way to bind, backfill, repair or attest a context.
    assert not [name for name in dir(MissionStore) if any(
        word in name.lower() for word in ('bind', 'backfill', 'repair', 'attest'))]


def test_the_primary_key_gives_one_row_per_dispatch_but_not_physical_immutability(
        store, db_path):
    mission_id = make_mission(store, prepared=['checkpoint_a'])
    dispatch_id = store.get_step_dispatch(mission_id, 'checkpoint_a').dispatch_id

    # The schema refuses a second row, and it does not refuse a change to the row. Raw SQL
    # can change it, so what keeps it fixed is the supported code, and only that.
    with pytest.raises(sqlite3.IntegrityError):
        raw(db_path, 'INSERT INTO dispatch_contexts VALUES (?, ?, ?, ?)',
            str(dispatch_id), 'a', 't', '')
    raw(db_path, "UPDATE dispatch_contexts SET action_name = '/changed by raw sql'")

    assert store.get_dispatch_context(mission_id, 'checkpoint_a').action_name == (
        '/changed by raw sql')


# I. Migration

V1_SCHEMA = """
CREATE TABLE missions (
    mission_id          TEXT PRIMARY KEY,
    request_id          TEXT NOT NULL UNIQUE,
    request_fingerprint TEXT NOT NULL,
    request_json        TEXT NOT NULL,
    max_attempts        INTEGER NOT NULL CHECK (max_attempts > 0),
    attempts_used       INTEGER NOT NULL DEFAULT 0 CHECK (attempts_used >= 0),
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL,
    CHECK (attempts_used <= max_attempts)
)
"""
V1_COLUMNS = [
    'mission_id', 'request_id', 'request_fingerprint', 'request_json', 'max_attempts',
    'attempts_used', 'created_at', 'updated_at',
]


def make_v1_database(path):
    with contextlib.closing(sqlite3.connect(path, isolation_level=None)) as conn:
        conn.execute(V1_SCHEMA)
        for index, attempts in enumerate((0, 2)):
            conn.execute(
                f'INSERT INTO missions ({", ".join(V1_COLUMNS)}) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                (f'{index}' * 8 + '-0000-4000-8000-000000000000', f'req-v1-{index}', 'fingerprint',
                 '{"x":1}', 3, attempts, V3_CREATED, V3_UPDATED))
        conn.execute('PRAGMA user_version = 1')


def make_v2_database(path):
    # Version 2 is version 3 without the dispatch table: steps in every state, none with a
    # dispatch.
    make_v3_database(path)
    with contextlib.closing(sqlite3.connect(path, isolation_level=None)) as conn:
        conn.execute('DROP TABLE step_dispatches')
        conn.execute('PRAGMA user_version = 2')


def legacy_rows(path, origin):
    """Return every row the older version held, over the columns that version had."""
    if origin == 'v1':
        return {'missions': raw(
            path, f'SELECT {", ".join(V1_COLUMNS)} FROM missions ORDER BY mission_id')}
    tables = V3_TABLES if origin == 'v3' else V3_TABLES[:2]
    return dump(path, tables)


ORIGINS = {'v1': make_v1_database, 'v2': make_v2_database, 'v3': make_v3_database}


def test_a_fresh_database_is_created_at_version_5(db_path):
    MissionStore(db_path).close()

    assert raw(db_path, 'PRAGMA user_version') == [(5,)]
    assert table_names(db_path) == set(COLUMNS)


@pytest.mark.parametrize('origin', list(ORIGINS))
def test_every_older_database_migrates_to_version_5_without_changing_a_row(
        db_path, tmp_path, origin):
    ORIGINS[origin](db_path)
    before = legacy_rows(db_path, origin)

    MissionStore(db_path).close()

    assert raw(db_path, 'PRAGMA user_version') == [(5,)]
    assert legacy_rows(db_path, origin) == before
    assert table_names(db_path) == set(COLUMNS)
    # No context was made up for a dispatch that has none.
    assert raw(db_path, 'SELECT COUNT(*) FROM dispatch_contexts') == [(0,)]
    # And no authorization record for one that may have been authorized before.
    assert raw(db_path, 'SELECT COUNT(*) FROM dispatch_authorizations') == [(0,)]
    MissionStore(tmp_path / 'fresh.db').close()
    assert schema_of(db_path) == schema_of(tmp_path / 'fresh.db')


def test_migrated_dispatches_keep_every_cell_and_gain_no_context(db_path):
    make_v3_database(db_path)
    before = dump(db_path, V3_TABLES)

    MissionStore(db_path).close()

    assert dump(db_path, V3_TABLES) == before
    assert raw(db_path, 'SELECT * FROM step_dispatches ORDER BY dispatch_id') == sorted(
        (dispatch_id, mission_id, key, V3_CREATED)
        for (mission_id, key), dispatch_id in V3_DISPATCH_IDS.items())
    assert dump(db_path)['dispatch_contexts'] == []


def test_the_version_3_migration_only_creates_the_two_tables(monkeypatch, db_path):
    make_v3_database(db_path)

    store, connection = open_faulty_store(monkeypatch, db_path)
    store.close()

    statements = connection.statements
    changing = writes(statements)
    assert len(changing) == 2
    assert changing[0].startswith('CREATE TABLE dispatch_contexts (')
    assert changing[1].startswith('CREATE TABLE dispatch_authorizations (')
    # No table was rebuilt: nothing dropped, renamed, altered, copied or filled.
    assert not [sql for sql in statements if sql.split()[0] in ('ALTER', 'DROP', 'INSERT')]
    # Version 4 is never committed on the way: the one write of the version is the last.
    assert 'PRAGMA user_version = 4' not in statements
    assert statements.count('PRAGMA user_version = 5') == 1
    assert statements.index(changing[1]) < statements.index('PRAGMA user_version = 5') < (
        statements.index('COMMIT'))


def test_reopening_a_version_5_database_changes_nothing(monkeypatch, db_path):
    make_v3_database(db_path)
    MissionStore(db_path).close()
    before = (dump(db_path), schema_of(db_path))

    store, connection = open_faulty_store(monkeypatch, db_path)
    store.close()

    assert (dump(db_path), schema_of(db_path)) == before
    assert writes(connection.statements) == []
    assert raw(db_path, 'PRAGMA user_version') == [(5,)]


def test_concurrent_opens_of_a_version_3_database_migrate_it_once(tmp_path):
    for round_index in range(5):
        path = tmp_path / f'v3-{round_index}.db'
        make_v3_database(path)
        before = dump(path, V3_TABLES)
        workers = 8
        barrier = threading.Barrier(workers, timeout=TIMEOUT)

        def open_and_close():
            barrier.wait()
            MissionStore(path).close()

        with ThreadPoolExecutor(max_workers=workers) as pool:
            for future in [pool.submit(open_and_close) for _ in range(workers)]:
                future.result(timeout=TIMEOUT)

        assert raw(path, 'PRAGMA user_version') == [(5,)]
        assert dump(path, V3_TABLES) == before
        assert raw(path, 'SELECT COUNT(*) FROM dispatch_contexts') == [(0,)]
        assert raw(path, 'SELECT COUNT(*) FROM dispatch_authorizations') == [(0,)]
        MissionStore(tmp_path / f'fresh-{round_index}.db').close()
        assert schema_of(path) == schema_of(tmp_path / f'fresh-{round_index}.db')


def assert_still_version_3(db_path, before, schema):
    assert raw(db_path, 'PRAGMA user_version') == [(3,)]
    assert table_names(db_path) == set(V3_TABLES)
    assert dump(db_path, V3_TABLES) == before
    assert schema_of(db_path) == schema
    # No lock was left behind.
    with contextlib.closing(sqlite3.connect(db_path, timeout=0.1, isolation_level=None)) as conn:
        conn.execute('BEGIN IMMEDIATE')
        conn.execute('ROLLBACK')


@pytest.mark.parametrize('failing', [
    'create the context table', 'create the authorization table', 'set the version', 'commit'])
def test_a_failure_while_migrating_from_version_3_rolls_back_cleanly(
        monkeypatch, db_path, failing):
    make_v3_database(db_path)
    before = dump(db_path, V3_TABLES)
    schema = schema_of(db_path)

    if failing.startswith('create'):
        ddl = {
            'create the context table': '_DISPATCH_CONTEXTS_DDL',
            'create the authorization table': '_DISPATCH_AUTHORIZATIONS_DDL',
        }[failing]
        with monkeypatch.context() as patch:
            patch.setattr(storage, ddl, 'THIS IS NOT SQL')
            with pytest.raises(sqlite3.OperationalError):
                MissionStore(db_path)
    else:
        fails = {
            'set the version': lambda sql: sql.strip() == 'PRAGMA user_version = 5',
            'commit': lambda sql: sql.strip().upper() == 'COMMIT',
        }[failing]
        with pytest.raises(sqlite3.OperationalError, match='injected failure'):
            open_faulty_store(monkeypatch, db_path, fail_when=fails)

    assert_still_version_3(db_path, before, schema)
    # A later, healthy open finishes the migration.
    with MissionStore(db_path):
        pass
    assert raw(db_path, 'PRAGMA user_version') == [(5,)]
    assert dump(db_path, V3_TABLES) == before


@pytest.mark.parametrize('origin', ['v1', 'v2'])
@pytest.mark.parametrize('ddl', ['_DISPATCH_CONTEXTS_DDL', '_DISPATCH_AUTHORIZATIONS_DDL'])
def test_a_failure_at_a_late_step_leaves_an_older_database_at_its_own_version(
        monkeypatch, db_path, origin, ddl):
    ORIGINS[origin](db_path)
    before = legacy_rows(db_path, origin)
    schema = schema_of(db_path)
    version = int(origin[1])

    with monkeypatch.context() as patch:
        patch.setattr(storage, ddl, 'THIS IS NOT SQL')
        with pytest.raises(sqlite3.OperationalError):
            MissionStore(db_path)

    # Not version 3 or 4, and not a database with a step or two of the chain applied.
    assert raw(db_path, 'PRAGMA user_version') == [(version,)]
    assert legacy_rows(db_path, origin) == before
    assert schema_of(db_path) == schema


# J. Faults and crashes around the first preparation

def normalized(sql):
    return ' '.join(sql.split()).upper()


PREPARE_STAGES = {
    'begin': lambda sql: sql.startswith('BEGIN'),
    'read the step': lambda sql: (
        sql.startswith('SELECT') and 'FROM MISSION_STEPS' in sql and 'STEP_KEY = ?' in sql),
    'read the dispatch': lambda sql: sql.startswith('SELECT') and 'FROM STEP_DISPATCHES' in sql,
    'insert the dispatch': lambda sql: sql.startswith('INSERT INTO STEP_DISPATCHES'),
    'insert the context': lambda sql: sql.startswith('INSERT INTO DISPATCH_CONTEXTS'),
    'update the step': lambda sql: sql.startswith('UPDATE MISSION_STEPS'),
    'commit': lambda sql: sql == 'COMMIT',
}

# What must already have run when a later stage fails: the rollback, not an early exit, is
# what leaves no part of the trio behind.
PREPARE_RAN_BEFORE = {
    'insert the context': ['INSERT INTO STEP_DISPATCHES'],
    'update the step': ['INSERT INTO STEP_DISPATCHES', 'INSERT INTO DISPATCH_CONTEXTS'],
    'commit': ['INSERT INTO STEP_DISPATCHES', 'INSERT INTO DISPATCH_CONTEXTS',
               'UPDATE MISSION_STEPS'],
}


@pytest.mark.parametrize('stage', list(PREPARE_STAGES))
def test_a_failure_before_the_commit_leaves_none_of_the_uuid_the_context_or_the_state(
        monkeypatch, db_path, stage):
    store, connection = open_faulty_store(monkeypatch, db_path)
    with store:
        mission_id = make_mission(store)
        before = dump(db_path)
        connection.statements.clear()
        connection.fail_when = lambda sql: PREPARE_STAGES[stage](normalized(sql))

        with pytest.raises(sqlite3.OperationalError, match='injected failure'):
            store.prepare_step_dispatch(mission_id, 'checkpoint_a', ACTION)

        connection.fail_when = None
        assert not connection.in_transaction
        logged = [normalized(sql) for sql in connection.statements]
        assert any(PREPARE_STAGES[stage](sql) for sql in logged), 'the stage was never reached'
        for prefix in PREPARE_RAN_BEFORE.get(stage, []):
            assert any(sql.startswith(prefix) for sql in logged)
        # No dispatch, no context and no PREPARED, not a partial trio.
        assert dump(db_path) == before
        assert raw(db_path, 'SELECT COUNT(*) FROM step_dispatches') == [(0,)]
        assert raw(db_path, 'SELECT COUNT(*) FROM dispatch_contexts') == [(0,)]
        assert store.get_step(mission_id, 'checkpoint_a').state is S.PENDING

        # And the store still works: all three commit together on the retry.
        record = store.prepare_step_dispatch(mission_id, 'checkpoint_a', ACTION)
        assert store.get_dispatch_context(mission_id, 'checkpoint_a').dispatch_id == (
            record.dispatch_id)


def commit_then_fail(connection):
    def fail_after_committing(sql):
        if normalized(sql) == 'COMMIT':
            connection._real.execute('COMMIT')      # the commit really happens
            return True
        return False
    return fail_after_committing


def test_an_error_after_a_successful_commit_leaves_all_three_and_a_replay_finds_them(
        monkeypatch, db_path):
    store, connection = open_faulty_store(monkeypatch, db_path)
    with store:
        mission_id = make_mission(store)
        connection.fail_when = commit_then_fail(connection)

        with pytest.raises(sqlite3.OperationalError, match='injected failure'):
            store.prepare_step_dispatch(mission_id, 'checkpoint_a', ACTION)
        connection.fail_when = None

        assert store.get_step(mission_id, 'checkpoint_a').state is S.DISPATCH_PREPARED
        assert raw(db_path, 'SELECT COUNT(*) FROM step_dispatches') == [(1,)]
        assert raw(db_path, 'SELECT COUNT(*) FROM dispatch_contexts') == [(1,)]
        bound = store.get_dispatch_context(mission_id, 'checkpoint_a')
        assert store.prepare_step_dispatch(mission_id, 'checkpoint_a', ACTION).dispatch_id == (
            bound.dispatch_id)
        with pytest.raises(DispatchContextConflict):
            store.prepare_step_dispatch(mission_id, 'checkpoint_a', replay_with(action_name='/x'))
        assert raw(db_path, 'SELECT COUNT(*) FROM dispatch_contexts') == [(1,)]


# Crashes. A real child process is killed at a chosen point in the real code.

_CHILD = """
import os
import signal
import sqlite3
import sys

from task_resilience import storage
from task_resilience.reconciliation import ExpectedActionContext
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
        hit = armed and ' '.join(sql.split()).upper().startswith(marker)
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
action = ExpectedActionContext(
    action_name='/navigate_to_pose', action_type='nav2_msgs/action/NavigateToPose',
    robot_namespace='/robot1')
record = store.prepare_step_dispatch(mission_id, step_key, action)
print(record.dispatch_id, flush=True)
die()
"""

EXIT_CODE = {'exit': 0, 'sigkill': -signal.SIGKILL}


def run_child(db_path, mission_id, step_key, when, marker, how):
    return subprocess.run(
        [sys.executable, '-c', _CHILD, str(db_path), mission_id, step_key, when, marker, how],
        env=package_env(), capture_output=True, text=True, timeout=30)


# (statement, before or after it, how the process dies, whether the trio survives)
FRESH_KILLS = [
    ('BEGIN', 'before', 'exit', False),
    ('INSERT INTO STEP_DISPATCHES', 'before', 'exit', False),
    ('INSERT INTO STEP_DISPATCHES', 'after', 'exit', False),
    ('INSERT INTO DISPATCH_CONTEXTS', 'before', 'exit', False),
    ('INSERT INTO DISPATCH_CONTEXTS', 'after', 'exit', False),
    ('UPDATE MISSION_STEPS', 'before', 'exit', False),
    ('UPDATE MISSION_STEPS', 'after', 'exit', False),
    ('COMMIT', 'before', 'exit', False),
    ('COMMIT', 'before', 'sigkill', False),
    ('COMMIT', 'after', 'exit', True),
    ('COMMIT', 'after', 'sigkill', True),
]


@pytest.mark.parametrize(('marker', 'when', 'how', 'survives'), FRESH_KILLS)
def test_a_process_killed_around_a_fresh_preparation_leaves_all_three_or_none(
        db_path, marker, when, how, survives):
    with MissionStore(db_path) as store:
        mission_id = make_mission(store)
        before = dump(db_path)

    child = run_child(db_path, mission_id, 'checkpoint_a', when, marker, how)

    assert child.returncode == EXIT_CODE[how]
    assert child.stdout == ''
    with MissionStore(db_path) as reopened:
        if not survives:
            # SQLite rolled the unfinished transaction back: not a UUID alone, not a context
            # alone, and not a PREPARED step without either.
            assert dump(db_path) == before
            assert reopened.get_step(mission_id, 'checkpoint_a').state is S.PENDING
            record = reopened.prepare_step_dispatch(mission_id, 'checkpoint_a', ACTION)
            assert reopened.get_dispatch_context(mission_id, 'checkpoint_a').dispatch_id == (
                record.dispatch_id)
        else:
            assert reopened.get_step(mission_id, 'checkpoint_a').state is S.DISPATCH_PREPARED
            assert raw(db_path, 'SELECT COUNT(*) FROM step_dispatches') == [(1,)]
            assert raw(db_path, 'SELECT COUNT(*) FROM dispatch_contexts') == [(1,)]
            found = reopened.get_step_dispatch(mission_id, 'checkpoint_a')
            assert reopened.get_dispatch_context(mission_id, 'checkpoint_a') == (
                DispatchContextRecord(
                    dispatch_id=found.dispatch_id, mission_id=mission_id,
                    step_key='checkpoint_a', action_name='/navigate_to_pose',
                    action_type='nav2_msgs/action/NavigateToPose', robot_namespace='/robot1'))
            # A replay finds the same identity and the same context.
            assert reopened.prepare_step_dispatch(mission_id, 'checkpoint_a', ACTION) == found
            assert reopened.get_by_mission_id(mission_id).attempts_used == 0


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


def race_missions(db_path, prepared):
    with MissionStore(db_path) as store:
        return [make_mission(store, request_id=f'req-{n}', keys=['checkpoint'],
                             prepared=['checkpoint'] if prepared else ())
                for n in range(ROUNDS)]


def try_prepare(store, mission_id, action):
    try:
        return store.prepare_step_dispatch(mission_id, 'checkpoint', action)
    except DispatchContextConflict as conflict:
        return conflict


def test_concurrent_preparation_with_one_context_agrees_on_one_dispatch(db_path):
    workers = 8
    mission_ids = race_missions(db_path, prepared=False)

    results = run_concurrently(
        db_path, workers, ROUNDS,
        lambda store, worker, n: try_prepare(store, mission_ids[n], ACTION))

    for n in range(ROUNDS):
        assert len({worker[n] for worker in results}) == 1, f'round {n}'
        assert isinstance(results[0][n], DispatchRecord)
    assert raw(db_path, 'SELECT COUNT(*) FROM step_dispatches') == [(ROUNDS,)]
    assert raw(db_path, 'SELECT COUNT(*) FROM dispatch_contexts') == [(ROUNDS,)]


def test_concurrent_preparation_with_different_contexts_binds_exactly_one(db_path):
    workers = 8
    contexts = [ACTION, replay_with(robot_namespace='/robot2')]
    mission_ids = race_missions(db_path, prepared=False)

    results = run_concurrently(
        db_path, workers, ROUNDS,
        lambda store, worker, n: try_prepare(store, mission_ids[n], contexts[worker % 2]))

    with MissionStore(db_path) as store:
        for n, mission_id in enumerate(mission_ids):
            winner = store.get_dispatch_context(mission_id, 'checkpoint').robot_namespace
            assert winner in ('/robot1', '/robot2')
            for worker in range(workers):
                outcome = results[worker][n]
                if contexts[worker % 2].robot_namespace == winner:
                    assert isinstance(outcome, DispatchRecord), f'round {n} worker {worker}'
                else:
                    assert isinstance(outcome, DispatchContextConflict)
                    assert outcome.fields == ('robot_namespace',)
    assert raw(db_path, 'SELECT COUNT(*) FROM dispatch_contexts') == [(ROUNDS,)]


def test_concurrent_replays_and_authorizations_of_unbound_steps_all_report_the_context_missing(
        db_path):
    workers = 8
    contexts = [ACTION, replay_with(robot_namespace='/robot2')]
    mission_ids = race_missions(db_path, prepared=True)
    unbind(db_path)
    before = dump(db_path)

    def operation(store, worker, n):
        try:
            if worker < 6:
                return store.prepare_step_dispatch(
                    mission_ids[n], 'checkpoint', contexts[worker % 2])
            return store.authorize_step_send(mission_ids[n], 'checkpoint')
        except DispatchContextMissing as refusal:
            return refusal

    results = run_concurrently(db_path, workers, ROUNDS, operation)

    for worker in range(workers):
        for n in range(ROUNDS):
            outcome = results[worker][n]
            assert isinstance(outcome, DispatchContextMissing), f'worker {worker} round {n}'
    # Nothing was bound, nothing was authorized, and not one cell of the database moved.
    assert dump(db_path) == before
    assert raw(db_path, 'SELECT COUNT(*) FROM dispatch_contexts') == [(0,)]
    assert raw(db_path, 'SELECT SUM(attempts_used) FROM missions') == [(0,)]
    assert raw(db_path, "SELECT COUNT(*) FROM mission_steps WHERE step_state = 'DISPATCH_PREPARED'"
               ) == [(ROUNDS,)]


# K. Task 008 is preserved

def test_authorization_is_still_the_only_operation_that_consumes_an_attempt():
    consuming = {name for name, function in inspect.getmembers(MissionStore, inspect.isfunction)
                 if '_CONSUME' in inspect.getsource(function)}

    assert consuming == {'authorize_step_send'}
    for name in ('prepare_step_dispatch', 'get_dispatch_context', '_require_matching_context',
                 '_require_context', '_fetch_context'):
        source = inspect.getsource(getattr(MissionStore, name))
        assert '_CONSUME' not in source and 'attempts_used' not in source, name
        assert 'StepState.DISPATCHED' not in source, name


def test_the_generic_dispatched_target_is_still_guarded_even_for_a_bound_step(store):
    mission_id = make_mission(store, prepared=['checkpoint_a'])

    with pytest.raises(SendAuthorizationRequired):
        store.transition_step_state(mission_id, 'checkpoint_a', S.DISPATCHED)

    assert store.get_step(mission_id, 'checkpoint_a').state is S.DISPATCH_PREPARED
    assert store.get_by_mission_id(mission_id).attempts_used == 0


def test_an_authorization_is_never_refunded_and_a_replay_is_still_refused(store):
    mission_id = make_mission(store, prepared=['checkpoint_a'])
    store.authorize_step_send(mission_id, 'checkpoint_a')

    with pytest.raises(StepNotAwaitingSend):
        store.authorize_step_send(mission_id, 'checkpoint_a')
    store.transition_step_state(mission_id, 'checkpoint_a', S.CANCELLED)

    assert store.get_by_mission_id(mission_id).attempts_used == 1
    # The context outlives the step's state, like the dispatch.
    assert store.get_dispatch_context(mission_id, 'checkpoint_a').step_key == 'checkpoint_a'


def test_the_receipt_and_the_authorization_signature_have_not_changed():
    assert [field.name for field in dataclasses.fields(SendAuthorization)] == [
        'mission_id', 'step_key', 'dispatch_id', 'attempt_number', 'max_attempts',
        'authorized_at']
    assert list(inspect.signature(MissionStore.authorize_step_send).parameters) == [
        'self', 'mission_id', 'step_key']
    assert inspect.signature(MissionStore.authorize_step_send).return_annotation is (
        SendAuthorization)


def test_a_context_refusal_is_not_a_state_machine_refusal():
    for error in (DispatchContextMissing('m', 'k'), CorruptDispatchContext('m', 'k', 'why')):
        assert not isinstance(error, (InvalidStateTransition, AttemptLimitReached))


# L. Boundaries

def test_the_context_operations_use_no_network_or_subprocess(store, monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError('external work in a context operation')

    with monkeypatch.context() as patch:
        patch.setattr(socket, 'socket', forbidden)
        patch.setattr(subprocess, 'Popen', forbidden)
        patch.setattr(os, 'system', forbidden)

        mission_id = make_mission(store, prepared=['checkpoint_a'])
        store.get_dispatch_context(mission_id, 'checkpoint_a')
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


def test_the_store_takes_only_the_context_type_from_the_decision_module():
    package = Path(task_resilience.__file__).resolve().parent
    tree = ast.parse((package / 'storage.py').read_text())
    imported = {alias.name for node in ast.walk(tree)
                if isinstance(node, ast.ImportFrom) and node.module == 'reconciliation'
                for alias in node.names}
    names = {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)}

    assert imported == {'ExpectedActionContext'}
    # It does not decide, construct observations or read a snapshot.
    assert not names & {'decide_reconciliation', 'RecoveryObservation', 'DurableSnapshot',
                        'ReconciliationDecision', 'ObservationKind', 'ActionStatus',
                        'DecisionKind', 'DecisionReason'}


def test_the_decision_module_still_reaches_neither_the_store_nor_the_model():
    package = Path(task_resilience.__file__).resolve().parent
    tree = ast.parse((package / 'reconciliation.py').read_text())
    imported = {(node.level, node.module) for node in ast.walk(tree)
                if isinstance(node, ast.ImportFrom)}

    assert imported == {(0, 'dataclasses'), (1, 'state_machine')}
    assert storage.ExpectedActionContext is reconciliation.ExpectedActionContext
    definitions = [path.name for path in package.glob('*.py')
                   if 'class ExpectedActionContext' in path.read_text()]
    assert definitions == ['reconciliation.py']


def test_the_state_definitions_are_unchanged():
    assert [state.value for state in MissionState] == [
        'ACCEPTED', 'RUNNING', 'CANCELLING', 'COMPLETED', 'FAILED', 'CANCELLED',
        'NEEDS_RECONCILIATION']
    assert [state.value for state in StepState] == [
        'PENDING', 'DISPATCH_PREPARED', 'DISPATCHED', 'ACCEPTED', 'EXECUTING', 'SUCCEEDED',
        'FAILED', 'CANCEL_REQUESTED', 'CANCELLED', 'OUTCOME_UNKNOWN']
    assert transition_step(S.PENDING, S.DISPATCH_PREPARED) is S.DISPATCH_PREPARED
    assert transition_step(S.DISPATCH_PREPARED, S.DISPATCHED) is S.DISPATCHED
    assert (Path(task_resilience.__file__).resolve().parent / 'state_machine.py').read_text(
    ).count('context') == 0


def test_the_store_still_offers_no_way_to_reconcile_or_send():
    public = [name for name in dir(MissionStore) if not name.startswith('_')]

    assert not [name for name in public if any(
        word in name for word in ('reconcil', 'resolve', 'force', 'override', 'set_state',
                                  'evidence', 'observation', 'retry', 'send_goal'))]
