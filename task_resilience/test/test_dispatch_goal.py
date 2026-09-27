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
Task 013: the durable binding between one dispatch UUID and one exact goal specification.

Groups, roughly in review order:
  A. The model: ExpectedGoalSpecification and DispatchGoalRecord
  B. Schema version 8
  C. Migration
  D. First preparation
  E. Replay
  F. Missing and corrupt goals; legacy dispatches are never repaired
  G. The send-authorization gate
  H. Historical dispatches that progressed before this table existed
  I. The read API
  J. Concurrency
  K. What the code is, and what it must not be
"""

import ast
from concurrent.futures import ThreadPoolExecutor
import contextlib
import dataclasses
import hashlib
import inspect
import json
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
import uuid

from fault_injection import open_faulty_store
import pytest

import task_resilience
from task_resilience import storage
from task_resilience.errors import (
    AttemptLimitReached,
    CorruptDispatchGoal,
    CorruptDispatchRecord,
    DispatchContextMissing,
    DispatchGoalConflict,
    DispatchGoalMissing,
    DispatchNotFound,
    InvalidRequest,
    InvalidStateTransition,
    MissionNotFound,
    MissionNotRunning,
    MissionReconciliationContextMissing,
    MissionReconciliationRequired,
    StepNotAwaitingSend,
    StepNotFound,
)
from task_resilience.model import DispatchGoalRecord, DispatchRecord, ExpectedGoalSpecification
from task_resilience.reconciliation import (
    ActionStatus,
    ExpectedActionContext,
    ObservationKind,
    RecoveryObservation,
)
from task_resilience.state_machine import MissionState, StepState
from task_resilience.storage import MissionStore
from test_dispatch_authorization import (
    fresh_schema,
    make_v6_database,
    MIGRATIONS,
    old_rows,
    ORIGINS as _BASE_ORIGINS,
    V6_TABLES,
    VERSION_OF as _BASE_VERSION_OF,
)
from test_dispatch_context import V3_DISPATCH_IDS, V3_MAIN_ID, V3_UNKNOWN_ID
from test_mission_reconciliation import CONTEXT_DDL, CONTEXT_TABLE

M = MissionState
S = StepState

KEYS = ['checkpoint_a', 'return_to_base']
GOAL_TABLE = 'dispatch_goals'

# Each round is an independent race, repeated because a single race can be won "by luck".
ROUNDS = 25
TIMEOUT = 10

ACTION = ExpectedActionContext(
    action_name='/navigate_to_pose', action_type='nav2_msgs/action/NavigateToPose',
    robot_namespace='/robot1')

GOAL = ExpectedGoalSpecification(
    goal_format='nav2/navigate_to_pose/v1', payload={'target': 'checkpoint-a'})

# The exact columns of the new table, written out here so a new column shows up.
GOAL_COLUMNS = ['dispatch_id', 'goal_format', 'goal_json', 'goal_fingerprint']

ALL_TABLES = (*V6_TABLES, CONTEXT_TABLE, GOAL_TABLE)

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
    """Run SQL that the schema would refuse, as a writer that bypasses the store could."""
    with contextlib.closing(sqlite3.connect(db_path, isolation_level=None)) as conn:
        conn.execute('PRAGMA ignore_check_constraints = ON')
        conn.execute(sql, params)


@contextlib.contextmanager
def strict_connection(db_path):
    with contextlib.closing(sqlite3.connect(db_path, isolation_level=None)) as conn:
        conn.execute('PRAGMA foreign_keys = ON')
        yield conn


def dump(db_path, tables=ALL_TABLES):
    """Return every row of the named tables, for comparing the database before and after."""
    return {table: raw(db_path, f'SELECT * FROM {table} ORDER BY 1, 2') for table in tables}


def table_diff(before, after):
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


def writes(statements):
    return [sql for sql in statements if sql.split()[0].upper() in WRITE_VERBS]


def goal_rows(db_path):
    return raw(db_path, f'SELECT * FROM {GOAL_TABLE} ORDER BY dispatch_id')


def make_mission(store, request_id='req-001', max_attempts=3, keys=KEYS, prepared=(),
                 route=(M.RUNNING,)):
    """Create a mission, prepare some of its steps with ACTION/GOAL and walk it along a route."""
    mission = store.create_or_get(request_id, {'demo': True}, max_attempts)
    store.initialize_steps(mission.mission_id, keys)
    for key in prepared:
        store.prepare_step_dispatch(mission.mission_id, key, ACTION, GOAL)
    for state in route:
        store.transition_mission_state(mission.mission_id, state)
    return mission.mission_id


def unbind_goal(db_path):
    """Remove every goal row, leaving dispatches that have an identity and a context only."""
    raw(db_path, 'DELETE FROM dispatch_goals')


def package_env():
    package_root = Path(task_resilience.__file__).resolve().parent.parent
    return {**os.environ, 'PYTHONPATH': str(package_root)}


def replay_with(**overrides):
    values = {'goal_format': 'nav2/navigate_to_pose/v1', 'payload': {'target': 'checkpoint-a'}}
    return ExpectedGoalSpecification(**{**values, **overrides})


# A version 7 database: the version 6 fixture (Task 011) plus the empty reconciliation table
# (Task 012). Written here because Task 013 is the first task after it, exactly as Task 012
# wrote make_v6_database in test_dispatch_authorization.py after Task 011.
def make_v7_database(path):
    """Build a version 7 database: the version 6 fixture plus the empty reconciliation table."""
    make_v6_database(path)
    with contextlib.closing(sqlite3.connect(path, isolation_level=None)) as conn:
        conn.execute(CONTEXT_DDL)
        conn.execute('PRAGMA user_version = 7')


ORIGINS = {**_BASE_ORIGINS, 'v7': make_v7_database}
VERSION_OF = {**_BASE_VERSION_OF, 'v7': 7}
V7_TABLES = [*V6_TABLES, CONTEXT_TABLE]


def names_in(function):
    tree = ast.parse(textwrap.dedent(inspect.getsource(function)))
    return tree, {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)}


def executed(tree):
    """Return the first argument of every .execute() call, in source order."""
    calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)
             and isinstance(node.func, ast.Attribute) and node.func.attr == 'execute']
    return [ast.unparse(call.args[0]) for call in sorted(calls, key=lambda call: call.lineno)]


# A. The model

def test_the_specification_and_the_record_are_frozen():
    with pytest.raises(dataclasses.FrozenInstanceError):
        GOAL.goal_format = 'other'
    record = DispatchGoalRecord(
        dispatch_id=uuid.uuid4(), mission_id='m', step_key='k', goal_format='f',
        payload={'x': 1}, goal_fingerprint='a' * 64)
    with pytest.raises(dataclasses.FrozenInstanceError):
        record.goal_format = 'other'


def test_goal_format_must_be_a_non_empty_string():
    with pytest.raises(TypeError):
        ExpectedGoalSpecification(goal_format=None, payload={})
    with pytest.raises(TypeError):
        ExpectedGoalSpecification(goal_format=5, payload={})
    with pytest.raises(ValueError):
        ExpectedGoalSpecification(goal_format='', payload={})


def test_goal_format_is_never_normalized():
    # Whitespace and case are exact, like action_name/action_type in Task 009.
    for text in (' nav2/navigate_to_pose/v1', 'nav2/navigate_to_pose/v1 ', 'NAV2/NAVIGATE'):
        assert ExpectedGoalSpecification(goal_format=text, payload={}).goal_format == text


@pytest.mark.parametrize('payload', [
    object(), {1, 2}, frozenset({1}), b'bytes', bytearray(b'x'), 1j, len, [object()],
    (1, 2), {'a': (1, 2)}, [{'b': (1,)}],
    {1: 'a'}, {None: 1}, {True: 1}, {1.5: 1}, {(1, 2): 1}, {b'k': 1},
])
def test_a_payload_outside_the_strict_json_domain_is_rejected(payload):
    with pytest.raises(InvalidRequest):
        ExpectedGoalSpecification(goal_format='f', payload=payload)


def test_subclasses_of_json_types_are_rejected():
    class StrSubclass(str):
        pass

    class DictSubclass(dict):
        pass

    for payload in (StrSubclass('x'), DictSubclass(a=1), {StrSubclass('k'): 1}):
        with pytest.raises(InvalidRequest):
            ExpectedGoalSpecification(goal_format='f', payload=payload)


def test_a_circular_payload_is_rejected():
    loop = []
    loop.append(loop)
    with pytest.raises(InvalidRequest):
        ExpectedGoalSpecification(goal_format='f', payload=loop)


def test_a_container_used_twice_is_not_circular():
    shared = [1, 2]
    spec = ExpectedGoalSpecification(goal_format='f', payload={'a': shared, 'b': shared})
    assert spec.goal_json == '{"a":[1,2],"b":[1,2]}'


def test_every_json_type_is_accepted():
    payload = {
        'null': None, 'flags': [True, False], 'int': -3, 'float': 2.5, 'text': 'text',
        'nested': {'list': [1, [2, {'deep': None}]], 'empty': {}, 'none': []},
    }
    spec = ExpectedGoalSpecification(goal_format='f', payload=payload)
    assert json.loads(spec.goal_json) == payload


def test_a_non_finite_float_is_rejected():
    with pytest.raises(InvalidRequest):
        ExpectedGoalSpecification(goal_format='f', payload={'x': float('nan')})
    with pytest.raises(InvalidRequest):
        ExpectedGoalSpecification(goal_format='f', payload={'x': float('inf')})


def test_an_int_and_the_equal_float_are_different_goals():
    # 1 and 1.0 are different JSON kinds; the canonicalizer must not blur them (int vs float
    # is exactly the distinction plain Python equality would lose, and replay must not lose it).
    as_int = ExpectedGoalSpecification(goal_format='f', payload={'x': 1})
    as_float = ExpectedGoalSpecification(goal_format='f', payload={'x': 1.0})

    assert as_int.goal_json == '{"x":1}'
    assert as_float.goal_json == '{"x":1.0}'
    assert as_int.goal_json != as_float.goal_json
    assert as_int.goal_fingerprint != as_float.goal_fingerprint


def test_key_order_does_not_affect_the_canonical_form():
    first = ExpectedGoalSpecification(goal_format='f', payload={'a': 1, 'b': 2})
    second = ExpectedGoalSpecification(goal_format='f', payload={'b': 2, 'a': 1})

    assert first.goal_json == second.goal_json == '{"a":1,"b":2}'
    assert first.goal_fingerprint == second.goal_fingerprint


def test_list_order_does_affect_the_canonical_form():
    first = ExpectedGoalSpecification(goal_format='f', payload=[1, 2, 3])
    second = ExpectedGoalSpecification(goal_format='f', payload=[3, 2, 1])

    assert first.goal_json != second.goal_json
    assert first.goal_fingerprint != second.goal_fingerprint


def test_the_fingerprint_is_sha256_of_the_complete_specification_not_the_payload_alone():
    spec = ExpectedGoalSpecification(
        goal_format='nav2/navigate_to_pose/v1', payload={'y': 2, 'x': 1})

    expected = hashlib.sha256(
        b'{"goal_format":"nav2/navigate_to_pose/v1","payload":{"x":1,"y":2}}').hexdigest()
    assert spec.goal_fingerprint == expected
    assert spec.goal_json == '{"x":1,"y":2}'
    # The fingerprint is not the fingerprint of goal_json alone: a bare hash of the payload
    # text would not depend on goal_format, and this one does.
    payload_only = hashlib.sha256(spec.goal_json.encode('utf-8')).hexdigest()
    assert spec.goal_fingerprint != payload_only


def test_the_same_payload_under_a_different_format_is_a_different_fingerprint():
    one = ExpectedGoalSpecification(goal_format='nav2/navigate_to_pose/v1', payload={'x': 1})
    two = ExpectedGoalSpecification(goal_format='nav2/navigate_to_pose/v2', payload={'x': 1})

    assert one.goal_json == two.goal_json
    assert one.goal_fingerprint != two.goal_fingerprint


def test_a_caller_mutating_the_payload_after_construction_cannot_change_the_snapshot():
    payload = {'nested': {'x': 1}, 'list': [1, 2]}
    spec = ExpectedGoalSpecification(goal_format='f', payload=payload)
    before_json, before_fingerprint = spec.goal_json, spec.goal_fingerprint

    payload['nested']['x'] = 999
    payload['list'].append(3)
    payload['new_key'] = 'sneaky'

    assert spec.goal_json == before_json
    assert spec.goal_fingerprint == before_fingerprint
    assert spec.goal_json == '{"list":[1,2],"nested":{"x":1}}'


def test_the_specification_never_retains_the_callers_payload_object():
    """Retaining the caller's mutable payload is an observable break of the snapshot contract."""
    payload = {'nested': [1]}
    spec = ExpectedGoalSpecification(goal_format='f', payload=payload)

    assert 'payload' not in vars(spec)
    assert not hasattr(spec, 'payload')
    assert [f.name for f in dataclasses.fields(ExpectedGoalSpecification)] == [
        'goal_format', 'goal_json', 'goal_fingerprint']


def test_excessive_nesting_is_rejected():
    deep = []
    for _ in range(10_000):
        deep = [deep]
    with pytest.raises(InvalidRequest):
        ExpectedGoalSpecification(goal_format='f', payload=deep)


def test_the_record_rejects_values_of_the_wrong_type():
    values = {
        'dispatch_id': uuid.uuid4(), 'mission_id': 'm-1', 'step_key': 'k',
        'goal_format': 'f', 'payload': {'x': 1}, 'goal_fingerprint': 'a' * 64,
    }
    for name, bad in (
        ('dispatch_id', str(uuid.uuid4())), ('dispatch_id', None), ('mission_id', 5),
        ('step_key', None), ('goal_format', b'f'), ('goal_fingerprint', 5),
    ):
        with pytest.raises(TypeError):
            DispatchGoalRecord(**{**values, name: bad})


def test_the_record_rejects_empty_identity_or_format():
    values = {
        'dispatch_id': uuid.uuid4(), 'mission_id': 'm-1', 'step_key': 'k',
        'goal_format': 'f', 'payload': {'x': 1}, 'goal_fingerprint': 'a' * 64,
    }
    for name in ('mission_id', 'step_key', 'goal_format'):
        with pytest.raises(ValueError):
            DispatchGoalRecord(**{**values, name: ''})


def test_the_record_exposes_the_required_fields_and_nothing_hidden():
    names = [field.name for field in dataclasses.fields(DispatchGoalRecord)]
    assert set(names) >= {
        'dispatch_id', 'mission_id', 'step_key', 'goal_format', 'payload', 'goal_fingerprint'}


def test_a_decoded_payload_is_a_plain_mutable_snapshot_that_never_writes_through(store, db_path):
    mission_id = make_mission(store)
    payload = {'nested': [1, 2]}
    store.prepare_step_dispatch(
        mission_id, 'checkpoint_a', ACTION,
        ExpectedGoalSpecification(goal_format='f', payload=payload))

    record = store.get_dispatch_goal(mission_id, 'checkpoint_a')
    record.payload['nested'].append(3)
    record.payload['new'] = 'x'

    reread = store.get_dispatch_goal(mission_id, 'checkpoint_a')
    assert reread.payload == {'nested': [1, 2]}


# B. Schema version 8

def test_schema_version_is_8(db_path):
    MissionStore(db_path).close()

    assert storage.SCHEMA_VERSION == 8
    assert raw(db_path, 'PRAGMA user_version') == [(8,)]


def test_exactly_one_new_table_since_version_7(db_path, tmp_path):
    MissionStore(db_path).close()
    v7 = tmp_path / 'v7.db'
    make_v7_database(v7)  # left native: opening it would migrate it to version 8 too

    assert table_names(db_path) - table_names(v7) == {GOAL_TABLE}


def test_the_goal_table_is_exactly_the_reviewed_shape(db_path):
    MissionStore(db_path).close()

    # (name, declared type, not null, default, primary key)
    assert [(row[1], row[2], row[3], row[4], row[5])
            for row in raw(db_path, f'PRAGMA table_info({GOAL_TABLE})')] == [
        ('dispatch_id', 'TEXT', 1, None, 1),
        ('goal_format', 'TEXT', 1, None, 0),
        ('goal_json', 'TEXT', 1, None, 0),
        ('goal_fingerprint', 'TEXT', 1, None, 0),
    ]
    foreign_keys = raw(db_path, f'PRAGMA foreign_key_list({GOAL_TABLE})')
    # (id, seq, table, from, to, on update, on delete, match)
    assert [(row[2], row[3], row[4], row[6]) for row in foreign_keys] == [
        ('step_dispatches', 'dispatch_id', 'dispatch_id', 'CASCADE')]


def test_the_goal_format_check_rejects_empty_text(db_path):
    MissionStore(db_path).close()
    stamp = '2026-01-01T00:00:00.000000+00:00'
    with strict_connection(db_path) as conn:
        conn.execute(
            'INSERT INTO missions (mission_id, request_id, request_fingerprint, request_json,'
            ' max_attempts, created_at, updated_at, mission_state)'
            ' VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
            ('m', 'r', 'f', '{}', 1, stamp, stamp, 'ACCEPTED'))
        conn.execute(
            'INSERT INTO mission_steps (mission_id, step_key, step_index, step_state,'
            ' created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)',
            ('m', 'a', 0, 'PENDING', stamp, stamp))
        conn.execute(
            'INSERT INTO step_dispatches (dispatch_id, mission_id, step_key, created_at)'
            ' VALUES (?, ?, ?, ?)', ('d', 'm', 'a', stamp))
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute('INSERT INTO dispatch_goals VALUES (?, ?, ?, ?)', ('d', '', '{}', 'f'))
        conn.execute('INSERT INTO dispatch_goals VALUES (?, ?, ?, ?)', ('d', 'ok', '{}', 'f'))
    assert raw(db_path, f'SELECT COUNT(*) FROM {GOAL_TABLE}') == [(1,)]


def test_no_explicit_index_trigger_or_view(db_path):
    MissionStore(db_path).close()

    assert raw(
        db_path, "SELECT type, name FROM sqlite_master WHERE type IN ('trigger', 'view')"
                 f" OR (type = 'index' AND tbl_name = '{GOAL_TABLE}' AND sql IS NOT NULL)"
    ) == []
    # Only the index SQLite makes for the primary key.
    # (seq, name, unique, origin, partial)
    assert [(row[2], row[3]) for row in raw(
        db_path, f'PRAGMA index_list({GOAL_TABLE})')] == [(1, 'pk')]


def test_the_older_tables_are_byte_identical_to_a_version_7_database(db_path, tmp_path):
    MissionStore(db_path).close()
    old = tmp_path / 'v7.db'
    make_v7_database(old)

    version_7 = {name: sql for _, name, sql in schema_of(old)}
    stored = {name: sql for _, name, sql in schema_of(db_path)}
    for name in V7_TABLES:
        assert stored[name] == version_7[name], name


@pytest.mark.parametrize('word', [
    'lease', 'owner', 'fence', 'revision', 'attempt', 'evidence', 'time', 'created', 'updated'])
def test_no_ownership_or_timestamp_field_is_stored(db_path, word):
    MissionStore(db_path).close()

    columns = [row[1] for row in raw(db_path, f'PRAGMA table_info({GOAL_TABLE})')]
    assert not [column for column in columns if word in column]
    assert not [field.name for field in dataclasses.fields(DispatchGoalRecord) if word in
                field.name]


# C. Migration

@pytest.mark.parametrize('origin', list(ORIGINS))
def test_every_older_database_migrates_to_version_8_and_invents_no_goal(
        db_path, tmp_path, origin):
    ORIGINS[origin](db_path)
    before = old_rows(db_path, origin) if origin != 'v7' else dump(db_path, V7_TABLES)

    MissionStore(db_path).close()

    assert raw(db_path, 'PRAGMA user_version') == [(8,)]
    if origin != 'v7':
        assert old_rows(db_path, origin) == before
    else:
        assert dump(db_path, V7_TABLES) == before
    assert table_names(db_path) == set(ALL_TABLES)
    assert goal_rows(db_path) == []
    assert schema_of(db_path) == fresh_schema(tmp_path)


def test_a_native_version_7_database_runs_only_the_last_step(monkeypatch, db_path):
    make_v7_database(db_path)
    called = []
    for name in MIGRATIONS:
        original = getattr(MissionStore, name)

        def spy(self, _name=name, _original=original):
            called.append(_name)
            return _original(self)

        monkeypatch.setattr(MissionStore, name, spy)

    store, connection = open_faulty_store(monkeypatch, db_path)
    store.close()

    assert called == ['_migrate_v7_to_v8']
    statements = connection.statements
    assert statements[:4] == [
        'PRAGMA synchronous = FULL', 'PRAGMA foreign_keys = ON', 'BEGIN IMMEDIATE',
        'PRAGMA user_version']
    assert statements[4].startswith('CREATE TABLE dispatch_goals')
    assert statements[5:] == ['PRAGMA user_version = 8', 'COMMIT']
    assert goal_rows(db_path) == []


def test_no_migration_step_alters_a_table_or_adds_a_column(monkeypatch, db_path):
    make_v7_database(db_path)

    store, connection = open_faulty_store(monkeypatch, db_path)
    store.close()

    assert not [sql for sql in connection.statements if sql.upper().startswith('ALTER')]
    assert not [sql for sql in connection.statements if 'ADD COLUMN' in sql.upper()]


def assert_still_version_7(db_path, before, schema):
    assert raw(db_path, 'PRAGMA user_version') == [(7,)]
    assert table_names(db_path) == set(V7_TABLES)
    assert dump(db_path, V7_TABLES) == before
    assert schema_of(db_path) == schema
    with contextlib.closing(sqlite3.connect(db_path, timeout=0.1, isolation_level=None)) as conn:
        conn.execute('BEGIN IMMEDIATE')
        conn.execute('ROLLBACK')


@pytest.mark.parametrize('failing', ['create the table', 'set the version', 'commit'])
def test_a_failure_while_migrating_from_version_7_rolls_back_cleanly(
        monkeypatch, db_path, failing):
    make_v7_database(db_path)
    before = dump(db_path, V7_TABLES)
    schema = schema_of(db_path)

    if failing == 'create the table':
        with monkeypatch.context() as patch:
            patch.setattr(storage, '_DISPATCH_GOALS_DDL', 'THIS IS NOT SQL')
            with pytest.raises(sqlite3.OperationalError):
                MissionStore(db_path)
    else:
        fails = {
            'set the version': lambda sql: sql.strip() == 'PRAGMA user_version = 8',
            'commit': lambda sql: sql.strip().upper() == 'COMMIT',
        }[failing]
        with pytest.raises(sqlite3.OperationalError, match='injected failure'):
            open_faulty_store(monkeypatch, db_path, fail_when=fails)

    assert_still_version_7(db_path, before, schema)
    with MissionStore(db_path):
        pass
    assert raw(db_path, 'PRAGMA user_version') == [(8,)]
    assert goal_rows(db_path) == []


@pytest.mark.parametrize('origin', list(ORIGINS))
def test_a_failed_commit_leaves_every_older_database_at_its_own_version(
        monkeypatch, db_path, origin):
    ORIGINS[origin](db_path)
    before = old_rows(db_path, origin) if origin != 'v7' else dump(db_path, V7_TABLES)
    schema = schema_of(db_path)

    with pytest.raises(sqlite3.OperationalError, match='injected failure'):
        open_faulty_store(
            monkeypatch, db_path, fail_when=lambda sql: sql.strip().upper() == 'COMMIT')

    assert raw(db_path, 'PRAGMA user_version') == [(VERSION_OF[origin],)]
    if origin != 'v7':
        assert old_rows(db_path, origin) == before
    else:
        assert dump(db_path, V7_TABLES) == before
    assert schema_of(db_path) == schema


def test_concurrent_opens_of_a_version_7_database_migrate_it_once(tmp_path):
    for round_index in range(5):
        path = tmp_path / f'v7-{round_index}.db'
        make_v7_database(path)
        before = dump(path, V7_TABLES)
        workers = 8
        barrier = threading.Barrier(workers, timeout=TIMEOUT)

        def open_and_close():
            barrier.wait()
            MissionStore(path).close()

        with ThreadPoolExecutor(max_workers=workers) as pool:
            for future in [pool.submit(open_and_close) for _ in range(workers)]:
                future.result(timeout=TIMEOUT)

        assert raw(path, 'PRAGMA user_version') == [(8,)]
        assert dump(path, V7_TABLES) == before
        assert goal_rows(path) == []
        fresh = tmp_path / f'fresh-{round_index}.db'
        MissionStore(fresh).close()
        assert schema_of(path) == schema_of(fresh)


@pytest.mark.parametrize('origin', list(ORIGINS))
def test_reopening_a_migrated_database_changes_nothing(monkeypatch, db_path, origin):
    ORIGINS[origin](db_path)
    MissionStore(db_path).close()
    before = (dump(db_path), schema_of(db_path))

    store, connection = open_faulty_store(monkeypatch, db_path)
    store.close()

    assert writes(connection.statements) == []
    assert (dump(db_path), schema_of(db_path)) == before
    assert raw(db_path, 'PRAGMA user_version') == [(8,)]


# D. First preparation

def test_preparation_binds_the_uuid_the_context_and_the_goal_together(store, db_path):
    mission_id = make_mission(store)

    record = store.prepare_step_dispatch(mission_id, 'checkpoint_a', ACTION, GOAL)

    assert isinstance(record, DispatchRecord)
    assert store.get_step(mission_id, 'checkpoint_a').state is S.DISPATCH_PREPARED
    stored = store.get_dispatch_goal(mission_id, 'checkpoint_a')
    assert stored == DispatchGoalRecord(
        dispatch_id=record.dispatch_id, mission_id=mission_id, step_key='checkpoint_a',
        goal_format=GOAL.goal_format, payload={'target': 'checkpoint-a'},
        goal_fingerprint=GOAL.goal_fingerprint)


def test_only_the_expected_rows_and_cells_change_on_first_preparation(store, db_path):
    mission_id = make_mission(store)
    before = dump(db_path)

    record = store.prepare_step_dispatch(mission_id, 'checkpoint_a', ACTION, GOAL)

    diff = table_diff(before, dump(db_path))
    assert set(diff) == {'mission_steps', 'step_dispatches', 'dispatch_contexts', GOAL_TABLE}
    assert diff[GOAL_TABLE] == (
        [], [(str(record.dispatch_id), GOAL.goal_format, GOAL.goal_json, GOAL.goal_fingerprint)])


def test_preparation_consumes_no_attempt_and_leaves_the_mission_row_alone(store):
    mission_id = make_mission(store, max_attempts=1)
    before = store.get_by_mission_id(mission_id)

    for key in KEYS:
        store.prepare_step_dispatch(mission_id, key, ACTION, GOAL)

    assert before.attempts_used == 0
    assert store.get_by_mission_id(mission_id) == before


def test_every_step_gets_its_own_goal(store):
    mission_id = make_mission(store)
    other = replay_with(payload={'target': 'return-to-base'})

    first = store.prepare_step_dispatch(mission_id, 'checkpoint_a', ACTION, GOAL)
    second = store.prepare_step_dispatch(mission_id, 'return_to_base', ACTION, other)

    assert first.dispatch_id != second.dispatch_id
    assert store.get_dispatch_goal(mission_id, 'checkpoint_a').payload == {
        'target': 'checkpoint-a'}
    assert store.get_dispatch_goal(mission_id, 'return_to_base').payload == {
        'target': 'return-to-base'}


def test_steps_may_declare_the_same_goal(store, db_path):
    make_mission(store, prepared=KEYS)

    assert raw(db_path, f'SELECT COUNT(*) FROM {GOAL_TABLE}') == [(2,)]
    assert raw(db_path, f'SELECT COUNT(DISTINCT dispatch_id) FROM {GOAL_TABLE}') == [(2,)]


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
        store.prepare_step_dispatch(mission or mission_id, step, ACTION, GOAL)

    assert dump(db_path) == before


def test_the_goal_survives_reopening_the_store(db_path):
    with MissionStore(db_path) as store:
        mission_id = make_mission(store, prepared=['checkpoint_a'])
        bound = store.get_dispatch_goal(mission_id, 'checkpoint_a')

    with MissionStore(db_path) as reopened:
        assert reopened.get_dispatch_goal(mission_id, 'checkpoint_a') == bound


NOT_A_GOAL = [
    pytest.param(None, id='none'),
    pytest.param('a string', id='string'),
    pytest.param(('nav2/navigate_to_pose/v1', '{}'), id='tuple'),
    pytest.param({'goal_format': 'f', 'payload': {}}, id='dict'),
    pytest.param(object(), id='object'),
    pytest.param(ACTION, id='action-context'),
]


@pytest.mark.parametrize('bad', NOT_A_GOAL)
def test_a_wrong_goal_argument_is_rejected_before_the_database_is_touched(db_path, bad):
    with MissionStore(db_path, busy_timeout=0.05) as store:
        blocker = sqlite3.connect(db_path, isolation_level=None)
        blocker.execute('BEGIN IMMEDIATE')
        try:
            with pytest.raises(TypeError, match='ExpectedGoalSpecification'):
                store.prepare_step_dispatch('no-such-mission', 'no-such-step', ACTION, bad)
        finally:
            blocker.execute('ROLLBACK')
            blocker.close()


def test_the_goal_is_a_required_argument(store):
    mission_id = make_mission(store)

    assert inspect.signature(MissionStore.prepare_step_dispatch).parameters[
        'goal'].default is inspect.Parameter.empty
    with pytest.raises(TypeError):
        store.prepare_step_dispatch(mission_id, 'checkpoint_a', ACTION)


def test_a_wrong_goal_binds_nothing_for_a_real_step(store, db_path):
    mission_id = make_mission(store)
    before = dump(db_path)

    with pytest.raises(TypeError):
        store.prepare_step_dispatch(mission_id, 'checkpoint_a', ACTION, 'not a goal')

    assert dump(db_path) == before


# Faults and crashes around the first preparation

def normalized(sql):
    return ' '.join(sql.split()).upper()


PREPARE_STAGES = {
    'begin': lambda sql: sql.startswith('BEGIN'),
    'insert the dispatch': lambda sql: sql.startswith('INSERT INTO STEP_DISPATCHES'),
    'insert the context': lambda sql: sql.startswith('INSERT INTO DISPATCH_CONTEXTS'),
    'insert the goal': lambda sql: sql.startswith('INSERT INTO DISPATCH_GOALS'),
    'update the step': lambda sql: sql.startswith('UPDATE MISSION_STEPS'),
    'commit': lambda sql: sql == 'COMMIT',
}

PREPARE_RAN_BEFORE = {
    'insert the context': ['INSERT INTO STEP_DISPATCHES'],
    'insert the goal': ['INSERT INTO STEP_DISPATCHES', 'INSERT INTO DISPATCH_CONTEXTS'],
    'update the step': ['INSERT INTO STEP_DISPATCHES', 'INSERT INTO DISPATCH_CONTEXTS',
                        'INSERT INTO DISPATCH_GOALS'],
    'commit': ['INSERT INTO STEP_DISPATCHES', 'INSERT INTO DISPATCH_CONTEXTS',
               'INSERT INTO DISPATCH_GOALS', 'UPDATE MISSION_STEPS'],
}


@pytest.mark.parametrize('stage', list(PREPARE_STAGES))
def test_a_failure_before_the_commit_leaves_none_of_the_quartet(monkeypatch, db_path, stage):
    store, connection = open_faulty_store(monkeypatch, db_path)
    with store:
        mission_id = make_mission(store)
        before = dump(db_path)
        connection.statements.clear()
        connection.fail_when = lambda sql: PREPARE_STAGES[stage](normalized(sql))

        with pytest.raises(sqlite3.OperationalError, match='injected failure'):
            store.prepare_step_dispatch(mission_id, 'checkpoint_a', ACTION, GOAL)

        connection.fail_when = None
        assert not connection.in_transaction
        logged = [normalized(sql) for sql in connection.statements]
        assert any(PREPARE_STAGES[stage](sql) for sql in logged), 'the stage was never reached'
        for prefix in PREPARE_RAN_BEFORE.get(stage, []):
            assert any(sql.startswith(prefix) for sql in logged)
        assert dump(db_path) == before
        assert raw(db_path, 'SELECT COUNT(*) FROM step_dispatches') == [(0,)]
        assert raw(db_path, f'SELECT COUNT(*) FROM {GOAL_TABLE}') == [(0,)]
        assert store.get_step(mission_id, 'checkpoint_a').state is S.PENDING

        record = store.prepare_step_dispatch(mission_id, 'checkpoint_a', ACTION, GOAL)
        assert store.get_dispatch_goal(mission_id, 'checkpoint_a').dispatch_id == (
            record.dispatch_id)


def commit_then_fail(connection):
    def fail_after_committing(sql):
        if normalized(sql) == 'COMMIT':
            connection._real.execute('COMMIT')
            return True
        return False
    return fail_after_committing


def test_an_error_after_a_successful_commit_leaves_all_four_and_a_replay_finds_them(
        monkeypatch, db_path):
    store, connection = open_faulty_store(monkeypatch, db_path)
    with store:
        mission_id = make_mission(store)
        connection.fail_when = commit_then_fail(connection)

        with pytest.raises(sqlite3.OperationalError, match='injected failure'):
            store.prepare_step_dispatch(mission_id, 'checkpoint_a', ACTION, GOAL)
        connection.fail_when = None

        assert store.get_step(mission_id, 'checkpoint_a').state is S.DISPATCH_PREPARED
        assert raw(db_path, f'SELECT COUNT(*) FROM {GOAL_TABLE}') == [(1,)]
        bound = store.get_dispatch_goal(mission_id, 'checkpoint_a')
        replayed = store.prepare_step_dispatch(mission_id, 'checkpoint_a', ACTION, GOAL)
        assert replayed.dispatch_id == bound.dispatch_id
        with pytest.raises(DispatchGoalConflict):
            store.prepare_step_dispatch(
                mission_id, 'checkpoint_a', ACTION, replay_with(payload={'target': 'elsewhere'}))
        assert raw(db_path, f'SELECT COUNT(*) FROM {GOAL_TABLE}') == [(1,)]


_CHILD = """
import os
import signal
import sqlite3
import sys

from task_resilience import storage
from task_resilience.model import ExpectedGoalSpecification
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
goal = ExpectedGoalSpecification(goal_format='nav2/navigate_to_pose/v1',
                                 payload={'target': 'checkpoint-a'})
record = store.prepare_step_dispatch(mission_id, step_key, action, goal)
print(record.dispatch_id, flush=True)
die()
"""

EXIT_CODE = {'exit': 0, 'sigkill': -signal.SIGKILL}


def run_child(db_path, mission_id, step_key, when, marker, how):
    return subprocess.run(
        [sys.executable, '-c', _CHILD, str(db_path), mission_id, step_key, when, marker, how],
        env=package_env(), capture_output=True, text=True, timeout=30)


FRESH_KILLS = [
    ('BEGIN', 'before', 'exit', False),
    ('INSERT INTO STEP_DISPATCHES', 'before', 'exit', False),
    ('INSERT INTO STEP_DISPATCHES', 'after', 'exit', False),
    ('INSERT INTO DISPATCH_CONTEXTS', 'before', 'exit', False),
    ('INSERT INTO DISPATCH_CONTEXTS', 'after', 'exit', False),
    ('INSERT INTO DISPATCH_GOALS', 'before', 'exit', False),
    ('INSERT INTO DISPATCH_GOALS', 'after', 'exit', False),
    ('UPDATE MISSION_STEPS', 'before', 'exit', False),
    ('UPDATE MISSION_STEPS', 'after', 'exit', False),
    ('COMMIT', 'before', 'exit', False),
    ('COMMIT', 'before', 'sigkill', False),
    ('COMMIT', 'after', 'exit', True),
    ('COMMIT', 'after', 'sigkill', True),
]


@pytest.mark.parametrize(('marker', 'when', 'how', 'survives'), FRESH_KILLS)
def test_a_process_killed_around_a_fresh_preparation_leaves_all_four_or_none(
        db_path, marker, when, how, survives):
    with MissionStore(db_path) as store:
        mission_id = make_mission(store)
        before = dump(db_path)

    child = run_child(db_path, mission_id, 'checkpoint_a', when, marker, how)

    assert child.returncode == EXIT_CODE[how]
    assert child.stdout == ''
    with MissionStore(db_path) as reopened:
        if not survives:
            assert dump(db_path) == before
            assert reopened.get_step(mission_id, 'checkpoint_a').state is S.PENDING
            record = reopened.prepare_step_dispatch(mission_id, 'checkpoint_a', ACTION, GOAL)
            assert reopened.get_dispatch_goal(mission_id, 'checkpoint_a').dispatch_id == (
                record.dispatch_id)
        else:
            assert reopened.get_step(mission_id, 'checkpoint_a').state is S.DISPATCH_PREPARED
            assert raw(db_path, f'SELECT COUNT(*) FROM {GOAL_TABLE}') == [(1,)]
            found = reopened.get_step_dispatch(mission_id, 'checkpoint_a')
            assert reopened.get_dispatch_goal(mission_id, 'checkpoint_a') == DispatchGoalRecord(
                dispatch_id=found.dispatch_id, mission_id=mission_id, step_key='checkpoint_a',
                goal_format=GOAL.goal_format, payload={'target': 'checkpoint-a'},
                goal_fingerprint=GOAL.goal_fingerprint)
            replayed = reopened.prepare_step_dispatch(mission_id, 'checkpoint_a', ACTION, GOAL)
            assert replayed.dispatch_id == found.dispatch_id
            assert reopened.get_by_mission_id(mission_id).attempts_used == 0


# E. Replay

def test_replaying_the_same_goal_returns_the_same_record_and_writes_nothing(monkeypatch, db_path):
    store, connection = open_faulty_store(monkeypatch, db_path)
    with store:
        mission_id = make_mission(store)
        first = store.prepare_step_dispatch(mission_id, 'checkpoint_a', ACTION, GOAL)
        before = dump(db_path)
        connection.statements.clear()

        # An equal specification, not the same object, and not reusing the caller's dict.
        second = store.prepare_step_dispatch(
            mission_id, 'checkpoint_a', ACTION,
            ExpectedGoalSpecification(
                goal_format='nav2/navigate_to_pose/v1', payload={'target': 'checkpoint-a'}))

        assert second == first
        assert dump(db_path) == before
        assert writes(connection.statements) == []
        assert store.get_step(mission_id, 'checkpoint_a').updated_at == first.created_at


def test_a_reordered_payload_key_replays_identically(store, db_path):
    mission_id = make_mission(store)
    first = store.prepare_step_dispatch(
        mission_id, 'checkpoint_a', ACTION,
        ExpectedGoalSpecification(goal_format='f', payload={'a': 1, 'b': 2}))
    before = dump(db_path)

    second = store.prepare_step_dispatch(
        mission_id, 'checkpoint_a', ACTION,
        ExpectedGoalSpecification(goal_format='f', payload={'b': 2, 'a': 1}))

    assert second == first
    assert dump(db_path) == before


def test_a_reordered_list_conflicts(store, db_path):
    mission_id = make_mission(store)
    store.prepare_step_dispatch(
        mission_id, 'checkpoint_a', ACTION,
        ExpectedGoalSpecification(goal_format='f', payload=[1, 2, 3]))
    before = dump(db_path)

    with pytest.raises(DispatchGoalConflict) as excinfo:
        store.prepare_step_dispatch(
            mission_id, 'checkpoint_a', ACTION,
            ExpectedGoalSpecification(goal_format='f', payload=[3, 2, 1]))

    assert excinfo.value.fields == ('goal_content',)
    assert dump(db_path) == before


def test_a_changed_format_conflicts(store, db_path):
    mission_id = make_mission(store)
    store.prepare_step_dispatch(mission_id, 'checkpoint_a', ACTION, GOAL)
    before = dump(db_path)

    with pytest.raises(DispatchGoalConflict) as excinfo:
        store.prepare_step_dispatch(
            mission_id, 'checkpoint_a', ACTION, replay_with(goal_format='other/format/v1'))

    assert excinfo.value.fields == ('goal_format',)
    assert dump(db_path) == before


def test_a_changed_payload_conflicts(store, db_path):
    mission_id = make_mission(store)
    store.prepare_step_dispatch(mission_id, 'checkpoint_a', ACTION, GOAL)
    before = dump(db_path)

    with pytest.raises(DispatchGoalConflict) as excinfo:
        store.prepare_step_dispatch(
            mission_id, 'checkpoint_a', ACTION, replay_with(payload={'target': 'elsewhere'}))

    assert excinfo.value.fields == ('goal_content',)
    assert dump(db_path) == before


def test_a_changed_format_and_payload_names_both_fields(store, db_path):
    mission_id = make_mission(store)
    store.prepare_step_dispatch(mission_id, 'checkpoint_a', ACTION, GOAL)

    with pytest.raises(DispatchGoalConflict) as excinfo:
        store.prepare_step_dispatch(
            mission_id, 'checkpoint_a', ACTION,
            replay_with(goal_format='other/format/v1', payload={'target': 'elsewhere'}))

    assert excinfo.value.fields == ('goal_format', 'goal_content')


def test_a_bound_goal_is_never_replaced_by_repeated_conflicting_replays(store, db_path):
    mission_id = make_mission(store)
    store.prepare_step_dispatch(mission_id, 'checkpoint_a', ACTION, GOAL)
    before = dump(db_path)

    for overrides in ({'goal_format': 'x'}, {'payload': {'target': 'y'}}):
        with pytest.raises(DispatchGoalConflict):
            store.prepare_step_dispatch(
                mission_id, 'checkpoint_a', ACTION, replay_with(**overrides))
        # Not even after the original goal comes back.
        store.prepare_step_dispatch(mission_id, 'checkpoint_a', ACTION, GOAL)

    assert dump(db_path) == before


def test_the_conflict_error_names_fields_and_is_not_another_error(store):
    mission_id = make_mission(store, prepared=['checkpoint_a'])

    with pytest.raises(DispatchGoalConflict) as excinfo:
        store.prepare_step_dispatch(
            mission_id, 'checkpoint_a', ACTION, replay_with(payload={'target': 'elsewhere'}))

    error = excinfo.value
    assert 'goal_content' in str(error)
    # No goal values in the routine conflict message.
    assert 'elsewhere' not in str(error) and 'checkpoint-a' not in str(error)
    for other in (CorruptDispatchGoal, CorruptDispatchRecord, DispatchNotFound,
                  DispatchGoalMissing, InvalidStateTransition):
        assert not isinstance(error, other)


def test_replay_equality_reads_the_canonical_text_not_only_the_fingerprint():
    # White-box: the comparison must be able to observe a difference that two colliding
    # fingerprints (which cannot be manufactured for SHA-256) would hide, so it must read the
    # canonical text, not merely the fingerprint field.
    source = inspect.getsource(MissionStore._require_matching_goal)
    tree = ast.parse(textwrap.dedent(source))
    attrs = {node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)}
    assert 'goal_json' in attrs
    assert 'goal_format' in attrs


# F. Missing and corrupt goals; legacy dispatches are never repaired

UNBOUND_ORIGINS = ['migrated version 7 row', 'native row with its goal deleted']


def unbound_prepared_step(db_path, origin):
    """Return (mission_id, step_key) of a DISPATCH_PREPARED step with a context and no goal."""
    if origin == 'migrated version 7 row':
        # make_v7_database's own chain (Task 009's make_v4_database) already binds a context to
        # every dispatch it creates, 'prepared' included; migrating it to version 8 invents no
        # goal for it.
        make_v7_database(db_path)
        MissionStore(db_path).close()  # migrates to version 8, inventing no goal
        return V3_MAIN_ID, 'prepared'
    with MissionStore(db_path) as store:
        mission_id = make_mission(store, prepared=['checkpoint_a'])
    # Removed by something outside the supported code. Nothing in the file says so.
    unbind_goal(db_path)
    return mission_id, 'checkpoint_a'


@pytest.mark.parametrize('origin', UNBOUND_ORIGINS)
def test_replaying_preparation_of_an_unbound_step_reports_it_missing_and_writes_nothing(
        monkeypatch, db_path, origin):
    mission_id, key = unbound_prepared_step(db_path, origin)
    store, connection = open_faulty_store(monkeypatch, db_path)
    with store:
        before = dump(db_path)
        step_before = store.get_step(mission_id, key)
        connection.statements.clear()

        for goal in (GOAL, replay_with(payload={'target': 'elsewhere'})):
            with pytest.raises(DispatchGoalMissing) as excinfo:
                store.prepare_step_dispatch(mission_id, key, ACTION, goal)
            assert (excinfo.value.mission_id, excinfo.value.step_key) == (mission_id, key)

        assert dump(db_path) == before
        assert writes(connection.statements) == []
        assert store.get_step(mission_id, key) == step_before
        assert step_before.state is S.DISPATCH_PREPARED
    assert raw(db_path, f'SELECT COUNT(*) FROM {GOAL_TABLE}') == [(0,)]


@pytest.mark.parametrize('origin', UNBOUND_ORIGINS)
def test_an_unbound_prepared_step_cannot_be_authorized_and_nothing_changes(db_path, origin):
    mission_id, key = unbound_prepared_step(db_path, origin)
    with MissionStore(db_path) as store:
        before = dump(db_path)
        mission_before = store.get_by_mission_id(mission_id)

        for _ in range(3):
            with pytest.raises(DispatchGoalMissing):
                store.authorize_step_send(mission_id, key)
            with pytest.raises(DispatchGoalMissing):
                store.get_dispatch_goal(mission_id, key)

        assert dump(db_path) == before
        assert store.get_by_mission_id(mission_id) == mission_before
        assert store.get_step(mission_id, key).state is S.DISPATCH_PREPARED


@pytest.mark.parametrize('origin', UNBOUND_ORIGINS)
def test_an_unbound_prepared_step_can_still_be_cancelled_and_stays_unbound(db_path, origin):
    mission_id, key = unbound_prepared_step(db_path, origin)
    with MissionStore(db_path) as store:
        assert store.transition_step_state(mission_id, key, S.CANCELLED).state is S.CANCELLED

    assert raw(db_path, f'SELECT COUNT(*) FROM {GOAL_TABLE}') == [(0,)]


def test_a_migrated_row_and_a_row_whose_goal_was_deleted_cannot_be_told_apart(tmp_path):
    shapes, outcomes = [], []
    for number, origin in enumerate(UNBOUND_ORIGINS):
        path = tmp_path / f'shape-{number}.db'
        mission_id, key = unbound_prepared_step(path, origin)
        shapes.append((
            raw(path, 'SELECT step_state FROM mission_steps WHERE mission_id = ?'
                ' AND step_key = ?', mission_id, key),
            raw(path, f'SELECT COUNT(*) FROM {GOAL_TABLE}'),
            raw(path, 'PRAGMA user_version')))
        with MissionStore(path) as store:
            found = []
            for operation in (lambda: store.prepare_step_dispatch(mission_id, key, ACTION, GOAL),
                              lambda: store.authorize_step_send(mission_id, key),
                              lambda: store.get_dispatch_goal(mission_id, key)):
                with pytest.raises(Exception) as excinfo:
                    operation()
                found.append(type(excinfo.value))
            outcomes.append(found)

    assert shapes[0] == shapes[1] == ([('DISPATCH_PREPARED',)], [(0,)], [(8,)])
    assert outcomes[0] == outcomes[1] == [DispatchGoalMissing] * 3


def test_a_pending_step_of_a_migrated_mission_is_prepared_and_bound_normally(db_path):
    make_v7_database(db_path)
    with MissionStore(db_path) as store:
        record = store.prepare_step_dispatch(V3_MAIN_ID, 'pending', ACTION, GOAL)

        assert store.get_step(V3_MAIN_ID, 'pending').state is S.DISPATCH_PREPARED
        assert store.get_dispatch_goal(V3_MAIN_ID, 'pending').dispatch_id == record.dispatch_id
    assert raw(db_path, f'SELECT COUNT(*) FROM {GOAL_TABLE}') == [(1,)]


@pytest.mark.parametrize(('mission_id', 'key'), [
    (V3_MAIN_ID, 'dispatched'), (V3_MAIN_ID, 'accepted'), (V3_MAIN_ID, 'executing'),
    (V3_MAIN_ID, 'cancel_requested'), (V3_UNKNOWN_ID, 'unknown'), (V3_MAIN_ID, 'succeeded'),
    (V3_MAIN_ID, 'failed'), (V3_MAIN_ID, 'cancelled'),
])
def test_preparation_never_binds_a_goal_to_a_step_that_has_progressed(db_path, mission_id, key):
    make_v7_database(db_path)
    with MissionStore(db_path) as store:
        before = dump(db_path)

        for goal in (GOAL, replay_with(payload={'target': 'elsewhere'})):
            with pytest.raises(InvalidStateTransition):
                store.prepare_step_dispatch(mission_id, key, ACTION, goal)

        assert dump(db_path) == before
        with pytest.raises(DispatchGoalMissing):
            store.get_dispatch_goal(mission_id, key)
    assert raw(db_path, f'SELECT COUNT(*) FROM {GOAL_TABLE}') == [(0,)]


CORRUPT_GOALS = [
    pytest.param('UPDATE dispatch_goals SET goal_json = ?', ('{"target":"tampered"}',),
                 id='json changed, fingerprint stale'),
    pytest.param('UPDATE dispatch_goals SET goal_fingerprint = ?', ('f' * 64,),
                 id='fingerprint changed alone'),
    pytest.param('UPDATE dispatch_goals SET goal_format = ?', ('other/format',),
                 id='format changed, fingerprint stale'),
    pytest.param('UPDATE dispatch_goals SET goal_json = ?',
                 ('{ "target" : "checkpoint-a" }',), id='noncanonical whitespace, same value'),
    pytest.param('UPDATE dispatch_goals SET goal_json = ?', ('not json at all',),
                 id='malformed json'),
    pytest.param('UPDATE dispatch_goals SET goal_json = ?', (b'\x00\x01',), id='json blob'),
    pytest.param('UPDATE dispatch_goals SET goal_format = ?', (b'\xff',), id='format blob'),
    pytest.param('UPDATE dispatch_goals SET goal_fingerprint = ?', (b'\x01',),
                 id='fingerprint blob'),
    # json.loads accepts these as non-standard extensions (NaN/Infinity/-Infinity, and a
    # magnitude that overflows to inf), decoding to a float this project's strict JSON domain
    # rejects. The reconstruction must report that domain rejection as corruption, not let the
    # model's own InvalidRequest escape as if the caller had asked for a new goal.
    pytest.param('UPDATE dispatch_goals SET goal_json = ?', ('NaN',), id='bare NaN'),
    pytest.param('UPDATE dispatch_goals SET goal_json = ?', ('Infinity',), id='bare Infinity'),
    pytest.param('UPDATE dispatch_goals SET goal_json = ?', ('-Infinity',), id='bare -Infinity'),
    pytest.param('UPDATE dispatch_goals SET goal_json = ?', ('1e9999',),
                 id='magnitude overflows to inf'),
    pytest.param('UPDATE dispatch_goals SET goal_json = ?', ('{"x":NaN}',),
                 id='NaN inside a dict'),
]


@pytest.mark.parametrize(('sql', 'params'), CORRUPT_GOALS)
def test_a_malformed_goal_is_corruption_and_authorization_writes_nothing(
        store, db_path, sql, params):
    mission_id = make_mission(store, prepared=['checkpoint_a'])
    corrupt(db_path, sql, *params)
    before = dump(db_path)

    with pytest.raises(CorruptDispatchGoal) as excinfo:
        store.authorize_step_send(mission_id, 'checkpoint_a')

    assert (excinfo.value.mission_id, excinfo.value.step_key) == (mission_id, 'checkpoint_a')
    assert excinfo.value.reason
    assert dump(db_path) == before
    assert store.get_by_mission_id(mission_id).attempts_used == 0
    assert store.get_step(mission_id, 'checkpoint_a').state is S.DISPATCH_PREPARED
    # Never repaired: a second read raises the same way.
    with pytest.raises(CorruptDispatchGoal):
        store.get_dispatch_goal(mission_id, 'checkpoint_a')
    assert dump(db_path) == before


def test_excessively_nested_stored_json_is_corruption_not_a_leaked_recursion_error(
        store, db_path):
    """A bounded depth past where json.loads raises RecursionError, not a DoS-sized payload."""
    mission_id = make_mission(store, prepared=['checkpoint_a'])
    depth = 20_000
    corrupt(db_path, 'UPDATE dispatch_goals SET goal_json = ?', '[' * depth + ']' * depth)
    before = dump(db_path)

    with pytest.raises(CorruptDispatchGoal) as excinfo:
        store.authorize_step_send(mission_id, 'checkpoint_a')

    assert (excinfo.value.mission_id, excinfo.value.step_key) == (mission_id, 'checkpoint_a')
    assert excinfo.value.reason
    assert dump(db_path) == before
    assert store.get_by_mission_id(mission_id).attempts_used == 0
    assert store.get_step(mission_id, 'checkpoint_a').state is S.DISPATCH_PREPARED
    with pytest.raises(CorruptDispatchGoal):
        store.get_dispatch_goal(mission_id, 'checkpoint_a')
    assert dump(db_path) == before


# G. The send-authorization gate

def test_a_bound_prepared_step_is_authorized_as_before(store):
    mission_id = make_mission(store, prepared=['checkpoint_a'])

    receipt = store.authorize_step_send(mission_id, 'checkpoint_a')

    assert receipt.dispatch_id == store.get_step_dispatch(mission_id, 'checkpoint_a').dispatch_id
    assert (receipt.attempt_number, receipt.max_attempts) == (1, 3)
    assert store.get_step(mission_id, 'checkpoint_a').state is S.DISPATCHED
    assert store.get_by_mission_id(mission_id).attempts_used == 1


def test_an_unbound_prepared_step_is_refused_and_nothing_changes(store, db_path):
    mission_id = make_mission(store, prepared=['checkpoint_a'])
    unbind_goal(db_path)
    before = dump(db_path)

    with pytest.raises(DispatchGoalMissing) as excinfo:
        store.authorize_step_send(mission_id, 'checkpoint_a')

    assert (excinfo.value.mission_id, excinfo.value.step_key) == (mission_id, 'checkpoint_a')
    assert dump(db_path) == before
    assert store.get_step(mission_id, 'checkpoint_a').state is S.DISPATCH_PREPARED
    assert store.get_by_mission_id(mission_id).attempts_used == 0


def test_a_refused_unbound_authorization_executes_no_write_statement(monkeypatch, db_path):
    store, connection = open_faulty_store(monkeypatch, db_path)
    with store:
        mission_id = make_mission(store, prepared=['checkpoint_a'])
        unbind_goal(db_path)
        connection.statements.clear()

        with pytest.raises(DispatchGoalMissing):
            store.authorize_step_send(mission_id, 'checkpoint_a')

        assert writes(connection.statements) == []


def test_replaying_preparation_does_not_make_an_unbound_step_authorizable(store, db_path):
    mission_id = make_mission(store, prepared=['checkpoint_a'])
    unbind_goal(db_path)
    before = dump(db_path)

    for _ in range(2):
        with pytest.raises(DispatchGoalMissing):
            store.authorize_step_send(mission_id, 'checkpoint_a')
        with pytest.raises(DispatchGoalMissing):
            store.prepare_step_dispatch(mission_id, 'checkpoint_a', ACTION, GOAL)

    assert dump(db_path) == before
    assert store.get_by_mission_id(mission_id).attempts_used == 0
    assert store.get_step(mission_id, 'checkpoint_a').state is S.DISPATCH_PREPARED


def refuse_when(db_path, condition):
    """Return a mission whose prepared step fails the given condition (goal always missing)."""
    with MissionStore(db_path) as store:
        route = (M.RUNNING,)
        if condition == 'mission not running':
            route = ()
        mission_id = make_mission(store, prepared=KEYS, route=route)
        if condition == 'step already authorized':
            store.authorize_step_send(mission_id, 'checkpoint_a')
        if condition == 'another step unknown':
            store.transition_step_state(mission_id, 'return_to_base', S.OUTCOME_UNKNOWN)
    unbind_goal(db_path)
    if condition == 'context also missing':
        raw(db_path, 'DELETE FROM dispatch_contexts')
    if condition == 'attempts exhausted':
        raw(db_path, 'UPDATE missions SET attempts_used = max_attempts')
    return mission_id


# Every check that comes first still wins when the goal is also missing. The context is
# checked immediately before the goal, so a mission missing both is reported for its context.
PRECEDENCE = [
    ('mission not running', MissionNotRunning),
    ('step already authorized', StepNotAwaitingSend),
    ('another step unknown', MissionReconciliationRequired),
    ('context also missing', DispatchContextMissing),
    # The goal comes before the budget, so an exhausted mission is told what is missing.
    ('attempts exhausted', DispatchGoalMissing),
]


@pytest.mark.parametrize(('condition', 'expected'), PRECEDENCE, ids=[p[0] for p in PRECEDENCE])
def test_the_goal_is_checked_after_the_context_and_before_the_budget(
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


def test_a_failure_reading_the_goal_during_authorization_leaves_everything_as_it_was(
        monkeypatch, db_path):
    store, connection = open_faulty_store(monkeypatch, db_path)
    with store:
        mission_id = make_mission(store, prepared=['checkpoint_a'])
        before = dump(db_path)
        connection.fail_when = lambda sql: (
            sql.startswith('SELECT') and 'FROM dispatch_goals' in sql)

        with pytest.raises(sqlite3.OperationalError, match='injected failure'):
            store.authorize_step_send(mission_id, 'checkpoint_a')

        connection.fail_when = None
        assert not connection.in_transaction
        assert dump(db_path) == before
        assert store.authorize_step_send(mission_id, 'checkpoint_a').attempt_number == 1


# H. Historical dispatches that progressed before this table existed

# Every step of the version 3/4 fixture that is past DISPATCH_PREPARED, so it can never receive
# a goal (see test_preparation_never_binds_a_goal_to_a_step_that_has_progressed) and never will.
LEGACY_PROGRESSED = [
    (V3_MAIN_ID, 'dispatched', S.ACCEPTED),
    (V3_MAIN_ID, 'accepted', S.EXECUTING),
    (V3_MAIN_ID, 'cancel_requested', S.CANCELLED),
]


@pytest.mark.parametrize(('mission_id', 'key', 'target'), LEGACY_PROGRESSED)
def test_historical_context_and_authorization_reads_are_unaffected_by_a_missing_goal(
        db_path, mission_id, key, target):
    make_v7_database(db_path)
    with MissionStore(db_path) as store:
        with pytest.raises(DispatchGoalMissing):
            store.get_dispatch_goal(mission_id, key)

        # None of these require a goal: correlation is by dispatch_id and action context alone.
        expected_dispatch_id = uuid.UUID(V3_DISPATCH_IDS[(mission_id, key)])
        assert store.get_dispatch_context(mission_id, key).dispatch_id == expected_dispatch_id
        assert store.transition_step_state(mission_id, key, target).state is target


def test_task_011_evidence_can_still_be_applied_to_a_dispatch_with_no_goal(db_path):
    make_v7_database(db_path)
    with MissionStore(db_path) as store:
        # 'prepared' is DISPATCH_PREPARED with no authorization row (the legacy fixture never
        # went through authorize_step_send) and, since version 8 exists but this row predates
        # it, no goal either. Task 011's UNPROVEN_AUTHORIZATION path needs neither.
        with pytest.raises(DispatchGoalMissing):
            store.get_dispatch_goal(V3_MAIN_ID, 'prepared')

        observation = RecoveryObservation(
            kind=ObservationKind.RESULT_RESPONSE, dispatch_id=uuid.UUID(
                V3_DISPATCH_IDS[(V3_MAIN_ID, 'prepared')]),
            action_name=ACTION.action_name, action_type=ACTION.action_type,
            robot_namespace=ACTION.robot_namespace, status=ActionStatus.SUCCEEDED)

        evidence = store.apply_recovery_observation(
            uuid.uuid4(), V3_MAIN_ID, 'prepared', observation)

        assert evidence.outcome.name == 'UNPROVEN_AUTHORIZATION'
        assert store.get_step(V3_MAIN_ID, 'prepared').state is S.OUTCOME_UNKNOWN
        with pytest.raises(DispatchGoalMissing):
            store.get_dispatch_goal(V3_MAIN_ID, 'prepared')


def test_task_012_reconciliation_of_a_mission_with_no_goal_rows_is_unaffected(db_path):
    make_v7_database(db_path)
    with MissionStore(db_path) as store:
        # V3_UNKNOWN_ID is already NEEDS_RECONCILIATION under the version 3/4 fixture, and its
        # dispatch has no goal. Migrated legacy reconciliation stays exactly what it was
        # (fails closed with MissionReconciliationContextMissing), and nothing here mentions
        # a goal, gets one, or is asked for one.
        with pytest.raises(MissionReconciliationContextMissing):
            store.get_mission_reconciliation_context(V3_UNKNOWN_ID)
        with pytest.raises(DispatchGoalMissing):
            store.get_dispatch_goal(V3_UNKNOWN_ID, 'unknown')


def test_no_reconciliation_operation_reads_a_goal_statement():
    for name in ('enter_mission_reconciliation', 'resolve_mission_reconciliation',
                 'request_cancellation_during_reconciliation',
                 'get_mission_reconciliation_context'):
        source = inspect.getsource(getattr(MissionStore, name))
        assert '_GOAL_OF_DISPATCH' not in source
        assert 'dispatch_goals' not in source.lower()
        assert '_require_goal' not in source and '_fetch_goal' not in source


def test_apply_recovery_observation_reads_no_goal_statement():
    source = inspect.getsource(MissionStore.apply_recovery_observation)
    assert '_GOAL_OF_DISPATCH' not in source
    assert '_require_goal' not in source and '_fetch_goal' not in source


# I. The read API

def test_get_dispatch_goal_returns_the_frozen_stored_record(store):
    mission_id = make_mission(store, prepared=['checkpoint_a'])

    bound = store.get_dispatch_goal(mission_id, 'checkpoint_a')

    assert bound == DispatchGoalRecord(
        dispatch_id=store.get_step_dispatch(mission_id, 'checkpoint_a').dispatch_id,
        mission_id=mission_id, step_key='checkpoint_a', goal_format=GOAL.goal_format,
        payload={'target': 'checkpoint-a'}, goal_fingerprint=GOAL.goal_fingerprint)
    with pytest.raises(dataclasses.FrozenInstanceError):
        bound.goal_format = '/other'


def test_reading_a_goal_for_something_that_does_not_exist_keeps_the_existing_errors(store):
    mission_id = make_mission(store)

    with pytest.raises(MissionNotFound):
        store.get_dispatch_goal('no-such-mission', 'checkpoint_a')
    with pytest.raises(StepNotFound):
        store.get_dispatch_goal(mission_id, 'no_such_step')
    with pytest.raises(DispatchNotFound):
        store.get_dispatch_goal(mission_id, 'checkpoint_a')


def test_a_dispatch_without_a_goal_is_reported_as_missing_a_goal(store, db_path):
    mission_id = make_mission(store, prepared=['checkpoint_a'])
    unbind_goal(db_path)

    with pytest.raises(DispatchGoalMissing):
        store.get_dispatch_goal(mission_id, 'checkpoint_a')
    assert store.get_step_dispatch(mission_id, 'checkpoint_a').step_key == 'checkpoint_a'


def test_reading_never_binds_or_repairs_a_goal(monkeypatch, db_path):
    store, connection = open_faulty_store(monkeypatch, db_path)
    with store:
        mission_id = make_mission(store, prepared=KEYS)
        unbind_goal(db_path)
        corrupt(
            db_path, "INSERT INTO dispatch_goals VALUES (?, 'f', 'not json', 'x')",
            str(store.get_step_dispatch(mission_id, 'return_to_base').dispatch_id))
        before = dump(db_path)
        connection.statements.clear()

        for _ in range(3):
            with pytest.raises(DispatchGoalMissing):
                store.get_dispatch_goal(mission_id, 'checkpoint_a')
            with pytest.raises(CorruptDispatchGoal):
                store.get_dispatch_goal(mission_id, 'return_to_base')

        assert dump(db_path) == before
        assert writes(connection.statements) == []
        assert not [sql for sql in connection.statements if sql.startswith('BEGIN')]


def test_repeated_reads_of_a_valid_goal_write_nothing(monkeypatch, db_path):
    store, connection = open_faulty_store(monkeypatch, db_path)
    with store:
        mission_id = make_mission(store, prepared=['checkpoint_a'])
        before = dump(db_path)
        connection.statements.clear()

        reads = [store.get_dispatch_goal(mission_id, 'checkpoint_a') for _ in range(3)]

        assert reads[0] == reads[1] == reads[2]
        assert dump(db_path) == before
        assert writes(connection.statements) == []


def test_deleting_a_mission_removes_its_goals(store, db_path):
    make_mission(store, prepared=KEYS)
    assert raw(db_path, f'SELECT COUNT(*) FROM {GOAL_TABLE}') == [(2,)]

    with strict_connection(db_path) as conn:
        conn.execute('DELETE FROM missions')

    assert raw(db_path, 'SELECT COUNT(*) FROM step_dispatches') == [(0,)]
    assert raw(db_path, f'SELECT COUNT(*) FROM {GOAL_TABLE}') == [(0,)]


def test_the_new_errors_carry_their_facts_and_are_distinct():
    missing = DispatchGoalMissing('m-1', 'k')
    conflict = DispatchGoalConflict('m-1', 'k', ('goal_format', 'goal_content'))
    corrupt_goal = CorruptDispatchGoal('m-1', 'k', 'goal_json is not text', b'\x01')

    assert (missing.mission_id, missing.step_key) == ('m-1', 'k')
    assert (conflict.mission_id, conflict.step_key, conflict.fields) == (
        'm-1', 'k', ('goal_format', 'goal_content'))
    assert (corrupt_goal.mission_id, corrupt_goal.step_key, corrupt_goal.reason,
            corrupt_goal.value) == ('m-1', 'k', 'goal_json is not text', b'\x01')
    errors = [missing, conflict, corrupt_goal]
    for error in errors:
        assert 'm-1' in str(error)
        assert not isinstance(error, (CorruptDispatchRecord, DispatchNotFound))
    for one in errors:
        assert len([other for other in errors if isinstance(one, type(other))]) == 1


# J. Concurrency. Each worker opens its own store, as separate callers would.

def run_concurrently(db_path, workers, rounds, operation):
    """Run operation(store, worker, round) in every worker, released together each round."""
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


def race_missions(db_path):
    with MissionStore(db_path) as store:
        return [make_mission(store, request_id=f'req-{n}', keys=['checkpoint'])
                for n in range(ROUNDS)]


def try_prepare(store, mission_id, goal):
    try:
        return store.prepare_step_dispatch(mission_id, 'checkpoint', ACTION, goal)
    except DispatchGoalConflict as conflict:
        return conflict


def test_concurrent_preparation_with_one_goal_agrees_on_one_dispatch(db_path):
    workers = 8
    mission_ids = race_missions(db_path)

    results = run_concurrently(
        db_path, workers, ROUNDS,
        lambda store, worker, n: try_prepare(store, mission_ids[n], GOAL))

    for n in range(ROUNDS):
        assert len({worker[n] for worker in results}) == 1, f'round {n}'
        assert isinstance(results[0][n], DispatchRecord)
    assert raw(db_path, f'SELECT COUNT(*) FROM {GOAL_TABLE}') == [(ROUNDS,)]


def test_concurrent_preparation_with_different_goals_binds_exactly_one(db_path):
    workers = 8
    goals = [GOAL, replay_with(payload={'target': 'elsewhere'})]
    mission_ids = race_missions(db_path)

    results = run_concurrently(
        db_path, workers, ROUNDS,
        lambda store, worker, n: try_prepare(store, mission_ids[n], goals[worker % 2]))

    with MissionStore(db_path) as store:
        for n, mission_id in enumerate(mission_ids):
            winner = store.get_dispatch_goal(mission_id, 'checkpoint').payload
            assert winner in ({'target': 'checkpoint-a'}, {'target': 'elsewhere'})
            for worker in range(workers):
                outcome = results[worker][n]
                if goals[worker % 2].goal_json == json.dumps(
                        winner, sort_keys=True, separators=(',', ':')):
                    assert isinstance(outcome, DispatchRecord), f'round {n} worker {worker}'
                else:
                    assert isinstance(outcome, DispatchGoalConflict)
                    assert outcome.fields == ('goal_content',)
    assert raw(db_path, f'SELECT COUNT(*) FROM {GOAL_TABLE}') == [(ROUNDS,)]


def test_a_prepared_step_never_has_a_context_without_a_goal_during_a_race(db_path):
    # Identity, context and goal are one atomic commit: authorization racing preparation can
    # never observe a step that is DISPATCH_PREPARED with a context and no goal yet.
    workers = 8
    mission_ids = race_missions(db_path)

    def operation(store, worker, n):
        if worker < 4:
            # Any refusal is fine here: the point is only that a partial bind is never observed.
            try:
                return store.prepare_step_dispatch(mission_ids[n], 'checkpoint', ACTION, GOAL)
            except DispatchGoalConflict as conflict:
                return conflict
        try:
            context = store.get_dispatch_context(mission_ids[n], 'checkpoint')
        except (DispatchNotFound, DispatchContextMissing):
            return 'no context yet'
        # If the context exists, the goal must too: nothing else could have committed one
        # without the other.
        goal = store.get_dispatch_goal(mission_ids[n], 'checkpoint')
        return (context.dispatch_id, goal.dispatch_id)

    results = run_concurrently(db_path, workers, ROUNDS, operation)

    for worker in range(4, workers):
        for n in range(ROUNDS):
            outcome = results[worker][n]
            assert outcome == 'no context yet' or (
                isinstance(outcome, tuple) and outcome[0] == outcome[1])


# K. What the code is, and what it must not be

def test_exactly_one_insert_writer_and_no_update_delete_replace_or_upsert():
    package = Path(task_resilience.__file__).resolve().parent
    source = (package / 'storage.py').read_text()

    assert len(re.findall(r'INSERT\s+INTO\s+dispatch_goals', source, re.I)) == 1
    assert {name for name, function in inspect.getmembers(MissionStore, inspect.isfunction)
            if '_INSERT_GOAL' in inspect.getsource(function)} == {'prepare_step_dispatch'}
    assert not re.search(r'UPDATE\s+dispatch_goals', source, re.I)
    assert not re.search(r'DELETE\s+FROM\s+dispatch_goals', source, re.I)
    assert not re.search(r'REPLACE\s+INTO\s+dispatch_goals', source, re.I)
    assert not re.search(r'INSERT\s+OR\s+(REPLACE|IGNORE)', source, re.I)
    assert not re.search(r'ON\s+CONFLICT', source, re.I)


def test_the_goal_is_inserted_before_the_step_state_and_never_in_the_replay_branch():
    tree, names = names_in(MissionStore.prepare_step_dispatch)
    assert executed(tree) == [
        '_INSERT_DISPATCH', '_INSERT_CONTEXT', '_INSERT_GOAL', '_SET_STEP_STATE']
    replay = [node for node in ast.walk(tree)
              if isinstance(node, ast.If) and ast.unparse(node.test) == 'new_state is step.state']
    assert len(replay) == 1
    branch = ast.Module(body=replay[0].body, type_ignores=[])
    assert executed(branch) == []
    assert '_INSERT_GOAL' not in {node.id for node in ast.walk(branch)
                                  if isinstance(node, ast.Name)}


def test_no_reader_or_helper_can_write_and_no_repair_path_exists():
    writers = {'_INSERT_GOAL', '_INSERT_DISPATCH', '_INSERT_CONTEXT', '_INSERT', '_INSERT_STEP',
               '_SET_STEP_STATE', '_SET_MISSION_STATE', '_CONSUME'}
    for name in ('get_dispatch_goal', '_fetch_goal', '_require_goal', '_require_matching_goal'):
        tree, names = names_in(getattr(MissionStore, name))
        assert not names & writers, name
        assert set(executed(tree)) <= {'_GOAL_OF_DISPATCH'}, name

    assert not [name for name in dir(MissionStore) if any(
        word in name.lower() for word in (
            'bind_goal', 'set_goal', 'update_goal', 'replace_goal', 'repair_goal',
            'backfill_goal', 'rebind_goal', 'backfill', 'repair', 'attest'))]


def test_only_the_first_preparation_takes_an_expectedgoalspecification():
    takers = [name for name, function in inspect.getmembers(MissionStore, inspect.isfunction)
              if not name.startswith('_') and any(
                  parameter.annotation is ExpectedGoalSpecification
                  or parameter.name == 'goal'
                  for parameter in inspect.signature(function).parameters.values())]
    assert takers == ['prepare_step_dispatch']


def test_authorize_step_send_reads_the_goal_before_the_provenance_precheck():
    source = inspect.getsource(MissionStore.authorize_step_send)
    assert source.index('_require_context') < source.index('_require_goal')
    assert source.index('_require_goal') < source.index('_authorization_of')
    assert source.index('_require_goal') < source.index('_CONSUME')
    assert source.index('_require_goal') < source.index('_INSERT_AUTHORIZATION')


def test_state_machine_and_reconciliation_modules_mention_no_goal():
    for module in (state_machine_module(), reconciliation_module()):
        text = module.read_text()
        assert 'goal' not in text.lower()


def state_machine_module():
    return Path(task_resilience.__file__).resolve().parent / 'state_machine.py'


def reconciliation_module():
    return Path(task_resilience.__file__).resolve().parent / 'reconciliation.py'


def test_the_state_definitions_are_unchanged():
    assert [state.value for state in MissionState] == [
        'ACCEPTED', 'RUNNING', 'CANCELLING', 'COMPLETED', 'FAILED', 'CANCELLED',
        'NEEDS_RECONCILIATION']
    assert [state.value for state in StepState] == [
        'PENDING', 'DISPATCH_PREPARED', 'DISPATCHED', 'ACCEPTED', 'EXECUTING', 'SUCCEEDED',
        'FAILED', 'CANCEL_REQUESTED', 'CANCELLED', 'OUTCOME_UNKNOWN']


def test_the_production_modules_import_no_ros_network_process_or_time_package():
    allowed = {'collections', 'contextlib', 'dataclasses', 'datetime', 'enum', 'hashlib', 'json',
               'math', 'os', 'sqlite3', 'typing', 'uuid'}
    package = Path(task_resilience.__file__).resolve().parent
    for filename in ('storage.py', 'model.py', 'errors.py'):
        tree = ast.parse((package / filename).read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                found = {alias.name.split('.')[0] for alias in node.names}
            elif isinstance(node, ast.ImportFrom) and node.level == 0:
                found = {node.module.split('.')[0]}
            else:
                continue
            assert found <= allowed, (filename, found - allowed)


def test_importing_the_store_loads_no_ros_network_or_llm_package():
    code = (
        'import sys\n'
        'import task_resilience.storage\n'
        "banned = {'rclpy', 'rcl_interfaces', 'rosidl_runtime_py', 'ament_index_python',\n"
        "          'action_msgs', 'unique_identifier_msgs', 'nav2_msgs', 'geometry_msgs',\n"
        "          'builtin_interfaces', 'socket', 'http', 'urllib', 'ssl', 'requests',\n"
        "          'httpx', 'anthropic', 'openai', 'fcntl', 'subprocess'}\n"
        "print(sorted(m for m in sys.modules if m.split('.')[0] in banned))\n")

    child = subprocess.run(
        [sys.executable, '-c', code], env=package_env(), capture_output=True, text=True,
        check=True, timeout=30)

    assert child.stdout.strip() == '[]'


def test_the_goal_operations_use_no_network_or_subprocess(store, monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError('external work in a goal operation')

    with monkeypatch.context() as patch:
        patch.setattr(socket, 'socket', forbidden)
        patch.setattr(subprocess, 'Popen', forbidden)
        patch.setattr(os, 'system', forbidden)

        mission_id = make_mission(store, prepared=['checkpoint_a'])
        store.get_dispatch_goal(mission_id, 'checkpoint_a')
        store.authorize_step_send(mission_id, 'checkpoint_a')


def test_no_ownership_lease_or_process_code_was_added():
    package = Path(task_resilience.__file__).resolve().parent
    source = (package / 'storage.py').read_text()
    for word in ('flock', 'fcntl', 'ownerclass', 'ownernode', 'PIDfile', 'fencing'):
        assert word.lower() not in source.lower()


def test_the_store_still_offers_no_way_to_force_reconcile_send_or_own():
    public = [name for name in dir(MissionStore) if not name.startswith('_')]

    assert not [name for name in public if any(
        word in name for word in (
            'force', 'override', 'set_state', 'retry', 'send_goal', 'lock', 'own', 'lease'))]
