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

# Task 012: mission-level, evidence-backed reconciliation.
#
# A mission enters NEEDS_RECONCILIATION only through enter_mission_reconciliation, because a
# persisted step is OUTCOME_UNKNOWN, and records in the same transaction the mission state to
# resume: RUNNING or CANCELLING. That context is durable ACTIVE intent, not history, evidence,
# authorization or a permission. resolve_mission_reconciliation restores exactly that state and
# nothing else, once no persisted step is unknown. Nothing here completes, fails or cancels a
# mission, authorizes a send, touches an attempt or reads recovery evidence, and the pure state
# machine gains no edge. The table is not tamper-evident.

import ast
from concurrent.futures import ThreadPoolExecutor
import contextlib
import dataclasses
import inspect
import os
from pathlib import Path
import random
import re
import signal
import socket
import sqlite3
import subprocess
import sys
import threading
import time
import uuid

from fault_injection import open_faulty_store
import pytest

import task_resilience
from task_resilience import model, reconciliation, state_machine, storage
from task_resilience.errors import (
    CorruptMissionReconciliationContext,
    CorruptStoredState,
    InvalidStateTransition,
    MissionCompletionInvariantError,
    MissionNotAwaitingReconciliation,
    MissionNotFound,
    MissionNotRunning,
    MissionReconciliationContextMissing,
    MissionReconciliationEntryRequired,
    MissionReconciliationRequired,
    NoUnknownStepToReconcile,
)
from task_resilience.model import (
    ExpectedGoalSpecification,
    MissionReconciliationContextRecord,
    MissionRecord,
)
from task_resilience.reconciliation import (
    ActionStatus,
    ExpectedActionContext,
    ObservationKind,
    RecoveryObservation,
)
from task_resilience.state_machine import MissionState, StepState, transition_mission
from task_resilience.storage import MissionStore
from test_dispatch_authorization import (
    fresh_schema,
    make_v6_database,
    MIGRATIONS,
    old_rows,
    ORIGINS,
    V6_TABLES,
    VERSION_OF,
)
from test_dispatch_context import V3_MAIN_ID, V3_UNKNOWN_ID

# The action every dispatch in these tests is declared to reach.
ACTION = ExpectedActionContext(
    action_name='/navigate_to_pose', action_type='nav2_msgs/action/NavigateToPose',
    robot_namespace='/robot1')

GOAL = ExpectedGoalSpecification(
    goal_format='test/goal/v1', payload={'target': 'checkpoint-a'})

M = MissionState
S = StepState
SUC, CAN, ABO = ActionStatus.SUCCEEDED, ActionStatus.CANCELED, ActionStatus.ABORTED

KEYS = ('a', 'b', 'c')
CONTEXT_TABLE = 'mission_reconciliation_contexts'
GOAL_TABLE = 'dispatch_goals'
ALL_TABLES = (*V6_TABLES, CONTEXT_TABLE, GOAL_TABLE)

# Each round is an independent race, repeated because a single race can be won "by luck".
ROUNDS = 25
TIMEOUT = 10

# The one new table, written out here independently of the store so any change shows up.
CONTEXT_DDL = """
CREATE TABLE mission_reconciliation_contexts (
    mission_id   TEXT NOT NULL PRIMARY KEY
                 REFERENCES missions (mission_id) ON DELETE CASCADE,
    resume_state TEXT NOT NULL CHECK (
        resume_state IN ('RUNNING', 'CANCELLING')
    )
)
"""

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
def foreign_key_connection(db_path):
    """Yield a connection that enforces foreign keys, as the store's own connections do."""
    with contextlib.closing(sqlite3.connect(db_path, isolation_level=None)) as conn:
        conn.execute('PRAGMA foreign_keys = ON')
        yield conn


def squashed(sql):
    return ' '.join(sql.split())


def normalized(sql):
    return ' '.join(sql.split()).upper()


def writes(statements):
    return [sql for sql in statements if sql.split()[0].upper() in WRITE_VERBS]


def schema_of(db_path):
    return raw(db_path, 'SELECT type, name, sql FROM sqlite_master ORDER BY type, name')


def table_names(db_path):
    return {name for (name,) in raw(
        db_path, "SELECT name FROM sqlite_master WHERE type = 'table'")}


def dump(db_path):
    """Return every row of every table, so that "nothing changed" means every cell."""
    return {table: raw(db_path, f'SELECT * FROM {table} ORDER BY 1, 2') for table in ALL_TABLES}


def changed_tables(before, after):
    return {table for table in before if before[table] != after[table]}


def context_rows(db_path):
    return raw(db_path, f'SELECT * FROM {CONTEXT_TABLE} ORDER BY mission_id')


def mission_row(db_path, mission_id):
    return raw(db_path, 'SELECT * FROM missions WHERE mission_id = ?', mission_id)


def package_env():
    package_root = Path(task_resilience.__file__).resolve().parent.parent
    return {**os.environ, 'PYTHONPATH': str(package_root)}


def observation(dispatch_id, status):
    return RecoveryObservation(
        kind=ObservationKind.RESULT_RESPONSE, dispatch_id=dispatch_id,
        action_name=ACTION.action_name, action_type=ACTION.action_type,
        robot_namespace=ACTION.robot_namespace, status=status, result_code=0)


def new_mission(store, keys=KEYS, route=(M.RUNNING,), request_id='req-001', max_attempts=5):
    """Create a mission with every step prepared, and walk it along an ordinary mission route."""
    mission_id = store.create_or_get(request_id, {'goal': 'demo'}, max_attempts).mission_id
    store.initialize_steps(mission_id, list(keys))
    for key in keys:
        store.prepare_step_dispatch(mission_id, key, ACTION, GOAL)
    for state in route:
        store.transition_mission_state(mission_id, state)
    return mission_id


def authorized_unknown(store, mission_id, key='a'):
    """Make a step unknown after its send was authorized, so it has durable provenance."""
    store.authorize_step_send(mission_id, key)
    store.transition_step_state(mission_id, key, S.OUTCOME_UNKNOWN)


def held_unknown(store, mission_id, key='a'):
    """Make a prepared step unknown by the ordinary edge: it has no authorization row."""
    store.transition_step_state(mission_id, key, S.OUTCOME_UNKNOWN)


def reconciling(store, resume=M.RUNNING, unknown=('a',), held=(), **kwargs):
    """Return a mission that entered reconciliation, with the given steps unknown."""
    mission_id = new_mission(store, **kwargs)
    # Every send is authorized first: none can be once a step is unknown.
    for key in unknown:
        store.authorize_step_send(mission_id, key)
    for key in unknown:
        store.transition_step_state(mission_id, key, S.OUTCOME_UNKNOWN)
    for key in held:
        held_unknown(store, mission_id, key)
    if resume is M.CANCELLING:
        store.transition_mission_state(mission_id, M.CANCELLING)
    store.enter_mission_reconciliation(mission_id)
    return mission_id


def settle(store, mission_id, key='a', status=SUC, evidence_id=None):
    """Record a terminal result for the step through Task 011, and return what was recorded."""
    dispatch_id = store.get_step_dispatch(mission_id, key).dispatch_id
    return store.apply_recovery_observation(
        evidence_id or uuid.uuid4(), mission_id, key, observation(dispatch_id, status))


def mission_in(store, state):
    """Return a mission in the given state, reached only by supported operations."""
    if state is M.ACCEPTED:
        return new_mission(store, route=())
    if state is M.RUNNING:
        return new_mission(store)
    if state is M.CANCELLING:
        return new_mission(store, route=(M.RUNNING, M.CANCELLING))
    if state is M.FAILED:
        return new_mission(store, route=(M.RUNNING, M.FAILED))
    if state is M.CANCELLED:
        return new_mission(store, route=(M.CANCELLED,))
    if state is M.COMPLETED:
        mission_id = new_mission(store, keys=('a',))
        store.authorize_step_send(mission_id, 'a')
        store.transition_step_state(mission_id, 'a', S.SUCCEEDED)
        store.transition_mission_state(mission_id, M.COMPLETED)
        return mission_id
    return reconciling(store)


ALL_STATES = list(M)


# A. The schema and its migration

def test_the_schema_is_exactly_version_8_with_two_new_tables_since_version_6(db_path):
    MissionStore(db_path).close()

    # This file's own contract is what version 7 (Task 012) added over version 6: exactly the
    # reconciliation table. Version 8 (Task 013) has since added the goal table; it changed
    # nothing else here, so the two new tables together are the full difference from version 6.
    assert storage.SCHEMA_VERSION == 8
    assert raw(db_path, 'PRAGMA user_version') == [(8,)]
    assert table_names(db_path) == set(ALL_TABLES)
    assert table_names(db_path) - set(V6_TABLES) == {CONTEXT_TABLE, GOAL_TABLE}


def test_the_context_table_is_exactly_the_reviewed_shape(db_path):
    MissionStore(db_path).close()

    stored = {name: sql for _, name, sql in schema_of(db_path)}
    assert squashed(stored[CONTEXT_TABLE]) == squashed(CONTEXT_DDL)
    # (name, declared type, not null, default, primary key): two columns, and no timestamp.
    assert [(row[1], row[2], row[3], row[4], row[5])
            for row in raw(db_path, f'PRAGMA table_info({CONTEXT_TABLE})')] == [
        ('mission_id', 'TEXT', 1, None, 1),
        ('resume_state', 'TEXT', 1, None, 0),
    ]
    # (id, seq, table, from, to, on update, on delete, match)
    assert [(row[2], row[3], row[4], row[6])
            for row in raw(db_path, f'PRAGMA foreign_key_list({CONTEXT_TABLE})')] == [
        ('missions', 'mission_id', 'mission_id', 'CASCADE')]


def test_the_schema_has_no_explicit_index_trigger_or_view(db_path):
    MissionStore(db_path).close()

    assert raw(db_path, "SELECT type, name FROM sqlite_master WHERE type IN ('trigger', 'view')"
                        " OR (type = 'index' AND sql IS NOT NULL)") == []
    # Only the index that SQLite makes for the primary key.
    # (seq, name, unique, origin, partial)
    assert [(row[2], row[3]) for row in raw(
        db_path, f'PRAGMA index_list({CONTEXT_TABLE})')] == [(1, 'pk')]


def test_the_older_tables_are_exactly_what_they_were(db_path, tmp_path):
    MissionStore(db_path).close()
    old = tmp_path / 'v6.db'
    make_v6_database(old)

    # The same text for every table that version 6 had: version 7 changed none of them.
    version_6 = {name: sql for _, name, sql in schema_of(old)}
    stored = {name: sql for _, name, sql in schema_of(db_path)}
    for name in V6_TABLES:
        assert stored[name] == version_6[name], name


@pytest.mark.parametrize('value', ['RUNNING', 'CANCELLING'])
def test_the_table_accepts_exactly_the_two_states_to_resume(store, db_path, value):
    mission_id = new_mission(store)

    with foreign_key_connection(db_path) as conn:
        conn.execute(f'INSERT INTO {CONTEXT_TABLE} VALUES (?, ?)', (mission_id, value))

    assert context_rows(db_path) == [(mission_id, value)]


@pytest.mark.parametrize('value', [
    'COMPLETED', 'FAILED', 'CANCELLED', 'ACCEPTED', 'NEEDS_RECONCILIATION', 'running',
    'Running', ' RUNNING', 'RUNNING ', 'cancelling', '', None, 5, b'RUNNING'])
def test_the_table_refuses_every_other_value(store, db_path, value):
    mission_id = new_mission(store)

    with pytest.raises(sqlite3.IntegrityError):
        with foreign_key_connection(db_path) as conn:
            conn.execute(f'INSERT INTO {CONTEXT_TABLE} VALUES (?, ?)', (mission_id, value))

    assert context_rows(db_path) == []


def test_the_table_needs_a_mission_and_allows_one_context_per_mission(store, db_path):
    mission_id = new_mission(store)

    with pytest.raises(sqlite3.IntegrityError):
        with foreign_key_connection(db_path) as conn:
            conn.execute(
                f'INSERT INTO {CONTEXT_TABLE} VALUES (?, ?)', ('no-such-mission', 'RUNNING'))
    with foreign_key_connection(db_path) as conn:
        conn.execute(f'INSERT INTO {CONTEXT_TABLE} VALUES (?, ?)', (mission_id, 'RUNNING'))
    with pytest.raises(sqlite3.IntegrityError):
        with foreign_key_connection(db_path) as conn:
            conn.execute(f'INSERT INTO {CONTEXT_TABLE} VALUES (?, ?)', (mission_id, 'CANCELLING'))

    assert context_rows(db_path) == [(mission_id, 'RUNNING')]


def test_a_context_is_removed_with_its_mission_and_not_with_a_step(store, db_path):
    mission_id = reconciling(store)
    other_id = reconciling(store, request_id='req-002')

    with foreign_key_connection(db_path) as conn:
        conn.execute('DELETE FROM missions WHERE mission_id = ?', (mission_id,))
        assert conn.execute(f'SELECT mission_id FROM {CONTEXT_TABLE}').fetchall() == [(other_id,)]
        conn.execute('DELETE FROM mission_steps WHERE mission_id = ?', (other_id,))
        assert conn.execute(f'SELECT mission_id FROM {CONTEXT_TABLE}').fetchall() == [(other_id,)]


@pytest.mark.parametrize('origin', list(ORIGINS))
def test_every_older_database_migrates_with_zero_contexts_and_the_schema_of_a_fresh_one(
        db_path, tmp_path, origin):
    ORIGINS[origin](db_path)
    before = old_rows(db_path, origin)

    MissionStore(db_path).close()

    assert raw(db_path, 'PRAGMA user_version') == [(8,)]
    assert old_rows(db_path, origin) == before
    assert table_names(db_path) == set(ALL_TABLES)
    assert context_rows(db_path) == []
    assert schema_of(db_path) == fresh_schema(tmp_path)


def test_a_native_version_6_database_runs_only_the_steps_after_version_6(monkeypatch, db_path):
    make_v6_database(db_path)
    called = []
    for name in MIGRATIONS:
        original = getattr(MissionStore, name)

        def spy(self, _name=name, _original=original):
            called.append(_name)
            return _original(self)

        monkeypatch.setattr(MissionStore, name, spy)

    store, connection = open_faulty_store(monkeypatch, db_path)
    store.close()

    assert called == ['_migrate_v6_to_v7', '_migrate_v7_to_v8']
    # Everything the open executed: the settings, the one transaction, the two tables, the
    # version, in that order, with the version set only once, to the current one.
    statements = connection.statements
    assert statements[:4] == [
        'PRAGMA synchronous = FULL', 'PRAGMA foreign_keys = ON', 'BEGIN IMMEDIATE',
        'PRAGMA user_version']
    assert statements[4] == squashed(CONTEXT_DDL)
    assert statements[5].startswith('CREATE TABLE dispatch_goals')
    assert 'PRAGMA user_version = 7' not in statements
    assert statements[6:] == ['PRAGMA user_version = 8', 'COMMIT']
    assert context_rows(db_path) == []


@pytest.mark.parametrize('origin', list(ORIGINS))
def test_no_migration_alters_a_table_and_none_adds_a_column(monkeypatch, db_path, origin):
    ORIGINS[origin](db_path)

    store, connection = open_faulty_store(monkeypatch, db_path)
    store.close()

    alters = [sql for sql in connection.statements if sql.upper().startswith('ALTER')]
    # Only the rebuild of the mission table that version 2 always needed.
    assert alters == (['ALTER TABLE missions RENAME TO missions_v1'] if origin == 'v1' else [])
    assert not [sql for sql in connection.statements if 'ADD COLUMN' in sql.upper()]


def test_a_mission_that_was_already_reconciling_gets_no_context_and_nothing_else_changes(db_path):
    make_v6_database(db_path)
    before = {table: raw(db_path, f'SELECT * FROM {table} ORDER BY 1, 2') for table in V6_TABLES}
    assert raw(db_path, 'SELECT mission_state FROM missions WHERE mission_id = ?',
               V3_UNKNOWN_ID) == [('NEEDS_RECONCILIATION',)]

    MissionStore(db_path).close()

    assert context_rows(db_path) == []
    assert {table: raw(db_path, f'SELECT * FROM {table} ORDER BY 1, 2')
            for table in V6_TABLES} == before


@pytest.mark.parametrize('failing', [
    'create the table', 'create the goal table', 'set the version', 'commit'])
def test_a_failure_while_migrating_from_version_6_rolls_back_cleanly(
        monkeypatch, db_path, failing):
    make_v6_database(db_path)
    before = {table: raw(db_path, f'SELECT * FROM {table} ORDER BY 1, 2') for table in V6_TABLES}
    schema = schema_of(db_path)

    if failing in ('create the table', 'create the goal table'):
        ddl = {'create the table': '_MISSION_RECONCILIATION_CONTEXTS_DDL',
               'create the goal table': '_DISPATCH_GOALS_DDL'}[failing]
        with monkeypatch.context() as patch:
            patch.setattr(storage, ddl, 'THIS IS NOT SQL')
            with pytest.raises(sqlite3.OperationalError):
                MissionStore(db_path)
    else:
        fails = {
            'set the version': lambda sql: sql.strip() == 'PRAGMA user_version = 8',
            'commit': lambda sql: sql.strip().upper() == 'COMMIT',
        }[failing]
        with pytest.raises(sqlite3.OperationalError, match='injected failure'):
            open_faulty_store(monkeypatch, db_path, fail_when=fails)

    assert raw(db_path, 'PRAGMA user_version') == [(6,)]
    assert table_names(db_path) == set(V6_TABLES)
    assert {table: raw(db_path, f'SELECT * FROM {table} ORDER BY 1, 2')
            for table in V6_TABLES} == before
    assert schema_of(db_path) == schema
    with contextlib.closing(sqlite3.connect(db_path, timeout=0.1, isolation_level=None)) as conn:
        conn.execute('BEGIN IMMEDIATE')
        conn.execute('ROLLBACK')
    # A later, healthy open finishes the migration.
    with MissionStore(db_path):
        pass
    assert raw(db_path, 'PRAGMA user_version') == [(8,)]
    assert context_rows(db_path) == []


def test_concurrent_opens_of_a_version_6_database_migrate_it_once(tmp_path):
    for round_index in range(5):
        path = tmp_path / f'v6-{round_index}.db'
        make_v6_database(path)
        before = {table: raw(path, f'SELECT * FROM {table} ORDER BY 1, 2') for table in V6_TABLES}
        workers = 8
        barrier = threading.Barrier(workers, timeout=TIMEOUT)

        def open_and_close():
            barrier.wait()
            MissionStore(path).close()

        with ThreadPoolExecutor(max_workers=workers) as pool:
            for future in [pool.submit(open_and_close) for _ in range(workers)]:
                future.result(timeout=TIMEOUT)

        assert raw(path, 'PRAGMA user_version') == [(8,)]
        assert {table: raw(path, f'SELECT * FROM {table} ORDER BY 1, 2')
                for table in V6_TABLES} == before
        assert context_rows(path) == []
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


def test_the_versions_the_fixtures_claim_are_the_versions():
    # Nothing reads a version from a table, and version 6 is the newest older one this file's
    # own fixtures reach. Task 013 has since added one more step, after this file's own.
    assert VERSION_OF == {'v1': 1, 'v2': 2, 'v3': 3, 'v4': 4, 'v5': 5, 'v6': 6}
    assert MIGRATIONS[-2:] == ['_migrate_v6_to_v7', '_migrate_v7_to_v8']


# B. Why the table exists: the older tables cannot say where a reconciling mission came from

UUID_TEXT = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')
TIME_TEXT = re.compile(r'\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}\+00:00')


def masked(db_path, tables=V6_TABLES):
    """Return the rows with every UUID replaced by its role and every time by TS, sorted."""
    roles = {}
    for (mission_id,) in raw(db_path, 'SELECT mission_id FROM missions'):
        roles[mission_id] = 'MISSION'
    for dispatch_id, key in raw(db_path, 'SELECT dispatch_id, step_key FROM step_dispatches'):
        roles[dispatch_id] = f'DISPATCH[{key}]'
    for evidence_id, dispatch_id in raw(
            db_path, 'SELECT evidence_id, dispatch_id FROM recovery_evidence'):
        roles[evidence_id] = f'EVIDENCE[{roles[dispatch_id]}]'
    out = {}
    for table in tables:
        rows = []
        for row in raw(db_path, f'SELECT * FROM {table}'):
            text = UUID_TEXT.sub(lambda match: roles[match.group(0)], repr(tuple(row)))
            rows.append(TIME_TEXT.sub('TS', text))
        out[table] = sorted(rows)
    return out


def origin_history(db_path, order):
    """Build the same durable facts with the mission entering reconciliation in a given way."""
    with MissionStore(db_path) as store:
        mission_id = new_mission(store)
        store.authorize_step_send(mission_id, 'b')
        store.transition_step_state(mission_id, 'b', S.SUCCEEDED)
        store.authorize_step_send(mission_id, 'a')
        if order == 'cancelling before the step is unknown':
            store.transition_mission_state(mission_id, M.CANCELLING)
        store.transition_step_state(mission_id, 'a', S.OUTCOME_UNKNOWN)
        if order == 'cancelling after the step is unknown':
            store.transition_mission_state(mission_id, M.CANCELLING)
        record = store.enter_mission_reconciliation(mission_id)
        if order == 'then settled':
            settle(store, mission_id, 'a', evidence_id=uuid.UUID(int=1))
    return record.resume_state


def test_the_older_tables_cannot_tell_a_running_origin_from_a_cancelling_one_and_the_context_can(
        tmp_path):
    histories = {
        'running': 'entered from running',
        'cancelling after': 'cancelling after the step is unknown',
        'cancelling before': 'cancelling before the step is unknown',
    }
    paths = {name: tmp_path / f'{index}.db' for index, name in enumerate(histories)}
    resumes = {name: origin_history(paths[name], order) for name, order in histories.items()}

    # Everything the six older tables hold is the same, once ids and times are masked: the mission
    # state, the step states, the attempts, the dispatches, the contexts, the authorizations.
    assert masked(paths['running']) == masked(paths['cancelling after'])
    assert masked(paths['running']) == masked(paths['cancelling before'])
    for name in histories:
        assert [state for (state,) in raw(paths[name], 'SELECT mission_state FROM missions')] == [
            'NEEDS_RECONCILIATION']
    # The new table is the only place that says which mission state to resume.
    assert resumes == {'running': M.RUNNING, 'cancelling after': M.CANCELLING,
                       'cancelling before': M.CANCELLING}
    assert [state for (_, state) in context_rows(paths['running'])] == ['RUNNING']
    assert [state for (_, state) in context_rows(paths['cancelling after'])] == ['CANCELLING']
    assert [state for (_, state) in context_rows(paths['cancelling before'])] == ['CANCELLING']


def test_the_comparison_that_shows_that_is_able_to_tell_real_differences_apart(tmp_path):
    before, after = tmp_path / 'before.db', tmp_path / 'after.db'
    origin_history(before, 'entered from running')
    origin_history(after, 'then settled')

    # A step that Task 011 settled is a difference the masked tables do show.
    assert masked(before) != masked(after)
    assert masked(before, ['mission_steps']) != masked(after, ['mission_steps'])
    assert masked(before, ['recovery_evidence']) != masked(after, ['recovery_evidence'])
    assert masked(before, ['missions']) == masked(after, ['missions'])


# C. A plain transition cannot enter NEEDS_RECONCILIATION

@pytest.mark.parametrize('state', ALL_STATES)
def test_a_plain_transition_into_needs_reconciliation_is_refused_from_every_state(
        store, db_path, state):
    mission_id = mission_in(store, state)
    before = dump(db_path)

    with pytest.raises(MissionReconciliationEntryRequired) as excinfo:
        store.transition_mission_state(mission_id, M.NEEDS_RECONCILIATION)

    assert excinfo.value.mission_id == mission_id
    assert dump(db_path) == before
    assert store.get_by_mission_id(mission_id).state is state


def test_a_same_state_replay_through_the_plain_transition_is_refused_too(store, db_path):
    mission_id = reconciling(store)
    before = dump(db_path)

    with pytest.raises(MissionReconciliationEntryRequired):
        store.transition_mission_state(mission_id, M.NEEDS_RECONCILIATION)

    assert dump(db_path) == before
    assert context_rows(db_path) == [(mission_id, 'RUNNING')]


def test_a_mission_that_does_not_exist_is_reported_as_an_entry_error_and_not_as_missing(store):
    with pytest.raises(MissionReconciliationEntryRequired) as excinfo:
        store.transition_mission_state('no-such-mission', M.NEEDS_RECONCILIATION)
    assert excinfo.value.mission_id == 'no-such-mission'

    # Any other target still finds that the mission is not there.
    with pytest.raises(MissionNotFound):
        store.transition_mission_state('no-such-mission', M.RUNNING)


def test_the_refusal_comes_before_any_database_access(monkeypatch, db_path):
    store, connection = open_faulty_store(monkeypatch, db_path)
    with store:
        running = new_mission(store)
        needs = reconciling(store, request_id='req-002')
        for mission_id in (running, needs, 'no-such-mission'):
            connection.statements.clear()

            with pytest.raises(MissionReconciliationEntryRequired):
                store.transition_mission_state(mission_id, M.NEEDS_RECONCILIATION)

            assert connection.statements == []


@pytest.mark.parametrize('bad', ['NEEDS_RECONCILIATION', 'RUNNING', None, S.PENDING, 1])
def test_the_target_type_is_checked_before_the_refusal(store, bad):
    mission_id = new_mission(store)

    with pytest.raises(TypeError):
        store.transition_mission_state(mission_id, bad)


# Written by hand, so that any change in what a plain transition does shows up: each row is the
# result of asking a mission in that state for that target.
OK, NOOP, INVALID, INVARIANT = 'moves', 'stays', InvalidStateTransition, (
    MissionCompletionInvariantError)
PLAIN_TRANSITIONS = {
    M.ACCEPTED: {M.ACCEPTED: NOOP, M.RUNNING: OK, M.CANCELLING: INVALID, M.COMPLETED: INVALID,
                 M.FAILED: INVALID, M.CANCELLED: OK},
    M.RUNNING: {M.ACCEPTED: INVALID, M.RUNNING: NOOP, M.CANCELLING: OK, M.COMPLETED: INVARIANT,
                M.FAILED: OK, M.CANCELLED: INVALID},
    M.CANCELLING: {M.ACCEPTED: INVALID, M.RUNNING: INVALID, M.CANCELLING: NOOP,
                   M.COMPLETED: INVARIANT, M.FAILED: OK, M.CANCELLED: OK},
    M.COMPLETED: {M.ACCEPTED: INVALID, M.RUNNING: INVALID, M.CANCELLING: INVALID,
                  M.COMPLETED: NOOP, M.FAILED: INVALID, M.CANCELLED: INVALID},
    M.FAILED: {M.ACCEPTED: INVALID, M.RUNNING: INVALID, M.CANCELLING: INVALID,
               M.COMPLETED: INVALID, M.FAILED: NOOP, M.CANCELLED: INVALID},
    M.CANCELLED: {M.ACCEPTED: INVALID, M.RUNNING: INVALID, M.CANCELLING: INVALID,
                  M.COMPLETED: INVALID, M.FAILED: INVALID, M.CANCELLED: NOOP},
    # No ordinary transition leaves NEEDS_RECONCILIATION. Entering it is refused, as above.
    M.NEEDS_RECONCILIATION: {M.ACCEPTED: INVALID, M.RUNNING: INVALID, M.CANCELLING: INVALID,
                             M.COMPLETED: INVALID, M.FAILED: INVALID, M.CANCELLED: INVALID},
}


@pytest.mark.parametrize(('state', 'target'), [
    (state, target) for state, row in PLAIN_TRANSITIONS.items() for target in row])
def test_every_other_plain_transition_behaves_as_it_always_did(store, db_path, state, target):
    mission_id = mission_in(store, state)
    before = dump(db_path)
    expected = PLAIN_TRANSITIONS[state][target]

    if expected in (OK, NOOP):
        record = store.transition_mission_state(mission_id, target)
        assert record.state is (target if expected is OK else state)
        if expected is NOOP:
            assert dump(db_path) == before
    else:
        with pytest.raises(expected):
            store.transition_mission_state(mission_id, target)
        assert dump(db_path) == before


def test_the_pure_graph_still_has_the_edge_in_and_no_edge_out():
    for source in (M.RUNNING, M.CANCELLING):
        assert transition_mission(
            source, M.NEEDS_RECONCILIATION, [S.OUTCOME_UNKNOWN]) is M.NEEDS_RECONCILIATION
    for target in M:
        if target is M.NEEDS_RECONCILIATION:
            assert transition_mission(target, target) is target
        else:
            with pytest.raises(InvalidStateTransition):
                transition_mission(M.NEEDS_RECONCILIATION, target, [S.SUCCEEDED])
    assert state_machine.is_mission_terminal(M.NEEDS_RECONCILIATION) is False


# D. Entering reconciliation

def test_entry_from_running_records_running_and_changes_only_the_mission_and_the_context(
        store, db_path):
    mission_id = new_mission(store)
    authorized_unknown(store, mission_id)
    before = dump(db_path)

    record = store.enter_mission_reconciliation(mission_id)

    assert record == MissionReconciliationContextRecord(mission_id, M.RUNNING)
    after = dump(db_path)
    assert changed_tables(before, after) == {'missions', CONTEXT_TABLE}
    (old,), (new,) = before['missions'], after['missions']
    # Everything but the state and its time is the same: the request, the budget, the attempts.
    assert (old[:7], old[8]) == (new[:7], 'RUNNING')
    assert new[8] == 'NEEDS_RECONCILIATION'
    assert after[CONTEXT_TABLE] == [(mission_id, 'RUNNING')]
    assert store.get_mission_reconciliation_context(mission_id) == record
    assert store.get_by_mission_id(mission_id).attempts_used == 1


def test_entry_from_cancelling_records_cancelling(store, db_path):
    mission_id = new_mission(store)
    authorized_unknown(store, mission_id)
    store.transition_mission_state(mission_id, M.CANCELLING)
    before = dump(db_path)

    record = store.enter_mission_reconciliation(mission_id)

    assert record == MissionReconciliationContextRecord(mission_id, M.CANCELLING)
    assert changed_tables(before, dump(db_path)) == {'missions', CONTEXT_TABLE}
    assert store.get_by_mission_id(mission_id).state is M.NEEDS_RECONCILIATION
    assert context_rows(db_path) == [(mission_id, 'CANCELLING')]


def pending_only(store):
    mission_id = store.create_or_get('req-001', {}, 3).mission_id
    store.initialize_steps(mission_id, list(KEYS))
    store.transition_mission_state(mission_id, M.RUNNING)
    return mission_id


def all_finished(store):
    mission_id = new_mission(store)
    for key, state in zip(KEYS, (S.SUCCEEDED, S.FAILED, S.CANCELLED)):
        if state is S.CANCELLED:
            store.transition_step_state(mission_id, key, S.CANCELLED)
        else:
            store.authorize_step_send(mission_id, key)
            store.transition_step_state(mission_id, key, state)
    return mission_id


def in_flight(store):
    mission_id = new_mission(store)
    store.authorize_step_send(mission_id, 'a')
    store.authorize_step_send(mission_id, 'b')
    store.transition_step_state(mission_id, 'b', S.ACCEPTED)
    return mission_id


@pytest.mark.parametrize('resume', [M.RUNNING, M.CANCELLING])
@pytest.mark.parametrize('layout', [new_mission, pending_only, all_finished, in_flight])
def test_a_mission_with_no_unknown_step_cannot_enter_reconciliation(
        store, db_path, layout, resume):
    mission_id = layout(store)
    if resume is M.CANCELLING:
        store.transition_mission_state(mission_id, M.CANCELLING)
    before = dump(db_path)

    with pytest.raises(NoUnknownStepToReconcile) as excinfo:
        store.enter_mission_reconciliation(mission_id)

    assert excinfo.value.mission_id == mission_id
    assert dump(db_path) == before
    assert store.get_by_mission_id(mission_id).state is resume


@pytest.mark.parametrize('unknown', [('a',), ('c',), ('a', 'b'), ('a', 'b', 'c')])
@pytest.mark.parametrize('resume', [M.RUNNING, M.CANCELLING])
def test_one_or_several_unknown_steps_are_enough(store, db_path, unknown, resume):
    mission_id = new_mission(store)
    authorized_unknown(store, mission_id, unknown[0])
    for key in unknown[1:]:
        held_unknown(store, mission_id, key)
    if resume is M.CANCELLING:
        store.transition_mission_state(mission_id, M.CANCELLING)

    record = store.enter_mission_reconciliation(mission_id)

    assert record.resume_state is resume
    assert [step.state for step in store.get_steps(mission_id)].count(S.OUTCOME_UNKNOWN) == len(
        unknown)
    assert context_rows(db_path) == [(mission_id, resume.value)]


def test_an_unknown_step_that_can_never_be_resolved_does_not_stop_the_entry(store, db_path):
    # A held step has no authorization row, and Task 011 will never settle it. The reason is the
    # persisted state and nothing else, so the entry is allowed, and the hold that follows is the
    # accepted outcome (see the resolution tests).
    mission_id = new_mission(store)
    held_unknown(store, mission_id, 'a')
    assert raw(db_path, 'SELECT COUNT(*) FROM dispatch_authorizations') == [(0,)]

    record = store.enter_mission_reconciliation(mission_id)

    assert record.resume_state is M.RUNNING
    assert store.get_by_mission_id(mission_id).state is M.NEEDS_RECONCILIATION


def route_with_unknown(store, state):
    """Return a mission in the state that has an unknown step, where the state allows one."""
    if state is M.ACCEPTED:
        mission_id = new_mission(store, route=())
        held_unknown(store, mission_id)
    elif state is M.FAILED:
        mission_id = new_mission(store)
        authorized_unknown(store, mission_id)
        store.transition_mission_state(mission_id, M.FAILED)
    elif state is M.CANCELLED:
        mission_id = new_mission(store, route=(M.CANCELLED,))
        held_unknown(store, mission_id)
    else:
        mission_id = mission_in(store, state)
    return mission_id


@pytest.mark.parametrize('state', [M.ACCEPTED, M.COMPLETED, M.FAILED, M.CANCELLED])
def test_only_the_pure_graph_decides_which_state_may_enter_and_others_are_refused(
        store, db_path, state):
    mission_id = route_with_unknown(store, state)
    before = dump(db_path)

    with pytest.raises(InvalidStateTransition) as excinfo:
        store.enter_mission_reconciliation(mission_id)

    assert (excinfo.value.entity, excinfo.value.current, excinfo.value.target) == (
        'mission', state, M.NEEDS_RECONCILIATION)
    assert dump(db_path) == before


def test_an_exact_replay_returns_the_existing_context_and_writes_nothing(monkeypatch, db_path):
    store, connection = open_faulty_store(monkeypatch, db_path)
    with store:
        mission_id = new_mission(store)
        authorized_unknown(store, mission_id)
        first = store.enter_mission_reconciliation(mission_id)
        before = dump(db_path)
        connection.statements.clear()

        again = store.enter_mission_reconciliation(mission_id)

        assert again == first
        assert writes(connection.statements) == []
        assert dump(db_path) == before
        assert context_rows(db_path) == [(mission_id, 'RUNNING')]


def test_a_replay_returns_the_intent_as_it_is_now_and_never_the_original(store, db_path):
    mission_id = reconciling(store)
    store.request_cancellation_during_reconciliation(mission_id)

    replay = store.enter_mission_reconciliation(mission_id)

    assert replay.resume_state is M.CANCELLING
    assert context_rows(db_path) == [(mission_id, 'CANCELLING')]


def test_a_mission_that_is_reconciling_without_a_context_is_never_adopted(store, db_path):
    mission_id = reconciling(store)
    raw(db_path, f'DELETE FROM {CONTEXT_TABLE}')
    before = dump(db_path)

    with pytest.raises(MissionReconciliationContextMissing) as excinfo:
        store.enter_mission_reconciliation(mission_id)

    assert excinfo.value.mission_id == mission_id
    assert dump(db_path) == before
    assert context_rows(db_path) == []


@pytest.mark.parametrize('state', [M.RUNNING, M.CANCELLING, M.FAILED, M.ACCEPTED])
@pytest.mark.parametrize('stray', ['RUNNING', 'CANCELLING'])
def test_a_context_on_a_mission_that_is_not_reconciling_is_corruption(
        store, db_path, state, stray):
    mission_id = route_with_unknown(store, state) if state is not M.CANCELLING else (
        mission_in(store, M.CANCELLING))
    raw(db_path, f'INSERT INTO {CONTEXT_TABLE} VALUES (?, ?)', mission_id, stray)
    before = dump(db_path)

    # It is judged before the graph and before the reason, so neither can hide it.
    with pytest.raises(CorruptMissionReconciliationContext) as excinfo:
        store.enter_mission_reconciliation(mission_id)

    assert excinfo.value.mission_id == mission_id
    assert excinfo.value.value == stray
    assert state.value in excinfo.value.reason
    assert dump(db_path) == before


def test_a_context_that_is_malformed_is_reported_before_anything_else(store, db_path):
    mission_id = new_mission(store)
    corrupt(db_path, f'INSERT INTO {CONTEXT_TABLE} VALUES (?, ?)', mission_id, 'COMPLETED')

    with pytest.raises(CorruptMissionReconciliationContext) as excinfo:
        store.enter_mission_reconciliation(mission_id)

    assert excinfo.value.value == 'COMPLETED'


def test_a_mission_that_does_not_exist_is_reported_first(store):
    with pytest.raises(MissionNotFound):
        store.enter_mission_reconciliation('no-such-mission')


ENTRY_STATEMENTS = [
    'BEGIN IMMEDIATE',
    'SELECT * FROM MISSIONS WHERE MISSION_ID = ?',
    'SELECT * FROM MISSION_RECONCILIATION_CONTEXTS WHERE MISSION_ID = ?',
    'SELECT * FROM MISSION_STEPS WHERE MISSION_ID = ? ORDER BY STEP_INDEX',
    'INSERT INTO MISSION_RECONCILIATION_CONTEXTS (MISSION_ID, RESUME_STATE) VALUES (?, ?)',
    'UPDATE MISSIONS SET MISSION_STATE = ?, UPDATED_AT = ? WHERE MISSION_ID = ?',
    'SELECT * FROM MISSION_RECONCILIATION_CONTEXTS WHERE MISSION_ID = ?',
    'COMMIT',
]


def test_entry_runs_exactly_these_statements_and_writes_the_mission_last(monkeypatch, db_path):
    store, connection = open_faulty_store(monkeypatch, db_path)
    with store:
        mission_id = new_mission(store)
        authorized_unknown(store, mission_id)
        connection.statements.clear()

        store.enter_mission_reconciliation(mission_id)

    logged = [normalized(sql) for sql in connection.statements]
    assert logged == ENTRY_STATEMENTS
    # The context is inserted before the mission is updated, and the mission is the last write. The
    # transaction is what makes that atomic. The order only makes a failure easy to place.
    assert writes(logged) == [ENTRY_STATEMENTS[4], ENTRY_STATEMENTS[5]]
    # No evidence, dispatch, context or authorization of a step is looked at.
    assert not [sql for sql in logged if any(word in sql for word in (
        'RECOVERY_EVIDENCE', 'STEP_DISPATCHES', 'DISPATCH_CONTEXTS', 'DISPATCH_AUTHORIZATIONS'))]


def test_entry_reads_the_clock_once_and_the_mission_time_is_that_reading(
        store, db_path, monkeypatch):
    mission_id = new_mission(store)
    authorized_unknown(store, mission_id)
    readings = ['2031-01-02T03:04:05.000006+00:00', '2032-02-03T04:05:06.000007+00:00']
    calls = []

    def clock():
        calls.append(1)
        return readings[len(calls) - 1]

    monkeypatch.setattr(storage, '_utc_now', clock)

    store.enter_mission_reconciliation(mission_id)

    assert len(calls) == 1
    assert raw(db_path, 'SELECT updated_at FROM missions') == [(readings[0],)]
    store.enter_mission_reconciliation(mission_id)
    assert len(calls) == 1


def test_entry_touches_no_attempt_authorization_step_or_evidence(store, db_path):
    mission_id = new_mission(store)
    authorized_unknown(store, mission_id)
    before = dump(db_path)

    store.enter_mission_reconciliation(mission_id)

    after = dump(db_path)
    for table in ('mission_steps', 'step_dispatches', 'dispatch_contexts',
                  'dispatch_authorizations', 'recovery_evidence'):
        assert after[table] == before[table], table
    assert after['missions'][0][5] == before['missions'][0][5] == 1
    assert after['missions'][0][4] == before['missions'][0][4]


# E. Resolving a reconciliation

RESOLVE_STATEMENTS = [
    'BEGIN IMMEDIATE',
    'SELECT * FROM MISSIONS WHERE MISSION_ID = ?',
    'SELECT * FROM MISSION_RECONCILIATION_CONTEXTS WHERE MISSION_ID = ?',
    'SELECT * FROM MISSION_STEPS WHERE MISSION_ID = ? ORDER BY STEP_INDEX',
    'DELETE FROM MISSION_RECONCILIATION_CONTEXTS WHERE MISSION_ID = ?',
    'UPDATE MISSIONS SET MISSION_STATE = ?, UPDATED_AT = ? WHERE MISSION_ID = ?',
    'SELECT * FROM MISSIONS WHERE MISSION_ID = ?',
    'COMMIT',
]

CANCEL_STATEMENTS = [
    'BEGIN IMMEDIATE',
    'SELECT * FROM MISSIONS WHERE MISSION_ID = ?',
    'SELECT * FROM MISSION_RECONCILIATION_CONTEXTS WHERE MISSION_ID = ?',
    "UPDATE MISSION_RECONCILIATION_CONTEXTS SET RESUME_STATE = 'CANCELLING' "
    "WHERE MISSION_ID = ? AND RESUME_STATE = 'RUNNING'",
    'SELECT * FROM MISSION_RECONCILIATION_CONTEXTS WHERE MISSION_ID = ?',
    'COMMIT',
]


@pytest.mark.parametrize('resume', [M.RUNNING, M.CANCELLING])
def test_a_mission_is_restored_to_the_state_it_was_to_resume_once_no_step_is_unknown(
        store, db_path, resume):
    mission_id = reconciling(store, resume=resume)
    settle(store, mission_id, 'a')
    before = dump(db_path)

    record = store.resolve_mission_reconciliation(mission_id)

    assert isinstance(record, MissionRecord)
    assert (record.mission_id, record.state) == (mission_id, resume)
    after = dump(db_path)
    # The mission and its context, and nothing else: no step, dispatch, authorization, evidence.
    assert changed_tables(before, after) == {'missions', CONTEXT_TABLE}
    assert after[CONTEXT_TABLE] == []
    (old,), (new,) = before['missions'], after['missions']
    assert old[:7] == new[:7]
    assert (old[8], new[8]) == ('NEEDS_RECONCILIATION', resume.value)
    assert store.get_by_mission_id(mission_id) == record
    assert record.attempts_used == 1


def test_a_mission_with_an_unknown_step_is_not_resolved_and_the_context_is_kept(store, db_path):
    mission_id = reconciling(store)
    before = dump(db_path)

    with pytest.raises(MissionReconciliationRequired) as excinfo:
        store.resolve_mission_reconciliation(mission_id)

    assert excinfo.value.mission_id == mission_id
    assert excinfo.value.step_keys == ('a',)
    assert dump(db_path) == before
    assert context_rows(db_path) == [(mission_id, 'RUNNING')]


def test_every_unknown_step_is_named_in_step_order_and_one_settled_is_not(store, db_path):
    keys = ('c_step', 'b_step', 'a_step', 'd_step')
    mission_id = reconciling(store, keys=keys, unknown=('a_step', 'b_step'))
    before = dump(db_path)

    with pytest.raises(MissionReconciliationRequired) as excinfo:
        store.resolve_mission_reconciliation(mission_id)
    # In step order, and not in the order they became unknown or alphabetically.
    assert excinfo.value.step_keys == ('b_step', 'a_step')
    assert dump(db_path) == before

    settle(store, mission_id, 'a_step')
    with pytest.raises(MissionReconciliationRequired) as excinfo:
        store.resolve_mission_reconciliation(mission_id)
    assert excinfo.value.step_keys == ('b_step',)

    settle(store, mission_id, 'b_step')
    assert store.resolve_mission_reconciliation(mission_id).state is M.RUNNING


@pytest.mark.parametrize('status', [SUC, CAN, ABO])
def test_a_held_unknown_step_keeps_the_mission_reconciling_however_often_it_is_observed(
        store, db_path, status):
    # A step that became unknown without an authorization row is one Task 011 will never settle.
    # The mission stays NEEDS_RECONCILIATION with its context, and that is the intended,
    # fail-closed outcome. It is not corruption.
    mission_id = reconciling(store, unknown=(), held=('a',))

    for _ in range(3):
        assert settle(store, mission_id, 'a', status=status).outcome.name == (
            'UNPROVEN_AUTHORIZATION')
        with pytest.raises(MissionReconciliationRequired) as excinfo:
            store.resolve_mission_reconciliation(mission_id)
        assert excinfo.value.step_keys == ('a',)

    assert store.get_step(mission_id, 'a').state is S.OUTCOME_UNKNOWN
    assert store.get_by_mission_id(mission_id).state is M.NEEDS_RECONCILIATION
    assert store.get_mission_reconciliation_context(mission_id).resume_state is M.RUNNING
    assert context_rows(db_path) == [(mission_id, 'RUNNING')]


def test_no_reconciliation_operation_takes_anything_but_the_mission():
    operations = {
        'enter_mission_reconciliation': MissionReconciliationContextRecord,
        'resolve_mission_reconciliation': MissionRecord,
        'request_cancellation_during_reconciliation': MissionReconciliationContextRecord,
        'get_mission_reconciliation_context': MissionReconciliationContextRecord,
    }
    for name, returned in operations.items():
        signature = inspect.signature(getattr(MissionStore, name))
        assert list(signature.parameters) == ['self', 'mission_id'], name
        assert signature.parameters['mission_id'].annotation is str
        assert signature.return_annotation is returned, name


def test_resolution_runs_exactly_these_statements_and_reads_no_evidence(monkeypatch, db_path):
    store, connection = open_faulty_store(monkeypatch, db_path)
    with store:
        mission_id = reconciling(store)
        settle(store, mission_id, 'a')
        connection.statements.clear()

        store.resolve_mission_reconciliation(mission_id)

    logged = [normalized(sql) for sql in connection.statements]
    assert logged == RESOLVE_STATEMENTS
    # The context is deleted before the mission is updated, and the mission is the last write.
    assert writes(logged) == [RESOLVE_STATEMENTS[4], RESOLVE_STATEMENTS[5]]
    assert not [sql for sql in logged if any(word in sql for word in (
        'RECOVERY_EVIDENCE', 'STEP_DISPATCHES', 'DISPATCH_CONTEXTS', 'DISPATCH_AUTHORIZATIONS'))]


def test_a_refused_resolution_only_reads(monkeypatch, db_path):
    store, connection = open_faulty_store(monkeypatch, db_path)
    with store:
        mission_id = reconciling(store)
        connection.statements.clear()

        with pytest.raises(MissionReconciliationRequired):
            store.resolve_mission_reconciliation(mission_id)

    logged = [normalized(sql) for sql in connection.statements]
    assert logged == RESOLVE_STATEMENTS[:4] + ['ROLLBACK']


def test_resolution_reads_the_clock_once_and_the_mission_time_is_that_reading(
        store, db_path, monkeypatch):
    mission_id = reconciling(store)
    settle(store, mission_id, 'a')
    calls = []

    def clock():
        calls.append(1)
        return '2031-01-02T03:04:05.000006+00:00'

    monkeypatch.setattr(storage, '_utc_now', clock)

    record = store.resolve_mission_reconciliation(mission_id)

    assert len(calls) == 1
    assert record.updated_at.isoformat(timespec='microseconds') == (
        '2031-01-02T03:04:05.000006+00:00')


def test_resolution_does_not_ask_the_ordinary_graph_and_the_graph_gains_no_edge(
        store, monkeypatch):
    mission_id = reconciling(store)
    settle(store, mission_id, 'a')

    def refuse(*args):
        raise AssertionError('the ordinary graph was asked')

    with monkeypatch.context() as patch:
        patch.setattr(storage, 'transition_mission', refuse)
        assert store.resolve_mission_reconciliation(mission_id).state is M.RUNNING

    # The pure graph still has no edge out of NEEDS_RECONCILIATION, resumed or not.
    for target in (M.RUNNING, M.CANCELLING):
        with pytest.raises(InvalidStateTransition):
            transition_mission(M.NEEDS_RECONCILIATION, target, [S.SUCCEEDED])


def test_a_mission_can_be_reconciled_again_and_again(store, db_path):
    mission_id = new_mission(store)
    authorized_unknown(store, mission_id, 'a')

    outcomes = []
    for episode, key in enumerate(('a', 'b')):
        if key == 'b':
            authorized_unknown(store, mission_id, 'b')
        entered = store.enter_mission_reconciliation(mission_id)
        outcomes.append((entered.resume_state, len(context_rows(db_path))))
        if key == 'b':
            store.request_cancellation_during_reconciliation(mission_id)
        settle(store, mission_id, key, evidence_id=uuid.UUID(int=episode + 1))
        resolved = store.resolve_mission_reconciliation(mission_id)
        outcomes.append((resolved.state, len(context_rows(db_path))))

    # One row while it is reconciling and none once it is resolved, each time, and the second
    # episode resumed as the first did not: the intent belongs to the episode.
    assert outcomes == [(M.RUNNING, 1), (M.RUNNING, 0), (M.RUNNING, 1), (M.CANCELLING, 0)]


def test_a_retry_after_a_committed_resolution_is_refused_and_says_where_the_mission_is(
        store, db_path):
    mission_id = reconciling(store)
    settle(store, mission_id, 'a')
    store.resolve_mission_reconciliation(mission_id)
    before = dump(db_path)

    with pytest.raises(MissionNotAwaitingReconciliation) as excinfo:
        store.resolve_mission_reconciliation(mission_id)

    assert (excinfo.value.mission_id, excinfo.value.mission_state) == (mission_id, M.RUNNING)
    assert dump(db_path) == before


def test_a_retry_after_a_committed_resolution_says_cancelling_when_it_resumed_cancelling(store):
    mission_id = reconciling(store, resume=M.CANCELLING)
    settle(store, mission_id, 'a')
    store.resolve_mission_reconciliation(mission_id)

    with pytest.raises(MissionNotAwaitingReconciliation) as excinfo:
        store.resolve_mission_reconciliation(mission_id)

    assert excinfo.value.mission_state is M.CANCELLING


def test_a_retry_after_a_newer_episode_began_acts_on_that_episode_under_the_same_rule(
        store, db_path):
    # Resolution is a state-based operation on the CURRENT reconciliation of the mission. It is
    # not an idempotency-keyed command, so a retry that was meant for an earlier episode cannot
    # tell the difference.
    mission_id = reconciling(store)
    settle(store, mission_id, 'a')
    store.resolve_mission_reconciliation(mission_id)
    authorized_unknown(store, mission_id, 'b')
    store.enter_mission_reconciliation(mission_id)

    with pytest.raises(MissionReconciliationRequired) as excinfo:
        store.resolve_mission_reconciliation(mission_id)
    assert excinfo.value.step_keys == ('b',)

    settle(store, mission_id, 'b')
    assert store.resolve_mission_reconciliation(mission_id).state is M.RUNNING
    assert context_rows(db_path) == []


@pytest.mark.parametrize('resume', [M.RUNNING, M.CANCELLING])
def test_resolution_never_completes_the_mission_even_when_every_step_succeeded(
        store, db_path, resume):
    mission_id = new_mission(store, keys=('a', 'b'))
    store.authorize_step_send(mission_id, 'b')
    store.transition_step_state(mission_id, 'b', S.SUCCEEDED)
    authorized_unknown(store, mission_id, 'a')
    if resume is M.CANCELLING:
        store.transition_mission_state(mission_id, M.CANCELLING)
    store.enter_mission_reconciliation(mission_id)
    settle(store, mission_id, 'a')
    assert [step.state for step in store.get_steps(mission_id)] == [S.SUCCEEDED, S.SUCCEEDED]

    record = store.resolve_mission_reconciliation(mission_id)

    # Restored, not terminalized. Ordinary policy decides what happens next.
    assert record.state is resume
    assert store.transition_mission_state(mission_id, M.COMPLETED).state is M.COMPLETED


def test_the_completion_invariant_still_holds_after_a_resolution(store):
    mission_id = reconciling(store)
    settle(store, mission_id, 'a')
    store.resolve_mission_reconciliation(mission_id)

    # Steps b and c are still prepared, so the ordinary check refuses, exactly as it always did.
    with pytest.raises(MissionCompletionInvariantError):
        store.transition_mission_state(mission_id, M.COMPLETED)
    assert store.get_by_mission_id(mission_id).state is M.RUNNING


@pytest.mark.parametrize('state', [M.COMPLETED, M.FAILED, M.CANCELLED])
def test_a_finished_mission_is_never_reopened(store, db_path, state):
    mission_id = mission_in(store, state)
    before = dump(db_path)

    with pytest.raises(MissionNotAwaitingReconciliation) as excinfo:
        store.resolve_mission_reconciliation(mission_id)
    assert excinfo.value.mission_state is state
    with pytest.raises(InvalidStateTransition):
        store.enter_mission_reconciliation(mission_id)

    assert dump(db_path) == before


def test_sends_are_refused_while_reconciling_and_after_a_cancelling_resume(store):
    mission_id = reconciling(store, keys=('a', 'b', 'c'))
    with pytest.raises(MissionNotRunning):
        store.authorize_step_send(mission_id, 'b')
    settle(store, mission_id, 'a')
    with pytest.raises(MissionNotRunning):
        store.authorize_step_send(mission_id, 'b')

    store.request_cancellation_during_reconciliation(mission_id)
    store.resolve_mission_reconciliation(mission_id)

    with pytest.raises(MissionNotRunning):
        store.authorize_step_send(mission_id, 'b')
    assert store.get_by_mission_id(mission_id).attempts_used == 1


def test_a_send_after_a_running_resume_is_decided_by_the_task_008_gates_alone(store, db_path):
    mission_id = reconciling(store)
    settle(store, mission_id, 'a')
    store.resolve_mission_reconciliation(mission_id)
    assert store.get_by_mission_id(mission_id).attempts_used == 1
    assert raw(db_path, 'SELECT COUNT(*) FROM dispatch_authorizations') == [(1,)]

    # Restoring the state authorized nothing. Only this call takes the next attempt.
    assert store.authorize_step_send(mission_id, 'b').attempt_number == 2
    assert raw(db_path, 'SELECT COUNT(*) FROM dispatch_authorizations') == [(2,)]


def test_a_running_mission_may_have_an_unknown_step_again_and_may_reconcile_again(store, db_path):
    # Resolution can win a race and restore RUNNING an instant before another step becomes
    # unknown. That is a supported state, as it always was: the step-level gate refuses the next
    # send, and a new reconciliation may be entered.
    mission_id = reconciling(store)
    settle(store, mission_id, 'a')
    store.resolve_mission_reconciliation(mission_id)
    held_unknown(store, mission_id, 'b')

    assert store.get_by_mission_id(mission_id).state is M.RUNNING
    with pytest.raises(MissionReconciliationRequired) as excinfo:
        store.authorize_step_send(mission_id, 'c')
    assert excinfo.value.step_keys == ('b',)
    assert store.enter_mission_reconciliation(mission_id).resume_state is M.RUNNING
    assert context_rows(db_path) == [(mission_id, 'RUNNING')]


@pytest.mark.parametrize('operation', [
    lambda s, m: s.enter_mission_reconciliation(m),
    lambda s, m: s.resolve_mission_reconciliation(m),
    lambda s, m: s.request_cancellation_during_reconciliation(m),
    lambda s, m: s.get_mission_reconciliation_context(m),
], ids=['enter', 'resolve', 'cancel', 'get'])
def test_a_mission_that_does_not_exist_is_reported_by_every_operation(store, operation):
    with pytest.raises(MissionNotFound) as excinfo:
        operation(store, 'no-such-mission')

    assert excinfo.value.identifier == 'no-such-mission'


# F. Cancellation intent while reconciling

def test_a_cancellation_request_raises_only_the_context(store, db_path):
    mission_id = reconciling(store)
    before = dump(db_path)
    mission_before = mission_row(db_path, mission_id)

    record = store.request_cancellation_during_reconciliation(mission_id)

    assert record == MissionReconciliationContextRecord(mission_id, M.CANCELLING)
    after = dump(db_path)
    assert changed_tables(before, after) == {CONTEXT_TABLE}
    assert after[CONTEXT_TABLE] == [(mission_id, 'CANCELLING')]
    # The mission row is not written: same state, same time, same attempts.
    assert mission_row(db_path, mission_id) == mission_before
    assert store.get_by_mission_id(mission_id).state is M.NEEDS_RECONCILIATION
    assert store.get_mission_reconciliation_context(mission_id) == record


def test_a_second_request_is_an_exact_replay_and_writes_nothing(monkeypatch, db_path):
    store, connection = open_faulty_store(monkeypatch, db_path)
    with store:
        mission_id = reconciling(store)
        first = store.request_cancellation_during_reconciliation(mission_id)
        before = dump(db_path)
        connection.statements.clear()

        again = store.request_cancellation_during_reconciliation(mission_id)

        assert again == first
        assert dump(db_path) == before
        logged = [normalized(sql) for sql in connection.statements]
        assert writes(logged) == []
        assert logged == CANCEL_STATEMENTS[:3] + ['COMMIT']


def test_the_update_is_monotonic_in_sql_itself(store, db_path):
    assert normalized(storage._RAISE_RESUME_STATE) == CANCEL_STATEMENTS[3]
    mission_id = reconciling(store)

    with contextlib.closing(sqlite3.connect(db_path, isolation_level=None)) as conn:
        assert conn.execute(storage._RAISE_RESUME_STATE, (mission_id,)).rowcount == 1
        assert context_rows(db_path) == [(mission_id, 'CANCELLING')]
        # Run against a CANCELLING context it matches no row, so it cannot go back.
        assert conn.execute(storage._RAISE_RESUME_STATE, (mission_id,)).rowcount == 0
    assert context_rows(db_path) == [(mission_id, 'CANCELLING')]


def test_nothing_the_store_offers_can_move_a_cancelling_context_back_to_running(store, db_path):
    mission_id = reconciling(store, resume=M.CANCELLING)
    operations = [
        lambda: store.enter_mission_reconciliation(mission_id),
        lambda: store.request_cancellation_during_reconciliation(mission_id),
        lambda: store.get_mission_reconciliation_context(mission_id),
        lambda: store.resolve_mission_reconciliation(mission_id),
        lambda: store.transition_mission_state(mission_id, M.RUNNING),
        lambda: store.transition_mission_state(mission_id, M.NEEDS_RECONCILIATION),
        lambda: store.authorize_step_send(mission_id, 'b'),
    ]

    for operation in operations:
        try:
            operation()
        except Exception:
            pass
        assert context_rows(db_path) == [(mission_id, 'CANCELLING')]


def test_a_request_does_not_read_the_steps_or_wait_for_the_unknown_step_to_clear(
        monkeypatch, db_path):
    store, connection = open_faulty_store(monkeypatch, db_path)
    with store:
        mission_id = reconciling(store)
        connection.statements.clear()

        store.request_cancellation_during_reconciliation(mission_id)

        logged = [normalized(sql) for sql in connection.statements]
        assert logged == CANCEL_STATEMENTS
        assert not [sql for sql in logged if 'MISSION_STEPS' in sql]
        # The unknown step is still unknown, and resolution is still refused, whatever the intent.
        with pytest.raises(MissionReconciliationRequired):
            store.resolve_mission_reconciliation(mission_id)
        assert store.get_by_mission_id(mission_id).state is M.NEEDS_RECONCILIATION


def test_a_request_can_be_made_while_a_held_step_can_never_clear(store, db_path):
    mission_id = reconciling(store, unknown=(), held=('a',))

    record = store.request_cancellation_during_reconciliation(mission_id)

    assert record.resume_state is M.CANCELLING
    assert store.get_step(mission_id, 'a').state is S.OUTCOME_UNKNOWN
    assert store.get_by_mission_id(mission_id).state is M.NEEDS_RECONCILIATION
    with pytest.raises(MissionReconciliationRequired):
        store.resolve_mission_reconciliation(mission_id)


def test_a_request_changes_no_attempt_step_dispatch_authorization_or_evidence(store, db_path):
    mission_id = reconciling(store)
    settle(store, mission_id, 'a')
    before = dump(db_path)

    store.request_cancellation_during_reconciliation(mission_id)

    after = dump(db_path)
    for table in V6_TABLES:
        assert after[table] == before[table], table


@pytest.mark.parametrize('state', [M.ACCEPTED, M.RUNNING, M.CANCELLING, M.COMPLETED, M.FAILED,
                                   M.CANCELLED])
def test_a_request_from_a_mission_that_is_not_reconciling_is_refused(store, db_path, state):
    mission_id = mission_in(store, state)
    before = dump(db_path)

    with pytest.raises(MissionNotAwaitingReconciliation) as excinfo:
        store.request_cancellation_during_reconciliation(mission_id)

    assert excinfo.value.mission_state is state
    assert dump(db_path) == before


def test_a_request_that_could_not_raise_the_context_is_corruption_and_writes_nothing(
        store, db_path, monkeypatch):
    mission_id = reconciling(store)
    before = dump(db_path)
    monkeypatch.setattr(
        storage, '_RAISE_RESUME_STATE',
        'UPDATE mission_reconciliation_contexts SET resume_state = resume_state'
        ' WHERE mission_id = ? AND 0')

    with pytest.raises(CorruptMissionReconciliationContext) as excinfo:
        store.request_cancellation_during_reconciliation(mission_id)

    assert excinfo.value.value == 'RUNNING'
    assert dump(db_path) == before


def test_a_resolution_after_a_request_restores_cancelling_and_nothing_more(store, db_path):
    mission_id = reconciling(store)
    store.request_cancellation_during_reconciliation(mission_id)
    settle(store, mission_id, 'a')

    record = store.resolve_mission_reconciliation(mission_id)

    # It resumed CANCELLING, and that is all: nothing was cancelled, and the ordinary policy
    # decides the rest.
    assert record.state is M.CANCELLING
    assert [step.state for step in store.get_steps(mission_id)] == [
        S.SUCCEEDED, S.DISPATCH_PREPARED, S.DISPATCH_PREPARED]
    assert store.transition_mission_state(mission_id, M.CANCELLED).state is M.CANCELLED


# G. The reader, and shapes that are not a valid reconciliation

def test_the_reader_returns_the_context_of_a_reconciling_mission(store):
    mission_id = reconciling(store, resume=M.CANCELLING)

    context = store.get_mission_reconciliation_context(mission_id)

    assert context == MissionReconciliationContextRecord(mission_id, M.CANCELLING)
    assert store.get_mission_reconciliation_context(mission_id) == context


@pytest.mark.parametrize('state', [M.ACCEPTED, M.RUNNING, M.CANCELLING, M.COMPLETED, M.FAILED,
                                   M.CANCELLED])
def test_the_reader_says_that_a_mission_that_is_not_reconciling_is_not(store, db_path, state):
    mission_id = mission_in(store, state)
    before = dump(db_path)

    with pytest.raises(MissionNotAwaitingReconciliation) as excinfo:
        store.get_mission_reconciliation_context(mission_id)

    assert (excinfo.value.mission_id, excinfo.value.mission_state) == (mission_id, state)
    assert dump(db_path) == before


def test_the_reader_reads_in_one_deferred_snapshot_and_writes_nothing(monkeypatch, db_path):
    store, connection = open_faulty_store(monkeypatch, db_path)
    with store:
        mission_id = reconciling(store)
        before = dump(db_path)
        connection.statements.clear()

        first = store.get_mission_reconciliation_context(mission_id)
        second = store.get_mission_reconciliation_context(mission_id)

        assert first == second
        logged = [normalized(sql) for sql in connection.statements]
        # A deferred transaction, both reads in it, and only a release at the end.
        assert logged[:3] == ['BEGIN', 'SELECT * FROM MISSIONS WHERE MISSION_ID = ?',
                              'SELECT * FROM MISSION_RECONCILIATION_CONTEXTS WHERE MISSION_ID = ?']
        assert logged[3] == 'ROLLBACK'
        assert writes(logged) == []
        assert not connection.in_transaction
        assert dump(db_path) == before


def test_a_reader_never_sees_a_mission_and_a_context_that_contradict_each_other(db_path):
    keys = tuple(f's{n}' for n in range(8))
    with MissionStore(db_path) as writer:
        mission_id = new_mission(writer, keys=keys, max_attempts=len(keys))
    started, stop = threading.Event(), threading.Event()
    seen = []

    def read():
        with MissionStore(db_path) as reader:
            while not stop.is_set():
                try:
                    seen.append(reader.get_mission_reconciliation_context(mission_id).resume_state)
                except MissionNotAwaitingReconciliation:
                    seen.append(None)
                except Exception as error:  # anything else is a torn read
                    seen.append(error)
                started.set()

    thread = threading.Thread(target=read)
    thread.start()
    try:
        assert started.wait(timeout=TIMEOUT)
        with MissionStore(db_path) as writer:
            for key in keys:
                # The reader has looked at least once more since the last cycle, and goes on
                # looking while this one runs.
                target = len(seen) + 1
                deadline = time.monotonic() + TIMEOUT
                while len(seen) < target and time.monotonic() < deadline:
                    time.sleep(0.001)
                authorized_unknown(writer, mission_id, key)
                writer.enter_mission_reconciliation(mission_id)
                settle(writer, mission_id, key)
                writer.resolve_mission_reconciliation(mission_id)
    finally:
        stop.set()
        thread.join(timeout=TIMEOUT)

    # Every read, made while the mission went in and out of reconciliation eight times, found
    # either an active reconciliation or a mission that is not reconciling. Never a mission and a
    # context that disagree, which would be reported as corruption or as a missing context.
    assert len(seen) > len(keys)
    assert all(item is None or item is M.RUNNING for item in seen), [
        item for item in seen if not (item is None or item is M.RUNNING)][:3]


def test_a_mission_that_was_already_reconciling_before_version_7_fails_closed(tmp_path):
    path = tmp_path / 'v6.db'
    make_v6_database(path)
    with MissionStore(path) as store:
        before = dump(path)
        assert store.get_by_mission_id(V3_UNKNOWN_ID).state is M.NEEDS_RECONCILIATION
        operations = {
            'get': store.get_mission_reconciliation_context,
            'enter': store.enter_mission_reconciliation,
            'resolve': store.resolve_mission_reconciliation,
            'cancel': store.request_cancellation_during_reconciliation,
        }

        for name, operation in operations.items():
            with pytest.raises(MissionReconciliationContextMissing) as excinfo:
                operation(V3_UNKNOWN_ID)
            assert excinfo.value.mission_id == V3_UNKNOWN_ID, name
        # Its ordinary exits are as closed as they were, and the way in is still the new one.
        for target in (M.ACCEPTED, M.RUNNING, M.CANCELLING, M.COMPLETED, M.FAILED, M.CANCELLED):
            with pytest.raises(InvalidStateTransition):
                store.transition_mission_state(V3_UNKNOWN_ID, target)
        with pytest.raises(MissionReconciliationEntryRequired):
            store.transition_mission_state(V3_UNKNOWN_ID, M.NEEDS_RECONCILIATION)

        # Nothing was written, adopted, guessed or repaired.
        assert dump(path) == before
        assert context_rows(path) == []


def test_a_migrated_mission_that_is_not_reconciling_simply_has_no_context(tmp_path):
    path = tmp_path / 'v6.db'
    make_v6_database(path)
    with MissionStore(path) as store:
        assert store.get_by_mission_id(V3_MAIN_ID).state is M.RUNNING
        for operation in (store.get_mission_reconciliation_context,
                          store.resolve_mission_reconciliation,
                          store.request_cancellation_during_reconciliation):
            with pytest.raises(MissionNotAwaitingReconciliation) as excinfo:
                operation(V3_MAIN_ID)
            assert excinfo.value.mission_state is M.RUNNING
        # It has no unknown step, so it does not qualify to enter one either.
        with pytest.raises(NoUnknownStepToReconcile):
            store.enter_mission_reconciliation(V3_MAIN_ID)
        assert context_rows(path) == []


MALFORMED = ['COMPLETED', 'FAILED', 'ACCEPTED', 'NEEDS_RECONCILIATION', 'running', 'Running',
             ' RUNNING', 'RUNNING ', 'cancelling', 'CANCELLING\n', '', '5', 5, 5.5,
             b'RUNNING', b'\x00', b'CANCELLING']


@pytest.mark.parametrize('stored', MALFORMED)
def test_a_stored_state_that_is_not_exactly_one_of_the_two_is_corruption_and_is_never_repaired(
        store, db_path, stored):
    mission_id = reconciling(store)
    corrupt(db_path, f'UPDATE {CONTEXT_TABLE} SET resume_state = ?', stored)
    before = dump(db_path)
    read_back = context_rows(db_path)[0][1]

    for operation in (store.get_mission_reconciliation_context, store.enter_mission_reconciliation,
                      store.resolve_mission_reconciliation,
                      store.request_cancellation_during_reconciliation):
        with pytest.raises(CorruptMissionReconciliationContext) as excinfo:
            operation(mission_id)
        assert excinfo.value.mission_id == mission_id
        assert excinfo.value.value == read_back
        assert 'RUNNING or CANCELLING' in excinfo.value.reason

    assert dump(db_path) == before


@pytest.mark.parametrize('state', [M.ACCEPTED, M.RUNNING, M.CANCELLING, M.COMPLETED, M.FAILED,
                                   M.CANCELLED])
@pytest.mark.parametrize('stray', ['RUNNING', 'CANCELLING'])
def test_a_context_on_a_mission_that_is_not_reconciling_is_reported_by_every_operation(
        store, db_path, state, stray):
    mission_id = mission_in(store, state)
    raw(db_path, f'INSERT INTO {CONTEXT_TABLE} VALUES (?, ?)', mission_id, stray)
    before = dump(db_path)

    for operation in (store.get_mission_reconciliation_context, store.enter_mission_reconciliation,
                      store.resolve_mission_reconciliation,
                      store.request_cancellation_during_reconciliation):
        with pytest.raises(CorruptMissionReconciliationContext) as excinfo:
            operation(mission_id)
        assert excinfo.value.value == stray
        assert state.value in excinfo.value.reason

    assert dump(db_path) == before


def test_a_damaged_mission_is_reported_by_every_operation_with_the_error_that_already_exists(
        store, db_path):
    mission_id = reconciling(store)
    corrupt(db_path, "UPDATE missions SET mission_state = 'BOGUS'")
    before = dump(db_path)

    for operation in (store.get_mission_reconciliation_context, store.enter_mission_reconciliation,
                      store.resolve_mission_reconciliation,
                      store.request_cancellation_during_reconciliation):
        with pytest.raises(CorruptStoredState) as excinfo:
            operation(mission_id)
        assert (excinfo.value.entity, excinfo.value.value) == ('mission', 'BOGUS')

    assert dump(db_path) == before


def test_a_damaged_step_is_reported_by_the_operations_that_read_the_steps_and_only_by_them(
        store, db_path):
    running = new_mission(store)
    authorized_unknown(store, running)
    reconciling_id = reconciling(store, request_id='req-002')
    corrupt(db_path, "UPDATE mission_steps SET step_state = 'BOGUS' WHERE step_key = 'b'")
    before = dump(db_path)

    with pytest.raises(CorruptStoredState) as excinfo:
        store.enter_mission_reconciliation(running)
    assert (excinfo.value.entity, excinfo.value.value) == ('step', 'BOGUS')
    with pytest.raises(CorruptStoredState):
        store.resolve_mission_reconciliation(reconciling_id)
    # The reader and the cancellation request never look at a step, so they are not affected.
    assert store.get_mission_reconciliation_context(reconciling_id).resume_state is M.RUNNING
    assert store.request_cancellation_during_reconciliation(reconciling_id).resume_state is (
        M.CANCELLING)
    assert changed_tables(before, dump(db_path)) == {CONTEXT_TABLE}


def test_a_context_that_was_removed_is_reported_and_never_replaced(store, db_path):
    mission_id = reconciling(store, resume=M.CANCELLING)
    raw(db_path, f'DELETE FROM {CONTEXT_TABLE}')
    before = dump(db_path)

    for operation in (store.get_mission_reconciliation_context, store.enter_mission_reconciliation,
                      store.resolve_mission_reconciliation,
                      store.request_cancellation_during_reconciliation):
        with pytest.raises(MissionReconciliationContextMissing):
            operation(mission_id)

    assert dump(db_path) == before
    assert context_rows(db_path) == []
    assert store.get_by_mission_id(mission_id).state is M.NEEDS_RECONCILIATION


# H. Task 011 and Task 012

def test_evidence_settles_a_step_of_a_reconciling_mission_and_touches_neither_it_nor_its_context(
        store, db_path):
    mission_id = reconciling(store)
    before = dump(db_path)

    record = settle(store, mission_id, 'a')

    assert record.outcome.name == 'APPLIED'
    after = dump(db_path)
    assert changed_tables(before, after) == {'mission_steps', 'recovery_evidence'}
    assert after['missions'] == before['missions']
    assert after[CONTEXT_TABLE] == before[CONTEXT_TABLE] == [(mission_id, 'RUNNING')]
    # Task 011 does not move the mission. Task 012 does, later, from the step states.
    assert store.get_by_mission_id(mission_id).state is M.NEEDS_RECONCILIATION
    assert store.resolve_mission_reconciliation(mission_id).state is M.RUNNING


def test_a_contradicting_result_on_a_finished_step_does_not_block_a_resolution(store, db_path):
    mission_id = new_mission(store, keys=('a', 'b'))
    store.authorize_step_send(mission_id, 'b')
    store.transition_step_state(mission_id, 'b', S.SUCCEEDED)
    authorized_unknown(store, mission_id, 'a')
    conflict = settle(store, mission_id, 'b', status=ABO)
    assert conflict.outcome.name == 'CONFLICT'
    store.enter_mission_reconciliation(mission_id)
    settle(store, mission_id, 'a')
    evidence_before = dump(db_path)['recovery_evidence']

    record = store.resolve_mission_reconciliation(mission_id)

    # The row is immutable and has no way to be cleared, so it cannot be a blocker.
    assert record.state is M.RUNNING
    assert dump(db_path)['recovery_evidence'] == evidence_before
    assert [row[8] for row in evidence_before].count('CONFLICT') == 1


def test_evidence_that_was_not_applied_to_a_step_that_is_not_unknown_does_not_block_either(
        store, db_path):
    mission_id = new_mission(store, keys=('a', 'b', 'c'))
    store.authorize_step_send(mission_id, 'c')
    # What a step migrated from version 4 looks like: in flight, with no authorization row.
    raw(db_path, 'DELETE FROM dispatch_authorizations WHERE dispatch_id = ?',
        str(store.get_step_dispatch(mission_id, 'c').dispatch_id))
    unproven = settle(store, mission_id, 'c')
    assert unproven.outcome.name == 'UNPROVEN_AUTHORIZATION'
    assert store.get_step(mission_id, 'c').state is S.DISPATCHED
    authorized_unknown(store, mission_id, 'a')
    store.enter_mission_reconciliation(mission_id)
    settle(store, mission_id, 'a')

    record = store.resolve_mission_reconciliation(mission_id)

    assert record.state is M.RUNNING
    assert store.get_step(mission_id, 'c').state is S.DISPATCHED


# I. Atomicity: a failure at any statement leaves nothing half done

def situation(kind, db_path):
    """Build the state in which the named operation has something to do, and return the mission."""
    with MissionStore(db_path) as store:
        if kind == 'enter':
            mission_id = new_mission(store)
            authorized_unknown(store, mission_id)
        elif kind == 'resolve':
            mission_id = reconciling(store)
            settle(store, mission_id, 'a')
        else:
            mission_id = reconciling(store)
    return mission_id


OPERATIONS = {
    'enter': lambda s, m: s.enter_mission_reconciliation(m),
    'resolve': lambda s, m: s.resolve_mission_reconciliation(m),
    'cancel': lambda s, m: s.request_cancellation_during_reconciliation(m),
}
STATEMENTS = {'enter': ENTRY_STATEMENTS, 'resolve': RESOLVE_STATEMENTS,
              'cancel': CANCEL_STATEMENTS}
# What the mission and the context look like once the operation has committed.
FINAL = {
    'enter': (M.NEEDS_RECONCILIATION, ['RUNNING']),
    'resolve': (M.RUNNING, []),
    'cancel': (M.NEEDS_RECONCILIATION, ['CANCELLING']),
}
FAULT_POINTS = [(kind, index) for kind in STATEMENTS for index in range(len(STATEMENTS[kind]))]


def assert_final(store, db_path, mission_id, kind):
    state, contexts = FINAL[kind]
    assert store.get_by_mission_id(mission_id).state is state
    assert [resume for (_, resume) in context_rows(db_path)] == contexts


@pytest.mark.parametrize(('kind', 'index'), FAULT_POINTS,
                         ids=[f'{kind}-{index}' for kind, index in FAULT_POINTS])
def test_a_failure_at_any_statement_leaves_every_table_as_it_was_and_a_retry_succeeds(
        monkeypatch, db_path, kind, index):
    mission_id = situation(kind, db_path)
    store, connection = open_faulty_store(monkeypatch, db_path)
    with store:
        before = dump(db_path)
        seen = []

        def fails(sql):
            seen.append(sql)
            return len(seen) == index + 1

        connection.statements.clear()
        connection.fail_when = fails

        with pytest.raises(sqlite3.OperationalError, match='injected failure'):
            OPERATIONS[kind](store, mission_id)

        connection.fail_when = None
        logged = [normalized(sql) for sql in connection.statements]
        # The statement that failed is the one this case names, and every write before it did
        # run: it is the rollback that leaves nothing behind.
        assert logged[index] == STATEMENTS[kind][index]
        assert writes(logged[:index]) == writes(STATEMENTS[kind][:index])
        assert not connection.in_transaction
        assert dump(db_path) == before

        # The store still works: the same call now does all of it.
        OPERATIONS[kind](store, mission_id)
        assert_final(store, db_path, mission_id, kind)
        assert store.get_by_mission_id(mission_id).attempts_used == 1


@pytest.mark.parametrize('kind', list(STATEMENTS))
def test_an_error_after_a_real_commit_returns_no_record_and_the_retry_is_the_designed_one(
        monkeypatch, db_path, kind):
    mission_id = situation(kind, db_path)
    store, connection = open_faulty_store(monkeypatch, db_path)
    with store:
        def commit_then_fail(sql):
            if normalized(sql) == 'COMMIT':
                connection._real.execute('COMMIT')      # the commit really happens
                return True
            return False

        connection.fail_when = commit_then_fail
        with pytest.raises(sqlite3.OperationalError, match='injected failure'):
            OPERATIONS[kind](store, mission_id)
        connection.fail_when = None

        # The caller got an error and no record, and the whole of it is durable.
        assert_final(store, db_path, mission_id, kind)
        before = dump(db_path)
        if kind == 'resolve':
            # A state-based operation on the current reconciliation: it is over, so it is refused.
            with pytest.raises(MissionNotAwaitingReconciliation) as excinfo:
                OPERATIONS[kind](store, mission_id)
            assert excinfo.value.mission_state is M.RUNNING
        else:
            # Entry and the cancellation request are exact replays, and change nothing.
            replay = OPERATIONS[kind](store, mission_id)
            assert replay == MissionReconciliationContextRecord(
                mission_id, M.CANCELLING if kind == 'cancel' else M.RUNNING)
        assert dump(db_path) == before


# J. Real processes are killed at chosen statements of the real code

_CHILD = """
import os
import signal
import sqlite3
import sys

from task_resilience import storage
from task_resilience.storage import MissionStore

db_path, mission_id, kind, marker, when, how = sys.argv[1:7]
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
operations = {
    'enter': store.enter_mission_reconciliation,
    'resolve': store.resolve_mission_reconciliation,
    'cancel': store.request_cancellation_during_reconciliation,
}
operations[kind](mission_id)
print('returned', flush=True)
die()
"""

EXIT_CODE = {'exit': 0, 'sigkill': -signal.SIGKILL}
# The statement that writes first, and the one that writes the mission: the two things that must
# never be seen apart.
WRITING = {
    'enter': ('INSERT INTO MISSION_RECONCILIATION_CONTEXTS', 'UPDATE MISSIONS'),
    'resolve': ('DELETE FROM MISSION_RECONCILIATION_CONTEXTS', 'UPDATE MISSIONS'),
    'cancel': ('UPDATE MISSION_RECONCILIATION_CONTEXTS', None),
}


def kill_points(kind):
    """Return (marker, when, how, committed): before COMMIT nothing is durable, at it all is."""
    first, second = WRITING[kind]
    points = [('BEGIN IMMEDIATE', 'before', 'exit', False),
              ('BEGIN IMMEDIATE', 'after', 'exit', False),         # the write lock held
              (first, 'before', 'exit', False),
              (first, 'after', 'exit', False),
              (first, 'after', 'sigkill', False)]
    if second:
        points += [(second, 'before', 'exit', False),
                   (second, 'after', 'exit', False),               # both written, not committed
                   (second, 'after', 'sigkill', False)]
    return points + [('COMMIT', 'before', 'exit', False),
                     ('COMMIT', 'before', 'sigkill', False),
                     ('COMMIT', 'after', 'exit', True),
                     ('COMMIT', 'after', 'sigkill', True),
                     ('-', '-', 'exit', True),                    # after the call returned
                     ('-', '-', 'sigkill', True)]


KILLS = [(kind, *point) for kind in STATEMENTS for point in kill_points(kind)]


@pytest.mark.parametrize(('kind', 'marker', 'when', 'how', 'committed'), KILLS)
def test_a_process_killed_around_the_write_leaves_all_of_it_or_none_of_it(
        db_path, kind, marker, when, how, committed):
    mission_id = situation(kind, db_path)
    before = dump(db_path)

    child = subprocess.run(
        [sys.executable, '-c', _CHILD, str(db_path), mission_id, kind, marker, when, how],
        env=package_env(), capture_output=True, text=True, timeout=60)

    assert child.returncode == EXIT_CODE[how], child.stderr
    with MissionStore(db_path) as reopened:
        if committed:
            assert_final(reopened, db_path, mission_id, kind)
        else:
            # SQLite rolled the unfinished transaction back on opening: not a cell different.
            assert dump(db_path) == before
            assert child.stdout == ''
        # No attempt was taken or given back and no authorization appeared, whichever it was.
        assert reopened.get_by_mission_id(mission_id).attempts_used == 1
        assert raw(db_path, 'SELECT COUNT(*) FROM dispatch_authorizations') == [(1,)]
        # And the caller's retry does what was designed.
        if kind == 'resolve' and committed:
            with pytest.raises(MissionNotAwaitingReconciliation):
                OPERATIONS[kind](reopened, mission_id)
        else:
            OPERATIONS[kind](reopened, mission_id)
        assert_final(reopened, db_path, mission_id, kind)


# K. Concurrency. Each worker opens its own store, as separate callers would

def brief(value):
    if isinstance(value, MissionReconciliationContextRecord):
        return f'context({value.resume_state.name})'
    if isinstance(value, MissionRecord):
        return f'mission({value.state.name})'
    if isinstance(value, Exception):
        return type(value).__name__
    return str(value)


def run_worker(path, worker, mission_id):
    with MissionStore(path) as store:
        try:
            return brief(worker(store, mission_id))
        except Exception as error:
            # The loser of a race is a result too, not a failure of the harness.
            return brief(error)


def history_of(path, mission_id, results):
    with MissionStore(path) as store:
        mission = store.get_by_mission_id(mission_id)
    contexts = tuple(resume for (_, resume) in context_rows(path))
    return (tuple(results), mission.state.name, contexts, mission.attempts_used)


def run_serially(path, setup, workers, order):
    with MissionStore(path) as store:
        mission_id = setup(store)
    results = {index: run_worker(path, workers[index], mission_id) for index in order}
    return history_of(path, mission_id, [results[index] for index in range(len(workers))])


def run_threaded(path, setup, workers):
    with MissionStore(path) as store:
        mission_id = setup(store)
    barrier = threading.Barrier(len(workers), timeout=TIMEOUT)

    def released(worker):
        barrier.wait()
        # Vary which worker gets in first, so that both orders are tried.
        time.sleep(random.random() * 0.004)
        return run_worker(path, worker, mission_id)

    with ThreadPoolExecutor(max_workers=len(workers)) as pool:
        futures = [pool.submit(released, worker) for worker in workers]
        results = [future.result(timeout=TIMEOUT * 6) for future in futures]
    return history_of(path, mission_id, results)


def unknown_and_running(store):
    mission_id = new_mission(store)
    authorized_unknown(store, mission_id)
    return mission_id


def entered(store):
    return reconciling(store)


def settled_and_entered(store):
    mission_id = reconciling(store)
    settle(store, mission_id, 'a')
    return mission_id


def enter(store, mission_id):
    return store.enter_mission_reconciliation(mission_id)


def resolve(store, mission_id):
    return store.resolve_mission_reconciliation(mission_id)


def cancel_intent(store, mission_id):
    return store.request_cancellation_during_reconciliation(mission_id)


def to_cancelling(store, mission_id):
    return store.transition_mission_state(mission_id, M.CANCELLING)


def to_failed(store, mission_id):
    return store.transition_mission_state(mission_id, M.FAILED)


def settle_a(store, mission_id):
    return f'evidence {settle(store, mission_id, "a", evidence_id=uuid.UUID(int=7)).outcome.name}'


def b_unknown(store, mission_id):
    return store.transition_step_state(mission_id, 'b', S.OUTCOME_UNKNOWN).state.name


def authorize_b(store, mission_id):
    store.authorize_step_send(mission_id, 'b')
    return 'SendAuthorization'


def delete_the_context(store, mission_id):
    # Something outside the supported code removes the context, on its own connection: the file
    # is found from the store's connection, because a worker is given nothing but the store.
    raw(store._conn.execute('PRAGMA database_list').fetchone()[2],
        f'DELETE FROM {CONTEXT_TABLE} WHERE mission_id = ?', mission_id)
    return 'context deleted'


NEEDS = 'NEEDS_RECONCILIATION'
# Each race: how to set it up, who races, and every history it may end in. A history is the result
# of each worker, in worker order, the mission state, the contexts, and the attempts used.
RACES = {
    'two entries': (unknown_and_running, [enter, enter], {
        (('context(RUNNING)', 'context(RUNNING)'), NEEDS, ('RUNNING',), 1)}),
    'entry against a plain cancellation': (unknown_and_running, [enter, to_cancelling], {
        # The cancellation came first, so the entry resumes CANCELLING.
        (('context(CANCELLING)', 'mission(CANCELLING)'), NEEDS, ('CANCELLING',), 1),
        # The entry came first: the plain request is refused, and can use the intent operation.
        (('context(RUNNING)', 'InvalidStateTransition'), NEEDS, ('RUNNING',), 1)}),
    'entry against the last unknown step being settled': (unknown_and_running, [enter, settle_a], {
        (('NoUnknownStepToReconcile', 'evidence APPLIED'), 'RUNNING', (), 1),
        (('context(RUNNING)', 'evidence APPLIED'), NEEDS, ('RUNNING',), 1)}),
    'resolution against the last unknown step being settled': (entered, [resolve, settle_a], {
        (('MissionReconciliationRequired', 'evidence APPLIED'), NEEDS, ('RUNNING',), 1),
        (('mission(RUNNING)', 'evidence APPLIED'), 'RUNNING', (), 1)}),
    # Step b is only prepared, so it can still become unknown, or be authorized.
    'resolution against another step becoming unknown': (
        settled_and_entered, [resolve, b_unknown], {
            (('MissionReconciliationRequired', 'OUTCOME_UNKNOWN'), NEEDS, ('RUNNING',), 1),
            # Allowed: RUNNING with an unknown step, which the step-level gate then blocks.
            (('mission(RUNNING)', 'OUTCOME_UNKNOWN'), 'RUNNING', (), 1)}),
    'resolution against a cancellation request': (settled_and_entered, [resolve, cancel_intent], {
        (('mission(CANCELLING)', 'context(CANCELLING)'), 'CANCELLING', (), 1),
        (('mission(RUNNING)', 'MissionNotAwaitingReconciliation'), 'RUNNING', (), 1)}),
    'two resolutions': (settled_and_entered, [resolve, resolve], {
        (('mission(RUNNING)', 'MissionNotAwaitingReconciliation'), 'RUNNING', (), 1),
        (('MissionNotAwaitingReconciliation', 'mission(RUNNING)'), 'RUNNING', (), 1)}),
    'resolution against a send authorization': (settled_and_entered, [resolve, authorize_b], {
        (('mission(RUNNING)', 'MissionNotRunning'), 'RUNNING', (), 1),
        (('mission(RUNNING)', 'SendAuthorization'), 'RUNNING', (), 2)}),
    'entry against a send authorization': (unknown_and_running, [enter, authorize_b], {
        # Either way the send is refused: by the mission state, or by the unknown step.
        (('context(RUNNING)', 'MissionNotRunning'), NEEDS, ('RUNNING',), 1),
        (('context(RUNNING)', 'MissionReconciliationRequired'), NEEDS, ('RUNNING',), 1)}),
    'resolution against a plain failure': (settled_and_entered, [resolve, to_failed], {
        (('mission(RUNNING)', 'InvalidStateTransition'), 'RUNNING', (), 1),
        (('mission(RUNNING)', 'mission(FAILED)'), 'FAILED', (), 1)}),
    'resolution against the context being removed': (settled_and_entered, [
        resolve, delete_the_context], {
            (('mission(RUNNING)', 'context deleted'), 'RUNNING', (), 1),
            # Nothing is guessed: the mission stays blocked, and this is the shape of a mission
            # from before version 7.
            (('MissionReconciliationContextMissing', 'context deleted'), NEEDS, (), 1)}),
    'two cancellation requests': (entered, [cancel_intent, cancel_intent], {
        (('context(CANCELLING)', 'context(CANCELLING)'), NEEDS, ('CANCELLING',), 1)}),
}


@pytest.mark.parametrize('name', list(RACES))
def test_every_order_of_the_race_gives_one_of_the_allowed_histories_and_both_are_reachable(
        tmp_path, name):
    setup, workers, allowed = RACES[name]
    observed = set()

    for number, order in enumerate([range(len(workers)), reversed(range(len(workers)))]):
        observed.add(run_serially(tmp_path / f'{number}.db', setup, workers, list(order)))

    # Written by hand, so an outcome nobody thought of cannot creep into the allowed set: every
    # allowed history is what one serial order produces, and each order produces one of them.
    assert observed == allowed


@pytest.mark.parametrize('name', list(RACES))
def test_threads_racing_each_other_only_ever_produce_an_allowed_history(tmp_path, name):
    setup, workers, allowed = RACES[name]

    for round_index in range(ROUNDS):
        history = run_threaded(tmp_path / f'{round_index}.db', setup, workers)
        assert history in allowed, f'round {round_index}: {history}'


def test_a_resolution_that_wins_leaves_a_step_that_became_unknown_to_the_step_level_gate(
        tmp_path):
    setup, workers, _ = RACES['resolution against another step becoming unknown']
    path = tmp_path / 'race.db'

    history = run_serially(path, setup, workers, [0, 1])

    assert history[1] == 'RUNNING'
    with MissionStore(path) as store:
        mission_id = store.get_by_request_id('req-001').mission_id
        # Task 008's gate, not this task, refuses the send, and a new reconciliation may begin.
        with pytest.raises(MissionReconciliationRequired) as excinfo:
            store.authorize_step_send(mission_id, 'c')
        assert excinfo.value.step_keys == ('b',)
        assert store.enter_mission_reconciliation(mission_id).resume_state is M.RUNNING


# L. What the code is, and what it must not be

PRODUCTION = {path.name: ast.parse(path.read_text())
              for path in sorted(Path(task_resilience.__file__).resolve().parent.glob('*.py'))}
RECONCILIATION_OPERATIONS = (
    'enter_mission_reconciliation', 'resolve_mission_reconciliation',
    'request_cancellation_during_reconciliation', 'get_mission_reconciliation_context')


def store_methods():
    [store_class] = [node for node in PRODUCTION['storage.py'].body
                     if isinstance(node, ast.ClassDef) and node.name == 'MissionStore']
    return {node.name: node for node in store_class.body if isinstance(node, ast.FunctionDef)}


def module_functions():
    return {node.name: node for node in PRODUCTION['storage.py'].body
            if isinstance(node, ast.FunctionDef)}


def names_in(node):
    return {child.id for child in ast.walk(node) if isinstance(child, ast.Name)}


def attribute_names_in(node):
    return {child.attr for child in ast.walk(node) if isinstance(child, ast.Attribute)}


def string_constants(tree):
    return [child.value for child in ast.walk(tree)
            if isinstance(child, ast.Constant) and isinstance(child.value, str)]


def executed(method):
    """Return the statement constants a method passes to self._conn.execute, in source order."""
    calls = [node for node in ast.walk(method) if isinstance(node, ast.Call)
             and isinstance(node.func, ast.Attribute) and node.func.attr == 'execute'
             and node.args and isinstance(node.args[0], ast.Name)]
    return [call.args[0].id for call in sorted(calls, key=lambda call: call.lineno)]


def test_each_writing_statement_has_exactly_one_writer():
    writers = {}
    for constant in ('_INSERT_RECONCILIATION_CONTEXT', '_RAISE_RESUME_STATE',
                     '_DELETE_RECONCILIATION_CONTEXT'):
        writers[constant] = (
            {name for name, method in store_methods().items() if constant in names_in(method)}
            | {name for name, function in module_functions().items()
               if constant in names_in(function)})

    assert writers == {
        '_INSERT_RECONCILIATION_CONTEXT': {'enter_mission_reconciliation'},
        '_RAISE_RESUME_STATE': {'request_cancellation_during_reconciliation'},
        '_DELETE_RECONCILIATION_CONTEXT': {'resolve_mission_reconciliation'},
    }


def test_the_statements_that_name_the_context_table_are_the_create_and_four_others():
    naming = {}
    for filename, tree in PRODUCTION.items():
        for text in string_constants(tree):
            if CONTEXT_TABLE in text.lower():
                naming.setdefault(filename, []).append(' '.join(text.split()).upper())

    assert set(naming) == {'storage.py'}
    statements = naming['storage.py']
    assert sorted(sql.split()[0] for sql in statements) == [
        'CREATE', 'DELETE', 'INSERT', 'SELECT', 'UPDATE']
    for sql in statements:
        # Nothing replaces, upserts or repairs: only the four statements of the writer contract.
        for word in ('REPLACE', 'UPSERT', 'ON CONFLICT', 'OR IGNORE', 'OR REPLACE'):
            assert word not in sql, (word, sql)
    update = [sql for sql in statements if sql.startswith('UPDATE')][0]
    assert update.split(' WHERE ')[0].endswith("SET RESUME_STATE = 'CANCELLING'")
    assert "RESUME_STATE = 'RUNNING'" in update.split(' WHERE ')[1]


def test_the_only_state_the_store_ever_writes_into_a_context_besides_the_first_is_cancelling():
    # The first value is the mission's own state at entry, taken from the mission and never
    # spelled out, and the only literal in any UPDATE of the table is CANCELLING.
    setters = [text for text in string_constants(PRODUCTION['storage.py'])
               if 'UPDATE MISSION_RECONCILIATION_CONTEXTS' in ' '.join(text.split()).upper()]
    assert len(setters) == 1
    assert "= 'RUNNING'" not in ' '.join(setters[0].split()).upper().split(' WHERE ')[0]
    enter = store_methods()['enter_mission_reconciliation']
    inserted = [node for node in ast.walk(enter) if isinstance(node, ast.Call)
                and node.args and isinstance(node.args[0], ast.Name)
                and node.args[0].id == '_INSERT_RECONCILIATION_CONTEXT']
    assert [ast.unparse(arg) for arg in inserted[0].args[1].elts] == [
        'mission_id', 'mission.state.value']


def test_the_plain_transition_is_refused_before_it_opens_a_transaction():
    method = store_methods()['transition_mission_state']
    statements = method.body
    # The type check, then the refusal, then the transaction.
    assert isinstance(statements[0], ast.Expr) and 'require_kind' in ast.unparse(statements[0])
    refusal = statements[1]
    assert isinstance(refusal, ast.If)
    assert ast.unparse(refusal.test) == 'target is MissionState.NEEDS_RECONCILIATION'
    assert ast.unparse(refusal.body[0]) == 'raise MissionReconciliationEntryRequired(mission_id)'
    assert isinstance(statements[2], ast.With)


def test_entry_asks_the_pure_graph_and_then_checks_the_reason():
    enter = store_methods()['enter_mission_reconciliation']
    names = names_in(enter)

    assert {'transition_mission', 'requires_reconciliation', 'NoUnknownStepToReconcile'} <= names
    assert executed(enter) == [
        '_INSERT_RECONCILIATION_CONTEXT', '_SET_MISSION_STATE']
    source = ast.unparse(enter)
    assert (source.index('transition_mission(') < source.index('requires_reconciliation(')
            < source.index('_INSERT_RECONCILIATION_CONTEXT'))


def test_resolution_takes_its_target_from_the_context_and_never_asks_the_graph():
    resolve = store_methods()['resolve_mission_reconciliation']
    names = names_in(resolve)

    assert {'requires_reconciliation', 'MissionReconciliationRequired'} <= names
    # Neither the graph nor a state name of its own: every state it writes comes from the context.
    assert 'transition_mission' not in names and 'MissionState' not in names
    assert executed(resolve) == ['_DELETE_RECONCILIATION_CONTEXT', '_SET_MISSION_STATE']
    set_state = [node for node in ast.walk(resolve) if isinstance(node, ast.Call)
                 and node.args and isinstance(node.args[0], ast.Name)
                 and node.args[0].id == '_SET_MISSION_STATE'][0]
    assert ast.unparse(set_state.args[1].elts[0]) == 'context.resume_state.value'
    # And the context is read before anything is decided, and deleted before the mission moves.
    source = ast.unparse(resolve)
    assert (source.index('_require_reconciling') < source.index('requires_reconciliation(')
            < source.index('_DELETE_RECONCILIATION_CONTEXT') < source.index('_SET_MISSION_STATE'))


def test_the_cancellation_request_changes_only_the_context():
    request = store_methods()['request_cancellation_during_reconciliation']

    assert executed(request) == ['_RAISE_RESUME_STATE']
    assert not names_in(request) & {'_SET_MISSION_STATE', '_fetch_steps', '_SET_STEP_STATE'}


def test_no_reconciliation_operation_touches_evidence_dispatches_authorizations_or_attempts():
    forbidden = {
        '_CONSUME', '_INSERT', '_INSERT_STEP', '_INSERT_DISPATCH', '_INSERT_CONTEXT',
        '_INSERT_AUTHORIZATION', '_INSERT_EVIDENCE', '_EVIDENCE_BY_ID', '_EVIDENCE_OF_DISPATCH',
        '_AUTHORIZATION_OF_DISPATCH', '_CONTEXT_OF_DISPATCH', '_DISPATCHES_OF_STEP',
        '_DISPATCH_BY_ID', '_SET_STEP_STATE', 'SendAuthorization', 'RecoveryEvidenceRecord',
        'EvidenceOutcome', 'decide_reconciliation', 'transition_step', '_fetch_dispatch',
        '_require_dispatch', '_fetch_context', '_require_context', '_fetch_authorization',
        '_require_authorization', '_authorization_of', 'apply_recovery_observation',
        'authorize_step_send', 'get_recovery_evidence'}
    methods = store_methods()
    involved = [methods[name] for name in RECONCILIATION_OPERATIONS] + [
        methods[name] for name in ('_fetch_reconciliation_context',
                                   '_require_reconciliation_context', '_require_reconciling')]

    for method in involved:
        assert not (names_in(method) | attribute_names_in(method)) & forbidden, method.name


def test_only_authorize_step_send_consumes_an_attempt():
    users = {name for name, method in store_methods().items() if '_CONSUME' in names_in(method)}

    assert users == {'authorize_step_send'}


def test_task_011_neither_reads_nor_writes_a_reconciliation_context():
    methods = store_methods()
    for name in ('apply_recovery_observation', 'get_recovery_evidence', '_replayed_evidence'):
        touched = names_in(methods[name]) | attribute_names_in(methods[name])
        assert not touched & {
            '_RECONCILIATION_CONTEXT_OF_MISSION', '_INSERT_RECONCILIATION_CONTEXT',
            '_RAISE_RESUME_STATE', '_DELETE_RECONCILIATION_CONTEXT', '_require_reconciling',
            '_fetch_reconciliation_context', 'MissionReconciliationContextRecord'}, name
        assert 'transition_mission' not in touched and '_SET_MISSION_STATE' not in touched


def test_only_the_reconciliation_operations_write_the_mission_state_besides_the_ordinary_one():
    users = {name for name, method in store_methods().items()
             if '_SET_MISSION_STATE' in names_in(method)}

    assert users == {'transition_mission_state', 'enter_mission_reconciliation',
                     'resolve_mission_reconciliation'}


def test_the_state_machine_and_the_decision_module_are_what_they_were():
    graph = {state: set(targets) for state, targets in state_machine._MISSION_TRANSITIONS.items()}
    assert graph == {
        M.ACCEPTED: {M.RUNNING, M.CANCELLED},
        M.RUNNING: {M.CANCELLING, M.COMPLETED, M.FAILED, M.NEEDS_RECONCILIATION},
        M.CANCELLING: {M.CANCELLED, M.COMPLETED, M.FAILED, M.NEEDS_RECONCILIATION},
        M.COMPLETED: set(), M.FAILED: set(), M.CANCELLED: set(), M.NEEDS_RECONCILIATION: set(),
    }
    assert state_machine.requires_reconciliation([S.SUCCEEDED, S.OUTCOME_UNKNOWN]) is True
    assert state_machine.requires_reconciliation([S.SUCCEEDED, S.FAILED]) is False
    for module in (state_machine, reconciliation):
        text = Path(module.__file__).read_text()
        for word in ('resume', 'reconciliation_context', 'enter_mission_reconciliation',
                     'resolve_mission_reconciliation', 'MissionReconciliationContextRecord',
                     'mission_reconciliation'):
            assert word not in text, (module.__name__, word)
    imported = {(node.level, node.module) for node in ast.walk(PRODUCTION['reconciliation.py'])
                if isinstance(node, ast.ImportFrom)}
    assert imported == {(0, 'dataclasses'), (1, 'state_machine')}


def test_errors_do_not_import_the_state_machine_or_anything_that_does():
    imported = {node.module for node in ast.walk(PRODUCTION['errors.py'])
                if isinstance(node, ast.ImportFrom)}
    plain = {alias.name for node in ast.walk(PRODUCTION['errors.py'])
             if isinstance(node, ast.Import) for alias in node.names}

    assert imported == set() and plain == {'enum', 'uuid'}


def test_the_production_modules_import_no_ros_network_process_or_time_package():
    allowed = {'collections', 'contextlib', 'dataclasses', 'datetime', 'enum', 'hashlib', 'json',
               'math', 'os', 'sqlite3', 'typing', 'uuid'}
    for filename, tree in PRODUCTION.items():
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                found = {alias.name.split('.')[0] for alias in node.names}
            elif isinstance(node, ast.ImportFrom) and node.level == 0:
                found = {node.module.split('.')[0]}
            else:
                continue
            assert found <= allowed, (filename, found - allowed)


def test_a_whole_reconciliation_does_no_network_process_or_ros_work(store, monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError('external work during a reconciliation')

    with monkeypatch.context() as patch:
        patch.setattr(socket, 'socket', forbidden)
        patch.setattr(subprocess, 'Popen', forbidden)
        patch.setattr(os, 'system', forbidden)

        mission_id = reconciling(store)
        store.request_cancellation_during_reconciliation(mission_id)
        store.get_mission_reconciliation_context(mission_id)
        settle(store, mission_id, 'a')
        store.resolve_mission_reconciliation(mission_id)


# M. The record and the errors

def test_the_record_carries_the_mission_and_the_state_to_resume_and_nothing_else():
    record = MissionReconciliationContextRecord('m-1', M.CANCELLING)

    assert [field.name for field in dataclasses.fields(MissionReconciliationContextRecord)] == [
        'mission_id', 'resume_state']
    assert (record.mission_id, record.resume_state) == ('m-1', M.CANCELLING)
    with pytest.raises(dataclasses.FrozenInstanceError):
        record.resume_state = M.RUNNING
    assert record == MissionReconciliationContextRecord('m-1', M.CANCELLING)
    assert record != MissionReconciliationContextRecord('m-1', M.RUNNING)


@pytest.mark.parametrize('resume', [M.RUNNING, M.CANCELLING])
def test_the_two_states_to_resume_build(resume):
    assert MissionReconciliationContextRecord('m-1', resume).resume_state is resume


@pytest.mark.parametrize('bad', [5, None, b'm-1', ['m-1'], uuid.uuid4()])
def test_a_mission_id_of_the_wrong_type_is_a_type_error(bad):
    with pytest.raises(TypeError):
        MissionReconciliationContextRecord(bad, M.RUNNING)


def test_an_empty_mission_id_is_a_value_error_and_not_a_type_error():
    with pytest.raises(ValueError) as excinfo:
        MissionReconciliationContextRecord('', M.RUNNING)

    assert not isinstance(excinfo.value, TypeError)


@pytest.mark.parametrize('bad', ['RUNNING', None, S.PENDING, 1, 'CANCELLING'])
def test_a_state_of_the_wrong_type_is_a_type_error(bad):
    with pytest.raises(TypeError):
        MissionReconciliationContextRecord('m-1', bad)


@pytest.mark.parametrize('state', [M.ACCEPTED, M.COMPLETED, M.FAILED, M.CANCELLED,
                                   M.NEEDS_RECONCILIATION])
def test_any_other_state_is_a_value_error_and_not_a_type_error(state):
    with pytest.raises(ValueError) as excinfo:
        MissionReconciliationContextRecord('m-1', state)

    assert not isinstance(excinfo.value, TypeError)


def test_the_new_errors_carry_their_facts_and_are_distinct():
    entry = MissionReconciliationEntryRequired('m-1')
    nothing = NoUnknownStepToReconcile('m-1')
    not_awaiting = MissionNotAwaitingReconciliation('m-1', M.RUNNING)
    missing = MissionReconciliationContextMissing('m-1')
    damaged = CorruptMissionReconciliationContext('m-1', 'why', 'value')
    bare = CorruptMissionReconciliationContext('m-1', 'why')

    for error in (entry, nothing, not_awaiting, missing, damaged):
        assert error.mission_id == 'm-1' and "'m-1'" in str(error)
    assert not_awaiting.mission_state is M.RUNNING and 'RUNNING' in str(not_awaiting)
    assert (damaged.reason, damaged.value, bare.value) == ('why', 'value', None)
    assert 'enter_mission_reconciliation' in str(entry)
    errors = [entry, nothing, not_awaiting, missing, damaged]
    for one in errors:
        assert isinstance(one, Exception) and not isinstance(one, sqlite3.Error)
        assert len([other for other in errors if isinstance(one, type(other))]) == 1


def test_the_error_for_a_step_that_is_still_unknown_is_the_one_that_already_existed():
    assert MissionReconciliationRequired.__module__ == 'task_resilience.errors'
    assert not hasattr(sys.modules['task_resilience.errors'], 'MissionReconciliationStillRequired')
    assert not hasattr(model, 'entered_at')
    assert not [name for name in dir(MissionStore) if 'entered_at' in name.lower()]


def test_resolution_and_the_send_gate_name_the_same_unknown_steps_with_the_same_error(store):
    mission_id = new_mission(store, keys=('a', 'b', 'c'))
    store.authorize_step_send(mission_id, 'a')
    store.authorize_step_send(mission_id, 'c')
    for key in ('a', 'c'):
        store.transition_step_state(mission_id, key, S.OUTCOME_UNKNOWN)
    # Before the mission has entered reconciliation, the step-level gate refuses the send.
    with pytest.raises(MissionReconciliationRequired) as from_the_gate:
        store.authorize_step_send(mission_id, 'b')
    store.enter_mission_reconciliation(mission_id)

    with pytest.raises(MissionReconciliationRequired) as from_resolution:
        store.resolve_mission_reconciliation(mission_id)

    assert type(from_resolution.value) is type(from_the_gate.value)
    assert from_the_gate.value.step_keys == from_resolution.value.step_keys == ('a', 'c')
    # While it is reconciling the mission state refuses the send first.
    with pytest.raises(MissionNotRunning):
        store.authorize_step_send(mission_id, 'b')
