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

# Task 011: durable terminal recovery evidence, and its atomic application to a step.
#
# What a row means: a terminal RESULT_RESPONSE that was supplied to the store and correlated with
# the dispatch's durable action context. The store does not authenticate where the observation
# came from. It is not proof that the task happened, not a permission, and the table is not
# tamper-evident. Authorization provenance (Task 010) gates whether a result is APPLIED to a step.
# It never gates whether a result is recorded. The SHA-256 of a message is a content identity to
# detect changed content, not a proof of uniqueness.

import ast
from concurrent.futures import ThreadPoolExecutor
import contextlib
import dataclasses
from datetime import datetime, timedelta, timezone
import hashlib
import inspect
import os
from pathlib import Path
import signal
import socket
import sqlite3
import subprocess
import sys
import threading
import uuid

from fault_injection import open_faulty_store
import pytest

import task_resilience
from task_resilience import model, reconciliation, state_machine, storage
from task_resilience.errors import (
    AttemptLimitReached,
    CorruptDispatchAuthorization,
    CorruptDispatchContext,
    CorruptDispatchRecord,
    CorruptRecoveryEvidence,
    DispatchContextMissing,
    DispatchNotFound,
    EvidenceIdConflict,
    EvidenceNotCorrelated,
    InvalidStateTransition,
    MissionNotFound,
    MissionReconciliationRequired,
    ObservationNotRecordable,
    StepNotAwaitingSend,
    StepNotFound,
)
from task_resilience.model import (
    EvidenceOutcome,
    MESSAGE_PREFIX_MAX_BYTES,
    RecoveryEvidenceRecord,
    SendAuthorization,
    TERMINAL_RESULT_STATUSES,
)
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
from task_resilience.storage import MissionStore
from test_dispatch_authorization import (
    fresh_schema,
    make_v4_database,
    make_v5_database,
    MIGRATIONS,
    old_rows,
    ORIGINS,
    V4_TABLES,
    V5_TABLES,
    VERSION_OF,
)
from test_dispatch_context import (
    make_v2_database,
    make_v3_database,
    V3_DISPATCH_IDS,
    V3_MAIN_ID,
    V3_UNKNOWN_ID,
)

# The action every dispatch in these tests is declared to reach.
ACTION = ExpectedActionContext(
    action_name='/navigate_to_pose', action_type='nav2_msgs/action/NavigateToPose',
    robot_namespace='/robot1')

M = MissionState
S = StepState
Outcome = EvidenceOutcome
SUC, CAN, ABO = ActionStatus.SUCCEEDED, ActionStatus.CANCELED, ActionStatus.ABORTED
TERMINAL = (SUC, CAN, ABO)

# Each round is an independent race, repeated because a single race can be won "by luck".
ROUNDS = 25
TIMEOUT = 10

# The evidence table, written out here independently of the store so any change shows up.
EVIDENCE_COLUMNS = [
    'evidence_id', 'dispatch_id', 'observed_status', 'result_code', 'result_message_prefix',
    'result_message_bytes', 'result_message_sha256', 'step_state_before', 'outcome',
    'recorded_at',
]
EVIDENCE_DDL = """
CREATE TABLE recovery_evidence (
    evidence_id            TEXT NOT NULL PRIMARY KEY,
    dispatch_id            TEXT NOT NULL
                           REFERENCES step_dispatches (dispatch_id)
                           ON DELETE CASCADE,
    observed_status        TEXT NOT NULL CHECK (
        observed_status IN ('SUCCEEDED', 'CANCELED', 'ABORTED')
    ),
    result_code            TEXT,
    result_message_prefix  BLOB,
    result_message_bytes   INTEGER,
    result_message_sha256  BLOB,
    step_state_before      TEXT NOT NULL CHECK (step_state_before IN (
        'DISPATCH_PREPARED',
        'DISPATCHED',
        'ACCEPTED',
        'EXECUTING',
        'SUCCEEDED',
        'FAILED',
        'CANCEL_REQUESTED',
        'CANCELLED',
        'OUTCOME_UNKNOWN'
    )),
    outcome                TEXT NOT NULL CHECK (outcome IN (
        'APPLIED',
        'ALREADY_SETTLED',
        'CONFLICT',
        'UNPROVEN_AUTHORIZATION'
    )),
    recorded_at            TEXT NOT NULL,

    CHECK (
        (result_message_prefix IS NULL
         AND result_message_bytes IS NULL
         AND result_message_sha256 IS NULL)
        OR
        (typeof(result_message_prefix) = 'blob'
         AND typeof(result_message_bytes) = 'integer'
         AND result_message_bytes >= length(result_message_prefix)
         AND length(result_message_prefix) <= 1024
         AND typeof(result_message_sha256) = 'blob'
         AND length(result_message_sha256) = 32)
    )
)
"""

# The step states an ordinary route reaches after a send authorization was committed.
ROUTES = {
    S.DISPATCHED: [],
    S.ACCEPTED: [S.ACCEPTED],
    S.EXECUTING: [S.ACCEPTED, S.EXECUTING],
    S.CANCEL_REQUESTED: [S.ACCEPTED, S.CANCEL_REQUESTED],
    S.SUCCEEDED: [S.SUCCEEDED],
    S.FAILED: [S.FAILED],
    S.CANCELLED: [S.CANCELLED],
    S.OUTCOME_UNKNOWN: [S.OUTCOME_UNKNOWN],
}

CANONICAL_TIME = '2026-03-04T05:06:07.123456+00:00'
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


def package_env():
    package_root = Path(task_resilience.__file__).resolve().parent.parent
    return {**os.environ, 'PYTHONPATH': str(package_root)}


def evidence_count(db_path):
    return raw(db_path, 'SELECT COUNT(*) FROM recovery_evidence')[0][0]


def tables(db_path, *names):
    """Return every row of the named tables, for comparing the database before and after."""
    return {name: raw(db_path, f'SELECT * FROM {name} ORDER BY 1, 2') for name in names}


ALL_TABLES = ('missions', 'mission_steps', 'step_dispatches', 'dispatch_contexts',
              'dispatch_authorizations', 'recovery_evidence')


def dump(db_path):
    return tables(db_path, *ALL_TABLES)


def observation(dispatch_id, status, *, code=None, message=None, **overrides):
    """Build a correlated RESULT_RESPONSE for the dispatch, unless an override says otherwise."""
    fields = {
        'kind': ObservationKind.RESULT_RESPONSE, 'dispatch_id': dispatch_id,
        'action_name': ACTION.action_name, 'action_type': ACTION.action_type,
        'robot_namespace': ACTION.robot_namespace, 'status': status, 'result_code': code,
        'result_message': message,
    }
    fields.update(overrides)
    return RecoveryObservation(**fields)


def build(store, db_path, state, provenance='PRESENT', *, unknown_via='authorized',
          after_route=(), max_attempts=3, request_id='req-001'):
    """
    Return (mission_id, dispatch_id) with step 'a' in the given state and provenance.

    A second step, 'b', is prepared. PRESENT is an authorization row as authorize_step_send
    writes it. MISSING is no row: a step that was never authorized, or, for a step that was
    authorized, what a version 4 database holds. CORRUPT is a row with a time that is not
    canonical. The mission is RUNNING, and after_route moves it on afterwards.
    """
    mission_id = store.create_or_get(request_id, {'goal': 'demo'}, max_attempts).mission_id
    store.initialize_steps(mission_id, ['a', 'b'])
    if state is S.PENDING:
        return mission_id, None
    dispatch_id = store.prepare_step_dispatch(mission_id, 'a', ACTION).dispatch_id
    store.prepare_step_dispatch(mission_id, 'b', ACTION)
    store.transition_mission_state(mission_id, M.RUNNING)
    text = str(dispatch_id)
    if state is S.DISPATCH_PREPARED:
        if provenance == 'PRESENT':
            raw(db_path, 'INSERT INTO dispatch_authorizations VALUES (?, ?)', text, CANONICAL_TIME)
        elif provenance == 'CORRUPT':
            raw(db_path, 'INSERT INTO dispatch_authorizations VALUES (?, ?)', text, 'garbage')
    elif state is S.OUTCOME_UNKNOWN and unknown_via == 'prepared':
        assert provenance == 'MISSING'
        store.transition_step_state(mission_id, 'a', S.OUTCOME_UNKNOWN)
    else:
        store.authorize_step_send(mission_id, 'a')
        for target in ROUTES[state]:
            store.transition_step_state(mission_id, 'a', target)
        if provenance == 'MISSING':
            raw(db_path, 'DELETE FROM dispatch_authorizations WHERE dispatch_id = ?', text)
        elif provenance == 'CORRUPT':
            raw(db_path, "UPDATE dispatch_authorizations SET authorized_at = 'garbage'"
                         ' WHERE dispatch_id = ?', text)
    for mission_state in after_route:
        store.transition_mission_state(mission_id, mission_state)
    return mission_id, dispatch_id


def apply(store, mission_id, obs, evidence_id=None, key='a'):
    return store.apply_recovery_observation(evidence_id or uuid.uuid4(), mission_id, key, obs)


def step_row(db_path, mission_id, key='a'):
    return raw(db_path, 'SELECT * FROM mission_steps WHERE mission_id = ? AND step_key = ?',
               mission_id, key)


# A. The schema and its migration

def test_the_schema_is_exactly_version_6_with_one_new_table(db_path):
    MissionStore(db_path).close()

    assert storage.SCHEMA_VERSION == 6
    assert raw(db_path, 'PRAGMA user_version') == [(6,)]
    assert table_names(db_path) == set(ALL_TABLES)
    assert table_names(db_path) - set(V5_TABLES) == {'recovery_evidence'}
    assert [row[1] for row in raw(db_path, 'PRAGMA table_info(recovery_evidence)')] == (
        EVIDENCE_COLUMNS)


def test_the_evidence_table_is_exactly_the_reviewed_shape(db_path):
    MissionStore(db_path).close()

    stored = {name: sql for _, name, sql in schema_of(db_path)}
    assert squashed(stored['recovery_evidence']) == squashed(EVIDENCE_DDL)
    # (name, declared type, not null, default, primary key)
    assert [(row[1], row[2], row[3], row[4], row[5])
            for row in raw(db_path, 'PRAGMA table_info(recovery_evidence)')] == [
        ('evidence_id', 'TEXT', 1, None, 1),
        ('dispatch_id', 'TEXT', 1, None, 0),
        ('observed_status', 'TEXT', 1, None, 0),
        ('result_code', 'TEXT', 0, None, 0),
        ('result_message_prefix', 'BLOB', 0, None, 0),
        ('result_message_bytes', 'INTEGER', 0, None, 0),
        ('result_message_sha256', 'BLOB', 0, None, 0),
        ('step_state_before', 'TEXT', 1, None, 0),
        ('outcome', 'TEXT', 1, None, 0),
        ('recorded_at', 'TEXT', 1, None, 0),
    ]
    # (id, seq, table, from, to, on update, on delete, match)
    assert [(row[2], row[3], row[4], row[6]) for row in raw(
        db_path, 'PRAGMA foreign_key_list(recovery_evidence)')] == [
        ('step_dispatches', 'dispatch_id', 'dispatch_id', 'CASCADE')]


def test_no_table_gains_an_index_of_its_own(db_path):
    MissionStore(db_path).close()

    assert raw(db_path, 'SELECT name FROM sqlite_master WHERE type = ? AND sql IS NOT NULL',
               'index') == []
    # Only the index that SQLite makes for the primary key.
    # (seq, name, unique, origin, partial)
    assert [(row[2], row[3]) for row in raw(
        db_path, 'PRAGMA index_list(recovery_evidence)')] == [(1, 'pk')]


def test_the_older_tables_are_exactly_what_they_were(db_path, tmp_path):
    MissionStore(db_path).close()
    old = tmp_path / 'v5.db'
    make_v5_database(old)

    # The same text for every table that version 5 had: version 6 changed none of them.
    version_5 = {name: sql for _, name, sql in schema_of(old)}
    stored = {name: sql for _, name, sql in schema_of(db_path)}
    for name in V5_TABLES:
        assert stored[name] == version_5[name], name


def test_an_evidence_row_is_removed_with_its_dispatch_and_its_mission(store, db_path):
    mission_id, dispatch_id = build(store, db_path, S.DISPATCHED)
    apply(store, mission_id, observation(dispatch_id, SUC))
    other_id, other_dispatch = build(store, db_path, S.DISPATCHED, request_id='req-002')
    apply(store, other_id, observation(other_dispatch, SUC))

    with foreign_key_connection(db_path) as conn:
        conn.execute('DELETE FROM step_dispatches WHERE dispatch_id = ?', (str(dispatch_id),))
        assert conn.execute('SELECT COUNT(*) FROM recovery_evidence').fetchall() == [(1,)]
        conn.execute('DELETE FROM missions WHERE mission_id = ?', (other_id,))
        assert conn.execute('SELECT COUNT(*) FROM recovery_evidence').fetchall() == [(0,)]


# What SQLite's CHECK does with NULL is easy to get wrong, so every shape is tried raw.

EVIDENCE_INSERT = (
    'INSERT INTO recovery_evidence (evidence_id, dispatch_id, observed_status, result_code,'
    ' result_message_prefix, result_message_bytes, result_message_sha256, step_state_before,'
    ' outcome, recorded_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)')
DIGEST = hashlib.sha256(b'hi').digest()


def insert_raw(db_path, dispatch_id, **overrides):
    values = {
        'status': 'SUCCEEDED', 'code': '0x0', 'prefix': b'hi', 'size': 2, 'digest': DIGEST,
        'before': 'DISPATCHED', 'outcome': 'APPLIED', 'dispatch': str(dispatch_id),
        'evidence': str(uuid.uuid4()),
    }
    values.update(overrides)
    with foreign_key_connection(db_path) as conn:
        conn.execute(EVIDENCE_INSERT, (
            values['evidence'], values['dispatch'], values['status'], values['code'],
            values['prefix'], values['size'], values['digest'], values['before'],
            values['outcome'], CANONICAL_TIME))


@pytest.mark.parametrize('shape', [
    {},
    {'prefix': None, 'size': None, 'digest': None},
    {'code': None},
    {'prefix': b'', 'size': 0, 'digest': hashlib.sha256(b'').digest()},
    {'prefix': b'x' * 1024, 'size': 99999},
    {'prefix': b'x' * 1024, 'size': 1024},
    {'prefix': b'a\x00b', 'size': 3},
    {'code': hex(10 ** 5000)},
    {'code': '-0x5'},
], ids=['whole message', 'no message', 'no result_code', 'empty message', 'cut message',
        'prefix at the bound', 'NUL bytes', 'a code of more than 4300 decimal digits',
        'negative code'])
def test_the_table_accepts_every_shape_that_is_a_valid_row(store, db_path, shape):
    mission_id, dispatch_id = build(store, db_path, S.DISPATCHED)

    insert_raw(db_path, dispatch_id, **shape)

    assert evidence_count(db_path) == 1


def test_an_empty_message_is_stored_as_an_empty_blob_and_not_as_null(store, db_path):
    mission_id, dispatch_id = build(store, db_path, S.DISPATCHED)

    apply(store, mission_id, observation(dispatch_id, SUC, message=''))

    assert raw(db_path, 'SELECT typeof(result_message_prefix), length(result_message_prefix),'
                        ' result_message_bytes FROM recovery_evidence') == [('blob', 0, 0)]


PARTIAL = [
    {'prefix': b'hi', 'size': 2, 'digest': None},
    {'prefix': b'hi', 'size': None, 'digest': DIGEST},
    {'prefix': None, 'size': 2, 'digest': DIGEST},
    {'prefix': b'hi', 'size': None, 'digest': None},
    {'prefix': None, 'size': 2, 'digest': None},
    {'prefix': None, 'size': None, 'digest': DIGEST},
]


@pytest.mark.parametrize('shape', PARTIAL + [
    {'prefix': 'hi'}, {'size': 'abc'}, {'size': 2.5}, {'size': -1},
    {'digest': 'x' * 32}, {'digest': b'x' * 31}, {'digest': b'x' * 33}, {'digest': b''},
    {'prefix': b'x' * 1025, 'size': 5000}, {'prefix': b'hello', 'size': 2},
    {'status': 'UNKNOWN'}, {'status': 'EXECUTING'}, {'status': 'succeeded'}, {'status': None},
    {'before': 'PENDING'}, {'before': 'NOPE'}, {'before': None},
    {'outcome': 'SETTLED'}, {'outcome': None}, {'dispatch': str(uuid.uuid4())},
    {'evidence': None},
], ids=[f'partial {n}' for n in range(len(PARTIAL))] + [
    'prefix as text', 'size not a number', 'size as a real', 'negative size',
    'digest as text', 'digest of 31 bytes', 'digest of 33 bytes', 'empty digest',
    'prefix over the bound', 'size below the prefix', 'status UNKNOWN', 'status EXECUTING',
    'status in lower case', 'no status', 'state PENDING', 'unknown state', 'no state',
    'unknown outcome', 'no outcome', 'no such dispatch', 'no evidence_id'])
def test_the_table_rejects_every_malformed_shape(store, db_path, shape):
    mission_id, dispatch_id = build(store, db_path, S.DISPATCHED)

    with pytest.raises(sqlite3.IntegrityError):
        insert_raw(db_path, dispatch_id, **shape)

    assert evidence_count(db_path) == 0


def test_the_primary_key_refuses_a_second_row_with_the_same_id(store, db_path):
    mission_id, dispatch_id = build(store, db_path, S.DISPATCHED)
    evidence = str(uuid.uuid4())
    insert_raw(db_path, dispatch_id, evidence=evidence)

    with pytest.raises(sqlite3.IntegrityError):
        insert_raw(db_path, dispatch_id, evidence=evidence)


def test_a_numeric_string_size_is_coerced_by_the_integer_affinity(store, db_path):
    mission_id, dispatch_id = build(store, db_path, S.DISPATCHED)

    insert_raw(db_path, dispatch_id, size='2')

    # Documented here rather than relied on: SQLite turns "2" into 2. The parser reads integers.
    assert raw(db_path, 'SELECT typeof(result_message_bytes) FROM recovery_evidence') == [
        ('integer',)]


# Migration. The chains up to version 5 are the fixtures Tasks 009 and 010 already use.

@pytest.mark.parametrize('origin', list(ORIGINS))
def test_every_older_database_migrates_to_version_6_and_invents_no_evidence(
        db_path, tmp_path, origin):
    ORIGINS[origin](db_path)
    before = old_rows(db_path, origin)

    MissionStore(db_path).close()

    assert raw(db_path, 'PRAGMA user_version') == [(6,)]
    assert old_rows(db_path, origin) == before
    assert table_names(db_path) == set(ALL_TABLES)
    assert evidence_count(db_path) == 0
    assert schema_of(db_path) == fresh_schema(tmp_path)


def test_a_native_version_5_database_runs_only_the_version_5_step(monkeypatch, db_path):
    make_v5_database(db_path)
    called = []
    for name in MIGRATIONS:
        original = getattr(MissionStore, name)

        def spy(self, _name=name, _original=original):
            called.append(_name)
            return _original(self)

        monkeypatch.setattr(MissionStore, name, spy)

    MissionStore(db_path).close()

    assert called == ['_migrate_v5_to_v6']


def test_the_version_5_migration_is_one_create_table_and_the_new_version(
        monkeypatch, db_path):
    make_v5_database(db_path)
    # A version 5 database that is not empty: an authorization was committed under it.
    raw(db_path, 'INSERT INTO dispatch_authorizations VALUES (?, ?)',
        V3_DISPATCH_IDS[(V3_MAIN_ID, 'dispatched')], CANONICAL_TIME)
    before = tables(db_path, *V5_TABLES)

    store, connection = open_faulty_store(monkeypatch, db_path)
    store.close()

    statements = connection.statements
    changing = writes(statements)
    assert len(changing) == 1 and changing[0].startswith('CREATE TABLE recovery_evidence (')
    # Nothing was dropped, renamed, altered, copied or filled, and no earlier step ran.
    assert not [sql for sql in statements if sql.split()[0] in ('ALTER', 'DROP', 'INSERT')]
    assert not [sql for sql in statements if sql.startswith('CREATE TABLE dispatch_')]
    assert statements.count('BEGIN IMMEDIATE') == 1 and statements.count('COMMIT') == 1
    assert statements.count('PRAGMA user_version = 6') == 1
    assert statements.index(changing[0]) < statements.index('PRAGMA user_version = 6') < (
        statements.index('COMMIT'))
    assert tables(db_path, *V5_TABLES) == before
    assert evidence_count(db_path) == 0


@pytest.mark.parametrize('origin', list(ORIGINS))
def test_reopening_a_migrated_database_changes_nothing(monkeypatch, db_path, origin):
    ORIGINS[origin](db_path)
    MissionStore(db_path).close()
    before = (dump(db_path), schema_of(db_path))

    store, connection = open_faulty_store(monkeypatch, db_path)
    store.close()

    assert writes(connection.statements) == []
    assert (dump(db_path), schema_of(db_path)) == before
    assert raw(db_path, 'PRAGMA user_version') == [(6,)]


def test_concurrent_opens_of_a_version_5_database_migrate_it_once(tmp_path):
    for round_index in range(5):
        path = tmp_path / f'v5-{round_index}.db'
        make_v5_database(path)
        before = tables(path, *V5_TABLES)
        workers = 8
        barrier = threading.Barrier(workers, timeout=TIMEOUT)

        def open_and_close():
            barrier.wait()
            MissionStore(path).close()

        with ThreadPoolExecutor(max_workers=workers) as pool:
            for future in [pool.submit(open_and_close) for _ in range(workers)]:
                future.result(timeout=TIMEOUT)

        assert raw(path, 'PRAGMA user_version') == [(6,)]
        assert tables(path, *V5_TABLES) == before
        assert evidence_count(path) == 0
        fresh = tmp_path / f'fresh-{round_index}.db'
        MissionStore(fresh).close()
        assert schema_of(path) == schema_of(fresh)


@pytest.mark.parametrize('failing', ['create the table', 'set the version', 'commit'])
def test_a_failure_while_migrating_from_version_5_rolls_back_cleanly(
        monkeypatch, db_path, failing):
    make_v5_database(db_path)
    before = tables(db_path, *V5_TABLES)
    schema = schema_of(db_path)

    if failing == 'create the table':
        with monkeypatch.context() as patch:
            patch.setattr(storage, '_RECOVERY_EVIDENCE_DDL', 'THIS IS NOT SQL')
            with pytest.raises(sqlite3.OperationalError):
                MissionStore(db_path)
    else:
        fails = {
            'set the version': lambda sql: sql.strip() == 'PRAGMA user_version = 6',
            'commit': lambda sql: sql.strip().upper() == 'COMMIT',
        }[failing]
        with pytest.raises(sqlite3.OperationalError, match='injected failure'):
            open_faulty_store(monkeypatch, db_path, fail_when=fails)

    assert raw(db_path, 'PRAGMA user_version') == [(5,)]
    assert table_names(db_path) == set(V5_TABLES)
    assert tables(db_path, *V5_TABLES) == before
    assert schema_of(db_path) == schema
    with contextlib.closing(sqlite3.connect(db_path, timeout=0.1, isolation_level=None)) as conn:
        conn.execute('BEGIN IMMEDIATE')
        conn.execute('ROLLBACK')
    # A later, healthy open finishes the migration.
    with MissionStore(db_path):
        pass
    assert raw(db_path, 'PRAGMA user_version') == [(6,)]
    assert evidence_count(db_path) == 0


def test_migrated_databases_are_told_apart_only_by_their_older_tables():
    # Nothing reads a version from a table: the versions the fixtures claim are the versions.
    assert VERSION_OF == {'v1': 1, 'v2': 2, 'v3': 3, 'v4': 4, 'v5': 5}
    assert V4_TABLES == V5_TABLES[:4]


# B. Only a terminal result response is recordable, and that is decided before BEGIN

NON_RECORDABLE = {
    'STATUS_SAMPLE succeeded': lambda d: observation(
        d, SUC, kind=ObservationKind.STATUS_SAMPLE),
    'STATUS_SAMPLE executing': lambda d: observation(
        d, ActionStatus.EXECUTING, kind=ObservationKind.STATUS_SAMPLE),
    'LOCAL_TIMEOUT': lambda d: RecoveryObservation(
        kind=ObservationKind.LOCAL_TIMEOUT, dispatch_id=d),
    'SERVICE_UNAVAILABLE': lambda d: RecoveryObservation(
        kind=ObservationKind.SERVICE_UNAVAILABLE, dispatch_id=d),
    'TRANSPORT_ERROR': lambda d: RecoveryObservation(
        kind=ObservationKind.TRANSPORT_ERROR, dispatch_id=d),
    'RESULT UNKNOWN': lambda d: observation(d, ActionStatus.UNKNOWN),
    'RESULT ACCEPTED': lambda d: observation(d, ActionStatus.ACCEPTED),
    'RESULT EXECUTING': lambda d: observation(d, ActionStatus.EXECUTING),
    'RESULT CANCELING': lambda d: observation(d, ActionStatus.CANCELING),
}


@pytest.mark.parametrize('label', list(NON_RECORDABLE))
def test_an_observation_that_is_not_a_terminal_result_never_opens_a_transaction(
        monkeypatch, db_path, label):
    store, connection = open_faulty_store(monkeypatch, db_path)
    with store:
        # The mission does not exist: a rejection that needed the database would say so.
        connection.statements.clear()
        obs = NON_RECORDABLE[label](uuid.uuid4())

        with pytest.raises(ObservationNotRecordable) as excinfo:
            apply(store, 'no-such-mission', obs)

        assert excinfo.value.kind is obs.kind and excinfo.value.status is obs.status
        assert excinfo.value.reason
        assert connection.statements == []


@pytest.mark.parametrize('bad', [
    'a string', 12, None, uuid.uuid4().hex, str(uuid.uuid4()), b'x' * 16])
def test_a_wrong_evidence_id_type_is_refused_before_the_database_is_touched(
        monkeypatch, db_path, bad):
    store, connection = open_faulty_store(monkeypatch, db_path)
    with store:
        connection.statements.clear()

        with pytest.raises(TypeError):
            store.apply_recovery_observation(
                bad, 'm', 'a', observation(uuid.uuid4(), SUC))

        assert connection.statements == []


@pytest.mark.parametrize('bad', [
    'a string', None, {'status': 'SUCCEEDED'}, ReconciliationDecision(
        kind=DecisionKind.ALREADY_SETTLED, reason=DecisionReason.RESULT_MATCHES_DURABLE_STATE)])
def test_a_wrong_observation_type_is_refused_before_the_database_is_touched(
        monkeypatch, db_path, bad):
    store, connection = open_faulty_store(monkeypatch, db_path)
    with store:
        connection.statements.clear()

        with pytest.raises(TypeError):
            store.apply_recovery_observation(uuid.uuid4(), 'm', 'a', bad)

        assert connection.statements == []


def test_the_recordable_statuses_are_exactly_the_three_terminal_ones():
    assert TERMINAL_RESULT_STATUSES == {SUC, CAN, ABO}


def test_the_recordable_statuses_agree_with_what_the_decision_module_calls_terminal():
    dispatch = uuid.uuid4()
    for status in ActionStatus:
        # A step that is not settled: only a terminal result can be a candidate for it.
        decision = decide_reconciliation(
            DurableSnapshot(
                mission_state=M.RUNNING, step_state=S.DISPATCHED, dispatch_id=dispatch),
            ACTION, observation(dispatch, status))
        is_candidate = decision.kind is DecisionKind.TERMINAL_RESULT_CANDIDATE
        assert (status in TERMINAL_RESULT_STATUSES) is is_candidate, status


def test_a_decision_that_is_not_about_a_terminal_result_is_refused_inside_the_transaction(
        store, db_path, monkeypatch):
    mission_id, dispatch_id = build(store, db_path, S.DISPATCHED)
    before = dump(db_path)
    for kind, reason in (
            (DecisionKind.INCONCLUSIVE, DecisionReason.NONTERMINAL_RESULT_STATUS),
            (DecisionKind.SERVER_UNKNOWN, DecisionReason.RESULT_UNKNOWN),
            (DecisionKind.STATUS_ONLY, DecisionReason.STATUS_IS_ADVISORY),
            (DecisionKind.MISSING_DISPATCH_IDENTITY, DecisionReason.NO_DURABLE_DISPATCH_ID)):
        monkeypatch.setattr(
            storage, 'decide_reconciliation',
            lambda *args, _kind=kind, _reason=reason: ReconciliationDecision(
                kind=_kind, reason=_reason))

        with pytest.raises(ObservationNotRecordable):
            apply(store, mission_id, observation(dispatch_id, SUC))

        assert dump(db_path) == before


def test_an_exact_replay_does_not_get_past_a_decision_that_is_not_about_a_terminal_result(
        monkeypatch, db_path):
    store, connection = open_faulty_store(monkeypatch, db_path)
    with store:
        mission_id, dispatch_id, evidence_id, obs, first = recorded(
            store, db_path, code=7, message='ok')
        # Under the real Task 007 this very call is an exact replay, and returns the stored row.
        assert store.apply_recovery_observation(evidence_id, mission_id, 'a', obs) == first
        before = dump(db_path)

        for kind, reason in (
                (DecisionKind.INCONCLUSIVE, DecisionReason.NONTERMINAL_RESULT_STATUS),
                (DecisionKind.SERVER_UNKNOWN, DecisionReason.RESULT_UNKNOWN),
                (DecisionKind.STATUS_ONLY, DecisionReason.STATUS_IS_ADVISORY),
                (DecisionKind.MISSING_DISPATCH_IDENTITY, DecisionReason.NO_DURABLE_DISPATCH_ID)):
            connection.statements.clear()
            with monkeypatch.context() as patch:
                patch.setattr(
                    storage, 'decide_reconciliation',
                    lambda *args, _kind=kind, _reason=reason: ReconciliationDecision(
                        kind=_kind, reason=_reason))

                with pytest.raises(ObservationNotRecordable) as excinfo:
                    store.apply_recovery_observation(evidence_id, mission_id, 'a', obs)

            assert excinfo.value.reason == f'unexpected decision {kind.name}'
            assert (excinfo.value.kind, excinfo.value.status) == (obs.kind, obs.status)
            # The transaction was opened and the durable state read, and the refusal came before
            # the evidence ID was looked up: the lookup that answers a replay never ran.
            logged = [normalized(sql) for sql in connection.statements]
            assert logged[0] == 'BEGIN IMMEDIATE'
            assert any(sql.startswith('SELECT * FROM DISPATCH_CONTEXTS') for sql in logged)
            assert not [sql for sql in logged if 'FROM RECOVERY_EVIDENCE' in sql]
            assert writes(logged) == []
            assert dump(db_path) == before
            assert evidence_count(db_path) == 1

        assert store.get_recovery_evidence(mission_id, 'a') == (first,)
        assert store.apply_recovery_observation(evidence_id, mission_id, 'a', obs) == first


def test_an_integer_of_any_size_is_recordable(store, db_path):
    mission_id, dispatch_id = build(store, db_path, S.DISPATCHED)

    record = apply(store, mission_id, observation(dispatch_id, SUC, code=2 ** 300))

    assert record.result_code == 2 ** 300


def test_a_bool_is_not_a_result_code_and_the_contract_says_so_before_the_store_is_reached():
    with pytest.raises(TypeError):
        observation(uuid.uuid4(), SUC, code=True)
    with pytest.raises(TypeError):
        observation(uuid.uuid4(), SUC, code=False)


# C. result_code: any integer Task 007 accepts, kept exactly, never interpreted

CODES = [
    None, 0, 1, -1, 5, -5, 255, 2 ** 31, -2 ** 31, 2 ** 63 - 1, 2 ** 63, -2 ** 63, -2 ** 63 - 1,
    2 ** 64 - 1, 2 ** 64, 2 ** 127, -2 ** 127, 2 ** 128, 2 ** 200, -2 ** 200,
    # More decimal digits than Python is willing to convert: hexadecimal has no such limit.
    10 ** 5000, -10 ** 5000, 7 ** 6000,
]


@pytest.mark.parametrize('code', CODES, ids=[
    'None' if c is None else f'{c:x}'[:24] + ('...' if abs(c) > 2 ** 100 else '') for c in CODES])
def test_a_result_code_round_trips_exactly(store, db_path, code):
    mission_id, dispatch_id = build(store, db_path, S.DISPATCHED)

    written = apply(store, mission_id, observation(dispatch_id, SUC, code=code))

    assert written.result_code == code
    [read] = store.get_recovery_evidence(mission_id, 'a')
    assert read == written and read.result_code == code
    stored = raw(db_path, 'SELECT result_code, typeof(result_code) FROM recovery_evidence')
    assert stored == ([(None, 'null')] if code is None else [(hex(code), 'text')])


def test_the_stored_form_is_canonical_hexadecimal(store, db_path):
    mission_id, dispatch_id = build(store, db_path, S.EXECUTING, max_attempts=9)
    for code in (0, 5, -5, 255, -256):
        apply(store, mission_id, observation(dispatch_id, SUC, code=code))

    assert sorted(raw(db_path, 'SELECT result_code FROM recovery_evidence')) == sorted([
        ('0x0',), ('0x5',), ('-0x5',), ('0xff',), ('-0x100',)])


@pytest.mark.parametrize('status', TERMINAL)
def test_a_result_code_of_zero_is_not_success(store, db_path, status):
    mission_id, dispatch_id = build(store, db_path, S.DISPATCHED)

    record = apply(store, mission_id, observation(dispatch_id, status, code=0))

    # The step goes wherever the STATUS says, and a zero code changes nothing about it.
    expected = {SUC: S.SUCCEEDED, CAN: S.CANCELLED, ABO: S.FAILED}[status]
    assert store.get_step(mission_id, 'a').state is expected
    assert record.result_code == 0


@pytest.mark.parametrize('code', [1, -1, 42])
def test_a_nonzero_code_does_not_change_the_outcome_either(store, db_path, code):
    mission_id, dispatch_id = build(store, db_path, S.DISPATCHED)

    apply(store, mission_id, observation(dispatch_id, SUC, code=code))

    assert store.get_step(mission_id, 'a').state is S.SUCCEEDED


NOT_CANONICAL = ['5', '05', '0X5', '+0x5', ' 0x5', '0x5 ', '0x', '', '-0x0', '0x0_1', 'garbage',
                 '0x5.0', '1e3', '0x-5', '--0x5', '0b101', '0o7', 'None', '0xG']


@pytest.mark.parametrize('stored', NOT_CANONICAL + [b'0x5', 5, 5.5, 2 ** 40],
                         ids=[f'text {t!r}' for t in NOT_CANONICAL] + [
                             'a blob', 'an integer', 'a real', 'a large integer'])
def test_a_stored_result_code_that_is_not_canonical_hex_is_corruption(
        store, db_path, stored):
    mission_id, dispatch_id = build(store, db_path, S.DISPATCHED)
    apply(store, mission_id, observation(dispatch_id, SUC, code=5))
    corrupt(db_path, 'UPDATE recovery_evidence SET result_code = ?', stored)
    before = dump(db_path)

    with pytest.raises(CorruptRecoveryEvidence) as excinfo:
        store.get_recovery_evidence(mission_id, 'a')

    assert 'result_code' in excinfo.value.reason
    assert (excinfo.value.mission_id, excinfo.value.step_key) == (mission_id, 'a')
    assert dump(db_path) == before


# D. Messages: a bounded prefix, the exact size, and the SHA-256 of the whole message

def whole(message):
    return message.encode('utf-8', 'surrogatepass')


def expected_prefix(message):
    """Return the longest run of whole characters that is at most 1024 bytes, the slow way."""
    best = 0
    for count in range(min(len(message), 1024) + 1):
        if len(whole(message[:count])) <= 1024:
            best = count
    return message[:best]


MESSAGES = {
    'ascii': 'Goal was canceled',
    'multibyte': 'café ☃ \U0001f600 done',
    'embedded NUL': 'a\x00b\x00',
    'exactly 1024 bytes': 'x' * 1024,
    'one byte over': 'x' * 1025,
    'a 3-byte character would cross the bound': 'a' * 1022 + '☃',
    'a 3-byte character ends exactly at the bound': 'a' * 1021 + '☃',
    'a 4-byte character would cross the bound': 'a' * 1021 + '\U0001f600',
    'a 4-byte character ends exactly at the bound': 'a' * 1020 + '\U0001f600',
    'many 2-byte characters': 'é' * 700,
    'many 4-byte characters': '\U0001f600' * 500,
    'a lone surrogate': 'a\ud800b',
    'two lone surrogates that look like a pair': chr(0xD83D) + chr(0xDE00),
    'surrogates around the bound': 'a' * 1019 + '\ud800' + '\udfff' + 'z' * 10,
    'a very large diagnostic': 'diagnostic ' * 500_000,
}


@pytest.mark.parametrize('label', list(MESSAGES))
def test_a_message_is_recorded_as_a_bounded_prefix_its_exact_size_and_its_digest(
        store, db_path, label):
    message = MESSAGES[label]
    mission_id, dispatch_id = build(store, db_path, S.DISPATCHED)

    record = apply(store, mission_id, observation(dispatch_id, ABO, code=3, message=message))

    # The terminal result is recorded and applied whatever the size of the diagnostic.
    assert record.outcome is Outcome.APPLIED and store.get_step(mission_id, 'a').state is S.FAILED
    everything = whole(message)
    prefix = expected_prefix(message)
    assert record.result_message_prefix == prefix
    assert len(whole(record.result_message_prefix)) <= 1024
    assert message.startswith(record.result_message_prefix)
    assert record.result_message_bytes == len(everything)
    assert record.result_message_sha256 == hashlib.sha256(everything).digest()
    assert record.result_message_truncated is (len(everything) > len(whole(prefix)))
    assert store.get_recovery_evidence(mission_id, 'a') == (record,)
    # The columns hold exactly that, in the storage classes the schema requires.
    assert raw(db_path, 'SELECT typeof(result_message_prefix), result_message_prefix,'
                        ' typeof(result_message_bytes), result_message_bytes,'
                        ' typeof(result_message_sha256), result_message_sha256'
                        ' FROM recovery_evidence') == [
        ('blob', whole(prefix), 'integer', len(everything), 'blob',
         hashlib.sha256(everything).digest())]


@pytest.mark.parametrize('label', [
    'exactly 1024 bytes', 'a 3-byte character ends exactly at the bound',
    'a 4-byte character ends exactly at the bound', 'ascii'])
def test_a_message_that_fits_is_not_truncated(store, db_path, label):
    mission_id, dispatch_id = build(store, db_path, S.DISPATCHED)

    record = apply(store, mission_id, observation(dispatch_id, SUC, message=MESSAGES[label]))

    assert record.result_message_prefix == MESSAGES[label]
    assert record.result_message_truncated is False


@pytest.mark.parametrize('label', [
    'one byte over', 'a 3-byte character would cross the bound',
    'a 4-byte character would cross the bound', 'many 2-byte characters'])
def test_a_message_that_does_not_fit_is_cut_at_a_character_and_says_so(
        store, db_path, label):
    mission_id, dispatch_id = build(store, db_path, S.DISPATCHED)

    record = apply(store, mission_id, observation(dispatch_id, SUC, message=MESSAGES[label]))

    assert record.result_message_truncated is True
    # Whole characters only: nothing was sliced in the middle of a character and dropped.
    assert record.result_message_prefix == expected_prefix(MESSAGES[label])
    assert len(whole(record.result_message_prefix)) > 1020


def test_no_message_and_an_empty_message_are_different_things(store, db_path):
    mission_id, dispatch_id = build(store, db_path, S.EXECUTING, max_attempts=9)

    none = apply(store, mission_id, observation(dispatch_id, SUC, message=None))
    empty = apply(store, mission_id, observation(dispatch_id, SUC, message=''))

    assert (none.result_message_prefix, none.result_message_bytes,
            none.result_message_sha256) == (None, None, None)
    assert none.result_message_truncated is False
    assert (empty.result_message_prefix, empty.result_message_bytes,
            empty.result_message_sha256) == ('', 0, hashlib.sha256(b'').digest())
    assert empty.result_message_truncated is False


def test_a_lone_surrogate_survives_the_round_trip_unchanged(store, db_path):
    mission_id, dispatch_id = build(store, db_path, S.DISPATCHED)

    record = apply(store, mission_id, observation(dispatch_id, SUC, message='a\ud800b\udfffc'))

    [read] = store.get_recovery_evidence(mission_id, 'a')
    assert read.result_message_prefix == 'a\ud800b\udfffc'
    # One byte each for a, b and c, and three each for the two surrogates.
    assert read.result_message_bytes == len(whole('a\ud800b\udfffc')) == 9
    assert read == record


TAILS = [
    ('x' * 1024 + 'A' * 100, 'x' * 1024 + 'B' * 100),
    ('x' * 1024 + 'é' * 50, 'x' * 1024 + 'e' * 100),
    ('y' * 5000 + 'one', 'y' * 5000 + 'two'),
    ('☃' * 400 + 'A', '☃' * 400 + 'B'),
]


@pytest.mark.parametrize(('first', 'second'), TAILS)
def test_two_messages_with_the_same_prefix_and_length_but_different_tails_differ(
        store, db_path, first, second):
    assert expected_prefix(first) == expected_prefix(second)
    assert len(whole(first)) == len(whole(second)) and first != second
    mission_id, dispatch_id = build(store, db_path, S.EXECUTING, max_attempts=9)
    evidence_id = uuid.uuid4()
    written = apply(store, mission_id, observation(dispatch_id, SUC, message=first), evidence_id)
    before = dump(db_path)

    # Under one evidence ID the second is a changed fact, however alike the stored part is.
    with pytest.raises(EvidenceIdConflict):
        apply(store, mission_id, observation(dispatch_id, SUC, message=second), evidence_id)
    assert dump(db_path) == before

    # Under a new ID it is a distinct observation, with a distinct digest.
    other = apply(store, mission_id, observation(dispatch_id, SUC, message=second))
    assert other.result_message_prefix == written.result_message_prefix
    assert other.result_message_bytes == written.result_message_bytes
    assert other.result_message_sha256 != written.result_message_sha256
    assert evidence_count(db_path) == 2


MESSAGE_DAMAGE = {
    'prefix as text': ('result_message_prefix', 'hi'),
    'prefix that is not UTF-8': ('result_message_prefix', b'\xff\xfe'),
    'prefix in an overlong form': ('result_message_prefix', b'\xc0\x80'),
    'prefix over the bound': ('result_message_prefix', b'x' * 1025),
    'a digest that does not match a message that was not cut': (
        'result_message_sha256', hashlib.sha256(b'other').digest()),
    'a digest of the wrong length': ('result_message_sha256', b'x' * 31),
    'a digest as text': ('result_message_sha256', 'x' * 32),
    'a size below the prefix': ('result_message_bytes', 1),
    'a size that is not an integer': ('result_message_bytes', 'many'),
    'a size that is a real': ('result_message_bytes', 2.5),
    'a size but no prefix': ('result_message_prefix', None),
    'a prefix but no size': ('result_message_bytes', None),
    'a prefix but no digest': ('result_message_sha256', None),
    'a cut message whose prefix is far short of the bound': ('result_message_bytes', 5000),
}


@pytest.mark.parametrize('label', list(MESSAGE_DAMAGE))
def test_a_damaged_message_representation_is_corruption_and_is_never_repaired(
        store, db_path, label):
    mission_id, dispatch_id = build(store, db_path, S.DISPATCHED)
    apply(store, mission_id, observation(dispatch_id, SUC, message='hi'))
    column, value = MESSAGE_DAMAGE[label]
    corrupt(db_path, f'UPDATE recovery_evidence SET {column} = ?', value)
    before = dump(db_path)

    with pytest.raises(CorruptRecoveryEvidence) as excinfo:
        store.get_recovery_evidence(mission_id, 'a')

    assert excinfo.value.reason
    assert dump(db_path) == before


# E. An evidence ID binds to one observation for good

def recorded(store, db_path, state=S.DISPATCHED, status=SUC, **fields):
    """Record one observation on a fresh step, and return what is needed to replay it."""
    mission_id, dispatch_id = build(store, db_path, state)
    evidence_id = uuid.uuid4()
    obs = observation(dispatch_id, status, **fields)
    return mission_id, dispatch_id, evidence_id, obs, apply(store, mission_id, obs, evidence_id)


@pytest.mark.parametrize('version', [1, 3, 4, 5])
def test_any_uuid_is_an_acceptable_evidence_id(store, db_path, version):
    mission_id, dispatch_id = build(store, db_path, S.DISPATCHED)
    identifier = {1: uuid.uuid1, 3: lambda: uuid.uuid3(uuid.NAMESPACE_DNS, 'x'),
                  4: uuid.uuid4, 5: lambda: uuid.uuid5(uuid.NAMESPACE_DNS, 'x')}[version]()

    record = store.apply_recovery_observation(
        identifier, mission_id, 'a', observation(dispatch_id, SUC))

    assert record.evidence_id == identifier
    assert raw(db_path, 'SELECT evidence_id FROM recovery_evidence') == [(str(identifier),)]


def test_the_evidence_id_is_stored_as_canonical_text(store, db_path):
    mission_id, dispatch_id, evidence_id, _, _ = recorded(store, db_path)

    assert raw(db_path, 'SELECT evidence_id FROM recovery_evidence') == [(str(evidence_id),)]
    assert str(evidence_id) == str(evidence_id).lower() and len(str(evidence_id)) == 36


def test_an_exact_replay_returns_the_stored_record_and_writes_nothing(
        monkeypatch, db_path):
    store, connection = open_faulty_store(monkeypatch, db_path)
    with store:
        mission_id, dispatch_id, evidence_id, obs, first = recorded(
            store, db_path, code=7, message='ok')
        before = dump(db_path)
        connection.statements.clear()

        again = store.apply_recovery_observation(evidence_id, mission_id, 'a', obs)

        assert again == first
        assert writes(connection.statements) == []
        assert dump(db_path) == before
        assert connection.statements.count('BEGIN IMMEDIATE') == 1


def test_a_replay_returns_the_original_outcome_even_though_the_step_has_moved_on(
        store, db_path):
    mission_id, dispatch_id, evidence_id, obs, first = recorded(store, db_path)
    assert first.outcome is Outcome.APPLIED and first.step_state_before is S.DISPATCHED
    step_before = step_row(db_path, mission_id)

    again = store.apply_recovery_observation(evidence_id, mission_id, 'a', obs)

    # Task 007 would now call the same result ALREADY_SETTLED. The stored row is not recomputed.
    assert again.outcome is Outcome.APPLIED and again.step_state_before is S.DISPATCHED
    assert again == first
    assert step_row(db_path, mission_id) == step_before
    assert evidence_count(db_path) == 1


CHANGES = {
    'status': lambda d: observation(d, ABO, code=7, message='ok'),
    'result_code': lambda d: observation(d, SUC, code=8, message='ok'),
    'result_code dropped': lambda d: observation(d, SUC, message='ok'),
    'result_code added where there was none': None,
    'message text': lambda d: observation(d, SUC, code=7, message='no'),
    'message length': lambda d: observation(d, SUC, code=7, message='ok!'),
    'message dropped': lambda d: observation(d, SUC, code=7),
    'message emptied': lambda d: observation(d, SUC, code=7, message=''),
    'code 0 against 7': lambda d: observation(d, SUC, code=0, message='ok'),
}


@pytest.mark.parametrize('label', [label for label, make in CHANGES.items() if make])
def test_the_same_id_with_any_changed_fact_is_a_conflict_and_writes_nothing(
        store, db_path, label):
    mission_id, dispatch_id, evidence_id, obs, first = recorded(
        store, db_path, code=7, message='ok')
    before = dump(db_path)

    with pytest.raises(EvidenceIdConflict) as excinfo:
        store.apply_recovery_observation(
            evidence_id, mission_id, 'a', CHANGES[label](dispatch_id))

    assert excinfo.value.evidence_id == evidence_id
    assert dump(db_path) == before
    assert store.get_recovery_evidence(mission_id, 'a') == (first,)


def test_an_id_first_recorded_without_a_code_or_message_conflicts_with_one_that_has_them(
        store, db_path):
    mission_id, dispatch_id, evidence_id, _, _ = recorded(store, db_path)

    for changed in (observation(dispatch_id, SUC, code=0),
                    observation(dispatch_id, SUC, message='x'),
                    observation(dispatch_id, SUC, message='')):
        with pytest.raises(EvidenceIdConflict):
            store.apply_recovery_observation(evidence_id, mission_id, 'a', changed)


def test_the_same_id_for_another_dispatch_is_a_conflict_not_a_corruption(store, db_path):
    mission_id, dispatch_id, evidence_id, _, _ = recorded(store, db_path)
    other = store.get_step_dispatch(mission_id, 'b').dispatch_id
    before = dump(db_path)

    with pytest.raises(EvidenceIdConflict):
        store.apply_recovery_observation(evidence_id, mission_id, 'b', observation(other, SUC))

    assert dump(db_path) == before


def test_the_same_id_for_a_dispatch_of_another_mission_is_a_conflict_too(store, db_path):
    mission_id, dispatch_id, evidence_id, _, _ = recorded(store, db_path)
    other_mission, other_dispatch = build(store, db_path, S.DISPATCHED, request_id='req-002')

    with pytest.raises(EvidenceIdConflict):
        store.apply_recovery_observation(
            evidence_id, other_mission, 'a', observation(other_dispatch, SUC))

    assert evidence_count(db_path) == 1


@pytest.mark.parametrize('field', ['action_name', 'action_type', 'robot_namespace', 'dispatch_id'])
def test_a_replay_with_a_changed_correlation_field_is_not_correlated_and_never_a_replay(
        store, db_path, field):
    mission_id, dispatch_id, evidence_id, obs, first = recorded(store, db_path)
    changed = {'action_name': '/other', 'action_type': 'other/Type', 'robot_namespace': '',
               'dispatch_id': uuid.uuid4()}[field]
    before = dump(db_path)

    with pytest.raises(EvidenceNotCorrelated):
        store.apply_recovery_observation(
            evidence_id, mission_id, 'a', dataclasses.replace(obs, **{field: changed}))

    assert dump(db_path) == before


def test_a_stored_prefix_that_is_not_the_whole_message_it_claims_is_corruption_on_a_replay(
        store, db_path):
    mission_id, dispatch_id, evidence_id, obs, first = recorded(store, db_path, message='hi')
    # The digest and the size still say "hi", but the prefix no longer does. Nothing was cut, so
    # the record itself can tell.
    corrupt(db_path, 'UPDATE recovery_evidence SET result_message_prefix = ?', b'ho')

    with pytest.raises(CorruptRecoveryEvidence):
        store.apply_recovery_observation(evidence_id, mission_id, 'a', obs)


def test_the_stored_prefix_of_a_cut_message_is_checked_on_a_replay(store, db_path):
    message = 'a' * 2000
    mission_id, dispatch_id, evidence_id, obs, first = recorded(
        store, db_path, message=message)
    assert first.result_message_prefix == 'a' * 1024 and first.result_message_bytes == 2000
    # A well-formed prefix of the right length, which the record cannot tell from the real one:
    # only the replay knows the message, and the message says these bytes are wrong.
    corrupt(db_path, 'UPDATE recovery_evidence SET result_message_prefix = ?', b'b' * 1024)
    assert store.get_recovery_evidence(mission_id, 'a')[0].result_message_prefix == 'b' * 1024

    with pytest.raises(CorruptRecoveryEvidence) as raised:
        store.apply_recovery_observation(evidence_id, mission_id, 'a', obs)

    assert raised.value.evidence_id == str(evidence_id)
    assert evidence_count(db_path) == 1


def test_different_ids_with_identical_facts_are_stored_as_distinct_rows(store, db_path):
    mission_id, dispatch_id = build(store, db_path, S.DISPATCHED)
    obs = observation(dispatch_id, SUC, code=3, message='same')
    ids = [uuid.uuid4() for _ in range(4)]

    records = [store.apply_recovery_observation(i, mission_id, 'a', obs) for i in ids]

    # Nothing is folded onto another row, so an ID is never left free to be reused.
    assert [r.evidence_id for r in records] == ids
    assert evidence_count(db_path) == 4
    assert [r.outcome for r in records] == [
        Outcome.APPLIED, Outcome.ALREADY_SETTLED, Outcome.ALREADY_SETTLED, Outcome.ALREADY_SETTLED]
    assert {(r.observed_status, r.result_code, r.result_message_sha256) for r in records} == {
        (SUC, 3, hashlib.sha256(b'same').digest())}
    # Every one of those IDs is now bound to these facts and to nothing else.
    for evidence_id in ids:
        with pytest.raises(EvidenceIdConflict):
            store.apply_recovery_observation(
                evidence_id, mission_id, 'a', observation(dispatch_id, ABO))


def test_an_id_that_was_refused_is_not_consumed(store, db_path):
    mission_id, dispatch_id = build(store, db_path, S.DISPATCHED)
    evidence_id = uuid.uuid4()

    with pytest.raises(EvidenceNotCorrelated):
        store.apply_recovery_observation(
            evidence_id, mission_id, 'a', observation(dispatch_id, SUC, action_name='/other'))
    assert evidence_count(db_path) == 0

    record = store.apply_recovery_observation(
        evidence_id, mission_id, 'a', observation(dispatch_id, ABO))
    assert record.evidence_id == evidence_id and record.observed_status is ABO


# F. Every state, every provenance, every terminal status

def err(error):
    return (('ERR', error, None),) * 3


def held(state):
    return (('OUTCOME', Outcome.UNPROVEN_AUTHORIZATION, state),) * 3


def applied():
    return (('OUTCOME', Outcome.APPLIED, S.SUCCEEDED), ('OUTCOME', Outcome.APPLIED, S.CANCELLED),
            ('OUTCOME', Outcome.APPLIED, S.FAILED))


def settled(state):
    # Written out by hand: which result matches which terminal state, and which contradicts it.
    outcomes = {
        S.SUCCEEDED: (Outcome.ALREADY_SETTLED, Outcome.CONFLICT, Outcome.CONFLICT),
        S.FAILED: (Outcome.CONFLICT, Outcome.CONFLICT, Outcome.ALREADY_SETTLED),
        S.CANCELLED: (Outcome.CONFLICT, Outcome.ALREADY_SETTLED, Outcome.CONFLICT),
    }[state]
    return tuple(('OUTCOME', outcome, state) for outcome in outcomes)


# (step state, provenance, how to build it, what each of SUCCEEDED, CANCELED and ABORTED does)
MATRIX = [
    (S.PENDING, 'no dispatch', {}, err(DispatchNotFound)),
    (S.DISPATCH_PREPARED, 'MISSING', {}, held(S.OUTCOME_UNKNOWN)),
    (S.DISPATCH_PREPARED, 'PRESENT', {}, err(CorruptDispatchAuthorization)),
    (S.DISPATCH_PREPARED, 'CORRUPT', {}, err(CorruptDispatchAuthorization)),
]
for _state in (S.DISPATCHED, S.ACCEPTED, S.EXECUTING, S.CANCEL_REQUESTED):
    MATRIX += [
        (_state, 'PRESENT', {}, applied()),
        (_state, 'MISSING', {}, held(_state)),
        (_state, 'CORRUPT', {}, err(CorruptDispatchAuthorization)),
    ]
MATRIX += [
    (S.OUTCOME_UNKNOWN, 'PRESENT', {}, applied()),
    (S.OUTCOME_UNKNOWN, 'MISSING', {'unknown_via': 'prepared'}, held(S.OUTCOME_UNKNOWN)),
    (S.OUTCOME_UNKNOWN, 'MISSING', {}, held(S.OUTCOME_UNKNOWN)),
    (S.OUTCOME_UNKNOWN, 'CORRUPT', {}, err(CorruptDispatchAuthorization)),
]
for _state in (S.SUCCEEDED, S.FAILED, S.CANCELLED):
    MATRIX += [
        (_state, 'PRESENT', {}, settled(_state)),
        (_state, 'MISSING', {}, settled(_state)),
        (_state, 'CORRUPT', {}, err(CorruptDispatchAuthorization)),
    ]

CELLS = [
    pytest.param(state, provenance, kwargs, expected[index], TERMINAL[index],
                 id=f'{state.name}-{provenance}{"-never-authorized" if kwargs else ""}-'
                    f'{TERMINAL[index].name}')
    for state, provenance, kwargs, expected in MATRIX for index in range(3)]


@pytest.mark.parametrize(('state', 'provenance', 'how', 'expected', 'status'), CELLS)
def test_the_state_by_provenance_by_status_matrix(
        store, db_path, state, provenance, how, expected, status):
    mission_id, dispatch_id = build(store, db_path, state, provenance, **how)
    obs = observation(dispatch_id or uuid.uuid4(), status)
    before = dump(db_path)
    kind, result, after_state = expected

    if kind == 'ERR':
        with pytest.raises(result):
            apply(store, mission_id, obs)
        assert dump(db_path) == before
        return

    record = apply(store, mission_id, obs)

    assert (record.outcome, store.get_step(mission_id, 'a').state) == (result, after_state)
    assert record.step_state_before is state and record.observed_status is status
    after = dump(db_path)
    # One evidence row, and nothing else changed but the step this evidence is about.
    assert after['recovery_evidence'] != before['recovery_evidence']
    assert len(after['recovery_evidence']) == 1
    for name in ALL_TABLES:
        if name not in ('mission_steps', 'recovery_evidence'):
            assert after[name] == before[name], name
    assert len(after['mission_steps']) == len(before['mission_steps'])
    other_before = [r for r in before['mission_steps'] if r[1] == 'b']
    assert [r for r in after['mission_steps'] if r[1] == 'b'] == other_before
    (old,) = [r for r in before['mission_steps'] if r[1] == 'a']
    (new,) = [r for r in after['mission_steps'] if r[1] == 'a']
    if after_state is state:
        assert new == old
    else:
        stamp = record.recorded_at.isoformat(timespec='microseconds')
        assert new == (old[0], old[1], old[2], after_state.value, old[4], stamp)


def mission_row(db_path, mission_id):
    return raw(db_path, 'SELECT * FROM missions WHERE mission_id = ?', mission_id)


# G. The direct anomaly: a prepared step, no authorization, and a terminal result

@pytest.mark.parametrize('status', TERMINAL)
def test_a_prepared_step_with_no_authorization_is_held_when_a_terminal_result_arrives(
        store, db_path, status):
    mission_id, dispatch_id = build(store, db_path, S.DISPATCH_PREPARED, 'MISSING')
    mission_before = mission_row(db_path, mission_id)
    other_before = step_row(db_path, mission_id, 'b')

    record = apply(store, mission_id, observation(dispatch_id, status, code=1, message='seen'))

    assert record.outcome is Outcome.UNPROVEN_AUTHORIZATION
    assert record.step_state_before is S.DISPATCH_PREPARED
    assert store.get_step(mission_id, 'a').state is S.OUTCOME_UNKNOWN
    # Neither an attempt nor the mission was touched, and no authorization appeared.
    assert mission_row(db_path, mission_id) == mission_before
    assert store.get_by_mission_id(mission_id).attempts_used == 0
    assert store.get_by_mission_id(mission_id).state is M.RUNNING
    assert raw(db_path, 'SELECT COUNT(*) FROM dispatch_authorizations') == [(0,)]
    assert step_row(db_path, mission_id, 'b') == other_before
    assert step_row(db_path, mission_id)[0][5] == record.recorded_at.isoformat(
        timespec='microseconds')


def test_after_that_authorize_step_send_no_longer_authorizes_the_same_dispatch(
        store, db_path):
    mission_id, dispatch_id = build(store, db_path, S.DISPATCH_PREPARED, 'MISSING')
    apply(store, mission_id, observation(dispatch_id, SUC))
    before = dump(db_path)

    with pytest.raises(StepNotAwaitingSend) as excinfo:
        store.authorize_step_send(mission_id, 'a')

    assert excinfo.value.step_state is S.OUTCOME_UNKNOWN
    assert dump(db_path) == before


def test_and_no_other_step_of_the_mission_can_be_authorized_either(store, db_path):
    mission_id, dispatch_id = build(store, db_path, S.DISPATCH_PREPARED, 'MISSING')
    apply(store, mission_id, observation(dispatch_id, CAN))
    before = dump(db_path)

    with pytest.raises(MissionReconciliationRequired) as excinfo:
        store.authorize_step_send(mission_id, 'b')

    # The mission is still RUNNING: one unknown step closes the mission-wide gate on its own.
    assert excinfo.value.step_keys == ('a',)
    assert store.get_by_mission_id(mission_id).state is M.RUNNING
    assert dump(db_path) == before
    assert step_row(db_path, mission_id, 'b')[0][3] == 'DISPATCH_PREPARED'


def test_the_move_to_unknown_uses_the_existing_ordinary_edge(store, db_path, monkeypatch):
    mission_id, dispatch_id = build(store, db_path, S.DISPATCH_PREPARED, 'MISSING')
    asked = []
    real = storage.transition_step

    def spy(current, target):
        asked.append((current, target))
        return real(current, target)

    monkeypatch.setattr(storage, 'transition_step', spy)

    apply(store, mission_id, observation(dispatch_id, SUC))

    assert asked == [(S.DISPATCH_PREPARED, S.OUTCOME_UNKNOWN)]
    assert transition_step(S.DISPATCH_PREPARED, S.OUTCOME_UNKNOWN) is S.OUTCOME_UNKNOWN


def test_a_held_step_has_no_ordinary_way_out(store, db_path):
    mission_id, dispatch_id = build(store, db_path, S.DISPATCH_PREPARED, 'MISSING')
    apply(store, mission_id, observation(dispatch_id, SUC))

    for target in (S.SUCCEEDED, S.FAILED, S.CANCELLED, S.ACCEPTED, S.CANCEL_REQUESTED):
        with pytest.raises(InvalidStateTransition):
            store.transition_step_state(mission_id, 'a', target)
    assert store.get_step(mission_id, 'a').state is S.OUTCOME_UNKNOWN


@pytest.mark.parametrize('route', [(), (M.CANCELLING,), (M.FAILED,)])
def test_the_move_to_unknown_does_not_depend_on_the_mission_state(store, db_path, route):
    mission_id, dispatch_id = build(store, db_path, S.DISPATCH_PREPARED, 'MISSING',
                                    after_route=route)
    before = mission_row(db_path, mission_id)

    record = apply(store, mission_id, observation(dispatch_id, ABO))

    assert record.outcome is Outcome.UNPROVEN_AUTHORIZATION
    assert store.get_step(mission_id, 'a').state is S.OUTCOME_UNKNOWN
    assert mission_row(db_path, mission_id) == before


# H. A held unknown step can never be settled by evidence

def test_repeated_observations_cannot_settle_a_step_that_was_never_authorized(store, db_path):
    mission_id, dispatch_id = build(store, db_path, S.DISPATCH_PREPARED, 'MISSING')
    sequence = [SUC, SUC, CAN, ABO, SUC, ABO, CAN, CAN, ABO, SUC]

    records = [apply(store, mission_id, observation(dispatch_id, status, code=index))
               for index, status in enumerate(sequence)]

    # Every one is kept, every one is unproven, and the step never leaves OUTCOME_UNKNOWN.
    assert [r.outcome for r in records] == [Outcome.UNPROVEN_AUTHORIZATION] * len(sequence)
    assert [r.step_state_before for r in records] == (
        [S.DISPATCH_PREPARED] + [S.OUTCOME_UNKNOWN] * (len(sequence) - 1))
    assert store.get_step(mission_id, 'a').state is S.OUTCOME_UNKNOWN
    assert evidence_count(db_path) == len(sequence)
    assert Outcome.APPLIED not in {r.outcome for r in store.get_recovery_evidence(mission_id, 'a')}
    assert raw(db_path, 'SELECT COUNT(*) FROM dispatch_authorizations') == [(0,)]


def test_no_supported_operation_gives_a_held_step_an_authorization(store, db_path):
    mission_id, dispatch_id = build(store, db_path, S.DISPATCH_PREPARED, 'MISSING')
    apply(store, mission_id, observation(dispatch_id, SUC))
    apply(store, mission_id, observation(dispatch_id, ABO))

    with pytest.raises(StepNotAwaitingSend):
        store.authorize_step_send(mission_id, 'a')
    with pytest.raises(InvalidStateTransition):
        store.prepare_step_dispatch(mission_id, 'a', ACTION)
    for status in TERMINAL:
        apply(store, mission_id, observation(dispatch_id, status))

    assert raw(db_path, 'SELECT COUNT(*) FROM dispatch_authorizations') == [(0,)]
    assert store.get_by_mission_id(mission_id).attempts_used == 0


def test_the_hold_comes_from_the_missing_authorization_and_not_from_evidence_rows(
        store, db_path):
    # A step that was unknown before anything was ever recorded for it is held all the same.
    mission_id, dispatch_id = build(store, db_path, S.OUTCOME_UNKNOWN, 'MISSING')
    assert evidence_count(db_path) == 0

    first = apply(store, mission_id, observation(dispatch_id, SUC))
    # And deleting every evidence row does not release it.
    raw(db_path, 'DELETE FROM recovery_evidence')
    second = apply(store, mission_id, observation(dispatch_id, SUC))

    assert (first.outcome, second.outcome) == (Outcome.UNPROVEN_AUTHORIZATION,) * 2
    assert store.get_step(mission_id, 'a').state is S.OUTCOME_UNKNOWN


def test_an_unproven_row_does_not_hold_a_step_that_has_an_authorization(store, db_path):
    mission_id, dispatch_id = build(store, db_path, S.OUTCOME_UNKNOWN, 'PRESENT')
    # Something outside the supported code writes an UNPROVEN row for a dispatch that has one.
    insert_raw(db_path, dispatch_id, outcome='UNPROVEN_AUTHORIZATION', before='OUTCOME_UNKNOWN')

    record = apply(store, mission_id, observation(dispatch_id, SUC))

    assert record.outcome is Outcome.APPLIED
    assert store.get_step(mission_id, 'a').state is S.SUCCEEDED


def test_a_later_authorization_looking_observation_does_not_release_a_held_step(
        store, db_path):
    mission_id, dispatch_id = build(store, db_path, S.OUTCOME_UNKNOWN, 'MISSING')
    apply(store, mission_id, observation(dispatch_id, ABO, code=9, message='first'))

    # Exactly what a properly authorized step would be settled by.
    record = apply(store, mission_id, observation(dispatch_id, SUC, code=0, message=''))

    assert record.outcome is Outcome.UNPROVEN_AUTHORIZATION
    assert store.get_step(mission_id, 'a').state is S.OUTCOME_UNKNOWN


@pytest.fixture
def v4_db(db_path):
    make_v4_database(db_path)
    with MissionStore(db_path) as opened:
        yield opened


def v4_dispatch(mission_id, key):
    return uuid.UUID(V3_DISPATCH_IDS[(mission_id, key)])


@pytest.mark.parametrize('status', TERMINAL)
@pytest.mark.parametrize('key', ['dispatched', 'accepted', 'executing', 'cancel_requested'])
def test_a_step_that_was_in_flight_when_version_4_migrated_keeps_its_state(
        v4_db, db_path, key, status):
    before_state = v4_db.get_step(V3_MAIN_ID, key).state
    mission_before = mission_row(db_path, V3_MAIN_ID)

    record = apply(v4_db, V3_MAIN_ID, observation(v4_dispatch(V3_MAIN_ID, key), status),
                   key=key)

    # Valid context, no authorization row: the result is recorded, and not applied.
    assert record.outcome is Outcome.UNPROVEN_AUTHORIZATION
    assert record.step_state_before is before_state
    assert v4_db.get_step(V3_MAIN_ID, key).state is before_state
    assert mission_row(db_path, V3_MAIN_ID) == mission_before
    assert v4_db.get_by_mission_id(V3_MAIN_ID).attempts_used == 3
    assert raw(db_path, 'SELECT COUNT(*) FROM dispatch_authorizations') == [(0,)]
    # It is still in flight, so its ordinary exits are still there.
    assert v4_db.transition_step_state(V3_MAIN_ID, key, S.CANCELLED).state is S.CANCELLED


@pytest.mark.parametrize('status', TERMINAL)
def test_a_migrated_version_4_unknown_step_is_held(v4_db, db_path, status):
    dispatch_id = v4_dispatch(V3_UNKNOWN_ID, 'unknown')
    mission_before = mission_row(db_path, V3_UNKNOWN_ID)

    first = apply(v4_db, V3_UNKNOWN_ID, observation(dispatch_id, status), key='unknown')
    second = apply(v4_db, V3_UNKNOWN_ID, observation(dispatch_id, status), key='unknown')

    assert (first.outcome, second.outcome) == (Outcome.UNPROVEN_AUTHORIZATION,) * 2
    assert v4_db.get_step(V3_UNKNOWN_ID, 'unknown').state is S.OUTCOME_UNKNOWN
    assert mission_row(db_path, V3_UNKNOWN_ID) == mission_before
    assert raw(db_path, 'SELECT COUNT(*) FROM dispatch_authorizations') == [(0,)]


def test_a_prepared_step_that_version_4_wrote_is_held_when_a_result_appears(v4_db, db_path):
    dispatch_id = v4_dispatch(V3_MAIN_ID, 'prepared')

    record = apply(v4_db, V3_MAIN_ID, observation(dispatch_id, SUC), key='prepared')

    assert record.outcome is Outcome.UNPROVEN_AUTHORIZATION
    assert record.step_state_before is S.DISPATCH_PREPARED
    assert v4_db.get_step(V3_MAIN_ID, 'prepared').state is S.OUTCOME_UNKNOWN
    assert v4_db.get_by_mission_id(V3_MAIN_ID).attempts_used == 3
    # The mission's other steps are blocked too, once one of them can be prepared to try.
    v4_db.prepare_step_dispatch(V3_MAIN_ID, 'pending', ACTION)
    with pytest.raises(MissionReconciliationRequired):
        v4_db.authorize_step_send(V3_MAIN_ID, 'pending')


@pytest.mark.parametrize(('key', 'status', 'outcome'), [
    ('succeeded', SUC, Outcome.ALREADY_SETTLED), ('succeeded', CAN, Outcome.CONFLICT),
    ('failed', ABO, Outcome.ALREADY_SETTLED), ('failed', SUC, Outcome.CONFLICT),
    ('cancelled', CAN, Outcome.ALREADY_SETTLED), ('cancelled', ABO, Outcome.CONFLICT)])
def test_a_migrated_version_4_finished_step_is_classified_by_task_007_alone(
        v4_db, db_path, key, status, outcome):
    step_before = step_row(db_path, V3_MAIN_ID, key)

    record = apply(v4_db, V3_MAIN_ID, observation(v4_dispatch(V3_MAIN_ID, key), status), key=key)

    assert record.outcome is outcome
    assert step_row(db_path, V3_MAIN_ID, key) == step_before


# I. A step that was authorized and then became unknown is settled by evidence

@pytest.mark.parametrize(('status', 'target'), [
    (SUC, S.SUCCEEDED), (CAN, S.CANCELLED), (ABO, S.FAILED)])
def test_an_authorized_unknown_step_is_settled_by_the_evidence_alone(
        store, db_path, status, target):
    mission_id, dispatch_id = build(store, db_path, S.OUTCOME_UNKNOWN, 'PRESENT')
    for other in (S.SUCCEEDED, S.FAILED, S.CANCELLED, S.ACCEPTED, S.EXECUTING):
        with pytest.raises(InvalidStateTransition):
            store.transition_step_state(mission_id, 'a', other)

    record = apply(store, mission_id, observation(dispatch_id, status))

    assert record.outcome is Outcome.APPLIED and record.step_state_before is S.OUTCOME_UNKNOWN
    assert store.get_step(mission_id, 'a').state is target
    # The way out is the evidence's alone: the ordinary graph has not gained an edge.
    for other in (S.SUCCEEDED, S.FAILED, S.CANCELLED, S.OUTCOME_UNKNOWN, S.ACCEPTED):
        if other is target:
            continue
        with pytest.raises(InvalidStateTransition):
            store.transition_step_state(mission_id, 'a', other)


def test_the_gate_opens_once_the_only_unknown_step_is_settled(store, db_path):
    mission_id, dispatch_id = build(store, db_path, S.OUTCOME_UNKNOWN, 'PRESENT')
    with pytest.raises(MissionReconciliationRequired):
        store.authorize_step_send(mission_id, 'b')

    apply(store, mission_id, observation(dispatch_id, SUC))

    assert store.authorize_step_send(mission_id, 'b').attempt_number == 2


# J. A step that is already terminal is never rewritten

@pytest.mark.parametrize('provenance', ['PRESENT', 'MISSING'])
@pytest.mark.parametrize('state', [S.SUCCEEDED, S.FAILED, S.CANCELLED])
@pytest.mark.parametrize('status', TERMINAL)
def test_a_terminal_step_is_neither_rewritten_nor_touched(
        store, db_path, state, provenance, status):
    mission_id, dispatch_id = build(store, db_path, state, provenance)
    step_before = step_row(db_path, mission_id)
    mission_before = mission_row(db_path, mission_id)

    record = apply(store, mission_id, observation(dispatch_id, status))

    assert record.outcome is settled(state)[TERMINAL.index(status)][1]
    assert record.step_state_before is state
    assert step_row(db_path, mission_id) == step_before
    assert mission_row(db_path, mission_id) == mission_before


def test_a_conflict_is_kept_and_a_later_matching_result_is_kept_too(store, db_path):
    mission_id, dispatch_id = build(store, db_path, S.DISPATCHED)
    applied_row = apply(store, mission_id, observation(dispatch_id, SUC))

    contradiction = apply(store, mission_id, observation(dispatch_id, ABO))
    match = apply(store, mission_id, observation(dispatch_id, SUC))

    assert (applied_row.outcome, contradiction.outcome, match.outcome) == (
        Outcome.APPLIED, Outcome.CONFLICT, Outcome.ALREADY_SETTLED)
    assert store.get_step(mission_id, 'a').state is S.SUCCEEDED
    assert evidence_count(db_path) == 3


# K. Correlation, context, and the shapes of older databases

CORRELATION = {
    'wrong dispatch': lambda d: observation(uuid.uuid4(), SUC),
    'no dispatch': lambda d: observation(None, SUC),
    'wrong action name': lambda d: observation(d, SUC, action_name='/navigate_to_pose2'),
    'action name without its slash': lambda d: observation(d, SUC, action_name='navigate_to_pose'),
    'no action name': lambda d: observation(d, SUC, action_name=None),
    'wrong action type': lambda d: observation(d, SUC, action_type='nav2_msgs/action/Other'),
    'no action type': lambda d: observation(d, SUC, action_type=None),
    'namespace without its slash': lambda d: observation(d, SUC, robot_namespace='robot1'),
    'namespace in another case': lambda d: observation(d, SUC, robot_namespace='/Robot1'),
    'empty namespace': lambda d: observation(d, SUC, robot_namespace=''),
    'slash namespace': lambda d: observation(d, SUC, robot_namespace='/'),
    'no namespace': lambda d: observation(d, SUC, robot_namespace=None),
    'nothing but the status': lambda d: RecoveryObservation(
        kind=ObservationKind.RESULT_RESPONSE, status=SUC),
}


@pytest.mark.parametrize('state', [S.DISPATCHED, S.OUTCOME_UNKNOWN, S.FAILED])
@pytest.mark.parametrize('label', list(CORRELATION))
def test_an_observation_that_does_not_correlate_is_refused_and_writes_nothing(
        store, db_path, label, state):
    mission_id, dispatch_id = build(store, db_path, state)
    before = dump(db_path)

    with pytest.raises(EvidenceNotCorrelated) as excinfo:
        apply(store, mission_id, CORRELATION[label](dispatch_id))

    assert (excinfo.value.mission_id, excinfo.value.step_key) == (mission_id, 'a')
    assert dump(db_path) == before


def test_a_wrong_uuid_result_against_a_settled_step_is_not_a_conflict(store, db_path):
    mission_id, dispatch_id = build(store, db_path, S.FAILED)

    with pytest.raises(EvidenceNotCorrelated):
        apply(store, mission_id, observation(uuid.uuid4(), SUC))

    assert evidence_count(db_path) == 0


def test_a_missing_context_is_refused_and_no_evidence_is_stored(store, db_path):
    mission_id, dispatch_id = build(store, db_path, S.DISPATCHED)
    raw(db_path, 'DELETE FROM dispatch_contexts WHERE dispatch_id = ?', str(dispatch_id))
    before = dump(db_path)

    with pytest.raises(DispatchContextMissing) as excinfo:
        apply(store, mission_id, observation(dispatch_id, SUC))

    assert (excinfo.value.mission_id, excinfo.value.step_key) == (mission_id, 'a')
    assert dump(db_path) == before


@pytest.mark.parametrize('damage', [
    "UPDATE dispatch_contexts SET action_name = ''",
    "UPDATE dispatch_contexts SET action_type = ''",
    "UPDATE dispatch_contexts SET robot_namespace = x'00'"])
def test_a_corrupt_context_is_refused_and_no_evidence_is_stored(store, db_path, damage):
    mission_id, dispatch_id = build(store, db_path, S.DISPATCHED)
    corrupt(db_path, damage)
    before = dump(db_path)

    with pytest.raises(CorruptDispatchContext):
        apply(store, mission_id, observation(dispatch_id, SUC))

    assert dump(db_path) == before


@pytest.mark.parametrize('key', ['prepared', 'dispatched', 'accepted', 'succeeded', 'failed'])
def test_a_dispatch_that_version_3_wrote_has_no_context_and_takes_no_evidence(db_path, key):
    make_v3_database(db_path)
    with MissionStore(db_path) as store:
        dispatch_id = v4_dispatch(V3_MAIN_ID, key)
        before = dump(db_path)

        with pytest.raises(DispatchContextMissing):
            apply(store, V3_MAIN_ID, observation(dispatch_id, SUC), key=key)

        assert dump(db_path) == before


def test_a_version_3_unknown_step_takes_no_evidence_either(db_path):
    make_v3_database(db_path)
    with MissionStore(db_path) as store:
        with pytest.raises(DispatchContextMissing):
            apply(store, V3_UNKNOWN_ID, observation(v4_dispatch(V3_UNKNOWN_ID, 'unknown'), SUC),
                  key='unknown')

        assert evidence_count(db_path) == 0


@pytest.mark.parametrize(('key', 'error'), [
    ('prepared', CorruptDispatchRecord), ('dispatched', CorruptDispatchRecord),
    ('accepted', CorruptDispatchRecord), ('executing', CorruptDispatchRecord),
    ('cancel_requested', CorruptDispatchRecord), ('succeeded', DispatchNotFound),
    ('failed', DispatchNotFound), ('cancelled', DispatchNotFound),
    ('pending', DispatchNotFound)])
def test_a_step_that_version_2_wrote_has_no_dispatch_and_takes_no_evidence(
        db_path, key, error):
    make_v2_database(db_path)
    with MissionStore(db_path) as store:
        before = dump(db_path)

        # In flight with no dispatch is a corruption, as it is for every other writer. A step
        # that never had one is simply not found.
        with pytest.raises(error):
            apply(store, V3_MAIN_ID, observation(uuid.uuid4(), SUC), key=key)

        assert dump(db_path) == before


def test_a_version_2_unknown_step_is_not_found(db_path):
    make_v2_database(db_path)
    with MissionStore(db_path) as store:
        with pytest.raises(DispatchNotFound):
            apply(store, V3_UNKNOWN_ID, observation(uuid.uuid4(), SUC), key='unknown')


def test_a_pending_step_has_no_dispatch_and_none_is_invented(store, db_path):
    mission_id, _ = build(store, db_path, S.PENDING)
    before = dump(db_path)

    with pytest.raises(DispatchNotFound) as excinfo:
        apply(store, mission_id, observation(uuid.uuid4(), SUC))

    assert (excinfo.value.mission_id, excinfo.value.step_key) == (mission_id, 'a')
    assert dump(db_path) == before
    assert raw(db_path, 'SELECT COUNT(*) FROM step_dispatches') == [(0,)]


@pytest.mark.parametrize('extras', [
    (), ('context',), ('authorization',), ('context', 'authorization')])
def test_a_pending_step_that_has_a_dispatch_is_a_corrupt_record_whatever_else_exists(
        store, db_path, extras):
    mission_id, _ = build(store, db_path, S.PENDING)
    dispatch_id = str(uuid.uuid4())
    raw(db_path, 'INSERT INTO step_dispatches VALUES (?, ?, ?, ?)',
        dispatch_id, mission_id, 'a', CANONICAL_TIME)
    if 'context' in extras:
        raw(db_path, 'INSERT INTO dispatch_contexts VALUES (?, ?, ?, ?)',
            dispatch_id, ACTION.action_name, ACTION.action_type, ACTION.robot_namespace)
    if 'authorization' in extras:
        raw(db_path, 'INSERT INTO dispatch_authorizations VALUES (?, ?)',
            dispatch_id, CANONICAL_TIME)
    before = dump(db_path)

    # Not made reconcilable, and not hidden behind a missing context.
    with pytest.raises(CorruptDispatchRecord) as excinfo:
        apply(store, mission_id, observation(uuid.UUID(dispatch_id), SUC))

    assert 'PENDING' in excinfo.value.reason
    assert dump(db_path) == before


def test_the_dispatch_and_its_provenance_are_judged_before_the_context(store, db_path):
    mission_id, dispatch_id = build(store, db_path, S.DISPATCH_PREPARED, 'PRESENT')
    raw(db_path, 'DELETE FROM dispatch_contexts')

    # A prepared step that has an authorization, and no context: the contradiction is reported.
    with pytest.raises(CorruptDispatchAuthorization):
        apply(store, mission_id, observation(dispatch_id, SUC))


def test_a_corrupt_authorization_is_reported_before_a_corrupt_context(store, db_path):
    mission_id, dispatch_id = build(store, db_path, S.DISPATCHED, 'CORRUPT')
    corrupt(db_path, "UPDATE dispatch_contexts SET action_name = ''")

    with pytest.raises(CorruptDispatchAuthorization):
        apply(store, mission_id, observation(dispatch_id, SUC))


def test_the_context_is_checked_before_correlation(store, db_path):
    mission_id, dispatch_id = build(store, db_path, S.DISPATCHED)
    raw(db_path, 'DELETE FROM dispatch_contexts')

    # The observation would not correlate either, and the missing context is what is reported.
    with pytest.raises(DispatchContextMissing):
        apply(store, mission_id, observation(uuid.uuid4(), SUC))


def test_the_mission_is_looked_up_before_the_step(store, db_path):
    mission_id, dispatch_id = build(store, db_path, S.DISPATCHED)

    with pytest.raises(MissionNotFound):
        apply(store, 'no-such-mission', observation(dispatch_id, SUC), key='no_such_step')
    with pytest.raises(StepNotFound):
        apply(store, mission_id, observation(dispatch_id, SUC), key='no_such_step')


def test_a_dispatch_id_that_is_not_canonical_is_a_corrupt_record(store, db_path):
    mission_id, dispatch_id = build(store, db_path, S.DISPATCHED)
    raw(db_path, 'UPDATE step_dispatches SET dispatch_id = upper(dispatch_id)'
                 " WHERE step_key = 'a'")
    before = dump(db_path)

    with pytest.raises(CorruptDispatchRecord):
        apply(store, mission_id, observation(dispatch_id, SUC))

    assert dump(db_path) == before


def test_a_step_with_two_dispatches_is_a_corrupt_record(store, db_path):
    mission_id, dispatch_id = build(store, db_path, S.DISPATCHED)
    # The UNIQUE constraint makes this impossible, so rebuild the table without it.
    raw(db_path, 'DROP TABLE step_dispatches')
    raw(db_path, 'CREATE TABLE step_dispatches'
                 ' (dispatch_id TEXT, mission_id TEXT, step_key TEXT, created_at TEXT)')
    for _ in range(2):
        raw(db_path, 'INSERT INTO step_dispatches VALUES (?, ?, ?, ?)',
            str(uuid.uuid4()), mission_id, 'a', CANONICAL_TIME)

    with pytest.raises(CorruptDispatchRecord):
        apply(store, mission_id, observation(dispatch_id, SUC))


# L. A replay is revalidated, so damaged metadata makes it fail closed

def test_a_replay_after_the_context_was_removed_fails_closed_and_the_evidence_stays_readable(
        store, db_path):
    mission_id, dispatch_id, evidence_id, obs, first = recorded(store, db_path)
    raw(db_path, 'DELETE FROM dispatch_contexts WHERE dispatch_id = ?', str(dispatch_id))
    before = dump(db_path)

    with pytest.raises(DispatchContextMissing):
        store.apply_recovery_observation(evidence_id, mission_id, 'a', obs)

    assert dump(db_path) == before
    assert store.get_recovery_evidence(mission_id, 'a') == (first,)


def test_a_replay_after_the_context_was_damaged_fails_closed(store, db_path):
    mission_id, dispatch_id, evidence_id, obs, first = recorded(store, db_path)
    corrupt(db_path, "UPDATE dispatch_contexts SET action_type = ''")

    with pytest.raises(CorruptDispatchContext):
        store.apply_recovery_observation(evidence_id, mission_id, 'a', obs)

    assert store.get_recovery_evidence(mission_id, 'a') == (first,)


def test_a_replay_after_the_context_was_changed_no_longer_correlates(store, db_path):
    mission_id, dispatch_id, evidence_id, obs, first = recorded(store, db_path)
    raw(db_path, "UPDATE dispatch_contexts SET action_name = '/somewhere_else'")

    with pytest.raises(EvidenceNotCorrelated):
        store.apply_recovery_observation(evidence_id, mission_id, 'a', obs)

    assert store.get_recovery_evidence(mission_id, 'a') == (first,)


def test_a_replay_after_the_authorization_was_damaged_fails_closed(store, db_path):
    mission_id, dispatch_id, evidence_id, obs, first = recorded(store, db_path)
    raw(db_path, "UPDATE dispatch_authorizations SET authorized_at = 'garbage'")

    with pytest.raises(CorruptDispatchAuthorization):
        store.apply_recovery_observation(evidence_id, mission_id, 'a', obs)

    assert store.get_recovery_evidence(mission_id, 'a') == (first,)


def test_a_replay_after_the_authorization_was_removed_still_replays(store, db_path):
    mission_id, dispatch_id, evidence_id, obs, first = recorded(store, db_path)
    # A missing authorization is no error: it is exactly the state a migrated database is in.
    raw(db_path, 'DELETE FROM dispatch_authorizations')

    assert store.apply_recovery_observation(evidence_id, mission_id, 'a', obs) == first


def test_when_the_dispatch_itself_is_damaged_the_reader_fails_too(store, db_path):
    mission_id, dispatch_id, evidence_id, obs, first = recorded(store, db_path)
    raw(db_path, 'UPDATE step_dispatches SET dispatch_id = upper(dispatch_id)'
                 " WHERE step_key = 'a'")

    with pytest.raises(CorruptDispatchRecord):
        store.apply_recovery_observation(evidence_id, mission_id, 'a', obs)
    with pytest.raises(CorruptDispatchRecord):
        store.get_recovery_evidence(mission_id, 'a')


# M. The reader

def test_a_step_with_no_evidence_has_an_empty_tuple(store, db_path):
    mission_id, dispatch_id = build(store, db_path, S.DISPATCHED)

    assert store.get_recovery_evidence(mission_id, 'a') == ()
    assert store.get_recovery_evidence(mission_id, 'b') == ()


def test_the_reader_returns_every_row_of_the_dispatch_and_only_those(store, db_path):
    mission_id, dispatch_id = build(store, db_path, S.EXECUTING)
    other_mission, other_dispatch = build(store, db_path, S.EXECUTING, request_id='req-002')
    ours = [apply(store, mission_id, observation(dispatch_id, status))
            for status in (SUC, ABO, CAN)]
    apply(store, other_mission, observation(other_dispatch, SUC))

    got = store.get_recovery_evidence(mission_id, 'a')

    assert isinstance(got, tuple) and set(got) == set(ours)
    assert all(r.dispatch_id == dispatch_id and r.mission_id == mission_id for r in got)


def test_the_order_is_by_evidence_id_and_is_not_chronological(store, db_path):
    mission_id, dispatch_id = build(store, db_path, S.EXECUTING, max_attempts=9)
    # Recorded in DECREASING order of ID, so the order they were recorded in is the reverse of
    # the order they come back in. That is the point: the order carries no chronology.
    ids = sorted((uuid.uuid4() for _ in range(6)), reverse=True)
    for evidence_id in ids:
        store.apply_recovery_observation(
            evidence_id, mission_id, 'a', observation(dispatch_id, SUC))

    got = store.get_recovery_evidence(mission_id, 'a')

    assert [str(r.evidence_id) for r in got] == sorted(str(i) for i in ids)
    assert [r.evidence_id for r in got] == ids[::-1]
    assert store.get_recovery_evidence(mission_id, 'a') == got


def test_the_reader_does_not_read_or_rely_on_the_wall_clock_or_the_rowid(monkeypatch, db_path):
    store, connection = open_faulty_store(monkeypatch, db_path)
    with store:
        mission_id, dispatch_id = build(store, db_path, S.EXECUTING)
        apply(store, mission_id, observation(dispatch_id, SUC))
        connection.statements.clear()

        store.get_recovery_evidence(mission_id, 'a')

        statements = [normalized(sql) for sql in connection.statements]
        query = [sql for sql in statements if 'FROM RECOVERY_EVIDENCE' in sql]
        assert len(query) == 1 and query[0].endswith('ORDER BY EVIDENCE_ID')
        assert 'ROWID' not in query[0] and 'RECORDED_AT' not in query[0]


def test_the_reader_only_reads_and_makes_no_claim_about_the_step_as_it_is_now(
        monkeypatch, db_path):
    store, connection = open_faulty_store(monkeypatch, db_path)
    with store:
        mission_id, dispatch_id = build(store, db_path, S.DISPATCHED)
        record = apply(store, mission_id, observation(dispatch_id, SUC))
        # Put the step back to something the evidence would never have left it in.
        raw(db_path, "UPDATE mission_steps SET step_state = 'DISPATCHED' WHERE step_key = 'a'")
        connection.statements.clear()

        assert store.get_recovery_evidence(mission_id, 'a') == (record,)

        assert {sql.split()[0].upper() for sql in connection.statements} == {'SELECT'}
        joined = ' '.join(normalized(sql) for sql in connection.statements)
        assert 'DISPATCH_CONTEXTS' not in joined and 'DISPATCH_AUTHORIZATIONS' not in joined


def test_the_reader_reports_a_missing_mission_step_and_dispatch(store, db_path):
    mission_id, dispatch_id = build(store, db_path, S.DISPATCHED)
    pending_mission, _ = build(store, db_path, S.PENDING, request_id='req-002')

    with pytest.raises(MissionNotFound):
        store.get_recovery_evidence('no-such-mission', 'a')
    with pytest.raises(StepNotFound):
        store.get_recovery_evidence(mission_id, 'no_such_step')
    with pytest.raises(DispatchNotFound):
        store.get_recovery_evidence(pending_mission, 'a')


def test_the_reader_finds_evidence_by_its_dispatch_and_so_never_sees_another_dispatchs(
        store, db_path):
    mission_id, dispatch_id, _, _, mine = recorded(store, db_path)
    other = uuid.uuid4()
    raw(db_path, 'UPDATE recovery_evidence SET dispatch_id = ?', str(other))

    # A row that names some other dispatch is not this step's evidence, whatever else is wrong.
    assert store.get_recovery_evidence(mission_id, 'a') == ()


def test_a_replay_whose_stored_dispatch_is_unreadable_is_a_corrupt_row(store, db_path):
    mission_id, dispatch_id, evidence_id, obs, first = recorded(store, db_path)
    raw(db_path, 'UPDATE recovery_evidence SET dispatch_id = upper(dispatch_id)')

    with pytest.raises(CorruptRecoveryEvidence) as excinfo:
        store.apply_recovery_observation(evidence_id, mission_id, 'a', obs)

    assert 'dispatch_id' in excinfo.value.reason


def test_one_corrupt_row_makes_the_whole_read_fail_and_nothing_is_repaired(store, db_path):
    mission_id, dispatch_id = build(store, db_path, S.EXECUTING)
    first = apply(store, mission_id, observation(dispatch_id, SUC))
    apply(store, mission_id, observation(dispatch_id, ABO))
    raw(db_path, "UPDATE recovery_evidence SET recorded_at = 'garbage' WHERE evidence_id = ?",
        str(first.evidence_id))
    before = dump(db_path)

    for _ in range(2):
        with pytest.raises(CorruptRecoveryEvidence) as excinfo:
            store.get_recovery_evidence(mission_id, 'a')

    assert excinfo.value.evidence_id == str(first.evidence_id)
    assert dump(db_path) == before


# Every stored field, damaged in every way that a valid row could not be

def damaged(store, db_path, sql, *params, state=S.DISPATCHED, **fields):
    mission_id, dispatch_id = build(store, db_path, state)
    apply(store, mission_id, observation(dispatch_id, SUC, **fields))
    corrupt(db_path, sql, *params)
    return mission_id


FIELD_DAMAGE = {
    'evidence_id in upper case': (
        'UPDATE recovery_evidence SET evidence_id = upper(evidence_id)',),
    'evidence_id that is not a UUID': ("UPDATE recovery_evidence SET evidence_id = 'not-a-uuid'",),
    'evidence_id without hyphens': (
        "UPDATE recovery_evidence SET evidence_id = replace(evidence_id, '-', '')",),
    'evidence_id as a blob': ("UPDATE recovery_evidence SET evidence_id = x'00'",),
    'status UNKNOWN': ("UPDATE recovery_evidence SET observed_status = 'UNKNOWN'",),
    'status EXECUTING': ("UPDATE recovery_evidence SET observed_status = 'EXECUTING'",),
    'status in lower case': ("UPDATE recovery_evidence SET observed_status = 'succeeded'",),
    'status that is a number': ('UPDATE recovery_evidence SET observed_status = 5',),
    'state before PENDING': ("UPDATE recovery_evidence SET step_state_before = 'PENDING'",),
    'state before unknown': ("UPDATE recovery_evidence SET step_state_before = 'NOPE'",),
    'outcome unknown': ("UPDATE recovery_evidence SET outcome = 'SETTLED'",),
    'APPLIED to a step that was terminal': (
        "UPDATE recovery_evidence SET step_state_before = 'SUCCEEDED'",),
    'APPLIED to a step that was prepared': (
        "UPDATE recovery_evidence SET step_state_before = 'DISPATCH_PREPARED'",),
    'ALREADY_SETTLED for a step that was not terminal': (
        "UPDATE recovery_evidence SET outcome = 'ALREADY_SETTLED'",),
    'CONFLICT for a step that was not terminal': (
        "UPDATE recovery_evidence SET outcome = 'CONFLICT'",),
    'UNPROVEN for a step that was terminal': (
        "UPDATE recovery_evidence SET outcome = 'UNPROVEN_AUTHORIZATION',"
        " step_state_before = 'FAILED'",),
    'a time in the naive form': (
        "UPDATE recovery_evidence SET recorded_at = '2026-03-04T05:06:07.123456'",),
    'a time in the Z form': (
        "UPDATE recovery_evidence SET recorded_at = '2026-03-04T05:06:07.123456Z'",),
    'a time with another offset': (
        "UPDATE recovery_evidence SET recorded_at = '2026-03-04T05:06:07.123456+01:00'",),
    'a time without microseconds': (
        "UPDATE recovery_evidence SET recorded_at = '2026-03-04T05:06:07+00:00'",),
    'a time with a space for the T': (
        "UPDATE recovery_evidence SET recorded_at = '2026-03-04 05:06:07.123456+00:00'",),
    'a time that is garbage': ("UPDATE recovery_evidence SET recorded_at = 'garbage'",),
    'a time as a blob': ("UPDATE recovery_evidence SET recorded_at = x'00'",),
}


@pytest.mark.parametrize('label', list(FIELD_DAMAGE))
def test_every_damaged_field_is_corruption_and_is_left_exactly_as_it_was(
        store, db_path, label):
    mission_id = damaged(store, db_path, *FIELD_DAMAGE[label], message='hi')
    before = dump(db_path)

    with pytest.raises(CorruptRecoveryEvidence) as excinfo:
        store.get_recovery_evidence(mission_id, 'a')

    assert excinfo.value.reason
    assert (excinfo.value.mission_id, excinfo.value.step_key) == (mission_id, 'a')
    assert dump(db_path) == before


# The record type itself

def make_record(**overrides):
    fields = {
        'evidence_id': uuid.uuid4(), 'dispatch_id': uuid.uuid4(), 'mission_id': 'm-1',
        'step_key': 'k', 'observed_status': SUC, 'result_code': 7,
        'result_message_prefix': 'hi', 'result_message_bytes': 2,
        'result_message_sha256': hashlib.sha256(b'hi').digest(),
        'step_state_before': S.DISPATCHED, 'outcome': Outcome.APPLIED,
        'recorded_at': datetime(2026, 3, 4, 5, 6, 7, 8, tzinfo=timezone.utc),
    }
    fields.update(overrides)
    return RecoveryEvidenceRecord(**fields)


def test_a_record_carries_what_it_was_given_and_is_frozen():
    identifier, dispatch = uuid.uuid4(), uuid.uuid4()

    record = make_record(evidence_id=identifier, dispatch_id=dispatch)

    assert (record.evidence_id, record.dispatch_id, record.result_code) == (
        identifier, dispatch, 7)
    assert record.result_message_truncated is False
    with pytest.raises(dataclasses.FrozenInstanceError):
        record.outcome = Outcome.CONFLICT
    assert [f.name for f in dataclasses.fields(RecoveryEvidenceRecord)] == [
        'evidence_id', 'dispatch_id', 'mission_id', 'step_key', 'observed_status', 'result_code',
        'result_message_prefix', 'result_message_bytes', 'result_message_sha256',
        'step_state_before', 'outcome', 'recorded_at']


def test_a_record_with_no_message_has_none_of_the_three_fields():
    record = make_record(result_message_prefix=None, result_message_bytes=None,
                         result_message_sha256=None, result_code=None)

    assert record.result_message_truncated is False and record.result_code is None


def test_a_record_reports_a_cut_message_as_truncated():
    record = make_record(result_message_prefix='a' * 1024, result_message_bytes=5000,
                         result_message_sha256=hashlib.sha256(b'anything').digest())

    assert record.result_message_truncated is True


@pytest.mark.parametrize('override', [
    {'evidence_id': 'x'}, {'evidence_id': str(uuid.uuid4())}, {'dispatch_id': None},
    {'mission_id': 5}, {'step_key': None}, {'observed_status': 'SUCCEEDED'},
    {'result_code': True}, {'result_code': 1.5}, {'result_code': '0x5'},
    {'result_message_prefix': b'hi'}, {'result_message_bytes': True},
    {'result_message_bytes': 2.0}, {'result_message_sha256': 'x' * 32},
    {'step_state_before': 'DISPATCHED'}, {'outcome': 'APPLIED'},
    {'recorded_at': CANONICAL_TIME}, {'recorded_at': None}])
def test_a_record_rejects_values_of_the_wrong_type(override):
    with pytest.raises(TypeError):
        make_record(**override)


@pytest.mark.parametrize('override', [
    {'mission_id': ''}, {'step_key': ''},
    {'observed_status': ActionStatus.UNKNOWN}, {'observed_status': ActionStatus.EXECUTING},
    {'result_message_prefix': None}, {'result_message_bytes': None},
    {'result_message_sha256': None}, {'result_message_sha256': b'x' * 31},
    {'result_message_prefix': 'a' * 1025, 'result_message_bytes': 1025},
    {'result_message_bytes': 1},
    {'result_message_sha256': hashlib.sha256(b'other').digest()},
    {'result_message_prefix': 'a' * 10, 'result_message_bytes': 5000},
    {'step_state_before': S.PENDING},
    {'outcome': Outcome.ALREADY_SETTLED}, {'outcome': Outcome.CONFLICT},
    {'outcome': Outcome.APPLIED, 'step_state_before': S.SUCCEEDED},
    {'outcome': Outcome.APPLIED, 'step_state_before': S.DISPATCH_PREPARED},
    {'outcome': Outcome.UNPROVEN_AUTHORIZATION, 'step_state_before': S.FAILED},
    {'recorded_at': datetime(2026, 3, 4)},
    {'recorded_at': datetime(2026, 3, 4, tzinfo=timezone(timedelta(hours=1)))}])
def test_a_record_rejects_values_that_cannot_describe_evidence(override):
    with pytest.raises(ValueError):
        make_record(**override)


@pytest.mark.parametrize(('outcome', 'before'), [
    (Outcome.APPLIED, S.DISPATCHED), (Outcome.APPLIED, S.ACCEPTED), (Outcome.APPLIED, S.EXECUTING),
    (Outcome.APPLIED, S.CANCEL_REQUESTED), (Outcome.APPLIED, S.OUTCOME_UNKNOWN),
    (Outcome.ALREADY_SETTLED, S.SUCCEEDED), (Outcome.ALREADY_SETTLED, S.FAILED),
    (Outcome.CONFLICT, S.CANCELLED),
    (Outcome.UNPROVEN_AUTHORIZATION, S.DISPATCH_PREPARED),
    (Outcome.UNPROVEN_AUTHORIZATION, S.DISPATCHED),
    (Outcome.UNPROVEN_AUTHORIZATION, S.OUTCOME_UNKNOWN)])
def test_every_coherent_outcome_and_state_before_builds(outcome, before):
    assert make_record(outcome=outcome, step_state_before=before).outcome is outcome


def test_the_outcome_values_are_the_persisted_ones():
    assert [(o.name, o.value) for o in EvidenceOutcome] == [
        ('APPLIED', 'APPLIED'), ('ALREADY_SETTLED', 'ALREADY_SETTLED'),
        ('CONFLICT', 'CONFLICT'), ('UNPROVEN_AUTHORIZATION', 'UNPROVEN_AUTHORIZATION')]


def test_the_new_errors_carry_their_facts():
    identifier = uuid.uuid4()
    not_recordable = ObservationNotRecordable('why', ObservationKind.LOCAL_TIMEOUT, None)
    not_correlated = EvidenceNotCorrelated('m-1', 'k')
    conflict = EvidenceIdConflict(identifier)
    damaged_row = CorruptRecoveryEvidence('m-1', 'k', 'stored-id', 'why', 'value')
    bare = CorruptRecoveryEvidence('m-1', 'k', 'stored-id', 'why')

    assert (not_recordable.reason, not_recordable.kind, not_recordable.status) == (
        'why', ObservationKind.LOCAL_TIMEOUT, None)
    assert (not_correlated.mission_id, not_correlated.step_key) == ('m-1', 'k')
    assert conflict.evidence_id == identifier and str(identifier) in str(conflict)
    assert (damaged_row.mission_id, damaged_row.step_key, damaged_row.evidence_id,
            damaged_row.reason, damaged_row.value) == ('m-1', 'k', 'stored-id', 'why', 'value')
    assert bare.value is None
    for error in (not_correlated, damaged_row):
        assert "'m-1'" in str(error) and "'k'" in str(error)
    classes = (ObservationNotRecordable, EvidenceNotCorrelated, EvidenceIdConflict,
               CorruptRecoveryEvidence)
    for one in classes:
        assert issubclass(one, Exception) and not issubclass(one, sqlite3.Error)
        for other in classes:
            assert one is other or not issubclass(one, other)


# Timestamps, and the clock

def test_the_time_is_canonical_utc_and_is_the_step_time_when_the_step_moves(store, db_path):
    mission_id, dispatch_id = build(store, db_path, S.DISPATCHED)

    record = apply(store, mission_id, observation(dispatch_id, SUC))

    [(stored,)] = raw(db_path, 'SELECT recorded_at FROM recovery_evidence')
    parsed = datetime.fromisoformat(stored)
    assert parsed.utcoffset() == timedelta(0) and parsed == record.recorded_at
    assert parsed.isoformat(timespec='microseconds') == stored
    assert store.get_step(mission_id, 'a').updated_at == record.recorded_at


def test_the_clock_is_read_once_for_a_write_and_never_for_a_replay(
        store, db_path, monkeypatch):
    mission_id, dispatch_id = build(store, db_path, S.DISPATCHED)
    readings = ['2031-01-02T03:04:05.000006+00:00', '2032-02-03T04:05:06.000007+00:00']
    calls = []

    def clock():
        calls.append(1)
        return readings[len(calls) - 1]

    monkeypatch.setattr(storage, '_utc_now', clock)
    evidence_id = uuid.uuid4()
    obs = observation(dispatch_id, SUC)

    record = store.apply_recovery_observation(evidence_id, mission_id, 'a', obs)

    assert len(calls) == 1
    assert record.recorded_at == datetime.fromisoformat(readings[0])
    assert raw(db_path, "SELECT updated_at FROM mission_steps WHERE step_key = 'a'") == [
        (readings[0],)]
    store.apply_recovery_observation(evidence_id, mission_id, 'a', obs)
    assert len(calls) == 1


def test_a_write_that_does_not_move_the_step_leaves_its_time_alone(store, db_path):
    mission_id, dispatch_id = build(store, db_path, S.SUCCEEDED)
    step_before = step_row(db_path, mission_id)

    record = apply(store, mission_id, observation(dispatch_id, SUC))

    assert step_row(db_path, mission_id) == step_before
    assert record.recorded_at > store.get_step(mission_id, 'a').updated_at


# The mission and the attempts

def test_settling_a_step_of_a_mission_that_needs_reconciliation_moves_nothing_else(
        store, db_path):
    mission_id, dispatch_id = build(store, db_path, S.OUTCOME_UNKNOWN, 'PRESENT',
                                    after_route=(M.NEEDS_RECONCILIATION,))
    before = mission_row(db_path, mission_id)

    apply(store, mission_id, observation(dispatch_id, SUC))

    assert store.get_step(mission_id, 'a').state is S.SUCCEEDED
    assert mission_row(db_path, mission_id) == before
    # And nothing ordinary can move the mission on. A later task has to.
    for target in (M.RUNNING, M.COMPLETED, M.FAILED, M.CANCELLED):
        with pytest.raises(InvalidStateTransition):
            store.transition_mission_state(mission_id, target)


@pytest.mark.parametrize('route', [(M.CANCELLING,), (M.CANCELLING, M.CANCELLED), (M.FAILED,)])
def test_late_evidence_is_recorded_and_applied_under_a_mission_that_has_ended_or_is_ending(
        store, db_path, route):
    mission_id, dispatch_id = build(store, db_path, S.EXECUTING, 'PRESENT', after_route=route)
    before = mission_row(db_path, mission_id)

    record = apply(store, mission_id, observation(dispatch_id, CAN))

    assert record.outcome is Outcome.APPLIED
    assert store.get_step(mission_id, 'a').state is S.CANCELLED
    assert mission_row(db_path, mission_id) == before


def test_evidence_needs_no_attempt_and_is_recorded_when_none_is_left(store, db_path):
    mission_id, dispatch_id = build(store, db_path, S.DISPATCHED, max_attempts=1)
    assert store.get_by_mission_id(mission_id).attempts_used == 1
    with pytest.raises(AttemptLimitReached):
        store.authorize_step_send(mission_id, 'b')

    record = apply(store, mission_id, observation(dispatch_id, SUC))

    assert record.outcome is Outcome.APPLIED
    assert store.get_by_mission_id(mission_id).attempts_used == 1
    assert store.get_by_mission_id(mission_id).max_attempts == 1


# N. Atomicity: a failure at any statement leaves nothing half done

def matching(test, nth=1):
    """Return a predicate that is true for the nth statement that the test matches."""
    seen = []

    def fails(sql):
        text = normalized(sql)
        if test(text):
            seen.append(text)
            return len(seen) == nth
        return False

    return fails


def reads(table):
    return lambda sql: sql.startswith('SELECT') and f'FROM {table}' in sql


BY_ID = reads('RECOVERY_EVIDENCE WHERE EVIDENCE_ID')
STAGES = {
    'begin': (lambda sql: sql.startswith('BEGIN'), 1),
    'read the mission': (lambda sql: sql.startswith('SELECT') and 'FROM MISSIONS ' in sql, 1),
    'read the step': (lambda sql: (
        sql.startswith('SELECT') and 'FROM MISSION_STEPS' in sql and 'STEP_KEY = ?' in sql), 1),
    'read the dispatch': (reads('STEP_DISPATCHES'), 1),
    'read the authorization': (reads('DISPATCH_AUTHORIZATIONS'), 1),
    'read the context': (reads('DISPATCH_CONTEXTS'), 1),
    'look the evidence id up': (BY_ID, 1),
    'insert the evidence': (lambda sql: sql.startswith('INSERT INTO RECOVERY_EVIDENCE'), 1),
    'update the step': (lambda sql: sql.startswith('UPDATE MISSION_STEPS'), 1),
    # The second read by ID is the read-back that builds the record.
    'read the evidence back': (BY_ID, 2),
    'commit': (lambda sql: sql == 'COMMIT', 1),
}
INSERT = 'INSERT INTO RECOVERY_EVIDENCE'
UPDATE = 'UPDATE MISSION_STEPS'
# What must already have run when a later stage fails: the rollback, not an early exit, is what
# leaves the evidence and the step as they were.
RAN_BEFORE = {
    'update the step': [INSERT],
    'read the evidence back': [INSERT, UPDATE],
    'commit': [INSERT, UPDATE],
}
# Both write the evidence and change the step: one applies the result, the other holds it.
HISTORIES = {
    'authorized': (S.DISPATCHED, 'PRESENT', S.SUCCEEDED, Outcome.APPLIED),
    'held': (S.DISPATCH_PREPARED, 'MISSING', S.OUTCOME_UNKNOWN, Outcome.UNPROVEN_AUTHORIZATION),
}


@pytest.mark.parametrize('history', list(HISTORIES))
@pytest.mark.parametrize('stage', list(STAGES))
def test_a_failure_before_the_commit_leaves_everything_as_it_was(
        monkeypatch, db_path, stage, history):
    state, provenance, after_state, outcome = HISTORIES[history]
    store, connection = open_faulty_store(monkeypatch, db_path)
    with store:
        mission_id, dispatch_id = build(store, db_path, state, provenance)
        before = dump(db_path)
        connection.statements.clear()
        test, nth = STAGES[stage]
        connection.fail_when = matching(test, nth)
        evidence_id = uuid.uuid4()

        with pytest.raises(sqlite3.OperationalError, match='injected failure'):
            store.apply_recovery_observation(
                evidence_id, mission_id, 'a', observation(dispatch_id, SUC))

        connection.fail_when = None
        assert not connection.in_transaction
        logged = [normalized(sql) for sql in connection.statements]
        assert len([sql for sql in logged if test(sql)]) >= nth, 'the stage was never reached'
        for prefix in RAN_BEFORE.get(stage, []):
            assert any(sql.startswith(prefix) for sql in logged), prefix
        # No evidence, the step as it was, and not a cell different anywhere.
        assert dump(db_path) == before
        assert evidence_count(db_path) == 0
        assert store.get_step(mission_id, 'a').state is state

        # And the store still works: the same call now records it whole.
        record = store.apply_recovery_observation(
            evidence_id, mission_id, 'a', observation(dispatch_id, SUC))
        assert record.outcome is outcome
        assert store.get_step(mission_id, 'a').state is after_state
        assert evidence_count(db_path) == 1


@pytest.mark.parametrize('history', list(HISTORIES))
def test_an_error_after_a_real_commit_returns_no_record_and_the_replay_returns_it(
        monkeypatch, db_path, history):
    state, provenance, after_state, outcome = HISTORIES[history]
    store, connection = open_faulty_store(monkeypatch, db_path)
    with store:
        mission_id, dispatch_id = build(store, db_path, state, provenance)
        evidence_id = uuid.uuid4()
        obs = observation(dispatch_id, SUC, code=4, message='m')

        def commit_then_fail(sql):
            if normalized(sql) == 'COMMIT':
                connection._real.execute('COMMIT')      # the commit really happens
                return True
            return False

        connection.fail_when = commit_then_fail
        with pytest.raises(sqlite3.OperationalError, match='injected failure'):
            store.apply_recovery_observation(evidence_id, mission_id, 'a', obs)
        connection.fail_when = None

        # The caller got an error and no record, and the whole of it is durable.
        assert store.get_step(mission_id, 'a').state is after_state
        [stored] = store.get_recovery_evidence(mission_id, 'a')
        assert stored.evidence_id == evidence_id and stored.outcome is outcome
        # The caller's retry of the same store operation returns it and changes nothing.
        before = dump(db_path)
        assert store.apply_recovery_observation(evidence_id, mission_id, 'a', obs) == stored
        assert dump(db_path) == before


def test_a_settled_step_is_not_written_and_a_failure_before_the_commit_keeps_it_so(
        monkeypatch, db_path):
    store, connection = open_faulty_store(monkeypatch, db_path)
    with store:
        mission_id, dispatch_id = build(store, db_path, S.FAILED)
        before = dump(db_path)
        connection.statements.clear()
        connection.fail_when = matching(lambda sql: sql == 'COMMIT')

        with pytest.raises(sqlite3.OperationalError):
            apply(store, mission_id, observation(dispatch_id, SUC))

        connection.fail_when = None
        assert not [sql for sql in connection.statements if normalized(sql).startswith(UPDATE)]
        assert dump(db_path) == before


def test_the_reads_come_in_the_reviewed_order_and_the_step_is_the_last_write(
        monkeypatch, db_path):
    store, connection = open_faulty_store(monkeypatch, db_path)
    with store:
        mission_id, dispatch_id = build(store, db_path, S.OUTCOME_UNKNOWN, 'PRESENT')
        connection.statements.clear()

        apply(store, mission_id, observation(dispatch_id, SUC))

    logged = [normalized(sql) for sql in connection.statements]
    assert logged[0] == 'BEGIN IMMEDIATE' and logged[-1] == 'COMMIT'
    assert logged.count('BEGIN IMMEDIATE') == logged.count('COMMIT') == 1

    def at(prefix, nth=1):
        return [i for i, sql in enumerate(logged) if sql.startswith(prefix)][nth - 1]

    # mission, step, dispatch, provenance, context, then the ID, then the two writes, then the
    # read-back. The evidence comes first and the step last.
    assert (at('SELECT * FROM MISSIONS') < at('SELECT * FROM MISSION_STEPS WHERE')
            < at('SELECT * FROM STEP_DISPATCHES') < at('SELECT * FROM DISPATCH_AUTHORIZATIONS')
            < at('SELECT * FROM DISPATCH_CONTEXTS')
            < at('SELECT * FROM RECOVERY_EVIDENCE WHERE EVIDENCE_ID', 1)
            < at(INSERT) < at(UPDATE)
            < at('SELECT * FROM RECOVERY_EVIDENCE WHERE EVIDENCE_ID', 2) < logged.index('COMMIT'))
    # Two writes and no more: never the mission, never an attempt, never an authorization.
    assert [' '.join(sql.split()[:3]) for sql in writes(logged)] == [
        'INSERT INTO RECOVERY_EVIDENCE', 'UPDATE MISSION_STEPS SET']
    assert not [sql for sql in logged
                if 'MISSIONS SET' in sql or 'DISPATCH_AUTHORIZATIONS (' in sql]


# Outcome. Real processes are killed at chosen statements of the real code

_CHILD = """
import os
import signal
import sqlite3
import sys
import uuid

from task_resilience import storage
from task_resilience.reconciliation import ActionStatus, ObservationKind, RecoveryObservation
from task_resilience.storage import MissionStore

db_path, mission_id, dispatch_id, evidence_id, status, when, marker, how = sys.argv[1:9]
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
observation = RecoveryObservation(
    kind=ObservationKind.RESULT_RESPONSE, dispatch_id=uuid.UUID(dispatch_id),
    action_name='/navigate_to_pose', action_type='nav2_msgs/action/NavigateToPose',
    robot_namespace='/robot1', status=ActionStatus[status], result_code=3, result_message='m')
record = store.apply_recovery_observation(uuid.UUID(evidence_id), mission_id, 'a', observation)
print(record.evidence_id, record.outcome.name, flush=True)
die()
"""

EXIT_CODE = {'exit': 0, 'sigkill': -signal.SIGKILL}
# Before COMMIT nothing is durable, whatever was written. At and after it, everything is.
KILL_POINTS = [
    ('BEGIN IMMEDIATE', 'before', 'exit', False),
    ('BEGIN IMMEDIATE', 'after', 'exit', False),         # the write lock held, nothing written
    (INSERT, 'before', 'exit', False),
    (INSERT, 'after', 'exit', False),                    # the evidence written, not the step
    (INSERT, 'after', 'sigkill', False),
    (UPDATE, 'before', 'exit', False),
    (UPDATE, 'after', 'exit', False),                    # both written, not committed
    (UPDATE, 'after', 'sigkill', False),
    ('COMMIT', 'before', 'exit', False),
    ('COMMIT', 'before', 'sigkill', False),
    ('COMMIT', 'after', 'exit', True),
    ('COMMIT', 'after', 'sigkill', True),
    ('-', '-', 'exit', True),                            # after the return, before anything else
    ('-', '-', 'sigkill', True),
]


def run_child(db_path, mission_id, dispatch_id, evidence_id, when, marker, how):
    return subprocess.run(
        [sys.executable, '-c', _CHILD, str(db_path), mission_id, str(dispatch_id),
         str(evidence_id), 'SUCCEEDED', when, marker, how],
        env=package_env(), capture_output=True, text=True, timeout=30)


@pytest.mark.parametrize('history', list(HISTORIES))
@pytest.mark.parametrize(('marker', 'when', 'how', 'committed'), KILL_POINTS)
def test_a_process_killed_around_the_write_leaves_all_of_it_or_none_of_it(
        db_path, history, marker, when, how, committed):
    state, provenance, after_state, outcome = HISTORIES[history]
    with MissionStore(db_path) as store:
        mission_id, dispatch_id = build(store, db_path, state, provenance)
        before = dump(db_path)
    evidence_id = uuid.uuid4()

    child = run_child(db_path, mission_id, dispatch_id, evidence_id, when, marker, how)

    assert child.returncode == EXIT_CODE[how]
    with MissionStore(db_path) as reopened:
        if committed:
            assert evidence_count(db_path) == 1
            assert reopened.get_step(mission_id, 'a').state is after_state
            assert child.stdout.strip() in ('', f'{evidence_id} {outcome.name}')
        else:
            # SQLite rolled the unfinished transaction back on reopening: not a cell different.
            assert dump(db_path) == before
            assert child.stdout == ''
        # No attempt was ever taken or given back, and no authorization appeared.
        assert reopened.get_by_mission_id(mission_id).attempts_used == (
            before['missions'][0][5])
        assert raw(db_path, 'SELECT COUNT(*) FROM dispatch_authorizations') == [
            (len(before['dispatch_authorizations']),)]
        # The caller's retry of the same store operation gives the same durable fact.
        record = reopened.apply_recovery_observation(
            evidence_id, mission_id, 'a', observation(dispatch_id, SUC, code=3, message='m'))
        assert record.outcome is outcome and record.evidence_id == evidence_id
        assert evidence_count(db_path) == 1
        assert reopened.get_step(mission_id, 'a').state is after_state


def test_the_held_step_is_still_unauthorizable_after_the_process_that_held_it_died(db_path):
    with MissionStore(db_path) as store:
        mission_id, dispatch_id = build(store, db_path, S.DISPATCH_PREPARED, 'MISSING')

    run_child(db_path, mission_id, dispatch_id, uuid.uuid4(), 'after', 'COMMIT', 'sigkill')

    with MissionStore(db_path) as reopened:
        with pytest.raises(StepNotAwaitingSend):
            reopened.authorize_step_send(mission_id, 'a')
        with pytest.raises(MissionReconciliationRequired):
            reopened.authorize_step_send(mission_id, 'b')
        assert reopened.get_by_mission_id(mission_id).attempts_used == 0


# P. Concurrency. Each worker opens its own store, as separate callers would

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
                try:
                    results.append(operation(store, worker, round_index))
                except Exception as error:
                    # The loser of a race is a result too, not a failure of the harness.
                    results.append(error)
        return results

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(run_worker, worker) for worker in range(workers)]
        return [future.result(timeout=TIMEOUT * 6) for future in futures]


def situations(db_path, count, state, provenance='PRESENT', **how):
    with MissionStore(db_path) as store:
        return [build(store, db_path, state, provenance, request_id=f'req-{n}', **how)
                for n in range(count)]


def evidence_of(db_path, mission_id):
    return raw(db_path, 'SELECT e.outcome, e.step_state_before, e.observed_status'
                        ' FROM recovery_evidence e JOIN step_dispatches d'
                        ' ON d.dispatch_id = e.dispatch_id WHERE d.mission_id = ?'
                        ' ORDER BY e.evidence_id', mission_id)


def final_state(db_path, mission_id, key='a'):
    with MissionStore(db_path) as store:
        return store.get_step(mission_id, key).state


def test_callers_with_the_same_id_and_the_same_facts_get_one_row_and_one_state_change(db_path):
    workers = 8
    rounds = situations(db_path, ROUNDS, S.DISPATCHED)
    ids = [uuid.uuid4() for _ in rounds]

    results = run_concurrently(
        db_path, workers, ROUNDS,
        lambda store, worker, n: store.apply_recovery_observation(
            ids[n], rounds[n][0], 'a', observation(rounds[n][1], SUC, code=1)))

    for n, (mission_id, _) in enumerate(rounds):
        got = [worker[n] for worker in results]
        assert not [g for g in got if isinstance(g, Exception)], f'round {n}: {got}'
        assert len(set(got)) == 1 and got[0].outcome is Outcome.APPLIED
        assert len(evidence_of(db_path, mission_id)) == 1
        assert final_state(db_path, mission_id) is S.SUCCEEDED


def test_callers_with_the_same_id_and_different_facts_get_one_row_and_the_rest_a_conflict(
        db_path):
    workers = 6
    rounds = situations(db_path, ROUNDS, S.DISPATCHED)
    ids = [uuid.uuid4() for _ in rounds]
    statuses = [SUC, ABO, CAN, SUC, ABO, CAN]

    results = run_concurrently(
        db_path, workers, ROUNDS,
        lambda store, worker, n: store.apply_recovery_observation(
            ids[n], rounds[n][0], 'a', observation(rounds[n][1], statuses[worker])))

    for n, (mission_id, _) in enumerate(rounds):
        got = [worker[n] for worker in results]
        [(outcome, before, status)] = evidence_of(db_path, mission_id)
        assert outcome == 'APPLIED' and before == 'DISPATCHED'
        for worker, result in enumerate(got):
            if statuses[worker].name == status:
                assert not isinstance(result, Exception), f'round {n} worker {worker}: {result}'
            else:
                assert isinstance(result, EvidenceIdConflict), f'round {n} worker {worker}'


def test_callers_with_different_ids_and_the_same_facts_each_get_a_row_and_one_applies(db_path):
    workers = 8
    rounds = situations(db_path, ROUNDS, S.DISPATCHED)

    results = run_concurrently(
        db_path, workers, ROUNDS,
        lambda store, worker, n: store.apply_recovery_observation(
            uuid.uuid4(), rounds[n][0], 'a', observation(rounds[n][1], SUC)))

    for n, (mission_id, _) in enumerate(rounds):
        got = [worker[n] for worker in results]
        assert not [g for g in got if isinstance(g, Exception)], f'round {n}: {got}'
        outcomes = sorted(r[0] for r in evidence_of(db_path, mission_id))
        # Nothing is folded onto another row: eight callers, eight rows, one of them applied.
        assert outcomes == ['ALREADY_SETTLED'] * (workers - 1) + ['APPLIED'], f'round {n}'
        assert final_state(db_path, mission_id) is S.SUCCEEDED


def test_contradictory_results_are_both_kept_and_only_the_first_is_applied(db_path):
    rounds = situations(db_path, ROUNDS, S.DISPATCHED)

    run_concurrently(
        db_path, 2, ROUNDS,
        lambda store, worker, n: store.apply_recovery_observation(
            uuid.uuid4(), rounds[n][0], 'a', observation(rounds[n][1], (SUC, ABO)[worker])))

    for n, (mission_id, _) in enumerate(rounds):
        rows = evidence_of(db_path, mission_id)
        assert sorted(r[0] for r in rows) == ['APPLIED', 'CONFLICT'], f'round {n}'
        applied = [r for r in rows if r[0] == 'APPLIED'][0]
        assert final_state(db_path, mission_id) is {
            'SUCCEEDED': S.SUCCEEDED, 'ABORTED': S.FAILED}[applied[2]]


def test_evidence_racing_an_ordinary_transition_has_one_consistent_outcome(db_path):
    rounds = situations(db_path, ROUNDS, S.DISPATCHED)

    def act(store, worker, n):
        if worker == 0:
            return store.apply_recovery_observation(
                uuid.uuid4(), rounds[n][0], 'a', observation(rounds[n][1], SUC))
        return store.transition_step_state(rounds[n][0], 'a', S.ACCEPTED)

    results = run_concurrently(db_path, 2, ROUNDS, act)

    for n, (mission_id, _) in enumerate(rounds):
        evidence, ordinary = results[0][n], results[1][n]
        assert evidence.outcome is Outcome.APPLIED and evidence.step_state_before in (
            S.DISPATCHED, S.ACCEPTED), f'round {n}'
        assert final_state(db_path, mission_id) is S.SUCCEEDED
        # Either the result settled the step first, and the acceptance was refused, or the
        # step was accepted first, and the result found it accepted.
        if isinstance(ordinary, InvalidStateTransition):
            assert evidence.step_state_before is S.DISPATCHED, f'round {n}'
        else:
            assert ordinary.state is S.ACCEPTED, f'round {n}'
            assert evidence.step_state_before is S.ACCEPTED, f'round {n}'


def test_evidence_racing_a_mission_transition_never_interferes_with_it(db_path):
    rounds = situations(db_path, ROUNDS, S.DISPATCHED)

    def act(store, worker, n):
        if worker == 0:
            return store.apply_recovery_observation(
                uuid.uuid4(), rounds[n][0], 'a', observation(rounds[n][1], CAN))
        return store.transition_mission_state(rounds[n][0], M.CANCELLING)

    results = run_concurrently(db_path, 2, ROUNDS, act)

    with MissionStore(db_path) as store:
        for n, (mission_id, _) in enumerate(rounds):
            assert not isinstance(results[0][n], Exception) and not isinstance(
                results[1][n], Exception)
            assert store.get_step(mission_id, 'a').state is S.CANCELLED
            assert store.get_by_mission_id(mission_id).state is M.CANCELLING


def test_evidence_racing_the_authorization_of_the_same_prepared_step(db_path):
    rounds = situations(db_path, ROUNDS, S.DISPATCH_PREPARED, 'MISSING')

    def act(store, worker, n):
        if worker == 0:
            return store.apply_recovery_observation(
                uuid.uuid4(), rounds[n][0], 'a', observation(rounds[n][1], SUC))
        try:
            return store.authorize_step_send(rounds[n][0], 'a')
        except (StepNotAwaitingSend, MissionReconciliationRequired) as refusal:
            return refusal

    results = run_concurrently(db_path, 2, ROUNDS, act)

    with MissionStore(db_path) as store:
        for n, (mission_id, dispatch_id) in enumerate(rounds):
            evidence, authorization = results[0][n], results[1][n]
            attempts = store.get_by_mission_id(mission_id).attempts_used
            authorizations = raw(
                db_path, 'SELECT COUNT(*) FROM dispatch_authorizations WHERE dispatch_id = ?',
                str(dispatch_id))
            if isinstance(authorization, SendAuthorization):
                # Authorized first. The evidence then found an authorization, and applied.
                assert evidence.outcome is Outcome.APPLIED and attempts == 1, f'round {n}'
                assert authorizations == [(1,)]
                assert store.get_step(mission_id, 'a').state is S.SUCCEEDED
            else:
                # The evidence came first: the supported path can no longer authorize it.
                assert evidence.outcome is Outcome.UNPROVEN_AUTHORIZATION, f'round {n}'
                assert attempts == 0 and authorizations == [(0,)]
                assert isinstance(authorization, StepNotAwaitingSend)
                assert store.get_step(mission_id, 'a').state is S.OUTCOME_UNKNOWN


def test_concurrent_evidence_on_an_authorized_unknown_step_applies_once(db_path):
    rounds = situations(db_path, ROUNDS, S.OUTCOME_UNKNOWN)

    run_concurrently(
        db_path, 2, ROUNDS,
        lambda store, worker, n: store.apply_recovery_observation(
            uuid.uuid4(), rounds[n][0], 'a', observation(rounds[n][1], (ABO, CAN)[worker])))

    settled = {'ABORTED': ('FAILED', S.FAILED), 'CANCELED': ('CANCELLED', S.CANCELLED)}
    for n, (mission_id, _) in enumerate(rounds):
        rows = evidence_of(db_path, mission_id)
        assert sorted(r[0] for r in rows) == ['APPLIED', 'CONFLICT'], f'round {n}'
        [applied] = [r for r in rows if r[0] == 'APPLIED']
        [contradicted] = [r for r in rows if r[0] == 'CONFLICT']
        # The first result settled the unknown step. The other found it settled, and disagreed.
        state_name, state = settled[applied[2]]
        assert applied[1] == 'OUTCOME_UNKNOWN' and contradicted[1] == state_name, f'round {n}'
        assert contradicted[2] != applied[2]
        assert final_state(db_path, mission_id) is state


def test_concurrent_evidence_on_a_held_unknown_step_can_never_apply(db_path):
    workers = 6
    rounds = situations(db_path, ROUNDS, S.DISPATCH_PREPARED, 'MISSING')
    statuses = [SUC, CAN, ABO, SUC, CAN, ABO]

    results = run_concurrently(
        db_path, workers, ROUNDS,
        lambda store, worker, n: store.apply_recovery_observation(
            uuid.uuid4(), rounds[n][0], 'a', observation(rounds[n][1], statuses[worker])))

    for n, (mission_id, _) in enumerate(rounds):
        got = [worker[n] for worker in results]
        assert not [g for g in got if isinstance(g, Exception)], f'round {n}: {got}'
        rows = evidence_of(db_path, mission_id)
        assert len(rows) == workers and {r[0] for r in rows} == {'UNPROVEN_AUTHORIZATION'}
        assert final_state(db_path, mission_id) is S.OUTCOME_UNKNOWN
    assert raw(db_path, 'SELECT COUNT(*) FROM dispatch_authorizations') == [(0,)]


def test_evidence_racing_the_removal_of_the_context_records_only_what_it_validated(db_path):
    rounds = situations(db_path, ROUNDS, S.DISPATCHED)

    def act(store, worker, n):
        if worker == 0:
            return store.apply_recovery_observation(
                uuid.uuid4(), rounds[n][0], 'a', observation(rounds[n][1], SUC))
        # Something outside the supported code removes the context, on its own connection.
        with contextlib.closing(
                sqlite3.connect(db_path, timeout=TIMEOUT, isolation_level=None)) as conn:
            conn.execute('DELETE FROM dispatch_contexts WHERE dispatch_id = ?',
                         (str(rounds[n][1]),))

    results = run_concurrently(db_path, 2, ROUNDS, act)

    for n, (mission_id, _) in enumerate(rounds):
        evidence = results[0][n]
        rows = evidence_of(db_path, mission_id)
        if isinstance(evidence, DispatchContextMissing):
            assert rows == [] and final_state(db_path, mission_id) is S.DISPATCHED
        else:
            assert not isinstance(evidence, Exception), f'round {n}: {evidence}'
            assert len(rows) == 1 and final_state(db_path, mission_id) is S.SUCCEEDED


# Q. What this task must not have changed

PRODUCTION = {path.name: ast.parse(path.read_text())
              for path in sorted(Path(task_resilience.__file__).resolve().parent.glob('*.py'))}


def store_methods():
    [store_class] = [node for node in PRODUCTION['storage.py'].body
                     if isinstance(node, ast.ClassDef) and node.name == 'MissionStore']
    return {node.name: node for node in store_class.body if isinstance(node, ast.FunctionDef)}


def module_functions():
    return {node.name: node for node in PRODUCTION['storage.py'].body
            if isinstance(node, ast.FunctionDef)}


def names_in(node):
    return {child.id for child in ast.walk(node) if isinstance(child, ast.Name)}


def attributes_in(node):
    return {(child.value.id, child.attr) for child in ast.walk(node)
            if isinstance(child, ast.Attribute) and isinstance(child.value, ast.Name)}


def string_constants(tree):
    return [child.value for child in ast.walk(tree)
            if isinstance(child, ast.Constant) and isinstance(child.value, str)]


def test_only_authorize_step_send_consumes_an_attempt():
    users = {name for name, method in store_methods().items() if '_CONSUME' in names_in(method)}
    users |= {name for name, function in module_functions().items()
              if '_CONSUME' in names_in(function)}

    assert users == {'authorize_step_send'}


def test_only_apply_recovery_observation_runs_the_statement_that_inserts_evidence():
    users = {name for name, method in store_methods().items()
             if '_INSERT_EVIDENCE' in names_in(method)}
    users |= {name for name, function in module_functions().items()
              if '_INSERT_EVIDENCE' in names_in(function)}

    assert users == {'apply_recovery_observation'}


def test_the_evidence_path_writes_no_mission_no_attempt_and_no_authorization():
    forbidden = {'_CONSUME', '_INSERT_AUTHORIZATION', '_SET_MISSION_STATE', '_INSERT',
                 '_INSERT_STEP', '_INSERT_DISPATCH', '_INSERT_CONTEXT', 'SendAuthorization',
                 'transition_mission', '_DISPATCH_AUTHORIZATIONS_DDL'}
    apply_names = names_in(store_methods()['apply_recovery_observation'])

    assert not apply_names & forbidden
    assert 'authorize_step_send' not in {
        child.attr for child in ast.walk(store_methods()['apply_recovery_observation'])
        if isinstance(child, ast.Attribute)}
    # The step is written by the one statement every ordinary transition uses, and nothing else.
    assert '_SET_STEP_STATE' in apply_names


def test_the_statements_that_name_the_evidence_table_are_the_create_two_reads_and_the_insert():
    naming = {}
    for filename, tree in PRODUCTION.items():
        for text in string_constants(tree):
            if 'recovery_evidence' in text.lower():
                naming.setdefault(filename, []).append(' '.join(text.split()).upper())

    assert set(naming) == {'storage.py'}
    statements = sorted(naming['storage.py'])
    assert len(statements) == 4
    verbs = sorted(sql.split()[0] for sql in statements)
    assert verbs == ['CREATE', 'INSERT', 'SELECT', 'SELECT']
    for sql in statements:
        if not sql.startswith('CREATE'):
            for word in ('UPDATE', 'DELETE', 'REPLACE', 'ON CONFLICT', 'OR IGNORE', 'UPSERT'):
                assert word not in sql, (word, sql)
    # The one DELETE in the create is the foreign key's cascade with the dispatch.
    create = [sql for sql in statements if sql.startswith('CREATE')][0]
    assert create.count('DELETE') == 1 and 'ON DELETE CASCADE' in create


def test_the_read_path_cannot_write_and_nothing_can_repair_a_row():
    write_statements = {
        target.id for node in PRODUCTION['storage.py'].body if isinstance(node, ast.Assign)
        for target in node.targets if isinstance(target, ast.Name)
        if target.id.startswith(('_INSERT', '_SET_', '_CONSUME', '_RENAME', '_COPY', '_DROP'))
        or target.id.endswith('_DDL')}
    assert {'_INSERT_EVIDENCE', '_CONSUME', '_SET_STEP_STATE', '_RECOVERY_EVIDENCE_DDL'} <= (
        write_statements)
    read_path = {
        'get_recovery_evidence': store_methods()['get_recovery_evidence'],
        '_replayed_evidence': store_methods()['_replayed_evidence'],
        '_to_evidence': module_functions()['_to_evidence'],
    }

    for name, function in read_path.items():
        assert not names_in(function) & write_statements, name
    public = [name for name in dir(MissionStore) if not name.startswith('_')]
    assert not [name for name in public if any(word in name.lower() for word in (
        'repair', 'backfill', 'delete_evidence', 'update_evidence', 'purge', 'compact'))]


def test_no_public_operation_accepts_a_reconciliation_decision_as_authority():
    for name, function in inspect.getmembers(MissionStore, inspect.isfunction):
        if name.startswith('_'):
            continue
        for parameter in inspect.signature(function).parameters.values():
            assert parameter.annotation is not ReconciliationDecision, name
            assert parameter.name not in ('decision', 'reconciliation', 'candidate'), name
    signature = inspect.signature(MissionStore.apply_recovery_observation)
    assert list(signature.parameters) == ['self', 'evidence_id', 'mission_id', 'step_key',
                                          'observation']
    assert signature.parameters['observation'].annotation is RecoveryObservation
    assert signature.return_annotation is RecoveryEvidenceRecord
    reader = inspect.signature(MissionStore.get_recovery_evidence)
    assert list(reader.parameters) == ['self', 'mission_id', 'step_key']


def test_a_decision_object_is_refused_as_an_observation(store):
    decision = ReconciliationDecision(
        kind=DecisionKind.TERMINAL_RESULT_CANDIDATE,
        reason=DecisionReason.DURABLE_STATE_NOT_TERMINAL, candidate_step_state=S.SUCCEEDED)

    with pytest.raises(TypeError):
        store.apply_recovery_observation(uuid.uuid4(), 'm', 'a', decision)


def test_storage_holds_no_status_to_state_mapping_of_its_own():
    tree = PRODUCTION['storage.py']
    # No ActionStatus member is ever named: the enum is only called, to read a stored value.
    assert not {attr for owner, attr in attributes_in(tree) if owner == 'ActionStatus'}
    apply_attributes = {attr for owner, attr in attributes_in(
        store_methods()['apply_recovery_observation']) if owner == 'StepState'}
    # The two states it names are the two it moves a step to for a reason of its own.
    assert apply_attributes == {'DISPATCH_PREPARED', 'OUTCOME_UNKNOWN'}


@pytest.mark.parametrize('history', ['authorized', 'unknown'])
def test_the_target_state_comes_from_the_decision_and_from_nowhere_else(
        store, db_path, monkeypatch, history):
    state = S.DISPATCHED if history == 'authorized' else S.OUTCOME_UNKNOWN
    mission_id, dispatch_id = build(store, db_path, state, 'PRESENT')
    # Task 007 is made to say FAILED for a SUCCEEDED result. If storage mapped the status itself
    # the step would end up SUCCEEDED.
    monkeypatch.setattr(
        storage, 'decide_reconciliation',
        lambda *args: ReconciliationDecision(
            kind=DecisionKind.TERMINAL_RESULT_CANDIDATE,
            reason=DecisionReason.DURABLE_STATE_NOT_TERMINAL, candidate_step_state=S.FAILED))

    record = apply(store, mission_id, observation(dispatch_id, SUC))

    assert record.outcome is Outcome.APPLIED and record.observed_status is SUC
    assert store.get_step(mission_id, 'a').state is S.FAILED


@pytest.mark.parametrize(('status', 'said', 'reason', 'outcome'), [
    # Real Task 007 would say CONFLICT for the first and ALREADY_SETTLED for the second.
    (SUC, DecisionKind.ALREADY_SETTLED, DecisionReason.RESULT_MATCHES_DURABLE_STATE,
     Outcome.ALREADY_SETTLED),
    (ABO, DecisionKind.EVIDENCE_CONFLICT, DecisionReason.RESULT_CONTRADICTS_DURABLE_STATE,
     Outcome.CONFLICT)])
def test_the_classification_of_a_settled_step_is_task_007s_and_only_task_007s(
        store, db_path, monkeypatch, status, said, reason, outcome):
    mission_id, dispatch_id = build(store, db_path, S.FAILED, 'PRESENT')
    step_before = step_row(db_path, mission_id)
    monkeypatch.setattr(
        storage, 'decide_reconciliation',
        lambda *args: ReconciliationDecision(kind=said, reason=reason))

    record = apply(store, mission_id, observation(dispatch_id, status))

    # Whatever Task 007 says is what is recorded, and a settled step is never written.
    assert record.outcome is outcome
    assert step_row(db_path, mission_id) == step_before


def test_the_ordinary_graph_has_no_way_out_of_an_unknown_step():
    for target in StepState:
        if target is S.OUTCOME_UNKNOWN:
            continue
        with pytest.raises(InvalidStateTransition):
            transition_step(S.OUTCOME_UNKNOWN, target)
    assert state_machine.is_step_terminal(S.OUTCOME_UNKNOWN) is False


def test_the_state_machine_and_the_decision_module_do_not_know_the_evidence():
    for module in (state_machine, reconciliation):
        text = Path(module.__file__).read_text()
        for word in ('recovery_evidence', 'RecoveryEvidenceRecord', 'apply_recovery_observation',
                     'EvidenceOutcome', 'UNPROVEN_AUTHORIZATION', 'dispatch_authorizations'):
            assert word not in text, (module.__name__, word)


def test_the_import_graph_has_one_new_edge_and_no_cycle():
    graph = {}
    for name in ('errors', 'state_machine', 'reconciliation', 'model', 'storage'):
        graph[name] = sorted({node.module for node in ast.walk(PRODUCTION[f'{name}.py'])
                              if isinstance(node, ast.ImportFrom) and node.level == 1})
    assert graph == {
        'errors': [],
        'state_machine': ['errors'],
        'reconciliation': ['state_machine'],
        'model': ['errors', 'reconciliation', 'state_machine'],
        'storage': ['errors', 'model', 'reconciliation', 'state_machine'],
    }

    def reaches(start, target, seen=()):
        return any(nxt == target or (nxt not in seen and reaches(nxt, target, (*seen, nxt)))
                   for nxt in graph[start])

    assert not [name for name in graph if reaches(name, name)]


def test_recording_evidence_does_no_network_process_or_ros_work(store, db_path, monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError('external work while recording evidence')

    mission_id, dispatch_id = build(store, db_path, S.OUTCOME_UNKNOWN)
    with monkeypatch.context() as patch:
        patch.setattr(socket, 'socket', forbidden)
        patch.setattr(subprocess, 'Popen', forbidden)
        patch.setattr(os, 'system', forbidden)

        apply(store, mission_id, observation(dispatch_id, SUC))
        store.get_recovery_evidence(mission_id, 'a')


def test_the_evidence_path_names_no_external_module():
    names = names_in(store_methods()['apply_recovery_observation'])

    assert not names & {'os', 'sys', 'time', 'socket', 'subprocess', 'urllib', 'http',
                        'requests', 'threading', 'rclpy'}


def test_the_message_prefix_bound_is_the_one_the_schema_enforces(db_path):
    MissionStore(db_path).close()
    stored = {name: sql for _, name, sql in schema_of(db_path)}

    assert MESSAGE_PREFIX_MAX_BYTES == 1024
    assert f'<= {MESSAGE_PREFIX_MAX_BYTES}' in stored['recovery_evidence']
    assert model.MESSAGE_PREFIX_MAX_BYTES is MESSAGE_PREFIX_MAX_BYTES
