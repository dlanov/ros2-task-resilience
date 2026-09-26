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

# Task 010: the durable record that supported code committed a send authorization.
#
# A row means an authorization was committed. It does not mean a request was sent, that
# anything received it, or that a result exists, and its time is the store's own clock. The
# attempt number is not kept. No test here derives one from a row, a count or an ordering.

import ast
from concurrent.futures import ThreadPoolExecutor
import contextlib
import dataclasses
from datetime import datetime, timedelta, timezone
import hashlib
import inspect
import os
from pathlib import Path
import re
import signal
import sqlite3
import subprocess
import sys
import threading
import uuid

from fault_injection import open_faulty_store
import pytest

import task_resilience
from task_resilience import reconciliation, state_machine, storage
from task_resilience.errors import (
    AttemptLimitReached,
    CorruptDispatchAuthorization,
    CorruptDispatchContext,
    CorruptDispatchRecord,
    DispatchAuthorizationMissing,
    DispatchContextMissing,
    DispatchNotFound,
    DispatchPreparationRequired,
    InvalidStateTransition,
    MissionNotFound,
    MissionNotRunning,
    MissionReconciliationRequired,
    SendAuthorizationRequired,
    StepNotAwaitingSend,
    StepNotFound,
)
from task_resilience.model import DispatchAuthorizationRecord, SendAuthorization
from task_resilience.state_machine import MissionState, StepState, transition_step
from task_resilience.storage import MissionStore
from test_dispatch_context import (
    legacy_rows,
    make_v1_database,
    make_v2_database,
    make_v3_database,
    V3_DISPATCH_IDS,
    V3_MAIN_ID,
    V3_MAIN_STEPS,
    V3_MISSION_STEPS_DDL,
    V3_MISSIONS_DDL,
    V3_STEP_DISPATCHES_DDL,
    V3_UNKNOWN_ID,
)

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

# The tables as version 7 defines them, written out here so a new column or table shows up.
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
V4_TABLES = ['missions', 'mission_steps', 'step_dispatches', 'dispatch_contexts']
V5_TABLES = [*V4_TABLES, 'dispatch_authorizations']
V6_TABLES = [*V5_TABLES, 'recovery_evidence']
NEW_TABLE = 'dispatch_authorizations'

# The three tables that versions 4, 5 and 6 added, written out in full.
V4_DISPATCH_CONTEXTS_DDL = """
CREATE TABLE dispatch_contexts (
    dispatch_id     TEXT NOT NULL PRIMARY KEY
                    REFERENCES step_dispatches (dispatch_id) ON DELETE CASCADE,
    action_name     TEXT NOT NULL CHECK (action_name <> ''),
    action_type     TEXT NOT NULL CHECK (action_type <> ''),
    robot_namespace TEXT NOT NULL
)
"""

V5_DISPATCH_AUTHORIZATIONS_DDL = """
CREATE TABLE dispatch_authorizations (
    dispatch_id   TEXT NOT NULL PRIMARY KEY
                  REFERENCES step_dispatches (dispatch_id) ON DELETE CASCADE,
    authorized_at TEXT NOT NULL
)
"""

V6_RECOVERY_EVIDENCE_DDL = """
CREATE TABLE recovery_evidence (
    evidence_id           TEXT NOT NULL PRIMARY KEY,
    dispatch_id           TEXT NOT NULL
                          REFERENCES step_dispatches (dispatch_id) ON DELETE CASCADE,
    observed_status       TEXT NOT NULL CHECK (
        observed_status IN ('SUCCEEDED', 'CANCELED', 'ABORTED')
    ),
    result_code           TEXT,
    result_message_prefix BLOB,
    result_message_bytes  INTEGER,
    result_message_sha256 BLOB,
    step_state_before     TEXT NOT NULL CHECK (step_state_before IN (
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
    outcome               TEXT NOT NULL CHECK (outcome IN (
        'APPLIED',
        'ALREADY_SETTLED',
        'CONFLICT',
        'UNPROVEN_AUTHORIZATION'
    )),
    recorded_at           TEXT NOT NULL,
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

WRITE_VERBS = ('INSERT', 'UPDATE', 'DELETE', 'REPLACE', 'CREATE', 'ALTER', 'DROP')

# A well-formed stored time, and the one form the store accepts.
CANONICAL = '2026-03-04T05:06:07.123456+00:00'

# The step states a step can reach after its send was authorized, by an ordinary route.
ROUTES_FROM_DISPATCHED = {
    S.DISPATCHED: [],
    S.ACCEPTED: [S.ACCEPTED],
    S.EXECUTING: [S.ACCEPTED, S.EXECUTING],
    S.CANCEL_REQUESTED: [S.ACCEPTED, S.CANCEL_REQUESTED],
    S.SUCCEEDED: [S.SUCCEEDED],
    S.FAILED: [S.FAILED],
    S.CANCELLED: [S.CANCELLED],
    S.OUTCOME_UNKNOWN: [S.OUTCOME_UNKNOWN],
}
PROGRESSED = list(ROUTES_FROM_DISPATCHED)

# An ordinary route to each mission state that is not RUNNING. COMPLETED has none while a
# step is still prepared, because completion needs every step to have succeeded.
NOT_RUNNING_ROUTES = {
    M.ACCEPTED: (),
    M.CANCELLING: (M.RUNNING, M.CANCELLING),
    M.COMPLETED: None,
    M.FAILED: (M.RUNNING, M.FAILED),
    M.CANCELLED: (M.CANCELLED,),
    # Entered through enter_mission_reconciliation, once a step is unknown: see reconciling().
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


def squashed(sql):
    return ' '.join(sql.split())


def normalized(sql):
    return ' '.join(sql.split()).upper()


def writes(statements):
    return [sql for sql in statements if sql.split()[0].upper() in WRITE_VERBS]


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


def reconciling(store, mission_id, key='return_to_base'):
    """Make a step of the mission unknown, and enter reconciliation because of it."""
    store.transition_step_state(mission_id, key, S.OUTCOME_UNKNOWN)
    store.enter_mission_reconciliation(mission_id)


def dispatch_id_of(store, mission_id, key):
    return store.get_step_dispatch(mission_id, key).dispatch_id


def authorization_rows(db_path):
    return raw(db_path, 'SELECT dispatch_id, authorized_at FROM dispatch_authorizations'
                        ' ORDER BY dispatch_id')


def stamp_of(moment):
    return moment.isoformat(timespec='microseconds')


def plant(db_path, dispatch_id, authorized_at=CANONICAL):
    """Write an authorization row directly, as something outside the supported code could."""
    raw(db_path, 'INSERT INTO dispatch_authorizations (dispatch_id, authorized_at)'
                 ' VALUES (?, ?)', str(dispatch_id), authorized_at)


def set_step_state(db_path, mission_id, key, state):
    raw(db_path, 'UPDATE mission_steps SET step_state = ? WHERE mission_id = ? AND step_key = ?',
        state.value, mission_id, key)


def move_along(store, mission_id, key, route):
    for state in route:
        store.transition_step_state(mission_id, key, state)


def package_env():
    package_root = Path(task_resilience.__file__).resolve().parent.parent
    return {**os.environ, 'PYTHONPATH': str(package_root)}


def file_digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


# A. The schema

def test_the_schema_is_exactly_version_7_and_version_5_added_one_table(db_path):
    MissionStore(db_path).close()

    assert storage.SCHEMA_VERSION == 7
    assert raw(db_path, 'PRAGMA user_version') == [(7,)]
    assert table_names(db_path) == set(COLUMNS)
    # Version 5 added the authorization table to version 4's, version 6 the evidence table and
    # version 7 the reconciliation table.
    assert set(V5_TABLES) - set(V4_TABLES) == {NEW_TABLE}
    assert len(COLUMNS) == len(V5_TABLES) + 2
    assert set(COLUMNS) - set(V5_TABLES) == {
        'recovery_evidence', 'mission_reconciliation_contexts'}
    for table, columns in COLUMNS.items():
        assert [row[1] for row in raw(db_path, f'PRAGMA table_info({table})')] == columns


def test_the_version_4_tables_are_exactly_what_version_4_created(db_path):
    MissionStore(db_path).close()

    stored = {name: sql for _, name, sql in schema_of(db_path)}
    for name, ddl in (
            ('missions', V3_MISSIONS_DDL), ('mission_steps', V3_MISSION_STEPS_DDL),
            ('step_dispatches', V3_STEP_DISPATCHES_DDL),
            ('dispatch_contexts', V4_DISPATCH_CONTEXTS_DDL)):
        assert squashed(stored[name]) == squashed(ddl), name


def test_the_authorization_table_is_the_dispatch_and_its_time_and_nothing_else(db_path):
    MissionStore(db_path).close()

    # (name, declared type, not null, default, primary key)
    assert [(row[1], row[2], row[3], row[4], row[5])
            for row in raw(db_path, 'PRAGMA table_info(dispatch_authorizations)')] == [
        ('dispatch_id', 'TEXT', 1, None, 1),
        ('authorized_at', 'TEXT', 1, None, 0),
    ]
    foreign_keys = raw(db_path, 'PRAGMA foreign_key_list(dispatch_authorizations)')
    # (id, seq, table, from, to, on update, on delete, match)
    assert [(row[2], row[3], row[4], row[6]) for row in foreign_keys] == [
        ('step_dispatches', 'dispatch_id', 'dispatch_id', 'CASCADE')]
    stored = {name: sql for _, name, sql in schema_of(db_path)}
    assert squashed(stored[NEW_TABLE]) == squashed(V5_DISPATCH_AUTHORIZATIONS_DDL)


def test_the_new_table_adds_no_index_of_its_own(db_path):
    MissionStore(db_path).close()

    assert raw(db_path, 'SELECT name FROM sqlite_master WHERE type = ? AND sql IS NOT NULL',
               'index') == []
    # Only the index that SQLite makes for the primary key.
    # (seq, name, unique, origin, partial)
    assert [(row[2], row[3]) for row in raw(
        db_path, 'PRAGMA index_list(dispatch_authorizations)')] == [(1, 'pk')]


@pytest.mark.parametrize('word', [
    'attempt', 'mission_id', 'step_key', 'server', 'evidence', 'result', 'retry', 'owner',
    'lease', 'fence', 'goal', 'hash', 'sent', 'accepted', 'monotonic', 'epoch', 'incarnation',
    'observation', 'body', 'revision'])
def test_no_attempt_server_evidence_retry_or_ownership_field_is_stored(db_path, word):
    MissionStore(db_path).close()

    stored = {name: sql for _, name, sql in schema_of(db_path)}
    assert word not in stored[NEW_TABLE].lower()
    # The authorization table stores no evidence. The only tables named for evidence or for
    # reconciliation are those of Tasks 011 and 012.
    assert sorted(name for name in table_names(db_path)
                  if any(part in name for part in ('evidence', 'observation', 'reconcil'))) == [
        'mission_reconciliation_contexts', 'recovery_evidence']


def test_the_record_is_removed_with_its_dispatch_and_with_its_mission(store, db_path):
    mission_id = make_mission(store)
    for key in KEYS:
        store.authorize_step_send(mission_id, key)
    first, second = (str(dispatch_id_of(store, mission_id, key)) for key in KEYS)

    with foreign_key_connection(db_path) as conn:
        conn.execute('DELETE FROM step_dispatches WHERE dispatch_id = ?', (first,))
        assert conn.execute('SELECT dispatch_id FROM dispatch_authorizations').fetchall() == [
            (second,)]
        conn.execute('DELETE FROM missions WHERE mission_id = ?', (mission_id,))
        assert conn.execute('SELECT COUNT(*) FROM dispatch_authorizations').fetchall() == [(0,)]


def test_a_record_needs_a_dispatch_and_a_dispatch_has_at_most_one_record(store, db_path):
    mission_id = make_mission(store)
    dispatch_id = dispatch_id_of(store, mission_id, 'checkpoint_a')

    with foreign_key_connection(db_path) as conn:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute('INSERT INTO dispatch_authorizations VALUES (?, ?)',
                         (str(uuid.uuid4()), CANONICAL))
        conn.execute('INSERT INTO dispatch_authorizations VALUES (?, ?)',
                     (str(dispatch_id), CANONICAL))
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute('INSERT INTO dispatch_authorizations VALUES (?, ?)',
                         (str(dispatch_id), CANONICAL))


def test_the_primary_key_gives_one_row_per_dispatch_but_not_physical_immutability(
        store, db_path):
    mission_id = make_mission(store)
    store.authorize_step_send(mission_id, 'checkpoint_a')

    # The schema refuses a second row, and it does not refuse a change to the row. Raw SQL
    # can change it, so what keeps it fixed is the supported code, and only that. The code
    # then refuses to read a row that no longer has the form it writes.
    raw(db_path, "UPDATE dispatch_authorizations SET authorized_at = 'changed by raw sql'")

    with pytest.raises(CorruptDispatchAuthorization):
        store.get_dispatch_authorization(mission_id, 'checkpoint_a')
    assert authorization_rows(db_path)[0][1] == 'changed by raw sql'


# B. A normal authorization

def test_authorizing_a_send_writes_one_record_and_takes_one_attempt(store, db_path):
    mission_id = make_mission(store, max_attempts=3)
    dispatch_id = dispatch_id_of(store, mission_id, 'checkpoint_a')
    before = dump(db_path)
    assert before[NEW_TABLE] == []
    assert store.get_by_mission_id(mission_id).attempts_used == 0

    receipt = store.authorize_step_send(mission_id, 'checkpoint_a')

    assert receipt.dispatch_id == dispatch_id
    assert authorization_rows(db_path) == [(str(dispatch_id), stamp_of(receipt.authorized_at))]
    assert store.get_by_mission_id(mission_id).attempts_used == 1
    assert store.get_step(mission_id, 'checkpoint_a').state is S.DISPATCHED


def test_the_dispatch_and_its_context_are_untouched_and_only_the_expected_rows_change(
        store, db_path):
    mission_id = make_mission(store, max_attempts=3)
    before = dump(db_path)

    receipt = store.authorize_step_send(mission_id, 'checkpoint_a')

    diff = table_diff(before, dump(db_path))
    assert set(diff) == {'missions', 'mission_steps', NEW_TABLE}
    assert len(diff['missions'][0]) == len(diff['missions'][1]) == 1
    assert len(diff['mission_steps'][0]) == len(diff['mission_steps'][1]) == 1
    assert diff[NEW_TABLE] == ([], [(str(receipt.dispatch_id), stamp_of(receipt.authorized_at))])
    # The other step is not touched at all, and neither is any dispatch or context.
    other = dispatch_id_of(store, mission_id, 'return_to_base')
    assert raw(db_path, 'SELECT COUNT(*) FROM dispatch_authorizations WHERE dispatch_id = ?',
               str(other)) == [(0,)]


def test_the_attempt_the_record_and_the_step_share_one_timestamp(store, db_path):
    mission_id = make_mission(store)

    receipt = store.authorize_step_send(mission_id, 'checkpoint_a')

    text = stamp_of(receipt.authorized_at)
    assert raw(db_path, 'SELECT updated_at FROM missions') == [(text,)]
    assert raw(db_path, "SELECT updated_at FROM mission_steps WHERE step_key = 'checkpoint_a'"
               ) == [(text,)]
    assert raw(db_path, 'SELECT authorized_at FROM dispatch_authorizations') == [(text,)]
    record = store.get_dispatch_authorization(mission_id, 'checkpoint_a')
    assert record.authorized_at == receipt.authorized_at
    assert store.get_by_mission_id(mission_id).updated_at == record.authorized_at
    assert store.get_step(mission_id, 'checkpoint_a').updated_at == record.authorized_at


def test_the_stored_time_is_canonical_utc_with_microseconds(store, db_path):
    mission_id = make_mission(store)

    store.authorize_step_send(mission_id, 'checkpoint_a')

    [(_, text)] = authorization_rows(db_path)
    parsed = datetime.fromisoformat(text)
    assert parsed.utcoffset() == timedelta(0)
    assert parsed.isoformat(timespec='microseconds') == text
    assert re.fullmatch(r'\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}\+00:00', text)


def test_the_reader_returns_the_record_the_authorization_committed(store):
    mission_id = make_mission(store)

    receipt = store.authorize_step_send(mission_id, 'checkpoint_a')
    record = store.get_dispatch_authorization(mission_id, 'checkpoint_a')

    assert record == DispatchAuthorizationRecord(
        dispatch_id=receipt.dispatch_id, mission_id=mission_id, step_key='checkpoint_a',
        authorized_at=receipt.authorized_at)
    assert [field.name for field in dataclasses.fields(record)] == [
        'dispatch_id', 'mission_id', 'step_key', 'authorized_at']
    with pytest.raises(dataclasses.FrozenInstanceError):
        record.step_key = 'other'


def test_the_receipt_keeps_its_shape(store):
    mission_id = make_mission(store, max_attempts=3)

    receipt = store.authorize_step_send(mission_id, 'checkpoint_a')

    assert [field.name for field in dataclasses.fields(SendAuthorization)] == [
        'mission_id', 'step_key', 'dispatch_id', 'attempt_number', 'max_attempts',
        'authorized_at']
    assert list(inspect.signature(MissionStore.authorize_step_send).parameters) == [
        'self', 'mission_id', 'step_key']
    assert inspect.signature(MissionStore.authorize_step_send).return_annotation is (
        SendAuthorization)
    assert (receipt.mission_id, receipt.step_key, receipt.attempt_number,
            receipt.max_attempts) == (mission_id, 'checkpoint_a', 1, 3)


def test_the_clock_is_read_once_and_the_receipt_carries_the_stored_value(
        store, db_path, monkeypatch):
    mission_id = make_mission(store)
    readings = ['2031-01-02T03:04:05.000006+00:00', '2032-02-03T04:05:06.000007+00:00']
    calls = []

    def clock():
        calls.append(1)
        return readings[len(calls) - 1]

    monkeypatch.setattr(storage, '_utc_now', clock)

    receipt = store.authorize_step_send(mission_id, 'checkpoint_a')

    assert len(calls) == 1
    assert receipt.authorized_at == datetime.fromisoformat(readings[0])
    assert authorization_rows(db_path) == [
        (str(receipt.dispatch_id), readings[0])]
    assert raw(db_path, 'SELECT updated_at FROM missions') == [(readings[0],)]


def test_each_authorization_gets_its_own_record_and_the_next_attempt_number(store, db_path):
    mission_id = make_mission(store, max_attempts=3)

    first = store.authorize_step_send(mission_id, 'checkpoint_a')
    second = store.authorize_step_send(mission_id, 'return_to_base')

    assert (first.attempt_number, second.attempt_number) == (1, 2)
    assert first.dispatch_id != second.dispatch_id
    assert authorization_rows(db_path) == sorted([
        (str(first.dispatch_id), stamp_of(first.authorized_at)),
        (str(second.dispatch_id), stamp_of(second.authorized_at))])
    assert store.get_dispatch_authorization(mission_id, 'return_to_base').authorized_at == (
        second.authorized_at)


def test_a_replay_is_refused_and_leaves_the_record_and_the_counter_alone(store, db_path):
    mission_id = make_mission(store, max_attempts=3)
    receipt = store.authorize_step_send(mission_id, 'checkpoint_a')
    before = dump(db_path)

    for _ in range(2):
        with pytest.raises(StepNotAwaitingSend) as excinfo:
            store.authorize_step_send(mission_id, 'checkpoint_a')
        assert excinfo.value.step_state is S.DISPATCHED

    assert dump(db_path) == before
    assert authorization_rows(db_path) == [
        (str(receipt.dispatch_id), stamp_of(receipt.authorized_at))]
    assert store.get_by_mission_id(mission_id).attempts_used == 1


@pytest.mark.parametrize('state', PROGRESSED)
def test_the_record_survives_every_ordinary_step_state_unchanged(store, db_path, state):
    mission_id = make_mission(store, max_attempts=3)
    receipt = store.authorize_step_send(mission_id, 'checkpoint_a')
    expected = [(str(receipt.dispatch_id), stamp_of(receipt.authorized_at))]

    move_along(store, mission_id, 'checkpoint_a', ROUTES_FROM_DISPATCHED[state])

    assert store.get_step(mission_id, 'checkpoint_a').state is state
    assert authorization_rows(db_path) == expected
    assert store.get_dispatch_authorization(mission_id, 'checkpoint_a').authorized_at == (
        receipt.authorized_at)
    assert store.get_by_mission_id(mission_id).attempts_used == 1


def test_preparing_replaying_and_cancelling_never_write_a_record(store, db_path):
    mission_id = make_mission(store, prepared=[])
    for key in KEYS:
        store.prepare_step_dispatch(mission_id, key, ACTION)
        store.prepare_step_dispatch(mission_id, key, ACTION)
    store.transition_step_state(mission_id, 'checkpoint_a', S.CANCELLED)
    store.transition_step_state(mission_id, 'return_to_base', S.FAILED)
    store.transition_mission_state(mission_id, M.FAILED)

    assert authorization_rows(db_path) == []


# C. The final attempt

def test_the_final_available_attempt_is_authorized_once_and_leaves_one_record(store, db_path):
    mission_id = make_mission(store, max_attempts=1)

    first = store.authorize_step_send(mission_id, 'checkpoint_a')
    before = dump(db_path)
    with pytest.raises(AttemptLimitReached):
        store.authorize_step_send(mission_id, 'return_to_base')

    assert first.attempt_number == first.max_attempts == 1
    assert dump(db_path) == before
    assert len(authorization_rows(db_path)) == 1
    with pytest.raises(DispatchAuthorizationMissing):
        store.get_dispatch_authorization(mission_id, 'return_to_base')
    assert store.get_step(mission_id, 'return_to_base').state is S.DISPATCH_PREPARED


def test_two_attempts_leave_exactly_two_records_and_the_third_step_none(store, db_path):
    mission_id = make_mission(store, max_attempts=2, keys=['a', 'b', 'c'])

    receipts = [store.authorize_step_send(mission_id, key) for key in ('a', 'b')]
    with pytest.raises(AttemptLimitReached):
        store.authorize_step_send(mission_id, 'c')

    assert [r.attempt_number for r in receipts] == [1, 2]
    assert authorization_rows(db_path) == sorted(
        (str(r.dispatch_id), stamp_of(r.authorized_at)) for r in receipts)
    assert store.get_by_mission_id(mission_id).attempts_used == 2
    assert store.get_step(mission_id, 'c').state is S.DISPATCH_PREPARED


# D. A refused authorization writes nothing

def refused(store, db_path, mission_id, error, key='checkpoint_a'):
    """Authorize, expect this error, and check that nothing at all was written."""
    before = dump(db_path)
    with pytest.raises(error) as excinfo:
        store.authorize_step_send(mission_id, key)
    assert dump(db_path) == before
    return excinfo.value


@pytest.mark.parametrize('state', list(NOT_RUNNING_ROUTES))
def test_a_mission_that_is_not_running_is_refused_and_writes_nothing(store, db_path, state):
    route = NOT_RUNNING_ROUTES[state]
    mission_id = make_mission(store, route=route or ())
    if state is M.NEEDS_RECONCILIATION:
        reconciling(store, mission_id)
    if route is None:
        raw(db_path, "UPDATE missions SET mission_state = 'COMPLETED' WHERE mission_id = ?",
            mission_id)

    error = refused(store, db_path, mission_id, MissionNotRunning)

    assert error.mission_state is state
    assert authorization_rows(db_path) == []


@pytest.mark.parametrize('state', [state for state in S if state is not S.DISPATCH_PREPARED])
def test_a_step_that_is_not_prepared_is_refused_and_no_record_is_backfilled(
        store, db_path, state):
    mission_id = make_mission(store, max_attempts=3)
    set_step_state(db_path, mission_id, 'checkpoint_a', state)
    if state is S.OUTCOME_UNKNOWN:
        # The unknown-outcome gate reads every step, so keep it out of this case.
        set_step_state(db_path, mission_id, 'return_to_base', S.PENDING)
        raw(db_path, 'DELETE FROM step_dispatches WHERE step_key = ?', 'return_to_base')

    error = refused(store, db_path, mission_id, StepNotAwaitingSend)

    assert error.step_state is state
    assert authorization_rows(db_path) == []


def test_a_mission_with_an_unknown_step_is_refused_and_writes_nothing(store, db_path):
    mission_id = make_mission(store)
    set_step_state(db_path, mission_id, 'return_to_base', S.OUTCOME_UNKNOWN)

    error = refused(store, db_path, mission_id, MissionReconciliationRequired)

    assert error.step_keys == ('return_to_base',)
    assert authorization_rows(db_path) == []


def test_a_prepared_step_without_a_dispatch_is_refused_and_writes_nothing(store, db_path):
    mission_id = make_mission(store)
    raw(db_path, 'DELETE FROM step_dispatches WHERE step_key = ?', 'checkpoint_a')

    refused(store, db_path, mission_id, CorruptDispatchRecord)

    assert authorization_rows(db_path) == []


def test_a_dispatch_id_that_is_not_canonical_is_refused_and_writes_nothing(store, db_path):
    mission_id = make_mission(store)
    raw(db_path, 'UPDATE step_dispatches SET dispatch_id = upper(dispatch_id)'
                 " WHERE step_key = 'checkpoint_a'")

    refused(store, db_path, mission_id, CorruptDispatchRecord)

    assert authorization_rows(db_path) == []


def test_a_missing_context_is_refused_and_writes_nothing(store, db_path):
    mission_id = make_mission(store)
    raw(db_path, 'DELETE FROM dispatch_contexts WHERE dispatch_id = ?',
        str(dispatch_id_of(store, mission_id, 'checkpoint_a')))

    error = refused(store, db_path, mission_id, DispatchContextMissing)

    assert (error.mission_id, error.step_key) == (mission_id, 'checkpoint_a')
    assert authorization_rows(db_path) == []


def test_a_corrupt_context_is_refused_and_writes_nothing(store, db_path):
    mission_id = make_mission(store)
    corrupt(db_path, "UPDATE dispatch_contexts SET action_name = ''")

    refused(store, db_path, mission_id, CorruptDispatchContext)

    assert authorization_rows(db_path) == []


# The precedence: each existing check still comes first, and the new one comes after the
# context and before the state machine and the budget.

def planted(store, db_path, max_attempts=3, keys=KEYS):
    """Return a mission whose first, still prepared, step already has an authorization row."""
    mission_id = make_mission(store, max_attempts=max_attempts, keys=keys)
    plant(db_path, dispatch_id_of(store, mission_id, keys[0]))
    return mission_id


@pytest.mark.parametrize('state', list(NOT_RUNNING_ROUTES))
def test_a_mission_that_is_not_running_is_reported_before_a_planted_row(
        store, db_path, state):
    route = NOT_RUNNING_ROUTES[state]
    mission_id = make_mission(store, route=route or ())
    if state is M.NEEDS_RECONCILIATION:
        reconciling(store, mission_id)
    if route is None:
        raw(db_path, "UPDATE missions SET mission_state = 'COMPLETED' WHERE mission_id = ?",
            mission_id)
    plant(db_path, dispatch_id_of(store, mission_id, 'checkpoint_a'))

    refused(store, db_path, mission_id, MissionNotRunning)


def test_a_step_that_is_not_prepared_is_reported_before_a_planted_row(store, db_path):
    mission_id = make_mission(store)
    plant(db_path, dispatch_id_of(store, mission_id, 'checkpoint_a'))
    set_step_state(db_path, mission_id, 'checkpoint_a', S.DISPATCHED)

    refused(store, db_path, mission_id, StepNotAwaitingSend)


def test_an_unknown_step_is_reported_before_a_planted_row(store, db_path):
    mission_id = planted(store, db_path)
    set_step_state(db_path, mission_id, 'return_to_base', S.OUTCOME_UNKNOWN)

    refused(store, db_path, mission_id, MissionReconciliationRequired)


def test_a_bad_dispatch_is_reported_before_a_planted_row(store, db_path):
    mission_id = make_mission(store)
    dispatch_id = dispatch_id_of(store, mission_id, 'checkpoint_a')
    raw(db_path, 'UPDATE step_dispatches SET dispatch_id = ? WHERE dispatch_id = ?',
        str(dispatch_id).upper(), str(dispatch_id))
    plant(db_path, str(dispatch_id).upper())

    refused(store, db_path, mission_id, CorruptDispatchRecord)


def test_a_missing_context_is_reported_before_a_planted_row(store, db_path):
    mission_id = planted(store, db_path)
    raw(db_path, 'DELETE FROM dispatch_contexts')

    refused(store, db_path, mission_id, DispatchContextMissing)


def test_a_corrupt_context_is_reported_before_a_planted_row(store, db_path):
    mission_id = planted(store, db_path)
    corrupt(db_path, "UPDATE dispatch_contexts SET action_type = ''")

    refused(store, db_path, mission_id, CorruptDispatchContext)


def test_a_planted_row_is_reported_before_the_state_machine_is_asked(
        store, db_path, monkeypatch):
    mission_id = planted(store, db_path)

    def unreachable(current, target):
        raise AssertionError('the state machine was asked before the record was checked')

    monkeypatch.setattr(storage, 'transition_step', unreachable)

    refused(store, db_path, mission_id, CorruptDispatchAuthorization)


def test_a_planted_row_is_reported_before_the_budget_is_looked_at(store, db_path):
    mission_id = make_mission(store, max_attempts=1)
    store.authorize_step_send(mission_id, 'checkpoint_a')
    plant(db_path, dispatch_id_of(store, mission_id, 'return_to_base'))

    # The budget is spent, and the corrupt row is still what is reported.
    refused(store, db_path, mission_id, CorruptDispatchAuthorization, key='return_to_base')

    assert store.get_by_mission_id(mission_id).attempts_used == 1


def test_a_spent_budget_is_reported_when_there_is_no_planted_row(store, db_path):
    mission_id = make_mission(store, max_attempts=1)
    store.authorize_step_send(mission_id, 'checkpoint_a')

    refused(store, db_path, mission_id, AttemptLimitReached, key='return_to_base')


# E. A prepared step that already has a row

def test_a_prepared_step_with_a_row_is_corruption_and_nothing_changes(store, db_path):
    mission_id = make_mission(store, max_attempts=3)
    dispatch_id = dispatch_id_of(store, mission_id, 'checkpoint_a')
    plant(db_path, dispatch_id)
    before = dump(db_path)

    for _ in range(2):
        with pytest.raises(CorruptDispatchAuthorization) as excinfo:
            store.authorize_step_send(mission_id, 'checkpoint_a')

    error = excinfo.value
    assert (error.mission_id, error.step_key) == (mission_id, 'checkpoint_a')
    assert 'DISPATCH_PREPARED' in error.reason
    assert error.value == CANONICAL
    assert dump(db_path) == before
    assert store.get_by_mission_id(mission_id).attempts_used == 0
    assert store.get_step(mission_id, 'checkpoint_a').state is S.DISPATCH_PREPARED
    # The row is neither deleted nor replaced, and no earlier receipt is handed back.
    assert authorization_rows(db_path) == [(str(dispatch_id), CANONICAL)]


def test_a_step_that_was_authorized_and_then_put_back_is_never_authorized_again(
        store, db_path):
    mission_id = make_mission(store, max_attempts=3)
    receipt = store.authorize_step_send(mission_id, 'checkpoint_a')
    set_step_state(db_path, mission_id, 'checkpoint_a', S.DISPATCH_PREPARED)
    before = dump(db_path)

    with pytest.raises(CorruptDispatchAuthorization):
        store.authorize_step_send(mission_id, 'checkpoint_a')

    assert dump(db_path) == before
    assert store.get_by_mission_id(mission_id).attempts_used == 1
    assert authorization_rows(db_path) == [
        (str(receipt.dispatch_id), stamp_of(receipt.authorized_at))]


def test_a_planted_row_on_one_step_does_not_stop_another_step(store, db_path):
    mission_id = planted(store, db_path)
    planted_row = authorization_rows(db_path)

    receipt = store.authorize_step_send(mission_id, 'return_to_base')

    assert receipt.attempt_number == 1
    assert store.get_step(mission_id, 'return_to_base').state is S.DISPATCHED
    assert store.get_step(mission_id, 'checkpoint_a').state is S.DISPATCH_PREPARED
    # Its own row is new, and the planted one is exactly as it was.
    assert planted_row[0] in authorization_rows(db_path)
    assert len(authorization_rows(db_path)) == 2
    with pytest.raises(CorruptDispatchAuthorization):
        store.authorize_step_send(mission_id, 'checkpoint_a')
    assert store.get_by_mission_id(mission_id).attempts_used == 1


@pytest.mark.parametrize('text', ['garbage', '', CANONICAL.replace('+00:00', 'Z'), '5'])
def test_a_malformed_row_on_a_prepared_step_is_corruption_too(store, db_path, text):
    mission_id = make_mission(store)
    plant(db_path, dispatch_id_of(store, mission_id, 'checkpoint_a'), text)
    before = dump(db_path)

    with pytest.raises(CorruptDispatchAuthorization):
        store.authorize_step_send(mission_id, 'checkpoint_a')

    assert dump(db_path) == before


# F. The reader

def authorized(store, max_attempts=3):
    """Return a running mission whose first step has been authorized, and the receipt."""
    mission_id = make_mission(store, max_attempts=max_attempts)
    return mission_id, store.authorize_step_send(mission_id, 'checkpoint_a')


def test_a_prepared_step_that_was_never_authorized_has_no_record(store, db_path):
    mission_id = make_mission(store)
    before = dump(db_path)

    with pytest.raises(DispatchAuthorizationMissing) as excinfo:
        store.get_dispatch_authorization(mission_id, 'checkpoint_a')

    assert (excinfo.value.mission_id, excinfo.value.step_key) == (mission_id, 'checkpoint_a')
    assert dump(db_path) == before


@pytest.mark.parametrize('state', PROGRESSED)
def test_a_progressed_step_without_a_record_lacks_proof_and_is_not_corrupt(
        store, db_path, state):
    mission_id, _ = authorized(store)
    move_along(store, mission_id, 'checkpoint_a', ROUTES_FROM_DISPATCHED[state])
    # Removing the row is what a step authorized under version 4 looks like. Whether an
    # authorization was committed for it cannot be told, so the answer is "no record",
    # which is not "it did not happen".
    raw(db_path, 'DELETE FROM dispatch_authorizations')
    before = dump(db_path)

    for _ in range(2):
        with pytest.raises(DispatchAuthorizationMissing):
            store.get_dispatch_authorization(mission_id, 'checkpoint_a')

    assert dump(db_path) == before
    assert store.get_step(mission_id, 'checkpoint_a').state is state


def test_the_reader_tells_a_missing_mission_step_and_dispatch_apart(store):
    mission_id = make_mission(store, prepared=['checkpoint_a'])

    with pytest.raises(MissionNotFound):
        store.get_dispatch_authorization('no-such-mission', 'checkpoint_a')
    with pytest.raises(MissionNotFound):
        store.get_dispatch_authorization('no-such-mission', 'no_such_step')
    with pytest.raises(StepNotFound) as missing_step:
        store.get_dispatch_authorization(mission_id, 'no_such_step')
    with pytest.raises(DispatchNotFound) as missing_dispatch:
        store.get_dispatch_authorization(mission_id, 'return_to_base')

    assert (missing_step.value.mission_id, missing_step.value.step_key) == (
        mission_id, 'no_such_step')
    assert (missing_dispatch.value.mission_id, missing_dispatch.value.step_key) == (
        mission_id, 'return_to_base')


def test_a_prepared_step_whose_dispatch_row_is_gone_has_no_dispatch(store, db_path):
    mission_id = make_mission(store)
    raw(db_path, "DELETE FROM step_dispatches WHERE step_key = 'checkpoint_a'")

    with pytest.raises(DispatchNotFound):
        store.get_dispatch_authorization(mission_id, 'checkpoint_a')


@pytest.mark.parametrize('step_state', ['PENDING', 'DISPATCH_PREPARED', 'DISPATCHED'])
def test_a_duplicated_dispatch_is_a_dispatch_error_not_an_authorization_one(
        store, db_path, step_state):
    mission_id = make_mission(store)
    # The UNIQUE constraint makes this impossible, so rebuild the table without it.
    raw(db_path, 'DROP TABLE step_dispatches')
    raw(db_path, 'CREATE TABLE step_dispatches'
                 ' (dispatch_id TEXT, mission_id TEXT, step_key TEXT, created_at TEXT)')
    for _ in range(2):
        raw(db_path, 'INSERT INTO step_dispatches VALUES (?, ?, ?, ?)',
            str(uuid.uuid4()), mission_id, 'checkpoint_a', CANONICAL)
    raw(db_path, f"UPDATE mission_steps SET step_state = '{step_state}'"
                 " WHERE step_key = 'checkpoint_a'")

    with pytest.raises(CorruptDispatchRecord):
        store.get_dispatch_authorization(mission_id, 'checkpoint_a')


def test_a_dispatch_id_that_is_not_canonical_is_a_dispatch_error(store, db_path):
    mission_id, receipt = authorized(store)
    raw(db_path, 'UPDATE step_dispatches SET dispatch_id = upper(dispatch_id)')

    with pytest.raises(CorruptDispatchRecord):
        store.get_dispatch_authorization(mission_id, 'checkpoint_a')


@pytest.mark.parametrize('with_row', [False, True])
def test_a_pending_step_that_has_a_dispatch_stays_a_dispatch_corruption(
        store, db_path, with_row):
    mission_id = make_mission(store, prepared=[])
    dispatch_id = str(uuid.uuid4())
    raw(db_path, 'INSERT INTO step_dispatches (dispatch_id, mission_id, step_key, created_at)'
                 ' VALUES (?, ?, ?, ?)', dispatch_id, mission_id, 'checkpoint_a', CANONICAL)
    if with_row:
        plant(db_path, dispatch_id)
    before = dump(db_path)

    # It is not hidden behind the authorization: not a missing record, and not a valid one.
    with pytest.raises(CorruptDispatchRecord) as excinfo:
        store.get_dispatch_authorization(mission_id, 'checkpoint_a')

    assert 'PENDING' in excinfo.value.reason
    assert excinfo.value.value == dispatch_id
    assert dump(db_path) == before


def test_a_prepared_step_with_a_record_is_corruption_not_a_valid_record(store, db_path):
    mission_id = make_mission(store)
    plant(db_path, dispatch_id_of(store, mission_id, 'checkpoint_a'))
    before = dump(db_path)

    with pytest.raises(CorruptDispatchAuthorization) as excinfo:
        store.get_dispatch_authorization(mission_id, 'checkpoint_a')

    error = excinfo.value
    assert (error.mission_id, error.step_key) == (mission_id, 'checkpoint_a')
    assert 'DISPATCH_PREPARED' in error.reason
    assert error.value == CANONICAL
    assert dump(db_path) == before


def test_reading_writes_nothing_and_leaves_no_transaction_open(monkeypatch, db_path):
    with MissionStore(db_path) as setup:
        mission_id = make_mission(setup)
        setup.authorize_step_send(mission_id, 'checkpoint_a')
    store, connection = open_faulty_store(monkeypatch, db_path)
    with store:
        digest = file_digest(db_path)
        connection.statements.clear()

        for _ in range(3):
            assert store.get_dispatch_authorization(mission_id, 'checkpoint_a')
            with pytest.raises(DispatchAuthorizationMissing):
                store.get_dispatch_authorization(mission_id, 'return_to_base')
            with pytest.raises(MissionNotFound):
                store.get_dispatch_authorization('no-such-mission', 'checkpoint_a')
            assert not connection.in_transaction

        statements = list(connection.statements)
    assert writes(statements) == []
    assert {sql.split()[0].upper() for sql in statements} <= {'BEGIN', 'SELECT', 'ROLLBACK'}
    assert file_digest(db_path) == digest
    # No lock was left behind either.
    with contextlib.closing(sqlite3.connect(db_path, timeout=0.1, isolation_level=None)) as conn:
        conn.execute('BEGIN IMMEDIATE')
        conn.execute('ROLLBACK')


def test_reading_never_creates_repairs_or_replaces_a_record(store, db_path):
    mission_id, receipt = authorized(store)
    raw(db_path, "UPDATE dispatch_authorizations SET authorized_at = 'garbage'")
    other = dispatch_id_of(store, mission_id, 'return_to_base')

    for _ in range(3):
        with pytest.raises(CorruptDispatchAuthorization):
            store.get_dispatch_authorization(mission_id, 'checkpoint_a')
        with pytest.raises(DispatchAuthorizationMissing):
            store.get_dispatch_authorization(mission_id, 'return_to_base')

    assert authorization_rows(db_path) == [(str(receipt.dispatch_id), 'garbage')]
    assert raw(db_path, 'SELECT COUNT(*) FROM dispatch_authorizations WHERE dispatch_id = ?',
               str(other)) == [(0,)]


def test_a_read_cannot_be_torn_by_an_authorization_that_commits_between_its_statements(
        monkeypatch, db_path):
    store, connection = open_faulty_store(monkeypatch, db_path)
    with store, MissionStore(db_path, busy_timeout=0.05) as rival:
        mission_id = make_mission(store)
        attempts = []

        def rival_authorizes_between_the_reads(sql):
            # The step has been read as prepared. Here the rival tries to authorize and
            # commit before the record is read: were that possible, the read would see a
            # prepared step and its brand-new record, and report a corruption that never was.
            if normalized(sql).startswith('SELECT * FROM STEP_DISPATCHES') and not attempts:
                try:
                    attempts.append(rival.authorize_step_send(mission_id, 'checkpoint_a'))
                except sqlite3.OperationalError as error:
                    attempts.append(error)
            return False

        connection.fail_when = rival_authorizes_between_the_reads
        with pytest.raises(DispatchAuthorizationMissing):
            store.get_dispatch_authorization(mission_id, 'checkpoint_a')
        connection.fail_when = None

        # The read held one snapshot, so the rival could not commit inside it.
        assert len(attempts) == 1
        assert isinstance(attempts[0], sqlite3.OperationalError), attempts[0]
        assert authorization_rows(db_path) == []
        # Once the read is over the rival's authorization goes through, and is then read.
        receipt = rival.authorize_step_send(mission_id, 'checkpoint_a')
        assert store.get_dispatch_authorization(mission_id, 'checkpoint_a').authorized_at == (
            receipt.authorized_at)


def test_no_public_operation_accepts_the_record_as_authority():
    for name, function in inspect.getmembers(MissionStore, inspect.isfunction):
        if name.startswith('_'):
            continue
        for parameter in inspect.signature(function).parameters.values():
            assert parameter.annotation is not DispatchAuthorizationRecord, name
            assert parameter.name not in (
                'authorization', 'record', 'receipt', 'provenance'), name
    signature = inspect.signature(MissionStore.get_dispatch_authorization)
    assert list(signature.parameters) == ['self', 'mission_id', 'step_key']
    assert signature.return_annotation is DispatchAuthorizationRecord


def test_the_record_carries_no_attempt_number_and_no_send_or_result_claim():
    names = {field.name for field in dataclasses.fields(DispatchAuthorizationRecord)}

    assert names == {'dispatch_id', 'mission_id', 'step_key', 'authorized_at'}
    assert not names & {'attempt_number', 'attempt', 'max_attempts', 'sent_at', 'accepted_at',
                        'result', 'evidence', 'server_time'}


def make_record(**overrides):
    fields = {
        'dispatch_id': uuid.uuid4(), 'mission_id': 'm-1', 'step_key': 'k',
        'authorized_at': datetime(2026, 3, 4, 5, 6, 7, 8, tzinfo=timezone.utc),
    }
    fields.update(overrides)
    return DispatchAuthorizationRecord(**fields)


def test_a_record_carries_what_it_was_given():
    dispatch_id = uuid.uuid4()
    moment = datetime(2026, 3, 4, 5, 6, 7, 8, tzinfo=timezone.utc)

    record = make_record(dispatch_id=dispatch_id, authorized_at=moment)

    assert (record.dispatch_id, record.mission_id, record.step_key, record.authorized_at) == (
        dispatch_id, 'm-1', 'k', moment)
    with pytest.raises(dataclasses.FrozenInstanceError):
        record.mission_id = 'other'
    # A UTC time that is spelled differently is still UTC.
    make_record(authorized_at=datetime(2026, 1, 1, tzinfo=timezone(timedelta(0))))


@pytest.mark.parametrize('override', [
    {'dispatch_id': 'not-a-uuid'}, {'dispatch_id': str(uuid.uuid4())}, {'dispatch_id': None},
    {'mission_id': 5}, {'mission_id': None}, {'step_key': 5}, {'step_key': b'k'},
    {'authorized_at': CANONICAL}, {'authorized_at': None}, {'authorized_at': 1.5},
])
def test_a_record_rejects_values_of_the_wrong_type(override):
    with pytest.raises(TypeError):
        make_record(**override)


@pytest.mark.parametrize('override', [
    {'mission_id': ''}, {'step_key': ''},
    {'authorized_at': datetime(2026, 1, 1)},
    {'authorized_at': datetime(2026, 1, 1, tzinfo=timezone(timedelta(hours=1)))},
    {'authorized_at': datetime(2026, 1, 1, tzinfo=timezone(timedelta(hours=-5)))},
])
def test_a_record_rejects_values_that_cannot_describe_an_authorization(override):
    with pytest.raises(ValueError):
        make_record(**override)


def test_the_new_errors_carry_their_facts_and_are_distinct():
    missing = DispatchAuthorizationMissing('m-1', 'k')
    damaged = CorruptDispatchAuthorization('m-1', 'k', 'why', 'stored')
    bare = CorruptDispatchAuthorization('m-1', 'k', 'why')

    assert (missing.mission_id, missing.step_key) == ('m-1', 'k')
    assert (damaged.mission_id, damaged.step_key, damaged.reason, damaged.value) == (
        'm-1', 'k', 'why', 'stored')
    assert bare.value is None
    for error in (missing, damaged):
        assert "'m-1'" in str(error) and "'k'" in str(error)
    assert 'why' in str(damaged)
    for one, other in ((DispatchAuthorizationMissing, CorruptDispatchAuthorization),
                       (CorruptDispatchAuthorization, DispatchAuthorizationMissing),
                       (DispatchAuthorizationMissing, CorruptDispatchRecord),
                       (CorruptDispatchAuthorization, CorruptDispatchRecord)):
        assert not issubclass(one, other)


# G. The stored time is strictly canonical

MALFORMED_TEXT = {
    'garbage': 'garbage',
    'empty': '',
    'spaces': '   ',
    'date only': '2026-03-04',
    'naive': '2026-03-04T05:06:07.123456',
    'offset +01:00': '2026-03-04T05:06:07.123456+01:00',
    'offset -05:00': '2026-03-04T05:06:07.123456-05:00',
    'offset +00:01': '2026-03-04T05:06:07.123456+00:01',
    'Z': '2026-03-04T05:06:07.123456Z',
    'negative zero offset': '2026-03-04T05:06:07.123456-00:00',
    'no microseconds': '2026-03-04T05:06:07+00:00',
    'milliseconds': '2026-03-04T05:06:07.123+00:00',
    'seven digits': '2026-03-04T05:06:07.1234567+00:00',
    'space instead of T': '2026-03-04 05:06:07.123456+00:00',
    'lower case t': '2026-03-04t05:06:07.123456+00:00',
    'offset without colon': '2026-03-04T05:06:07.123456+0000',
    'offset hours only': '2026-03-04T05:06:07.123456+00',
    'basic format': '20260304T050607.123456+00:00',
    'week date': '2026-W10-3T05:06:07.123456+00:00',
    'leading space': ' ' + CANONICAL,
    'trailing space': CANONICAL + ' ',
    'trailing newline': CANONICAL + '\n',
    'trailing text': CANONICAL + 'x',
    'nul': CANONICAL + '\x00',
    'month 13': '2026-13-04T05:06:07.123456+00:00',
    'february 30': '2026-02-30T05:06:07.123456+00:00',
    'hour 24': '2026-03-04T24:00:00.000000+00:00',
    'an integer': '5',
    'a float': '5.5',
    'nan': 'nan',
    'a uuid': '7d4f6c3e-1b2a-4c5d-8e9f-0a1b2c3d4e5f',
    'many digits': '9' * 400,
    'an offset alone': '+00:00',
    'json': '{"authorized_at":1}',
    'a blob with the canonical bytes': CANONICAL.encode(),
    'a blob with garbage': b'\x00\xff',
}

ACCEPTED_TEXT = [
    CANONICAL,
    '2026-01-01T00:00:00.000000+00:00',
    '2000-02-29T12:00:00.000001+00:00',
    '1999-12-31T23:59:59.999999+00:00',
    # Nothing compares the time with another one: a wall clock can step backwards, so a value
    # before the mission existed and one far in the future are both just a canonical time.
    '0001-01-01T00:00:00.000000+00:00',
    '9999-12-31T23:59:59.999999+00:00',
]


def with_stored_time(store, db_path, value):
    """Authorize the first step, then overwrite the stored time as raw SQL could."""
    mission_id, receipt = authorized(store)
    raw(db_path, 'UPDATE dispatch_authorizations SET authorized_at = ?', value)
    return mission_id


@pytest.mark.parametrize('value', list(MALFORMED_TEXT.values()), ids=list(MALFORMED_TEXT))
def test_a_stored_time_that_is_not_canonical_is_refused_and_never_repaired(
        store, db_path, value):
    mission_id = with_stored_time(store, db_path, value)
    [(_, stored)] = authorization_rows(db_path)
    before = dump(db_path)

    for _ in range(2):
        with pytest.raises(CorruptDispatchAuthorization) as excinfo:
            store.get_dispatch_authorization(mission_id, 'checkpoint_a')

    error = excinfo.value
    assert (error.mission_id, error.step_key) == (mission_id, 'checkpoint_a')
    assert 'authorized_at' in error.reason
    assert error.value == stored
    assert dump(db_path) == before


@pytest.mark.parametrize('value', list(MALFORMED_TEXT.values()), ids=list(MALFORMED_TEXT))
def test_a_stored_time_that_is_not_canonical_on_a_prepared_step_is_corruption_too(
        store, db_path, value):
    mission_id = make_mission(store)
    plant(db_path, dispatch_id_of(store, mission_id, 'checkpoint_a'), value)
    before = dump(db_path)

    with pytest.raises(CorruptDispatchAuthorization):
        store.authorize_step_send(mission_id, 'checkpoint_a')

    assert dump(db_path) == before
    assert store.get_by_mission_id(mission_id).attempts_used == 0


@pytest.mark.parametrize('value', ACCEPTED_TEXT)
def test_exactly_the_canonical_utc_form_is_accepted_whatever_the_time(store, db_path, value):
    mission_id = with_stored_time(store, db_path, value)

    record = store.get_dispatch_authorization(mission_id, 'checkpoint_a')

    assert record.authorized_at == datetime.fromisoformat(value)
    assert record.authorized_at.utcoffset() == timedelta(0)
    assert stamp_of(record.authorized_at) == value


def test_random_and_mutated_text_is_accepted_only_when_it_is_canonical(store, db_path):
    import random
    rng = random.Random(20260304)
    alphabet = '0123456789-:.+TZt \n\x00abcdef'
    texts = [''.join(rng.choice(alphabet) for _ in range(rng.randint(0, 40)))
             for _ in range(150)]
    for _ in range(150):
        position = rng.randrange(len(CANONICAL))
        texts.append(CANONICAL[:position] + rng.choice(alphabet) + CANONICAL[position + 1:])
    mission_id = with_stored_time(store, db_path, CANONICAL)

    def canonical(text):
        # An oracle that shares no code with the store: the exact shape, and a real date.
        match = re.fullmatch(
            r'(\d{4})-(\d\d)-(\d\d)T(\d\d):(\d\d):(\d\d)\.(\d{6})\+00:00', text, re.ASCII)
        if not match:
            return False
        try:
            datetime(*(int(part) for part in match.groups()))
        except ValueError:
            return False
        return True

    for text in texts:
        raw(db_path, 'UPDATE dispatch_authorizations SET authorized_at = ?', text)
        if canonical(text):
            assert stamp_of(store.get_dispatch_authorization(
                mission_id, 'checkpoint_a').authorized_at) == text
        else:
            with pytest.raises(CorruptDispatchAuthorization):
                store.get_dispatch_authorization(mission_id, 'checkpoint_a')


# H. Migration

def make_v4_database(path):
    """Build a version 4 database: the version 3 fixture, plus a context for every dispatch."""
    make_v3_database(path)
    with contextlib.closing(sqlite3.connect(path, isolation_level=None)) as conn:
        conn.execute(V4_DISPATCH_CONTEXTS_DDL)
        for dispatch_id in sorted(V3_DISPATCH_IDS.values()):
            conn.execute(
                'INSERT INTO dispatch_contexts VALUES (?, ?, ?, ?)',
                (dispatch_id, ACTION.action_name, ACTION.action_type, ACTION.robot_namespace))
        conn.execute('PRAGMA user_version = 4')


def make_v5_database(path):
    """Build a version 5 database: the version 4 fixture plus the empty authorization table."""
    make_v4_database(path)
    with contextlib.closing(sqlite3.connect(path, isolation_level=None)) as conn:
        conn.execute(V5_DISPATCH_AUTHORIZATIONS_DDL)
        conn.execute('PRAGMA user_version = 5')


def make_v6_database(path):
    """Build a version 6 database: the version 5 fixture plus the empty evidence table."""
    make_v5_database(path)
    with contextlib.closing(sqlite3.connect(path, isolation_level=None)) as conn:
        conn.execute(V6_RECOVERY_EVIDENCE_DDL)
        conn.execute('PRAGMA user_version = 6')


ORIGINS = {
    'v1': make_v1_database,
    'v2': make_v2_database,
    'v3': make_v3_database,
    'v4': make_v4_database,
    'v5': make_v5_database,
    'v6': make_v6_database,
}
VERSION_OF = {'v1': 1, 'v2': 2, 'v3': 3, 'v4': 4, 'v5': 5, 'v6': 6}
MIGRATIONS = [
    '_migrate_v1_to_v2', '_migrate_v2_to_v3', '_migrate_v3_to_v4', '_migrate_v4_to_v5',
    '_migrate_v5_to_v6', '_migrate_v6_to_v7']


def old_rows(path, origin):
    """Return every row the older version held, over the columns that version had."""
    if origin == 'v6':
        return dump(path, V6_TABLES)
    if origin == 'v5':
        return dump(path, V5_TABLES)
    if origin == 'v4':
        return dump(path, V4_TABLES)
    return legacy_rows(path, origin)


def fresh_schema(tmp_path):
    fresh = tmp_path / 'fresh.db'
    MissionStore(fresh).close()
    return schema_of(fresh)


@pytest.mark.parametrize('origin', list(ORIGINS))
def test_every_older_database_migrates_to_version_7_and_invents_no_record(
        db_path, tmp_path, origin):
    ORIGINS[origin](db_path)
    before = old_rows(db_path, origin)

    MissionStore(db_path).close()

    assert raw(db_path, 'PRAGMA user_version') == [(7,)]
    assert old_rows(db_path, origin) == before
    assert table_names(db_path) == set(COLUMNS)
    assert raw(db_path, 'SELECT COUNT(*) FROM dispatch_authorizations') == [(0,)]
    assert raw(db_path, 'SELECT COUNT(*) FROM recovery_evidence') == [(0,)]
    assert raw(db_path, 'SELECT COUNT(*) FROM mission_reconciliation_contexts') == [(0,)]
    assert schema_of(db_path) == fresh_schema(tmp_path)


def test_a_fresh_database_is_created_at_version_7_with_no_record(db_path):
    MissionStore(db_path).close()

    assert raw(db_path, 'PRAGMA user_version') == [(7,)]
    assert table_names(db_path) == set(COLUMNS)
    assert raw(db_path, 'SELECT COUNT(*) FROM dispatch_authorizations') == [(0,)]
    assert raw(db_path, 'SELECT COUNT(*) FROM recovery_evidence') == [(0,)]
    assert raw(db_path, 'SELECT COUNT(*) FROM mission_reconciliation_contexts') == [(0,)]


@pytest.mark.parametrize(('origin', 'expected'), [
    ('v1', MIGRATIONS),
    ('v2', MIGRATIONS[1:]),
    ('v3', MIGRATIONS[2:]),
    ('v4', MIGRATIONS[3:]),
    ('v5', MIGRATIONS[4:]),
    ('v6', MIGRATIONS[5:]),
])
def test_each_origin_runs_exactly_the_steps_after_its_own_version(
        monkeypatch, db_path, origin, expected):
    ORIGINS[origin](db_path)
    called = []

    def spy_on(name):
        original = getattr(MissionStore, name)

        def spy(self):
            called.append(name)
            return original(self)

        monkeypatch.setattr(MissionStore, name, spy)

    for name in MIGRATIONS:
        spy_on(name)

    MissionStore(db_path).close()

    # A native database never goes through a step that an older version already ran.
    assert called == expected


def test_a_current_or_fresh_database_runs_no_migration_step(monkeypatch, db_path):
    called = []
    for name in MIGRATIONS:
        monkeypatch.setattr(MissionStore, name, lambda self, _name=name: called.append(_name))

    MissionStore(db_path).close()
    MissionStore(db_path).close()

    assert called == []


def test_the_version_4_migration_is_three_create_tables_and_only_the_last_version(
        monkeypatch, db_path):
    make_v4_database(db_path)

    store, connection = open_faulty_store(monkeypatch, db_path)
    store.close()

    statements = connection.statements
    changing = writes(statements)
    # The step to version 5 is still exactly one CREATE TABLE, and the steps to versions 6 and 7
    # one more each.
    assert len(changing) == 3
    assert changing[0].startswith('CREATE TABLE dispatch_authorizations (')
    assert changing[1].startswith('CREATE TABLE recovery_evidence (')
    assert changing[2].startswith('CREATE TABLE mission_reconciliation_contexts (')
    # No table was rebuilt: nothing dropped, renamed, altered, copied or filled, and the
    # version 3 step, which would fail on a native version 4 database, was not run.
    assert not [sql for sql in statements if sql.split()[0] in ('ALTER', 'DROP', 'INSERT')]
    assert not [sql for sql in statements if sql.startswith('CREATE TABLE dispatch_contexts')]
    assert statements.count('BEGIN IMMEDIATE') == 1
    assert statements.count('COMMIT') == 1
    # Versions 5 and 6 are never committed on the way.
    assert 'PRAGMA user_version = 5' not in statements
    assert 'PRAGMA user_version = 6' not in statements
    assert statements.count('PRAGMA user_version = 7') == 1
    assert (statements.index(changing[2]) < statements.index('PRAGMA user_version = 7')
            < statements.index('COMMIT'))


@pytest.mark.parametrize('origin', list(ORIGINS))
def test_reopening_a_migrated_database_changes_nothing(monkeypatch, db_path, origin):
    ORIGINS[origin](db_path)
    MissionStore(db_path).close()
    before = (dump(db_path), schema_of(db_path), file_digest(db_path))

    store, connection = open_faulty_store(monkeypatch, db_path)
    store.close()

    assert writes(connection.statements) == []
    assert (dump(db_path), schema_of(db_path), file_digest(db_path)) == before
    assert raw(db_path, 'PRAGMA user_version') == [(7,)]


def test_concurrent_opens_of_a_version_4_database_migrate_it_once(tmp_path):
    for round_index in range(5):
        path = tmp_path / f'v4-{round_index}.db'
        make_v4_database(path)
        before = dump(path, V4_TABLES)
        workers = 8
        barrier = threading.Barrier(workers, timeout=TIMEOUT)

        def open_and_close():
            barrier.wait()
            MissionStore(path).close()

        with ThreadPoolExecutor(max_workers=workers) as pool:
            for future in [pool.submit(open_and_close) for _ in range(workers)]:
                future.result(timeout=TIMEOUT)

        assert raw(path, 'PRAGMA user_version') == [(7,)]
        assert dump(path, V4_TABLES) == before
        assert raw(path, 'SELECT COUNT(*) FROM dispatch_authorizations') == [(0,)]
        assert raw(path, 'SELECT COUNT(*) FROM recovery_evidence') == [(0,)]
        assert raw(path, 'SELECT COUNT(*) FROM mission_reconciliation_contexts') == [(0,)]
        fresh = tmp_path / f'fresh-{round_index}.db'
        MissionStore(fresh).close()
        assert schema_of(path) == schema_of(fresh)


def assert_still_version_4(db_path, before, schema):
    assert raw(db_path, 'PRAGMA user_version') == [(4,)]
    assert table_names(db_path) == set(V4_TABLES)
    assert dump(db_path, V4_TABLES) == before
    assert schema_of(db_path) == schema
    # No lock was left behind.
    with contextlib.closing(sqlite3.connect(db_path, timeout=0.1, isolation_level=None)) as conn:
        conn.execute('BEGIN IMMEDIATE')
        conn.execute('ROLLBACK')


@pytest.mark.parametrize('failing', [
    'create the authorization table', 'create the evidence table',
    'create the reconciliation table', 'set the version', 'commit'])
def test_a_failure_while_migrating_from_version_4_rolls_back_cleanly(
        monkeypatch, db_path, failing):
    make_v4_database(db_path)
    before = dump(db_path, V4_TABLES)
    schema = schema_of(db_path)

    if failing.startswith('create'):
        ddl = {
            'create the authorization table': '_DISPATCH_AUTHORIZATIONS_DDL',
            'create the evidence table': '_RECOVERY_EVIDENCE_DDL',
            'create the reconciliation table': '_MISSION_RECONCILIATION_CONTEXTS_DDL',
        }[failing]
        with monkeypatch.context() as patch:
            patch.setattr(storage, ddl, 'THIS IS NOT SQL')
            with pytest.raises(sqlite3.OperationalError):
                MissionStore(db_path)
    else:
        fails = {
            'set the version': lambda sql: sql.strip() == 'PRAGMA user_version = 7',
            'commit': lambda sql: sql.strip().upper() == 'COMMIT',
        }[failing]
        with pytest.raises(sqlite3.OperationalError, match='injected failure'):
            open_faulty_store(monkeypatch, db_path, fail_when=fails)

    assert_still_version_4(db_path, before, schema)
    # A later, healthy open finishes the migration.
    with MissionStore(db_path):
        pass
    assert raw(db_path, 'PRAGMA user_version') == [(7,)]
    assert dump(db_path, V4_TABLES) == before
    assert raw(db_path, 'SELECT COUNT(*) FROM dispatch_authorizations') == [(0,)]
    assert raw(db_path, 'SELECT COUNT(*) FROM recovery_evidence') == [(0,)]
    assert raw(db_path, 'SELECT COUNT(*) FROM mission_reconciliation_contexts') == [(0,)]


# Each origin with each of the three newest DDLs that its own chain still runs. A database that
# already has a table never creates it again, so breaking that DDL there would break nothing.
LATE_STEPS = [
    (origin, ddl) for origin in ORIGINS
    for ddl, last_origin in (('_DISPATCH_AUTHORIZATIONS_DDL', 4), ('_RECOVERY_EVIDENCE_DDL', 5),
                             ('_MISSION_RECONCILIATION_CONTEXTS_DDL', 6))
    if VERSION_OF[origin] <= last_origin]


@pytest.mark.parametrize(('origin', 'ddl'), LATE_STEPS)
def test_a_failure_at_a_late_step_leaves_every_older_database_at_its_own_version(
        monkeypatch, db_path, origin, ddl):
    ORIGINS[origin](db_path)
    before = old_rows(db_path, origin)
    schema = schema_of(db_path)

    with monkeypatch.context() as patch:
        patch.setattr(storage, ddl, 'THIS IS NOT SQL')
        with pytest.raises(sqlite3.OperationalError):
            MissionStore(db_path)

    # Not an intermediate version, and not a database with some of the chain applied.
    assert raw(db_path, 'PRAGMA user_version') == [(VERSION_OF[origin],)]
    assert old_rows(db_path, origin) == before
    assert schema_of(db_path) == schema


@pytest.mark.parametrize('origin', list(ORIGINS))
def test_a_failed_commit_leaves_every_older_database_at_its_own_version(
        monkeypatch, db_path, origin):
    ORIGINS[origin](db_path)
    before = old_rows(db_path, origin)
    schema = schema_of(db_path)

    with pytest.raises(sqlite3.OperationalError, match='injected failure'):
        open_faulty_store(
            monkeypatch, db_path, fail_when=lambda sql: sql.strip().upper() == 'COMMIT')

    assert raw(db_path, 'PRAGMA user_version') == [(VERSION_OF[origin],)]
    assert old_rows(db_path, origin) == before
    assert schema_of(db_path) == schema


# I. A database that version 4 wrote

V4_STEPS_WITH_A_DISPATCH = [(key, state) for key, state in V3_MAIN_STEPS if state != 'PENDING']
MAIN_KEYS_WITH_A_DISPATCH = [key for key, _ in V4_STEPS_WITH_A_DISPATCH]
PROGRESSED_KEYS = [key for key, state in V3_MAIN_STEPS
                   if state not in ('PENDING', 'DISPATCH_PREPARED')]


@pytest.fixture
def v4_store(db_path):
    make_v4_database(db_path)
    with MissionStore(db_path) as opened:
        yield opened


@pytest.mark.parametrize('key', MAIN_KEYS_WITH_A_DISPATCH)
def test_no_dispatch_that_version_4_wrote_has_a_record(v4_store, key):
    with pytest.raises(DispatchAuthorizationMissing) as excinfo:
        v4_store.get_dispatch_authorization(V3_MAIN_ID, key)

    assert (excinfo.value.mission_id, excinfo.value.step_key) == (V3_MAIN_ID, key)


def test_the_unknown_step_that_version_4_wrote_has_no_record_and_the_pending_one_no_dispatch(
        v4_store, db_path):
    with pytest.raises(DispatchAuthorizationMissing):
        v4_store.get_dispatch_authorization(V3_UNKNOWN_ID, 'unknown')
    with pytest.raises(DispatchNotFound):
        v4_store.get_dispatch_authorization(V3_MAIN_ID, 'pending')

    assert authorization_rows(db_path) == []


@pytest.mark.parametrize('key', PROGRESSED_KEYS)
def test_a_step_that_progressed_under_version_4_is_missing_provenance_not_corrupt(
        v4_store, db_path, key):
    before = dump(db_path)

    for _ in range(2):
        with pytest.raises(DispatchAuthorizationMissing):
            v4_store.get_dispatch_authorization(V3_MAIN_ID, key)

    assert dump(db_path) == before


def test_ordinary_transitions_of_migrated_steps_are_unchanged_and_write_no_record(
        v4_store, db_path):
    assert v4_store.transition_step_state(V3_MAIN_ID, 'dispatched', S.ACCEPTED).state is (
        S.ACCEPTED)
    assert v4_store.transition_step_state(V3_MAIN_ID, 'accepted', S.EXECUTING).state is (
        S.EXECUTING)
    assert v4_store.transition_step_state(V3_MAIN_ID, 'executing', S.SUCCEEDED).state is (
        S.SUCCEEDED)
    assert v4_store.transition_step_state(V3_MAIN_ID, 'cancel_requested', S.CANCELLED).state is (
        S.CANCELLED)
    assert v4_store.transition_step_state(V3_MAIN_ID, 'prepared', S.CANCELLED).state is (
        S.CANCELLED)

    assert authorization_rows(db_path) == []
    for key in ('dispatched', 'accepted', 'executing', 'cancel_requested', 'prepared'):
        with pytest.raises(DispatchAuthorizationMissing):
            v4_store.get_dispatch_authorization(V3_MAIN_ID, key)
    assert v4_store.get_by_mission_id(V3_MAIN_ID).attempts_used == 3


@pytest.mark.parametrize('target', [state for state in S if state is not S.OUTCOME_UNKNOWN])
def test_a_migrated_unknown_step_still_has_no_exit_and_no_record_appears(
        v4_store, db_path, target):
    before = dump(db_path)
    expected = {
        S.DISPATCH_PREPARED: DispatchPreparationRequired,
        S.DISPATCHED: SendAuthorizationRequired,
    }.get(target, InvalidStateTransition)

    with pytest.raises(expected):
        v4_store.transition_step_state(V3_UNKNOWN_ID, 'unknown', target)

    assert dump(db_path) == before
    assert v4_store.get_step(V3_UNKNOWN_ID, 'unknown').state is S.OUTCOME_UNKNOWN
    with pytest.raises(DispatchAuthorizationMissing):
        v4_store.get_dispatch_authorization(V3_UNKNOWN_ID, 'unknown')


def test_a_migrated_unknown_step_cannot_be_prepared_or_authorized(v4_store, db_path):
    before = dump(db_path)

    with pytest.raises(InvalidStateTransition):
        v4_store.prepare_step_dispatch(V3_UNKNOWN_ID, 'unknown', ACTION)
    with pytest.raises(MissionNotRunning):
        v4_store.authorize_step_send(V3_UNKNOWN_ID, 'unknown')

    assert dump(db_path) == before


def test_a_prepared_step_that_version_4_wrote_is_authorized_normally_now(v4_store, db_path):
    dispatch_id = v4_store.get_step_dispatch(V3_MAIN_ID, 'prepared').dispatch_id
    assert str(dispatch_id) == V3_DISPATCH_IDS[(V3_MAIN_ID, 'prepared')]
    before = dump(db_path)

    receipt = v4_store.authorize_step_send(V3_MAIN_ID, 'prepared')

    # This is a new authorization, not a backfill: it takes an attempt, writes its record now
    # and moves the step, like any other.
    assert receipt.dispatch_id == dispatch_id
    assert receipt.attempt_number == 4
    assert v4_store.get_by_mission_id(V3_MAIN_ID).attempts_used == 4
    assert v4_store.get_step(V3_MAIN_ID, 'prepared').state is S.DISPATCHED
    assert authorization_rows(db_path) == [(str(dispatch_id), stamp_of(receipt.authorized_at))]
    diff = table_diff(before, dump(db_path))
    assert set(diff) == {'missions', 'mission_steps', NEW_TABLE}
    # Every other migrated dispatch is still without a record.
    for key in PROGRESSED_KEYS:
        with pytest.raises(DispatchAuthorizationMissing):
            v4_store.get_dispatch_authorization(V3_MAIN_ID, key)


def test_a_pending_step_that_version_4_wrote_is_prepared_and_authorized_normally(
        v4_store, db_path):
    dispatch = v4_store.prepare_step_dispatch(V3_MAIN_ID, 'pending', ACTION)
    assert authorization_rows(db_path) == []

    receipt = v4_store.authorize_step_send(V3_MAIN_ID, 'pending')

    assert receipt.dispatch_id == dispatch.dispatch_id and receipt.attempt_number == 4
    assert v4_store.get_dispatch_authorization(V3_MAIN_ID, 'pending').authorized_at == (
        receipt.authorized_at)


# J. A legacy attempt counter

def test_a_legacy_counter_continues_and_the_record_holds_no_attempt_number(v4_store, db_path):
    assert v4_store.get_by_mission_id(V3_MAIN_ID).attempts_used == 3

    receipt = v4_store.authorize_step_send(V3_MAIN_ID, 'prepared')

    # The counter carried on from the legacy value, and the receipt says so. The record has
    # only the dispatch and the time, and the store offers no way to ask which attempt it was.
    assert receipt.attempt_number == 4
    assert v4_store.get_by_mission_id(V3_MAIN_ID).attempts_used == 4
    assert [row[1] for row in raw(db_path, 'PRAGMA table_info(dispatch_authorizations)')] == [
        'dispatch_id', 'authorized_at']
    record = v4_store.get_dispatch_authorization(V3_MAIN_ID, 'prepared')
    assert not hasattr(record, 'attempt_number')


def test_the_number_of_records_is_not_the_attempt_counter(v4_store, db_path):
    v4_store.authorize_step_send(V3_MAIN_ID, 'prepared')
    v4_store.prepare_step_dispatch(V3_MAIN_ID, 'pending', ACTION)
    second = v4_store.authorize_step_send(V3_MAIN_ID, 'pending')

    # Three authorizations happened before this database was migrated and left no record. The
    # counter includes them and the rows cannot, so one says nothing about the other.
    assert second.attempt_number == 5
    assert v4_store.get_by_mission_id(V3_MAIN_ID).attempts_used == 5
    assert len(authorization_rows(db_path)) == 2
    mission = v4_store.get_by_mission_id(V3_MAIN_ID)
    assert mission.attempts_used == mission.max_attempts == 5


# The gap that version 5 closes

MASKS = [
    (re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'), '<uuid>'),
    (re.compile(r'\d{4}-\d\d-\d\dT[\d:.]+\+00:00'), '<time>'),
]


def masked(db_path):
    """Return the version 4 tables with every UUID and time replaced, so histories compare."""
    rows = dump(db_path, V4_TABLES)
    out = {}
    for table, table_rows in rows.items():
        out[table] = sorted(
            tuple(_mask(cell) for cell in row) for row in table_rows)
    return out


def _mask(cell):
    if not isinstance(cell, str):
        return cell
    for pattern, replacement in MASKS:
        cell = pattern.sub(replacement, cell)
    return cell


def unknown_after(db_path, authorized):
    """Build a mission whose step 'a' became OUTCOME_UNKNOWN, authorized or not."""
    with MissionStore(db_path) as store:
        mission_id = make_mission(store, keys=['a', 'b'])
        if authorized:
            store.authorize_step_send(mission_id, 'a')
        else:
            # What a counter left over from version 3 looks like: an attempt used by a step
            # that this database cannot name.
            raw(db_path, 'UPDATE missions SET attempts_used = 1')
        store.transition_step_state(mission_id, 'a', S.OUTCOME_UNKNOWN)
        store.enter_mission_reconciliation(mission_id)
    return mission_id


def test_provenance_tells_an_authorized_unknown_step_from_one_that_never_was(tmp_path):
    authorized_db, never_db = tmp_path / 'authorized.db', tmp_path / 'never.db'
    authorized_id = unknown_after(authorized_db, authorized=True)
    never_id = unknown_after(never_db, authorized=False)

    # Everything that version 4 kept is the same for the two histories once the IDs and the
    # times are masked: mission-wide counters and states cannot say which one crossed the line.
    assert masked(authorized_db) == masked(never_db)
    # The record can.
    with MissionStore(authorized_db) as store, MissionStore(never_db) as other:
        assert store.get_step(authorized_id, 'a').state is S.OUTCOME_UNKNOWN
        assert other.get_step(never_id, 'a').state is S.OUTCOME_UNKNOWN
        record = store.get_dispatch_authorization(authorized_id, 'a')
        assert record.step_key == 'a'
        with pytest.raises(DispatchAuthorizationMissing):
            other.get_dispatch_authorization(never_id, 'a')
    assert len(authorization_rows(authorized_db)) == 1
    assert authorization_rows(never_db) == []


# K. Failures injected around the connection, not by any hook in the store

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


STAGES = {
    'begin': (lambda sql: sql.startswith('BEGIN'), 1),
    'read the mission': (lambda sql: sql.startswith('SELECT') and 'FROM MISSIONS ' in sql, 1),
    'read the target step': (lambda sql: (
        sql.startswith('SELECT') and 'FROM MISSION_STEPS' in sql and 'STEP_KEY = ?' in sql), 1),
    'read every step': (lambda sql: (
        sql.startswith('SELECT') and 'FROM MISSION_STEPS' in sql and 'ORDER BY' in sql), 1),
    'read the dispatch': (reads('STEP_DISPATCHES'), 1),
    'read the context': (reads('DISPATCH_CONTEXTS'), 1),
    'read the authorization': (reads('DISPATCH_AUTHORIZATIONS'), 1),
    'update the attempt counter': (lambda sql: sql.startswith('UPDATE MISSIONS'), 1),
    'insert the authorization': (
        lambda sql: sql.startswith('INSERT INTO DISPATCH_AUTHORIZATIONS'), 1),
    'update the step': (lambda sql: sql.startswith('UPDATE MISSION_STEPS'), 1),
    # The second read of the record is the read-back that builds the receipt.
    'read the authorization back': (reads('DISPATCH_AUTHORIZATIONS'), 2),
    'commit': (lambda sql: sql == 'COMMIT', 1),
}

# Statements that must already have run when a later stage fails: the rollback, not an
# early exit, is what leaves the counter, the record and the step as they were.
ATTEMPT = 'UPDATE MISSIONS'
RECORD = 'INSERT INTO DISPATCH_AUTHORIZATIONS'
STEP = 'UPDATE MISSION_STEPS'
RAN_BEFORE = {
    'insert the authorization': [ATTEMPT],
    'update the step': [ATTEMPT, RECORD],
    'read the authorization back': [ATTEMPT, RECORD, STEP],
    'commit': [ATTEMPT, RECORD, STEP],
}


@pytest.mark.parametrize('stage', list(STAGES))
def test_a_failure_before_the_commit_leaves_everything_as_it_was(monkeypatch, db_path, stage):
    store, connection = open_faulty_store(monkeypatch, db_path)
    with store:
        mission_id = make_mission(store, max_attempts=2)
        before = dump(db_path)
        connection.statements.clear()
        test, nth = STAGES[stage]
        connection.fail_when = matching(test, nth)

        with pytest.raises(sqlite3.OperationalError, match='injected failure'):
            store.authorize_step_send(mission_id, 'checkpoint_a')

        connection.fail_when = None
        assert not connection.in_transaction
        logged = [normalized(sql) for sql in connection.statements]
        assert len([sql for sql in logged if test(sql)]) >= nth, 'the stage was never reached'
        for prefix in RAN_BEFORE.get(stage, []):
            assert any(sql.startswith(prefix) for sql in logged), prefix
        # Still PREPARED, the attempt not taken, no record, and not a cell different.
        assert dump(db_path) == before
        assert authorization_rows(db_path) == []
        assert store.get_step(mission_id, 'checkpoint_a').state is S.DISPATCH_PREPARED
        assert store.get_by_mission_id(mission_id).attempts_used == 0

        # And the store still works, and the next authorization is a whole one.
        receipt = store.authorize_step_send(mission_id, 'checkpoint_a')
        assert receipt.attempt_number == 1
        assert authorization_rows(db_path) == [
            (str(receipt.dispatch_id), stamp_of(receipt.authorized_at))]
        assert store.get_step(mission_id, 'checkpoint_a').state is S.DISPATCHED


def test_an_error_after_a_successful_commit_returns_no_receipt_and_keeps_all_three(
        monkeypatch, db_path):
    store, connection = open_faulty_store(monkeypatch, db_path)
    with store:
        mission_id = make_mission(store, max_attempts=2)
        dispatch_id = dispatch_id_of(store, mission_id, 'checkpoint_a')

        def commit_then_fail(sql):
            if normalized(sql) == 'COMMIT':
                connection._real.execute('COMMIT')      # the commit really happens
                return True
            return False

        connection.fail_when = commit_then_fail
        with pytest.raises(sqlite3.OperationalError, match='injected failure'):
            store.authorize_step_send(mission_id, 'checkpoint_a')
        connection.fail_when = None

        # The caller got an error and no receipt, and the whole authorization is durable.
        assert store.get_step(mission_id, 'checkpoint_a').state is S.DISPATCHED
        assert store.get_by_mission_id(mission_id).attempts_used == 1
        [(stored_id, _)] = authorization_rows(db_path)
        assert stored_id == str(dispatch_id)
        assert store.get_dispatch_authorization(mission_id, 'checkpoint_a').dispatch_id == (
            dispatch_id)
        # No second authorization can follow, and nothing more is written.
        before = dump(db_path)
        with pytest.raises(StepNotAwaitingSend):
            store.authorize_step_send(mission_id, 'checkpoint_a')
        assert dump(db_path) == before


def test_the_writes_run_in_the_order_attempt_record_step_and_the_step_is_last(
        monkeypatch, db_path):
    store, connection = open_faulty_store(monkeypatch, db_path)
    with store:
        mission_id = make_mission(store)
        connection.statements.clear()

        store.authorize_step_send(mission_id, 'checkpoint_a')

    logged = [normalized(sql) for sql in connection.statements]
    changing = writes(logged)
    assert [' '.join(sql.split()[:3]) for sql in changing] == [
        'UPDATE MISSIONS SET', 'INSERT INTO DISPATCH_AUTHORIZATIONS', 'UPDATE MISSION_STEPS SET']
    assert logged[0] == 'BEGIN IMMEDIATE' and logged[-1] == 'COMMIT'
    assert logged.count('BEGIN IMMEDIATE') == logged.count('COMMIT') == 1

    def at(prefix, nth=1):
        return [i for i, sql in enumerate(logged) if sql.startswith(prefix)][nth - 1]

    # Every check, the context and then the record among them, comes before any write; the
    # record is read once before the writes, and once more, after the last write, for the
    # receipt.
    assert (at('SELECT * FROM MISSIONS') < at('SELECT * FROM MISSION_STEPS WHERE')
            < at('SELECT * FROM STEP_DISPATCHES')
            < at('SELECT * FROM DISPATCH_CONTEXTS')
            < at('SELECT * FROM DISPATCH_AUTHORIZATIONS', 1)
            < at(ATTEMPT) < at(RECORD) < at(STEP)
            < at('SELECT * FROM DISPATCH_AUTHORIZATIONS', 2) < logged.index('COMMIT'))
    assert len([sql for sql in logged if sql.startswith('SELECT * FROM DISPATCH_AUTH')]) == 2


# L. Crashes. A real child process is killed at a chosen point in the real code

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
print(receipt.dispatch_id, receipt.attempt_number,
      receipt.authorized_at.isoformat(timespec='microseconds'), flush=True)
# Nothing is sent here, on purpose: the process dies with the receipt in hand.
die()
"""


def run_child(db_path, mission_id, when, marker, how):
    return subprocess.run(
        [sys.executable, '-c', _CHILD, str(db_path), mission_id, 'checkpoint_a', when, marker,
         how],
        env=package_env(), capture_output=True, text=True, timeout=30)


EXIT_CODE = {'exit': 0, 'sigkill': -signal.SIGKILL}


@pytest.mark.parametrize(('marker', 'when', 'how'), [
    ('BEGIN IMMEDIATE', 'before', 'exit'),
    ('BEGIN IMMEDIATE', 'after', 'exit'),           # the write lock held, nothing written
    ('UPDATE MISSIONS', 'before', 'exit'),          # before the attempt counter is touched
    ('UPDATE MISSIONS', 'after', 'exit'),           # the counter written, not the record
    ('UPDATE MISSIONS', 'after', 'sigkill'),
    (RECORD, 'before', 'exit'),                     # before the record is written
    (RECORD, 'after', 'exit'),                      # the counter and the record, not the step
    (RECORD, 'after', 'sigkill'),
    (STEP, 'before', 'exit'),
    (STEP, 'after', 'exit'),                        # all three written, not committed
    (STEP, 'after', 'sigkill'),
    ('COMMIT', 'before', 'exit'),                   # every write done, the commit not yet
    ('COMMIT', 'before', 'sigkill'),
])
def test_a_process_that_dies_before_the_commit_authorizes_nothing(
        db_path, marker, when, how):
    with MissionStore(db_path) as store:
        mission_id = make_mission(store, max_attempts=2)
        before = dump(db_path)

    child = run_child(db_path, mission_id, when, marker, how)

    assert child.returncode == EXIT_CODE[how]
    assert child.stdout == ''                   # it never got a receipt
    # SQLite rolled the unfinished transaction back on reopening: no attempt, no record.
    with MissionStore(db_path) as reopened:
        assert dump(db_path) == before
        assert authorization_rows(db_path) == []
        assert reopened.get_step(mission_id, 'checkpoint_a').state is S.DISPATCH_PREPARED
        assert reopened.get_by_mission_id(mission_id).attempts_used == 0
        with pytest.raises(DispatchAuthorizationMissing):
            reopened.get_dispatch_authorization(mission_id, 'checkpoint_a')
        # Nobody ever held a permission, so a later caller may be granted one, whole.
        receipt = reopened.authorize_step_send(mission_id, 'checkpoint_a')
        assert receipt.attempt_number == 1
        assert authorization_rows(db_path) == [
            (str(receipt.dispatch_id), stamp_of(receipt.authorized_at))]


@pytest.mark.parametrize('how', ['exit', 'sigkill'])
def test_a_process_that_dies_right_after_the_commit_leaves_all_three(db_path, how):
    with MissionStore(db_path) as store:
        mission_id = make_mission(store, max_attempts=2)
        dispatch_id = dispatch_id_of(store, mission_id, 'checkpoint_a')

    child = run_child(db_path, mission_id, 'after', 'COMMIT', how)

    assert child.returncode == EXIT_CODE[how]
    assert child.stdout == ''                   # it died before it could return the receipt
    for _ in range(2):
        with MissionStore(db_path) as reopened:
            assert reopened.get_step(mission_id, 'checkpoint_a').state is S.DISPATCHED
            assert reopened.get_by_mission_id(mission_id).attempts_used == 1
            record = reopened.get_dispatch_authorization(mission_id, 'checkpoint_a')
            assert record.dispatch_id == dispatch_id
            assert len(authorization_rows(db_path)) == 1
            before = dump(db_path)
            # No one holds a permission and nothing was sent, and no one may be granted a
            # second one: zero sends is the accepted outcome of this window.
            with pytest.raises(StepNotAwaitingSend) as excinfo:
                reopened.authorize_step_send(mission_id, 'checkpoint_a')
            assert excinfo.value.step_state is S.DISPATCHED
            assert dump(db_path) == before


@pytest.mark.parametrize('how', ['exit', 'sigkill'])
def test_a_process_that_dies_holding_the_receipt_leaves_no_second_authorization(db_path, how):
    with MissionStore(db_path) as store:
        mission_id = make_mission(store, max_attempts=2)

    child = run_child(db_path, mission_id, 'before', '-', how)

    assert child.returncode == EXIT_CODE[how]
    printed_id, printed_attempt, printed_time = child.stdout.split()
    with MissionStore(db_path) as reopened:
        # The receipt was for this very dispatch, and its time is the stored one. Nothing was
        # sent anywhere: the process died before doing any external send, which nothing here
        # can tell from a send.
        record = reopened.get_dispatch_authorization(mission_id, 'checkpoint_a')
        assert str(record.dispatch_id) == printed_id
        assert stamp_of(record.authorized_at) == printed_time
        assert printed_attempt == '1'
        assert reopened.get_step(mission_id, 'checkpoint_a').state is S.DISPATCHED
        assert reopened.get_by_mission_id(mission_id).attempts_used == 1
        before = dump(db_path)
        with pytest.raises(StepNotAwaitingSend):
            reopened.authorize_step_send(mission_id, 'checkpoint_a')
        assert dump(db_path) == before


# M. Concurrency. Each worker opens its own store, as separate callers would

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


REFUSALS = (StepNotAwaitingSend, AttemptLimitReached, MissionNotRunning, DispatchContextMissing)


def try_authorize(store, mission_id, key):
    try:
        return store.authorize_step_send(mission_id, key)
    except REFUSALS as refusal:
        return refusal


def missions(db_path, count, max_attempts, keys):
    with MissionStore(db_path) as store:
        return [make_mission(store, request_id=f'req-{n}', max_attempts=max_attempts, keys=keys)
                for n in range(count)]


def records_of(db_path, mission_id):
    """Return (step_key, authorized_at) of every record of the mission, from storage."""
    return raw(db_path, 'SELECT d.step_key, a.authorized_at FROM dispatch_authorizations a'
                        ' JOIN step_dispatches d ON d.dispatch_id = a.dispatch_id'
                        ' WHERE d.mission_id = ? ORDER BY d.step_key', mission_id)


def test_concurrent_callers_get_one_receipt_one_record_and_one_attempt(db_path):
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
            assert records_of(db_path, mission_id) == [
                ('checkpoint', stamp_of(receipts[0].authorized_at))], f'round {n}'
            assert store.get_dispatch_authorization(mission_id, 'checkpoint').dispatch_id == (
                receipts[0].dispatch_id)
            assert store.get_by_mission_id(mission_id).attempts_used == 1
    assert raw(db_path, 'SELECT COUNT(*) FROM dispatch_authorizations') == [(ROUNDS,)]


def test_two_steps_and_one_attempt_left_give_one_receipt_and_one_record(db_path):
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
            # One new record, for the step that won, and none for the one that did not.
            assert records_of(db_path, mission_id) == [
                (receipts[0].step_key, stamp_of(receipts[0].authorized_at))], f'round {n}'
            loser = next(key for key in KEYS if key != receipts[0].step_key)
            with pytest.raises(DispatchAuthorizationMissing):
                store.get_dispatch_authorization(mission_id, loser)
            assert store.get_step(mission_id, loser).state is S.DISPATCH_PREPARED
            assert store.get_by_mission_id(mission_id).attempts_used == 1


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
            assert cancellation.state is S.CANCELLED, f'round {n}'
            used = store.get_by_mission_id(mission_id).attempts_used
            records = records_of(db_path, mission_id)
            if isinstance(authorization, SendAuthorization):
                # Authorized first: the record and the attempt are there, and the later
                # cancellation refunds neither.
                assert used == 1 and len(records) == 1, f'round {n}'
            else:
                # The cancellation came first: no permission, no attempt and no record.
                assert isinstance(authorization, StepNotAwaitingSend), f'round {n}'
                assert used == 0 and records == [], f'round {n}'


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
            records = records_of(db_path, mission_id)
            assert mission.state is M.CANCELLING, f'round {n}'
            if isinstance(authorization, SendAuthorization):
                assert mission.attempts_used == 1 and len(records) == 1, f'round {n}'
            else:
                assert isinstance(authorization, MissionNotRunning), f'round {n}'
                assert mission.attempts_used == 0 and records == [], f'round {n}'


def test_an_authorization_racing_the_move_to_unknown_leaves_or_omits_the_record_consistently(
        db_path):
    mission_ids = missions(db_path, ROUNDS, 3, ['checkpoint'])

    def act(store, worker, n):
        if worker == 0:
            return try_authorize(store, mission_ids[n], 'checkpoint')
        return store.transition_step_state(mission_ids[n], 'checkpoint', S.OUTCOME_UNKNOWN)

    results = run_concurrently(db_path, 2, ROUNDS, act)

    with MissionStore(db_path) as store:
        for n, mission_id in enumerate(mission_ids):
            authorization, move = results[0][n], results[1][n]
            assert move.state is S.OUTCOME_UNKNOWN, f'round {n}'
            assert store.get_step(mission_id, 'checkpoint').state is S.OUTCOME_UNKNOWN
            used = store.get_by_mission_id(mission_id).attempts_used
            records = records_of(db_path, mission_id)
            if isinstance(authorization, SendAuthorization):
                # Authorized first: the step is unknown WITH its record.
                assert used == 1 and len(records) == 1, f'round {n}'
                assert store.get_dispatch_authorization(mission_id, 'checkpoint').dispatch_id == (
                    authorization.dispatch_id)
            else:
                # Unknown first: it is unknown WITHOUT one, and the authorization was refused.
                assert isinstance(authorization, StepNotAwaitingSend), f'round {n}'
                assert authorization.step_state is S.OUTCOME_UNKNOWN
                assert used == 0 and records == [], f'round {n}'
                with pytest.raises(DispatchAuthorizationMissing):
                    store.get_dispatch_authorization(mission_id, 'checkpoint')


def test_an_authorization_racing_a_context_deletion_never_leaves_a_record_without_a_receipt(
        db_path):
    mission_ids = missions(db_path, ROUNDS, 3, ['checkpoint'])
    with MissionStore(db_path) as setup:
        dispatch_ids = [dispatch_id_of(setup, mission_id, 'checkpoint')
                        for mission_id in mission_ids]

    def act(store, worker, n):
        if worker == 0:
            return try_authorize(store, mission_ids[n], 'checkpoint')
        # Something outside the supported code removes the context, on its own connection.
        with contextlib.closing(
                sqlite3.connect(db_path, timeout=TIMEOUT, isolation_level=None)) as conn:
            conn.execute('DELETE FROM dispatch_contexts WHERE dispatch_id = ?',
                         (str(dispatch_ids[n]),))
        return None

    results = run_concurrently(db_path, 2, ROUNDS, act)

    with MissionStore(db_path) as store:
        for n, mission_id in enumerate(mission_ids):
            authorization = results[0][n]
            used = store.get_by_mission_id(mission_id).attempts_used
            records = records_of(db_path, mission_id)
            if isinstance(authorization, SendAuthorization):
                # The context was still there when the authorization was checked, so the
                # record, the attempt and DISPATCHED all committed together.
                assert used == 1 and len(records) == 1, f'round {n}'
                assert store.get_step(mission_id, 'checkpoint').state is S.DISPATCHED
            else:
                # The context was already gone: nothing was written for this dispatch.
                assert isinstance(authorization, DispatchContextMissing), f'round {n}'
                assert used == 0 and records == [], f'round {n}'
                assert store.get_step(mission_id, 'checkpoint').state is S.DISPATCH_PREPARED


def test_readers_racing_an_authorization_only_ever_see_no_record_or_the_whole_record(db_path):
    workers = 4
    mission_ids = missions(db_path, ROUNDS, 3, ['checkpoint'])

    def act(store, worker, n):
        if worker == 0:
            return try_authorize(store, mission_ids[n], 'checkpoint')
        seen = []
        for _ in range(20):
            try:
                seen.append(store.get_dispatch_authorization(mission_ids[n], 'checkpoint'))
            except DispatchAuthorizationMissing as missing:
                seen.append(missing)
        return seen

    results = run_concurrently(db_path, workers, ROUNDS, act)

    for n in range(ROUNDS):
        receipt = results[0][n]
        assert isinstance(receipt, SendAuthorization), f'round {n}'
        for worker in range(1, workers):
            seen = results[worker][n]
            # Nothing but "no record yet" or the record, never a corruption, and once the
            # record has been seen it stays seen.
            assert all(isinstance(o, (DispatchAuthorizationMissing, DispatchAuthorizationRecord))
                       for o in seen), f'round {n}: {seen}'
            kinds = [isinstance(o, DispatchAuthorizationRecord) for o in seen]
            assert kinds == sorted(kinds), f'round {n}: the record went away again'
            for o in seen:
                if isinstance(o, DispatchAuthorizationRecord):
                    assert o.authorized_at == receipt.authorized_at
                    assert o.dispatch_id == receipt.dispatch_id


# N. Structural invariants

PACKAGE = Path(task_resilience.__file__).resolve().parent
PRODUCTION = {path.name: ast.parse(path.read_text()) for path in sorted(PACKAGE.glob('*.py'))}


def store_methods():
    [store_class] = [node for node in PRODUCTION['storage.py'].body
                     if isinstance(node, ast.ClassDef) and node.name == 'MissionStore']
    return {node.name: node for node in store_class.body if isinstance(node, ast.FunctionDef)}


def module_functions():
    return {node.name: node for node in PRODUCTION['storage.py'].body
            if isinstance(node, ast.FunctionDef)}


def names_in(node):
    return {child.id for child in ast.walk(node) if isinstance(child, ast.Name)}


def string_constants(tree):
    return [child.value for child in ast.walk(tree)
            if isinstance(child, ast.Constant) and isinstance(child.value, str)]


def test_only_authorize_step_send_consumes_an_attempt():
    users = {name for name, method in store_methods().items() if '_CONSUME' in names_in(method)}
    users |= {name for name, function in module_functions().items()
              if '_CONSUME' in names_in(function)}

    assert users == {'authorize_step_send'}


def test_only_authorize_step_send_runs_the_statement_that_inserts_a_record():
    users = {name for name, method in store_methods().items()
             if '_INSERT_AUTHORIZATION' in names_in(method)}
    users |= {name for name, function in module_functions().items()
              if '_INSERT_AUTHORIZATION' in names_in(function)}

    assert users == {'authorize_step_send'}
    executed = [call for call in ast.walk(store_methods()['authorize_step_send'])
                if isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute)
                and call.func.attr in ('execute', 'executemany')
                and call.args and isinstance(call.args[0], ast.Name)
                and call.args[0].id == '_INSERT_AUTHORIZATION']
    assert len(executed) == 1
    assert [call.func.attr for call in executed] == ['execute']


def test_the_statements_that_name_the_table_are_the_create_the_read_and_the_insert_only():
    naming = {}
    for filename, tree in PRODUCTION.items():
        for text in string_constants(tree):
            if 'dispatch_authorizations' in text.lower():
                naming.setdefault(filename, []).append(' '.join(text.split()).upper())

    assert set(naming) == {'storage.py'}
    statements = sorted(naming['storage.py'])
    assert len(statements) == 3
    create, insert, select = sorted(statements, key=lambda sql: sql.split()[0])
    assert create.startswith('CREATE TABLE DISPATCH_AUTHORIZATIONS (')
    assert insert.startswith('INSERT INTO DISPATCH_AUTHORIZATIONS (')
    assert select.startswith('SELECT * FROM DISPATCH_AUTHORIZATIONS WHERE')
    # No update, delete, replace or upsert of a record. The one DELETE in the text is the
    # foreign key's cascade, which removes a record only together with its dispatch.
    for sql in (insert, select):
        for word in ('UPDATE', 'DELETE', 'REPLACE', 'ON CONFLICT', 'OR IGNORE', 'UPSERT'):
            assert word not in sql, (word, sql)
    assert create.count('DELETE') == 1 and 'ON DELETE CASCADE' in create


def test_the_read_path_cannot_write_a_record_or_an_attempt():
    write_statements = {
        name for tree_node in PRODUCTION['storage.py'].body if isinstance(tree_node, ast.Assign)
        for target in tree_node.targets if isinstance(target, ast.Name)
        for name in [target.id]
        if name.startswith(('_INSERT', '_SET_', '_CONSUME', '_RENAME', '_COPY', '_DROP'))
        or name.endswith('_DDL')}
    assert {'_INSERT_AUTHORIZATION', '_CONSUME', '_SET_STEP_STATE',
            '_DISPATCH_AUTHORIZATIONS_DDL'} <= write_statements

    read_path = {
        'get_dispatch_authorization': store_methods()['get_dispatch_authorization'],
        '_snapshot': store_methods()['_snapshot'],
        '_fetch_authorization': store_methods()['_fetch_authorization'],
        '_require_authorization': store_methods()['_require_authorization'],
        '_authorization_of': store_methods()['_authorization_of'],
        '_to_authorization': module_functions()['_to_authorization'],
    }
    for name, function in read_path.items():
        assert not names_in(function) & write_statements, name
        # And nothing about an attempt: no number is derived from a record here.
        assert not [text for text in string_constants(function) if 'attempt' in text.lower()
                    and not ast.get_docstring(function, clean=False) == text], name
    # The one statement it executes for the record is the read.
    for name in ('_fetch_authorization',):
        assert {n for n in names_in(read_path[name]) if n.startswith('_') and n.isupper()} == {
            '_AUTHORIZATION_OF_DISPATCH'}


def test_no_method_creates_repairs_backfills_or_attests_a_record():
    names = list(dir(MissionStore))

    assert not [name for name in names if any(word in name.lower() for word in (
        'backfill', 'repair', 'attest', 'restore', 'grant', 'infer', 'reconstruct'))]
    # Task 010 added one read. Task 011 has since added the write and the read of evidence, and
    # Task 012 the four reconciliation operations. Nothing writes an authorization record but
    # authorize_step_send.
    assert sorted(name for name in names if not name.startswith('_')) == sorted([
        'apply_recovery_observation', 'authorize_step_send', 'close', 'create_or_get',
        'enter_mission_reconciliation', 'get_by_mission_id', 'get_by_request_id', 'get_dispatch',
        'get_dispatch_authorization', 'get_dispatch_context', 'get_mission_reconciliation_context',
        'get_recovery_evidence', 'get_step', 'get_step_dispatch', 'get_steps', 'initialize_steps',
        'prepare_step_dispatch', 'request_cancellation_during_reconciliation',
        'resolve_mission_reconciliation', 'transition_mission_state', 'transition_step_state'])


def test_the_production_modules_import_no_ros_network_process_or_time_package():
    allowed = {'collections', 'contextlib', 'dataclasses', 'datetime', 'enum', 'hashlib', 'json',
               'math', 'os', 'sqlite3', 'typing', 'uuid'}
    for filename in ('storage.py', 'model.py', 'errors.py'):
        for node in ast.walk(PRODUCTION[filename]):
            if isinstance(node, ast.Import):
                found = {alias.name.split('.')[0] for alias in node.names}
            elif isinstance(node, ast.ImportFrom) and node.level == 0:
                found = {node.module.split('.')[0]}
            else:
                continue
            assert found <= allowed, (filename, found - allowed)


def test_the_authorization_takes_no_callback_and_does_no_external_work():
    names = names_in(store_methods()['authorize_step_send'])
    signature = inspect.signature(MissionStore.authorize_step_send)

    assert list(signature.parameters) == ['self', 'mission_id', 'step_key']
    assert not names & {'os', 'sys', 'time', 'socket', 'subprocess', 'urllib', 'http',
                        'requests', 'threading'}


def test_the_state_machine_and_the_reconciliation_decision_do_not_know_the_record():
    for module in (state_machine, reconciliation):
        text = Path(module.__file__).read_text()
        assert 'dispatch_authorizations' not in text
        assert 'DispatchAuthorizationRecord' not in text
        assert 'get_dispatch_authorization' not in text
    imported = {(node.level, node.module) for node in ast.walk(
        ast.parse(Path(reconciliation.__file__).read_text())) if isinstance(node, ast.ImportFrom)}
    assert not {module for _, module in imported if module and 'storage' in module}


def test_the_ordinary_edges_that_matter_here_are_unchanged():
    # Preparing and then failing to learn the outcome is still an ordinary edge, and an
    # unknown outcome still has no way out. Neither depends on whether a record exists.
    assert transition_step(S.DISPATCH_PREPARED, S.OUTCOME_UNKNOWN) is S.OUTCOME_UNKNOWN
    assert transition_step(S.DISPATCHED, S.OUTCOME_UNKNOWN) is S.OUTCOME_UNKNOWN
    for target in S:
        if target is S.OUTCOME_UNKNOWN:
            continue
        with pytest.raises(InvalidStateTransition):
            transition_step(S.OUTCOME_UNKNOWN, target)


def test_a_prepared_step_can_still_become_unknown_and_that_writes_no_record(store, db_path):
    mission_id = make_mission(store)

    moved = store.transition_step_state(mission_id, 'checkpoint_a', S.OUTCOME_UNKNOWN)

    assert moved.state is S.OUTCOME_UNKNOWN
    assert authorization_rows(db_path) == []
    with pytest.raises(DispatchAuthorizationMissing):
        store.get_dispatch_authorization(mission_id, 'checkpoint_a')
