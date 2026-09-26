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
import re
import sqlite3

import pytest

from task_resilience.state_machine import MissionState, StepState
from task_resilience.storage import MissionStore

TIMESTAMP = '2026-01-01T00:00:00.000000+00:00'

STEP_DISPATCHES_DDL = """CREATE TABLE step_dispatches (
    dispatch_id TEXT NOT NULL PRIMARY KEY,
    mission_id  TEXT NOT NULL,
    step_key    TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    FOREIGN KEY (mission_id, step_key)
        REFERENCES mission_steps (mission_id, step_key)
        ON DELETE CASCADE,
    UNIQUE (mission_id, step_key)
)"""


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / 'missions.db'


@pytest.fixture
def conn(db_path):
    # Foreign keys on, as they are on the store's own connection.
    MissionStore(db_path).close()
    connection = sqlite3.connect(db_path, isolation_level=None)
    connection.execute('PRAGMA foreign_keys = ON')
    yield connection
    connection.close()


def insert_mission(conn, mission_id='m1', state='ACCEPTED'):
    conn.execute(
        'INSERT INTO missions (mission_id, request_id, request_fingerprint, request_json,'
        ' max_attempts, created_at, updated_at, mission_state)'
        ' VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
        (mission_id, f'req-{mission_id}', 'fingerprint', '{}', 3, TIMESTAMP, TIMESTAMP, state))


def insert_step(conn, mission_id='m1', key='a', index=0, state='PENDING'):
    conn.execute(
        'INSERT INTO mission_steps (mission_id, step_key, step_index, step_state,'
        ' created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)',
        (mission_id, key, index, state, TIMESTAMP, TIMESTAMP))


def insert_dispatch(conn, dispatch_id='d1', mission_id='m1', key='a'):
    conn.execute(
        'INSERT INTO step_dispatches (dispatch_id, mission_id, step_key, created_at)'
        ' VALUES (?, ?, ?, ?)',
        (dispatch_id, mission_id, key, TIMESTAMP))


def columns(conn, table):
    return [row[1] for row in conn.execute(f'PRAGMA table_info({table})')]


def check_list(conn, table, column):
    """Return the values in a column's CHECK (column IN (...)) list, as stored."""
    sql = conn.execute('SELECT sql FROM sqlite_master WHERE name = ?', (table,)).fetchone()[0]
    match = re.search(rf'{column} IN \(([^)]*)\)', sql)
    return re.findall(r"'([^']*)'", match.group(1))


def test_fresh_database_is_created_at_version_6(conn):
    assert conn.execute('PRAGMA user_version').fetchone()[0] == 6
    tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert tables == {
        'missions', 'mission_steps', 'step_dispatches', 'dispatch_contexts',
        'dispatch_authorizations', 'recovery_evidence',
    }


def test_missions_table_gains_only_mission_state(conn):
    assert columns(conn, 'missions') == [
        'mission_id', 'request_id', 'request_fingerprint', 'request_json', 'max_attempts',
        'attempts_used', 'created_at', 'updated_at', 'mission_state',
    ]


def test_step_dispatches_table_holds_only_the_identity(conn):
    assert columns(conn, 'step_dispatches') == [
        'dispatch_id', 'mission_id', 'step_key', 'created_at',
    ]


def test_dispatch_authorizations_table_holds_only_the_dispatch_and_its_time(conn):
    assert columns(conn, 'dispatch_authorizations') == ['dispatch_id', 'authorized_at']


def test_recovery_evidence_table_has_exactly_the_evidence_columns(conn):
    assert columns(conn, 'recovery_evidence') == [
        'evidence_id', 'dispatch_id', 'observed_status', 'result_code', 'result_message_prefix',
        'result_message_bytes', 'result_message_sha256', 'step_state_before', 'outcome',
        'recorded_at',
    ]


def test_mission_steps_table_holds_only_identity_order_and_state(conn):
    assert columns(conn, 'mission_steps') == [
        'mission_id', 'step_key', 'step_index', 'step_state', 'created_at', 'updated_at',
    ]


def test_store_connections_enforce_foreign_keys(db_path):
    with MissionStore(db_path) as store:
        assert store._conn.execute('PRAGMA foreign_keys').fetchone()[0] == 1
        with pytest.raises(sqlite3.IntegrityError):
            insert_step(store._conn, mission_id='no-such-mission')


def test_check_lists_are_exactly_the_persisted_enum_values(conn):
    # A new state needs a new schema version, so this fails until it gets one.
    mission_values = check_list(conn, 'missions', 'mission_state')
    step_values = check_list(conn, 'mission_steps', 'step_state')

    assert sorted(mission_values) == sorted(state.value for state in MissionState)
    assert sorted(step_values) == sorted(state.value for state in StepState)
    assert len(set(mission_values)) == len(mission_values)
    assert len(set(step_values)) == len(step_values)


@pytest.mark.parametrize('state', list(MissionState))
def test_every_mission_state_can_be_stored(conn, state):
    insert_mission(conn, state=state.value)

    assert conn.execute('SELECT mission_state FROM missions').fetchone()[0] == state.value


@pytest.mark.parametrize(
    'value', ['RUNNIN', 'running', 'Running', '', ' RUNNING', 'PENDING', 5, None])
def test_invalid_mission_state_is_rejected(conn, value):
    with pytest.raises(sqlite3.IntegrityError):
        insert_mission(conn, state=value)


def test_mission_state_has_no_default(conn):
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            'INSERT INTO missions (mission_id, request_id, request_fingerprint, request_json,'
            ' max_attempts, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)',
            ('m1', 'req-1', 'fingerprint', '{}', 3, TIMESTAMP, TIMESTAMP))


@pytest.mark.parametrize('state', list(StepState))
def test_every_step_state_can_be_stored(conn, state):
    insert_mission(conn)

    insert_step(conn, state=state.value)

    assert conn.execute('SELECT step_state FROM mission_steps').fetchone()[0] == state.value


@pytest.mark.parametrize('value', ['EXECUTIN', 'executing', '', ' PENDING', 'RUNNING', 5, None])
def test_invalid_step_state_is_rejected(conn, value):
    insert_mission(conn)

    with pytest.raises(sqlite3.IntegrityError):
        insert_step(conn, state=value)


def test_step_index_cannot_be_negative(conn):
    insert_mission(conn)

    with pytest.raises(sqlite3.IntegrityError):
        insert_step(conn, index=-1)


def test_step_key_cannot_be_empty(conn):
    insert_mission(conn)

    with pytest.raises(sqlite3.IntegrityError):
        insert_step(conn, key='')


def test_duplicate_step_index_in_one_mission_is_rejected(conn):
    insert_mission(conn)
    insert_step(conn, key='a', index=0)

    with pytest.raises(sqlite3.IntegrityError):
        insert_step(conn, key='b', index=0)


def test_duplicate_step_key_in_one_mission_is_rejected(conn):
    insert_mission(conn)
    insert_step(conn, key='a', index=0)

    with pytest.raises(sqlite3.IntegrityError):
        insert_step(conn, key='a', index=1)


def test_step_keys_and_indexes_may_repeat_across_missions(conn):
    insert_mission(conn, 'm1')
    insert_mission(conn, 'm2')

    insert_step(conn, 'm1', key='a', index=0)
    insert_step(conn, 'm2', key='a', index=0)

    assert conn.execute('SELECT COUNT(*) FROM mission_steps').fetchone()[0] == 2


def test_step_cannot_reference_a_missing_mission(conn):
    with pytest.raises(sqlite3.IntegrityError):
        insert_step(conn, mission_id='no-such-mission')


def test_deleting_a_mission_deletes_its_steps(conn):
    insert_mission(conn, 'm1')
    insert_mission(conn, 'm2')
    insert_step(conn, 'm1', key='a', index=0)
    insert_step(conn, 'm2', key='a', index=0)

    conn.execute("DELETE FROM missions WHERE mission_id = 'm1'")

    remaining = conn.execute('SELECT mission_id FROM mission_steps').fetchall()
    assert remaining == [('m2',)]


# Dispatches

@pytest.fixture
def prepared(conn):
    """Give the connection a mission with two steps, so dispatches have something to reference."""
    insert_mission(conn, 'm1')
    insert_step(conn, 'm1', key='a', index=0)
    insert_step(conn, 'm1', key='b', index=1)
    return conn


def test_step_dispatches_ddl_is_exactly_as_specified(conn):
    sql = conn.execute(
        "SELECT sql FROM sqlite_master WHERE name = 'step_dispatches'").fetchone()[0]

    assert sql == STEP_DISPATCHES_DDL


def test_dispatch_id_is_a_required_primary_key(conn):
    info = {row[1]: row for row in conn.execute('PRAGMA table_info(step_dispatches)')}

    # (cid, name, type, notnull, default, pk)
    assert info['dispatch_id'][2:] == ('TEXT', 1, None, 1)


def test_a_null_dispatch_id_is_rejected(prepared):
    with pytest.raises(sqlite3.IntegrityError, match='NOT NULL'):
        insert_dispatch(prepared, None, 'm1', 'a')

    insert_dispatch(prepared, 'd1', 'm1', 'a')
    with pytest.raises(sqlite3.IntegrityError, match='NOT NULL'):
        prepared.execute('UPDATE step_dispatches SET dispatch_id = NULL')

    assert prepared.execute('SELECT dispatch_id FROM step_dispatches').fetchall() == [('d1',)]


def test_a_dispatch_can_be_recorded_for_a_step(prepared):
    insert_dispatch(prepared, 'd1', 'm1', 'a')

    assert prepared.execute('SELECT * FROM step_dispatches').fetchall() == [
        ('d1', 'm1', 'a', TIMESTAMP)]


def test_dispatch_id_is_unique(prepared):
    insert_dispatch(prepared, 'd1', 'm1', 'a')

    with pytest.raises(sqlite3.IntegrityError):
        insert_dispatch(prepared, 'd1', 'm1', 'b')


def test_a_step_can_have_only_one_dispatch(prepared):
    insert_dispatch(prepared, 'd1', 'm1', 'a')

    with pytest.raises(sqlite3.IntegrityError):
        insert_dispatch(prepared, 'd2', 'm1', 'a')


def test_different_steps_have_different_dispatches(prepared):
    insert_dispatch(prepared, 'd1', 'm1', 'a')
    insert_dispatch(prepared, 'd2', 'm1', 'b')

    assert prepared.execute('SELECT COUNT(*) FROM step_dispatches').fetchone()[0] == 2


@pytest.mark.parametrize(('mission_id', 'key'), [
    ('m1', 'no-such-step'),
    ('no-such-mission', 'a'),
    ('no-such-mission', 'no-such-step'),
], ids=['unknown step', 'unknown mission', 'neither'])
def test_a_dispatch_cannot_reference_an_unknown_step(prepared, mission_id, key):
    with pytest.raises(sqlite3.IntegrityError):
        insert_dispatch(prepared, 'd1', mission_id, key)


def test_a_dispatch_cannot_borrow_another_missions_step(prepared):
    insert_mission(prepared, 'm2')

    with pytest.raises(sqlite3.IntegrityError):
        insert_dispatch(prepared, 'd1', 'm2', 'a')


def test_deleting_a_step_deletes_its_dispatch(prepared):
    insert_dispatch(prepared, 'd1', 'm1', 'a')
    insert_dispatch(prepared, 'd2', 'm1', 'b')

    prepared.execute("DELETE FROM mission_steps WHERE step_key = 'a'")

    assert prepared.execute('SELECT dispatch_id FROM step_dispatches').fetchall() == [('d2',)]


def test_deleting_a_mission_deletes_its_steps_and_their_dispatches(prepared):
    insert_mission(prepared, 'm2')
    insert_step(prepared, 'm2', key='a', index=0)
    insert_dispatch(prepared, 'd1', 'm1', 'a')
    insert_dispatch(prepared, 'd2', 'm1', 'b')
    insert_dispatch(prepared, 'd3', 'm2', 'a')

    prepared.execute("DELETE FROM missions WHERE mission_id = 'm1'")

    assert prepared.execute('SELECT dispatch_id FROM step_dispatches').fetchall() == [('d3',)]
    assert prepared.execute('SELECT mission_id FROM mission_steps').fetchall() == [('m2',)]


def test_defining_steps_creates_no_dispatch(db_path):
    with MissionStore(db_path) as store:
        mission = store.create_or_get('req-001', {}, 1)
        store.initialize_steps(mission.mission_id, ['a', 'b'])

    with contextlib.closing(sqlite3.connect(db_path)) as conn:
        assert conn.execute('SELECT COUNT(*) FROM step_dispatches').fetchone()[0] == 0
