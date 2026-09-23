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

from concurrent.futures import ThreadPoolExecutor
import contextlib
from datetime import datetime
import hashlib
import json
import sqlite3
import threading

import pytest

from task_resilience import storage
from task_resilience.errors import (
    AttemptLimitReached,
    IdempotencyConflict,
    MissionCompletionInvariantError,
)
from task_resilience.state_machine import MissionState, StepState
from task_resilience.storage import MissionStore

# The schema exactly as Task 002 created it.
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


def fingerprint(canonical_request: bytes) -> str:
    # Task 002's algorithm, spelled out: SHA-256 of the canonical request text.
    return hashlib.sha256(canonical_request).hexdigest()


LEGACY = [
    {
        'mission_id': '11111111-1111-4111-8111-111111111111',
        'request_id': 'req-legacy-1',
        'request_fingerprint': fingerprint(b'{"max_attempts":3,"payload":{"x":1,"y":2}}'),
        'request_json': '{"x":1,"y":2}',
        'max_attempts': 3,
        'attempts_used': 2,
        'created_at': '2026-01-02T03:04:05.123456+00:00',
        'updated_at': '2026-01-02T04:05:06.654321+00:00',
    },
    {
        'mission_id': '22222222-2222-4222-8222-222222222222',
        'request_id': 'req-legacy-2',
        'request_fingerprint': fingerprint(b'{"max_attempts":5,"payload":[1,2,3]}'),
        'request_json': '[1,2,3]',
        'max_attempts': 5,
        'attempts_used': 0,
        'created_at': '2026-02-03T00:00:00.000000+00:00',
        'updated_at': '2026-02-03T00:00:00.000000+00:00',
    },
    {
        'mission_id': '33333333-3333-4333-8333-333333333333',
        'request_id': 'req-legacy-3',
        'request_fingerprint': fingerprint(
            b'{"max_attempts":1,"payload":{"a":{"b":null}}}'),
        'request_json': '{"a":{"b":null}}',
        'max_attempts': 1,
        'attempts_used': 1,
        'created_at': '2026-03-04T05:06:07.000001+00:00',
        'updated_at': '2026-03-05T05:06:07.999999+00:00',
    },
]


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / 'missions.db'


def make_v1_database(path, rows):
    with contextlib.closing(sqlite3.connect(path, isolation_level=None)) as conn:
        conn.execute(V1_SCHEMA)
        for row in rows:
            conn.execute(
                f'INSERT INTO missions ({", ".join(V1_COLUMNS)}) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                [row[column] for column in V1_COLUMNS])
        conn.execute('PRAGMA user_version = 1')


def raw(path, sql):
    with contextlib.closing(sqlite3.connect(path)) as conn:
        return conn.execute(sql).fetchall()


def v1_snapshot(path):
    return raw(path, f'SELECT {", ".join(V1_COLUMNS)} FROM missions ORDER BY mission_id')


def schema_of(path):
    return raw(path, 'SELECT type, name, sql FROM sqlite_master ORDER BY type, name')


def assert_still_version_1(path, expected_snapshot):
    assert raw(path, 'PRAGMA user_version') == [(1,)]
    assert raw(path, "SELECT name FROM sqlite_master WHERE type = 'table'") == [('missions',)]
    assert [row[1] for row in raw(path, 'PRAGMA table_info(missions)')] == V1_COLUMNS
    assert v1_snapshot(path) == expected_snapshot


def test_version_1_database_migrates_when_opened(db_path):
    make_v1_database(db_path, LEGACY[:1])
    before = v1_snapshot(db_path)

    MissionStore(db_path).close()

    assert raw(db_path, 'PRAGMA user_version') == [(2,)]
    assert v1_snapshot(db_path) == before


def test_migrated_mission_is_unchanged_and_accepted(db_path):
    make_v1_database(db_path, LEGACY[:1])
    legacy = LEGACY[0]

    with MissionStore(db_path) as store:
        mission = store.get_by_mission_id(legacy['mission_id'])

    assert mission.mission_id == legacy['mission_id']
    assert mission.request_id == legacy['request_id']
    assert mission.request_fingerprint == legacy['request_fingerprint']
    assert mission.payload == json.loads(legacy['request_json'])
    assert mission.max_attempts == 3
    assert mission.attempts_used == 2
    assert mission.created_at == datetime.fromisoformat(legacy['created_at'])
    assert mission.updated_at == datetime.fromisoformat(legacy['updated_at'])
    assert mission.state is MissionState.ACCEPTED


def test_migration_keeps_every_mission_and_invents_no_steps(db_path):
    make_v1_database(db_path, LEGACY)
    before = v1_snapshot(db_path)

    with MissionStore(db_path) as store:
        missions = [store.get_by_mission_id(row['mission_id']) for row in LEGACY]
        steps = [store.get_steps(row['mission_id']) for row in LEGACY]

    assert v1_snapshot(db_path) == before
    assert [mission.attempts_used for mission in missions] == [2, 0, 1]
    assert {mission.state for mission in missions} == {MissionState.ACCEPTED}
    assert steps == [(), (), ()]
    assert raw(db_path, 'SELECT COUNT(*) FROM mission_steps') == [(0,)]


def test_empty_version_1_database_migrates(db_path):
    make_v1_database(db_path, [])

    MissionStore(db_path).close()

    assert raw(db_path, 'PRAGMA user_version') == [(2,)]
    assert raw(db_path, 'SELECT COUNT(*) FROM missions') == [(0,)]


def test_migrated_schema_is_identical_to_a_fresh_one(db_path, tmp_path):
    make_v1_database(db_path, LEGACY)
    MissionStore(db_path).close()
    MissionStore(tmp_path / 'fresh.db').close()

    assert schema_of(db_path) == schema_of(tmp_path / 'fresh.db')


def test_consumed_attempts_survive_migration(db_path):
    make_v1_database(db_path, LEGACY)

    with MissionStore(db_path) as store:
        assert store.get_by_mission_id(LEGACY[0]['mission_id']).attempts_used == 2
        assert store.consume_attempt(LEGACY[0]['mission_id']).attempts_used == 3
        with pytest.raises(AttemptLimitReached):
            store.consume_attempt(LEGACY[0]['mission_id'])
        with pytest.raises(AttemptLimitReached):
            store.consume_attempt(LEGACY[2]['mission_id'])


def test_requests_accepted_before_migration_still_match_on_replay(db_path):
    make_v1_database(db_path, LEGACY)

    with MissionStore(db_path) as store:
        replay = store.create_or_get('req-legacy-1', {'y': 2, 'x': 1}, 3)
        assert replay.mission_id == LEGACY[0]['mission_id']
        assert replay.attempts_used == 2

        with pytest.raises(IdempotencyConflict):
            store.create_or_get('req-legacy-1', {'x': 1, 'y': 3}, 3)
        with pytest.raises(IdempotencyConflict):
            store.create_or_get('req-legacy-1', {'x': 1, 'y': 2}, 4)
        assert store.create_or_get('req-legacy-2', [1, 2, 3], 5).mission_id == (
            LEGACY[1]['mission_id'])


def test_new_apis_work_on_a_migrated_mission(db_path):
    make_v1_database(db_path, LEGACY)
    mission_id = LEGACY[0]['mission_id']

    with MissionStore(db_path) as store:
        steps = store.initialize_steps(mission_id, ['checkpoint_a', 'return_to_base'])
        assert [step.state for step in steps] == [StepState.PENDING, StepState.PENDING]

        assert store.transition_mission_state(mission_id, MissionState.RUNNING).state is (
            MissionState.RUNNING)
        with pytest.raises(MissionCompletionInvariantError):
            store.transition_mission_state(mission_id, MissionState.COMPLETED)

        for key in ('checkpoint_a', 'return_to_base'):
            for state in (StepState.DISPATCH_PREPARED, StepState.DISPATCHED,
                          StepState.SUCCEEDED):
                store.transition_step_state(mission_id, key, state)
        completed = store.transition_mission_state(mission_id, MissionState.COMPLETED)

    assert completed.state is MissionState.COMPLETED
    assert completed.attempts_used == 2


def test_reopening_a_migrated_database_does_not_reset_state(db_path):
    make_v1_database(db_path, LEGACY[:1])
    mission_id = LEGACY[0]['mission_id']
    with MissionStore(db_path) as store:
        store.transition_mission_state(mission_id, MissionState.RUNNING)

    with MissionStore(db_path) as reopened:
        assert reopened.get_by_mission_id(mission_id).state is MissionState.RUNNING


def test_concurrent_opens_of_a_version_1_database_migrate_it_once(db_path):
    make_v1_database(db_path, LEGACY)
    before = v1_snapshot(db_path)
    workers = 8
    barrier = threading.Barrier(workers, timeout=10)

    def open_and_close():
        barrier.wait()
        MissionStore(db_path).close()

    with ThreadPoolExecutor(max_workers=workers) as pool:
        for future in [pool.submit(open_and_close) for _ in range(workers)]:
            future.result(timeout=10)

    assert raw(db_path, 'PRAGMA user_version') == [(2,)]
    assert v1_snapshot(db_path) == before
    assert raw(db_path, 'SELECT DISTINCT mission_state FROM missions') == [('ACCEPTED',)]


# Rollback. Failures are injected without any production hook: by breaking one
# migration statement at a time, and by making COMMIT itself fail.

@pytest.mark.parametrize('statement', [
    '_MISSIONS_DDL',         # after the old table was renamed
    '_COPY_V1_MISSIONS',     # after the new table was created
    '_DROP_V1_MISSIONS',     # after the rows were copied
    '_MISSION_STEPS_DDL',    # after the old table was dropped: the last step
])
def test_migration_failing_partway_leaves_the_version_1_database_intact(
        db_path, monkeypatch, statement):
    make_v1_database(db_path, LEGACY)
    before = v1_snapshot(db_path)
    schema = schema_of(db_path)

    with monkeypatch.context() as patch:
        patch.setattr(storage, statement, 'THIS IS NOT SQL')
        with pytest.raises(sqlite3.OperationalError):
            MissionStore(db_path)

    assert_still_version_1(db_path, before)
    assert schema_of(db_path) == schema

    # A later, healthy open picks the migration up again.
    with MissionStore(db_path) as store:
        assert store.get_by_mission_id(LEGACY[0]['mission_id']).state is MissionState.ACCEPTED
    assert raw(db_path, 'PRAGMA user_version') == [(2,)]


class FailingCommit:
    """A connection whose COMMIT fails, as a full disk or a lock timeout might."""

    def __init__(self, real):
        object.__setattr__(self, '_real', real)

    def execute(self, sql, *args):
        if sql.strip().upper() == 'COMMIT':
            raise sqlite3.OperationalError('injected commit failure')
        return self._real.execute(sql, *args)

    def __getattr__(self, name):
        return getattr(self._real, name)

    def __setattr__(self, name, value):
        setattr(self._real, name, value)


def test_failed_commit_cannot_leave_version_2_with_a_partial_schema(db_path, monkeypatch):
    make_v1_database(db_path, LEGACY)
    before = v1_snapshot(db_path)
    real_connect = sqlite3.connect

    with monkeypatch.context() as patch:
        # Every statement, including PRAGMA user_version = 2, has run by now.
        patch.setattr(
            storage.sqlite3, 'connect', lambda *a, **kw: FailingCommit(real_connect(*a, **kw)))
        with pytest.raises(sqlite3.OperationalError, match='injected commit failure'):
            MissionStore(db_path)

    assert_still_version_1(db_path, before)
    with contextlib.closing(sqlite3.connect(db_path, timeout=0.1, isolation_level=None)) as conn:
        conn.execute('BEGIN IMMEDIATE')
        conn.execute('ROLLBACK')

    with MissionStore(db_path):
        pass
    assert raw(db_path, 'PRAGMA user_version') == [(2,)]
