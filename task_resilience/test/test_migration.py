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

from fault_injection import open_faulty_store
import pytest

from task_resilience import storage
from task_resilience.errors import (
    AttemptLimitReached,
    CorruptDispatchRecord,
    DispatchNotFound,
    DispatchPreparationRequired,
    IdempotencyConflict,
    InvalidStateTransition,
    MissionCompletionInvariantError,
)
from task_resilience.reconciliation import ExpectedActionContext
from task_resilience.state_machine import MissionState, StepState
from task_resilience.storage import MissionStore

# The action every dispatch in these tests is declared to reach.
ACTION = ExpectedActionContext(
    action_name='/navigate_to_pose', action_type='nav2_msgs/action/NavigateToPose',
    robot_namespace='/robot1')

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


def table_names(path):
    return {row[0] for row in raw(path, "SELECT name FROM sqlite_master WHERE type = 'table'")}


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

    assert raw(db_path, 'PRAGMA user_version') == [(4,)]
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

    assert raw(db_path, 'PRAGMA user_version') == [(4,)]
    assert raw(db_path, 'SELECT COUNT(*) FROM missions') == [(0,)]


def test_migrated_schema_is_identical_to_a_fresh_one(db_path, tmp_path):
    make_v1_database(db_path, LEGACY)
    MissionStore(db_path).close()
    MissionStore(tmp_path / 'fresh.db').close()

    assert schema_of(db_path) == schema_of(tmp_path / 'fresh.db')


def test_consumed_attempts_survive_migration(db_path):
    make_v1_database(db_path, LEGACY)

    with MissionStore(db_path) as store:
        # The counts written before the migration are kept as they were.
        assert store.get_by_mission_id(LEGACY[0]['mission_id']).attempts_used == 2
        assert store.get_by_mission_id(LEGACY[2]['mission_id']).attempts_used == 1

        for mission_id in (LEGACY[0]['mission_id'], LEGACY[2]['mission_id']):
            store.initialize_steps(mission_id, ['checkpoint_a', 'return_to_base'])
            store.transition_mission_state(mission_id, MissionState.RUNNING)
            store.prepare_step_dispatch(mission_id, 'checkpoint_a', ACTION)
            store.prepare_step_dispatch(mission_id, 'return_to_base', ACTION)

        # A legacy mission continues from its count: 2 used of 3 leaves one send.
        first = store.authorize_step_send(LEGACY[0]['mission_id'], 'checkpoint_a')
        assert first.attempt_number == 3
        with pytest.raises(AttemptLimitReached):
            store.authorize_step_send(LEGACY[0]['mission_id'], 'return_to_base')
        # A mission that was already spent stays spent.
        with pytest.raises(AttemptLimitReached):
            store.authorize_step_send(LEGACY[2]['mission_id'], 'checkpoint_a')
        assert store.get_by_mission_id(LEGACY[2]['mission_id']).attempts_used == 1


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
    # LEGACY[1] has used none of its 5 attempts: enough for both sends.
    mission_id = LEGACY[1]['mission_id']

    with MissionStore(db_path) as store:
        steps = store.initialize_steps(mission_id, ['checkpoint_a', 'return_to_base'])
        assert [step.state for step in steps] == [StepState.PENDING, StepState.PENDING]

        assert store.transition_mission_state(mission_id, MissionState.RUNNING).state is (
            MissionState.RUNNING)
        with pytest.raises(MissionCompletionInvariantError):
            store.transition_mission_state(mission_id, MissionState.COMPLETED)

        for key in ('checkpoint_a', 'return_to_base'):
            store.prepare_step_dispatch(mission_id, key, ACTION)
            store.authorize_step_send(mission_id, key)
            store.transition_step_state(mission_id, key, StepState.SUCCEEDED)
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

    assert raw(db_path, 'PRAGMA user_version') == [(4,)]
    assert v1_snapshot(db_path) == before
    assert raw(db_path, 'SELECT DISTINCT mission_state FROM missions') == [('ACCEPTED',)]


# Rollback. Failures are injected without any production hook: by breaking one
# migration statement at a time, and by making COMMIT itself fail.

@pytest.mark.parametrize('statement', [
    '_MISSIONS_DDL',         # after the old table was renamed
    '_COPY_V1_MISSIONS',     # after the new table was created
    '_DROP_V1_MISSIONS',     # after the rows were copied
    '_MISSION_STEPS_DDL',    # after the old table was dropped
    '_STEP_DISPATCHES_DDL',  # after all of version 2 exists: the last step to version 3
    '_DISPATCH_CONTEXTS_DDL',  # after all of version 3 exists: the last step to version 4
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
    assert raw(db_path, 'PRAGMA user_version') == [(4,)]


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


def test_failed_commit_cannot_leave_a_new_version_with_a_partial_schema(db_path, monkeypatch):
    make_v1_database(db_path, LEGACY)
    before = v1_snapshot(db_path)
    real_connect = sqlite3.connect

    with monkeypatch.context() as patch:
        # Every statement, including PRAGMA user_version = 4, has run by now.
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
    assert raw(db_path, 'PRAGMA user_version') == [(4,)]


# Version 2 databases

# The schema exactly as Task 004 created it.
V2_MISSIONS_SCHEMA = """
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

V2_STEPS_SCHEMA = """
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

V2_MISSION_COLUMNS = V1_COLUMNS + ['mission_state']
V2_STEP_COLUMNS = [
    'mission_id', 'step_key', 'step_index', 'step_state', 'created_at', 'updated_at',
]

RUNNING_ID, RECONCILING_ID, COMPLETED_ID, INFLIGHT_ID = (
    'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
    'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb',
    'cccccccc-cccc-4ccc-8ccc-cccccccccccc',
    'dddddddd-dddd-4ddd-8ddd-dddddddddddd',
)


def v2_mission(mission_id, request_id, state, attempts, canonical, payload_json, max_attempts):
    return {
        'mission_id': mission_id,
        'request_id': request_id,
        'request_fingerprint': fingerprint(canonical),
        'request_json': payload_json,
        'max_attempts': max_attempts,
        'attempts_used': attempts,
        'created_at': '2026-04-05T06:07:08.111111+00:00',
        'updated_at': '2026-04-06T07:08:09.222222+00:00',
        'mission_state': state,
    }


V2_MISSIONS = [
    v2_mission(RUNNING_ID, 'req-v2-running', 'RUNNING', 1,
               b'{"max_attempts":3,"payload":{"go":"a"}}', '{"go":"a"}', 3),
    v2_mission(RECONCILING_ID, 'req-v2-reconciling', 'NEEDS_RECONCILIATION', 2,
               b'{"max_attempts":4,"payload":[]}', '[]', 4),
    v2_mission(COMPLETED_ID, 'req-v2-completed', 'COMPLETED', 0,
               b'{"max_attempts":1,"payload":null}', 'null', 1),
    v2_mission(INFLIGHT_ID, 'req-v2-inflight', 'RUNNING', 2,
               b'{"max_attempts":5,"payload":{"go":"c"}}', '{"go":"c"}', 5),
]

V2_STEPS = [
    # The spec's example: one step finished, one still pending.
    (RUNNING_ID, 'checkpoint_a', 0, 'SUCCEEDED'),
    (RUNNING_ID, 'return_to_base', 1, 'PENDING'),
    # Legitimate in version 2, and neither has a dispatch.
    (RECONCILING_ID, 'prepared_step', 0, 'DISPATCH_PREPARED'),
    (RECONCILING_ID, 'unknown_step', 1, 'OUTCOME_UNKNOWN'),
    (COMPLETED_ID, 'only_step', 0, 'SUCCEEDED'),
    # One step in every in-flight state, and a cancelled one. None has a dispatch.
    (INFLIGHT_ID, 'prepared', 0, 'DISPATCH_PREPARED'),
    (INFLIGHT_ID, 'dispatched', 1, 'DISPATCHED'),
    (INFLIGHT_ID, 'accepted', 2, 'ACCEPTED'),
    (INFLIGHT_ID, 'executing', 3, 'EXECUTING'),
    (INFLIGHT_ID, 'cancel_requested', 4, 'CANCEL_REQUESTED'),
    (INFLIGHT_ID, 'cancelled', 5, 'CANCELLED'),
]

# A way out of each migrated in-flight step that a plain transition allows. DISPATCHED is
# entered only by authorize_step_send, so the plain way out of a prepared step is cancellation.
INFLIGHT_EXITS = [
    ('prepared', StepState.CANCELLED),
    ('dispatched', StepState.ACCEPTED),
    ('accepted', StepState.EXECUTING),
    ('executing', StepState.SUCCEEDED),
    ('cancel_requested', StepState.CANCELLED),
]


def make_v2_database(path):
    with contextlib.closing(sqlite3.connect(path, isolation_level=None)) as conn:
        conn.execute(V2_MISSIONS_SCHEMA)
        conn.execute(V2_STEPS_SCHEMA)
        for row in V2_MISSIONS:
            conn.execute(
                f'INSERT INTO missions ({", ".join(V2_MISSION_COLUMNS)})'
                ' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                [row[column] for column in V2_MISSION_COLUMNS])
        for mission_id, key, index, state in V2_STEPS:
            conn.execute(
                f'INSERT INTO mission_steps ({", ".join(V2_STEP_COLUMNS)})'
                ' VALUES (?, ?, ?, ?, ?, ?)',
                (mission_id, key, index, state, '2026-04-05T06:07:08.111111+00:00',
                 '2026-04-05T09:00:00.333333+00:00'))
        conn.execute('PRAGMA user_version = 2')


def v2_snapshot(path):
    return (
        raw(path, f'SELECT {", ".join(V2_MISSION_COLUMNS)} FROM missions ORDER BY mission_id'),
        raw(path, f'SELECT {", ".join(V2_STEP_COLUMNS)} FROM mission_steps'
                  ' ORDER BY mission_id, step_index'),
    )


def assert_still_version_2(path, expected_snapshot, expected_schema):
    assert raw(path, 'PRAGMA user_version') == [(2,)]
    assert table_names(path) == {'missions', 'mission_steps'}
    assert v2_snapshot(path) == expected_snapshot
    assert schema_of(path) == expected_schema


def test_version_2_database_migrates_to_version_4_when_opened(db_path):
    make_v2_database(db_path)
    before = v2_snapshot(db_path)

    MissionStore(db_path).close()

    assert raw(db_path, 'PRAGMA user_version') == [(4,)]
    assert v2_snapshot(db_path) == before
    assert table_names(db_path) == {
        'missions', 'mission_steps', 'step_dispatches', 'dispatch_contexts'}


def test_migrated_version_2_states_and_attempts_are_unchanged(db_path):
    make_v2_database(db_path)

    with MissionStore(db_path) as store:
        running = store.get_by_mission_id(RUNNING_ID)
        steps = store.get_steps(RUNNING_ID)

        assert running.state is MissionState.RUNNING
        assert running.attempts_used == 1
        assert running.created_at == datetime.fromisoformat(V2_MISSIONS[0]['created_at'])
        assert running.updated_at == datetime.fromisoformat(V2_MISSIONS[0]['updated_at'])
        assert [(step.step_key, step.state) for step in steps] == [
            ('checkpoint_a', StepState.SUCCEEDED), ('return_to_base', StepState.PENDING)]
        assert store.get_by_mission_id(RECONCILING_ID).state is MissionState.NEEDS_RECONCILIATION
        assert store.get_by_mission_id(COMPLETED_ID).state is MissionState.COMPLETED


def test_no_dispatch_is_invented_for_existing_steps(db_path):
    make_v2_database(db_path)

    with MissionStore(db_path) as store:
        for mission_id, key, _, _ in V2_STEPS:
            with pytest.raises(DispatchNotFound):
                store.get_step_dispatch(mission_id, key)

    assert raw(db_path, 'SELECT COUNT(*) FROM step_dispatches') == [(0,)]
    assert raw(db_path, 'SELECT COUNT(*) FROM dispatch_contexts') == [(0,)]


def test_a_version_2_step_already_prepared_migrates_unchanged_and_gets_no_identity(db_path):
    make_v2_database(db_path)
    row = "SELECT * FROM mission_steps WHERE step_key = 'prepared_step'"
    before = raw(db_path, row)
    assert before[0][3] == 'DISPATCH_PREPARED'

    with MissionStore(db_path) as store:
        step = store.get_step(RECONCILING_ID, 'prepared_step')
        assert step.state is StepState.DISPATCH_PREPARED
        with pytest.raises(DispatchNotFound):
            store.get_step_dispatch(RECONCILING_ID, 'prepared_step')

        # Not repaired: preparation refuses rather than making an identity up,
        # and a plain transition cannot make one either.
        with pytest.raises(CorruptDispatchRecord):
            store.prepare_step_dispatch(RECONCILING_ID, 'prepared_step', ACTION)
        with pytest.raises(DispatchPreparationRequired):
            store.transition_step_state(
                RECONCILING_ID, 'prepared_step', StepState.DISPATCH_PREPARED)
        assert store.get_step(RECONCILING_ID, 'prepared_step') == step

    assert raw(db_path, row) == before
    assert raw(db_path, 'SELECT COUNT(*) FROM step_dispatches') == [(0,)]


@pytest.mark.parametrize(('key', 'exit_to'), INFLIGHT_EXITS)
def test_a_migrated_in_flight_step_is_kept_but_cannot_progress(db_path, key, exit_to):
    make_v2_database(db_path)
    before = v2_snapshot(db_path)

    with MissionStore(db_path) as store:
        mission_before = store.get_by_mission_id(INFLIGHT_ID)
        step_before = store.get_step(INFLIGHT_ID, key)

        with pytest.raises(CorruptDispatchRecord) as excinfo:
            store.transition_step_state(INFLIGHT_ID, key, exit_to)

        assert excinfo.value.mission_id == INFLIGHT_ID
        assert excinfo.value.step_key == key
        assert step_before.state.value in excinfo.value.reason
        assert store.get_step(INFLIGHT_ID, key) == step_before
        # Neither the mission nor its attempts moved.
        assert store.get_by_mission_id(INFLIGHT_ID) == mission_before
        assert mission_before.state is MissionState.RUNNING
        assert mission_before.attempts_used == 2

    # Nothing was written, timestamps included, and no ID was made up.
    assert v2_snapshot(db_path) == before
    assert raw(db_path, 'SELECT COUNT(*) FROM step_dispatches') == [(0,)]


@pytest.mark.parametrize(('mission_id', 'key', 'state'), [
    (RUNNING_ID, 'checkpoint_a', StepState.SUCCEEDED),
    (COMPLETED_ID, 'only_step', StepState.SUCCEEDED),
    (INFLIGHT_ID, 'cancelled', StepState.CANCELLED),
    (RECONCILING_ID, 'unknown_step', StepState.OUTCOME_UNKNOWN),
])
def test_a_migrated_finished_or_unknown_step_stays_readable_without_a_dispatch(
        db_path, mission_id, key, state):
    make_v2_database(db_path)

    with MissionStore(db_path) as store:
        step = store.get_step(mission_id, key)

        assert step.state is state
        assert step in store.get_steps(mission_id)
        with pytest.raises(DispatchNotFound):
            store.get_step_dispatch(mission_id, key)
        # The dispatch check does not apply to these states.
        assert store.transition_step_state(mission_id, key, state) == step


def test_a_migrated_succeeded_step_without_a_dispatch_still_counts_toward_completion(db_path):
    make_v2_database(db_path)

    with MissionStore(db_path) as store:
        legacy = store.get_step(RUNNING_ID, 'checkpoint_a')
        assert legacy.state is StepState.SUCCEEDED

        # It counts as done; the pending step is what blocks completion.
        with pytest.raises(MissionCompletionInvariantError) as excinfo:
            store.transition_mission_state(RUNNING_ID, MissionState.COMPLETED)
        assert excinfo.value.step_states == (StepState.SUCCEEDED, StepState.PENDING)

        # The pending step is finished natively, with a dispatch of its own.
        store.prepare_step_dispatch(RUNNING_ID, 'return_to_base', ACTION)
        store.authorize_step_send(RUNNING_ID, 'return_to_base')
        for state in (StepState.ACCEPTED, StepState.EXECUTING, StepState.SUCCEEDED):
            store.transition_step_state(RUNNING_ID, 'return_to_base', state)
        completed = store.transition_mission_state(RUNNING_ID, MissionState.COMPLETED)

        assert completed.state is MissionState.COMPLETED
        assert store.get_step(RUNNING_ID, 'checkpoint_a') == legacy

    assert raw(db_path, 'SELECT COUNT(*) FROM step_dispatches') == [(1,)]


def test_a_native_step_in_a_migrated_mission_progresses_normally(db_path):
    make_v2_database(db_path)

    with MissionStore(db_path) as store:
        # A step of a migrated mission that is still PENDING is fully native from here.
        prepared = store.prepare_step_dispatch(RUNNING_ID, 'return_to_base', ACTION)
        store.authorize_step_send(RUNNING_ID, 'return_to_base')
        assert store.get_step(RUNNING_ID, 'return_to_base').state is StepState.DISPATCHED
        for state in (StepState.ACCEPTED, StepState.EXECUTING):
            assert store.transition_step_state(RUNNING_ID, 'return_to_base', state).state is state

        assert store.get_step_dispatch(RUNNING_ID, 'return_to_base') == prepared


def test_migrated_version_2_schema_is_identical_to_a_fresh_one(db_path, tmp_path):
    make_v2_database(db_path)
    MissionStore(db_path).close()
    MissionStore(tmp_path / 'fresh.db').close()

    assert schema_of(db_path) == schema_of(tmp_path / 'fresh.db')


def test_dispatch_preparation_works_on_a_migrated_version_2_database(db_path):
    make_v2_database(db_path)

    with MissionStore(db_path) as store:
        record = store.prepare_step_dispatch(RUNNING_ID, 'return_to_base', ACTION)
        assert store.get_step(RUNNING_ID, 'return_to_base').state is StepState.DISPATCH_PREPARED
        assert store.get_step_dispatch(RUNNING_ID, 'return_to_base') == record

        # A finished step cannot be prepared.
        with pytest.raises(InvalidStateTransition):
            store.prepare_step_dispatch(RUNNING_ID, 'checkpoint_a', ACTION)
        # A version 2 step that was already prepared has no identity to return,
        # and none is made up for it.
        with pytest.raises(CorruptDispatchRecord):
            store.prepare_step_dispatch(RECONCILING_ID, 'prepared_step', ACTION)

    assert raw(db_path, 'SELECT COUNT(*) FROM step_dispatches') == [(1,)]


def test_existing_apis_keep_working_on_a_migrated_version_2_database(db_path):
    make_v2_database(db_path)

    with MissionStore(db_path) as store:
        # RUNNING_ID has used 1 of its 3 attempts; the next send is the second.
        store.prepare_step_dispatch(RUNNING_ID, 'return_to_base', ACTION)
        assert store.authorize_step_send(RUNNING_ID, 'return_to_base').attempt_number == 2
        store.transition_step_state(RUNNING_ID, 'return_to_base', StepState.CANCELLED)
        with pytest.raises(MissionCompletionInvariantError):
            store.transition_mission_state(RUNNING_ID, MissionState.COMPLETED)
        replay = store.create_or_get('req-v2-completed', None, 1)
        assert replay.mission_id == COMPLETED_ID
        assert replay.state is MissionState.COMPLETED


def test_concurrent_opens_of_a_version_2_database_migrate_it_once(db_path):
    make_v2_database(db_path)
    before = v2_snapshot(db_path)
    workers = 8
    barrier = threading.Barrier(workers, timeout=10)

    def open_and_close():
        barrier.wait()
        MissionStore(db_path).close()

    with ThreadPoolExecutor(max_workers=workers) as pool:
        for future in [pool.submit(open_and_close) for _ in range(workers)]:
            future.result(timeout=10)

    assert raw(db_path, 'PRAGMA user_version') == [(4,)]
    assert v2_snapshot(db_path) == before


def test_a_failed_version_2_migration_leaves_the_database_at_version_2(db_path, monkeypatch):
    make_v2_database(db_path)
    before = v2_snapshot(db_path)
    schema = schema_of(db_path)

    with monkeypatch.context() as patch:
        patch.setattr(storage, '_STEP_DISPATCHES_DDL', 'THIS IS NOT SQL')
        with pytest.raises(sqlite3.OperationalError):
            MissionStore(db_path)

    assert_still_version_2(db_path, before, schema)
    with MissionStore(db_path):
        pass
    assert raw(db_path, 'PRAGMA user_version') == [(4,)]


def test_a_failed_commit_leaves_a_version_2_database_at_version_2(db_path, monkeypatch):
    make_v2_database(db_path)
    before = v2_snapshot(db_path)
    schema = schema_of(db_path)

    with pytest.raises(sqlite3.OperationalError, match='injected failure'):
        open_faulty_store(
            monkeypatch, db_path, fail_when=lambda sql: sql.strip().upper() == 'COMMIT')

    assert_still_version_2(db_path, before, schema)
    with contextlib.closing(sqlite3.connect(db_path, timeout=0.1, isolation_level=None)) as conn:
        conn.execute('BEGIN IMMEDIATE')
        conn.execute('ROLLBACK')


def test_a_version_1_open_is_one_transaction_and_never_commits_an_intermediate_version(
        db_path, monkeypatch):
    make_v1_database(db_path, LEGACY)

    store, connection = open_faulty_store(monkeypatch, db_path)
    store.close()

    statements = connection.statements
    assert statements.count('BEGIN IMMEDIATE') == 1
    assert statements.count('COMMIT') == 1
    assert 'PRAGMA user_version = 2' not in statements
    assert 'PRAGMA user_version = 3' not in statements
    assert statements.count('PRAGMA user_version = 4') == 1
    # The whole chain, every step, ran before the single commit.
    assert (statements.index('ALTER TABLE missions RENAME TO missions_v1')
            < next(i for i, sql in enumerate(statements)
                   if sql.startswith('CREATE TABLE step_dispatches'))
            < next(i for i, sql in enumerate(statements)
                   if sql.startswith('CREATE TABLE dispatch_contexts'))
            < statements.index('PRAGMA user_version = 4')
            < statements.index('COMMIT'))


def test_a_version_1_migration_failing_at_the_last_step_leaves_version_1_not_an_intermediate(
        db_path, monkeypatch):
    make_v1_database(db_path, LEGACY)
    before = v1_snapshot(db_path)
    schema = schema_of(db_path)

    with monkeypatch.context() as patch:
        patch.setattr(storage, '_DISPATCH_CONTEXTS_DDL', 'THIS IS NOT SQL')
        with pytest.raises(sqlite3.OperationalError):
            MissionStore(db_path)

    assert_still_version_1(db_path, before)
    assert schema_of(db_path) == schema
