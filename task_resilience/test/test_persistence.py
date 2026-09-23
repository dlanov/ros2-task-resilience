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
import os
from pathlib import Path
import sqlite3
import subprocess
import sys

import pytest

import task_resilience
from task_resilience.errors import (
    AttemptLimitReached,
    IdempotencyConflict,
    UnsupportedSchemaVersion,
)
from task_resilience.storage import MissionStore


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / 'missions.db'


def pragma(db_path, name):
    with contextlib.closing(sqlite3.connect(db_path)) as conn:
        return conn.execute(f'PRAGMA {name}').fetchone()[0]


def test_attempts_persist_after_reopening_the_store(db_path):
    with MissionStore(db_path) as store:
        mission = store.create_or_get('req-001', {'x': 1}, 3)
        consumed = store.consume_attempt(mission.mission_id)

    with MissionStore(db_path) as reopened:
        record = reopened.get_by_mission_id(mission.mission_id)

    assert record.attempts_used == 1
    assert record == consumed


def test_reopening_does_not_replenish_attempts(db_path):
    with MissionStore(db_path) as store:
        mission = store.create_or_get('req-001', {'x': 1}, 1)
        store.consume_attempt(mission.mission_id)

    with MissionStore(db_path) as reopened:
        # A restarted client resubmits its request and gets the spent mission back.
        replay = reopened.create_or_get('req-001', {'x': 1}, 1)
        assert replay.mission_id == mission.mission_id
        assert replay.attempts_used == 1

        with pytest.raises(AttemptLimitReached):
            reopened.consume_attempt(mission.mission_id)
        assert reopened.get_by_mission_id(mission.mission_id).attempts_used == 1


def test_request_identity_persists_after_reopening_the_store(db_path):
    with MissionStore(db_path) as store:
        mission = store.create_or_get('req-001', {'x': 1}, 3)

    with MissionStore(db_path) as reopened:
        assert reopened.create_or_get('req-001', {'x': 1}, 3) == mission
        with pytest.raises(IdempotencyConflict):
            reopened.create_or_get('req-001', {'x': 2}, 3)


def test_committed_state_does_not_depend_on_closing_the_store(db_path):
    store = MissionStore(db_path)
    try:
        mission = store.create_or_get('req-001', {'x': 1}, 3)
        store.consume_attempt(mission.mission_id)

        with MissionStore(db_path) as other:
            assert other.get_by_mission_id(mission.mission_id).attempts_used == 1
    finally:
        store.close()


_CHILD = """
import os
import sys

from task_resilience.storage import MissionStore

store = MissionStore(sys.argv[1])
store.consume_attempt(sys.argv[2])
# Die without closing the store or running any cleanup.
os._exit(0)
"""


def test_attempt_survives_a_process_that_exits_abruptly(db_path):
    with MissionStore(db_path) as store:
        mission = store.create_or_get('req-001', {'x': 1}, 2)

    package_root = Path(task_resilience.__file__).resolve().parent.parent
    subprocess.run(
        [sys.executable, '-c', _CHILD, str(db_path), mission.mission_id],
        env={**os.environ, 'PYTHONPATH': str(package_root)},
        check=True,
        timeout=30,
    )

    with MissionStore(db_path) as reopened:
        assert reopened.get_by_mission_id(mission.mission_id).attempts_used == 1
        assert reopened.consume_attempt(mission.mission_id).attempts_used == 2
        with pytest.raises(AttemptLimitReached):
            reopened.consume_attempt(mission.mission_id)


def test_schema_version_is_recorded(db_path):
    MissionStore(db_path).close()

    assert pragma(db_path, 'user_version') == 2


@pytest.mark.parametrize('version', [3, 99, -1])
def test_unsupported_schema_version_is_refused_and_leaves_no_lock_behind(db_path, version):
    MissionStore(db_path).close()
    with contextlib.closing(sqlite3.connect(db_path)) as conn:
        conn.execute(f'PRAGMA user_version = {version}')

    with pytest.raises(UnsupportedSchemaVersion) as excinfo:
        MissionStore(db_path)
    assert excinfo.value.version == version

    # The refused open must have rolled back and closed its connection.
    with contextlib.closing(sqlite3.connect(db_path, timeout=0.1, isolation_level=None)) as conn:
        conn.execute('BEGIN IMMEDIATE')
        conn.execute('ROLLBACK')
