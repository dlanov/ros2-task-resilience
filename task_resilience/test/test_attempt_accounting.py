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
import sqlite3

import pytest

from task_resilience.errors import AttemptLimitReached, MissionNotFound
from task_resilience.storage import MissionStore


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / 'missions.db'


@pytest.fixture
def store(db_path):
    with MissionStore(db_path) as opened:
        yield opened


def test_consume_increments_from_zero_to_one(store):
    mission = store.create_or_get('req-001', {'x': 1}, 3)

    updated = store.consume_attempt(mission.mission_id)

    assert updated.attempts_used == 1
    assert updated.mission_id == mission.mission_id
    assert updated.created_at == mission.created_at
    assert updated.updated_at >= mission.updated_at
    assert store.get_by_mission_id(mission.mission_id) == updated


def test_consumption_stops_at_the_accepted_maximum(store):
    mission = store.create_or_get('req-001', {'x': 1}, 2)

    assert store.consume_attempt(mission.mission_id).attempts_used == 1
    assert store.consume_attempt(mission.mission_id).attempts_used == 2
    exhausted = store.get_by_mission_id(mission.mission_id)

    for _ in range(2):
        with pytest.raises(AttemptLimitReached) as excinfo:
            store.consume_attempt(mission.mission_id)
        assert excinfo.value.mission_id == mission.mission_id
        assert excinfo.value.max_attempts == 2

    # A refused attempt changes nothing, including the timestamp.
    assert store.get_by_mission_id(mission.mission_id) == exhausted
    assert exhausted.attempts_used == 2


def test_unknown_mission_raises_mission_not_found(store):
    with pytest.raises(MissionNotFound) as excinfo:
        store.consume_attempt('no-such-mission')
    assert excinfo.value.identifier == 'no-such-mission'

    with pytest.raises(MissionNotFound):
        store.get_by_mission_id('no-such-mission')
    with pytest.raises(MissionNotFound):
        store.get_by_request_id('no-such-request')


def test_replaying_a_request_does_not_replenish_attempts(store):
    mission = store.create_or_get('req-001', {'x': 1}, 2)
    store.consume_attempt(mission.mission_id)
    store.consume_attempt(mission.mission_id)

    replay = store.create_or_get('req-001', {'x': 1}, 2)

    assert replay.mission_id == mission.mission_id
    assert replay.attempts_used == 2
    with pytest.raises(AttemptLimitReached):
        store.consume_attempt(mission.mission_id)


def test_attempts_are_counted_per_mission(store):
    first = store.create_or_get('req-001', {'x': 1}, 1)
    second = store.create_or_get('req-002', {'x': 1}, 1)

    store.consume_attempt(first.mission_id)

    assert store.get_by_mission_id(second.mission_id).attempts_used == 0
    assert store.consume_attempt(second.mission_id).attempts_used == 1


@pytest.mark.parametrize('statement', [
    'UPDATE missions SET attempts_used = 4',    # above max_attempts
    'UPDATE missions SET attempts_used = -1',
    'UPDATE missions SET max_attempts = 0',
    'UPDATE missions SET max_attempts = 1',     # below attempts_used
])
def test_schema_rejects_inconsistent_counters(store, db_path, statement):
    mission = store.create_or_get('req-001', {'x': 1}, 3)
    store.consume_attempt(mission.mission_id)
    store.consume_attempt(mission.mission_id)

    with contextlib.closing(sqlite3.connect(db_path)) as conn:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(statement)

    assert store.get_by_mission_id(mission.mission_id).attempts_used == 2
