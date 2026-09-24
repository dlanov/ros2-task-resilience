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
from task_resilience.state_machine import MissionState, StepState
from task_resilience.storage import MissionStore

KEYS = ['checkpoint_a', 'return_to_base', 'final_check']


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / 'missions.db'


@pytest.fixture
def store(db_path):
    with MissionStore(db_path) as opened:
        yield opened


def running_mission(store, request_id, max_attempts, keys=KEYS[:2]):
    """Create a RUNNING mission whose steps are all prepared, and return its ID."""
    mission = store.create_or_get(request_id, {'x': 1}, max_attempts)
    store.initialize_steps(mission.mission_id, keys)
    store.transition_mission_state(mission.mission_id, MissionState.RUNNING)
    for key in keys:
        store.prepare_step_dispatch(mission.mission_id, key)
    return mission.mission_id


# An attempt is one committed authorization to send. Only authorize_step_send consumes
# one; test_send_authorization.py covers the authorization rules themselves.

def test_authorizing_a_send_increments_from_zero_to_one(store):
    mission_id = running_mission(store, 'req-001', 3)
    before = store.get_by_mission_id(mission_id)

    receipt = store.authorize_step_send(mission_id, 'checkpoint_a')

    updated = store.get_by_mission_id(mission_id)
    assert receipt.attempt_number == 1
    assert updated.attempts_used == 1
    assert updated.mission_id == mission_id
    assert updated.created_at == before.created_at
    assert updated.updated_at > before.updated_at


def test_authorization_stops_at_the_accepted_maximum(store):
    # Three prepared steps, but only two attempts.
    mission_id = running_mission(store, 'req-001', 2, KEYS)

    assert store.authorize_step_send(mission_id, 'checkpoint_a').attempt_number == 1
    assert store.authorize_step_send(mission_id, 'return_to_base').attempt_number == 2
    exhausted = store.get_by_mission_id(mission_id)

    for _ in range(2):
        with pytest.raises(AttemptLimitReached) as excinfo:
            store.authorize_step_send(mission_id, 'final_check')
        assert excinfo.value.mission_id == mission_id
        assert excinfo.value.max_attempts == 2

    # A refused authorization changes nothing, including the timestamp.
    assert store.get_by_mission_id(mission_id) == exhausted
    assert exhausted.attempts_used == 2
    assert store.get_step(mission_id, 'final_check').state is StepState.DISPATCH_PREPARED


def test_unknown_mission_raises_mission_not_found(store):
    with pytest.raises(MissionNotFound) as excinfo:
        store.authorize_step_send('no-such-mission', 'checkpoint_a')
    assert excinfo.value.identifier == 'no-such-mission'

    with pytest.raises(MissionNotFound):
        store.get_by_mission_id('no-such-mission')
    with pytest.raises(MissionNotFound):
        store.get_by_request_id('no-such-request')


def test_replaying_a_request_does_not_replenish_attempts(store):
    mission_id = running_mission(store, 'req-001', 2, KEYS)
    store.authorize_step_send(mission_id, 'checkpoint_a')
    store.authorize_step_send(mission_id, 'return_to_base')

    replay = store.create_or_get('req-001', {'x': 1}, 2)

    assert replay.mission_id == mission_id
    assert replay.attempts_used == 2
    with pytest.raises(AttemptLimitReached):
        store.authorize_step_send(mission_id, 'final_check')


def test_attempts_are_counted_per_mission(store):
    first = running_mission(store, 'req-001', 1)
    second = running_mission(store, 'req-002', 1)

    store.authorize_step_send(first, 'checkpoint_a')

    assert store.get_by_mission_id(second).attempts_used == 0
    assert store.authorize_step_send(second, 'checkpoint_a').attempt_number == 1


@pytest.mark.parametrize('statement', [
    'UPDATE missions SET attempts_used = 4',    # above max_attempts
    'UPDATE missions SET attempts_used = -1',
    'UPDATE missions SET max_attempts = 0',
    'UPDATE missions SET max_attempts = 1',     # below attempts_used
])
def test_schema_rejects_inconsistent_counters(store, db_path, statement):
    mission_id = running_mission(store, 'req-001', 3)
    store.authorize_step_send(mission_id, 'checkpoint_a')
    store.authorize_step_send(mission_id, 'return_to_base')

    with contextlib.closing(sqlite3.connect(db_path)) as conn:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(statement)

    assert store.get_by_mission_id(mission_id).attempts_used == 2
