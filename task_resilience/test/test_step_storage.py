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

from datetime import timedelta

import pytest

from task_resilience.errors import (
    InvalidRequest,
    MissionNotFound,
    StepDefinitionConflict,
    StepNotFound,
)
from task_resilience.model import ExpectedGoalSpecification, StepRecord
from task_resilience.reconciliation import ExpectedActionContext
from task_resilience.state_machine import StepState
from task_resilience.storage import MissionStore

# The action every dispatch in these tests is declared to reach.
ACTION = ExpectedActionContext(
    action_name='/navigate_to_pose', action_type='nav2_msgs/action/NavigateToPose',
    robot_namespace='/robot1')

GOAL = ExpectedGoalSpecification(
    goal_format='test/goal/v1', payload={'target': 'checkpoint-a'})

KEYS = ['checkpoint_a', 'return_to_base']


class StrSubclass(str):
    pass


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / 'missions.db'


@pytest.fixture
def store(db_path):
    with MissionStore(db_path) as opened:
        yield opened


@pytest.fixture
def mission(store):
    return store.create_or_get('req-001', {'goal': 'demo'}, 3)


def test_initialize_creates_pending_steps_in_the_supplied_order(store, mission):
    steps = store.initialize_steps(mission.mission_id, KEYS)

    assert isinstance(steps, tuple)
    assert all(isinstance(step, StepRecord) for step in steps)
    assert [step.step_key for step in steps] == ['checkpoint_a', 'return_to_base']
    assert [step.step_index for step in steps] == [0, 1]
    assert {step.state for step in steps} == {StepState.PENDING}
    assert {step.mission_id for step in steps} == {mission.mission_id}
    assert store.get_steps(mission.mission_id) == steps


def test_step_timestamps_are_utc_and_equal_on_creation(store, mission):
    steps = store.initialize_steps(mission.mission_id, KEYS)

    for step in steps:
        assert step.created_at == step.updated_at
        assert step.created_at.utcoffset() == timedelta(0)


def test_supplied_order_decides_the_index_not_the_key(store, mission):
    steps = store.initialize_steps(mission.mission_id, ['b', 'a', 'c'])

    assert [(step.step_key, step.step_index) for step in steps] == [
        ('b', 0), ('a', 1), ('c', 2)]


def test_any_iterable_of_keys_is_accepted(store):
    first = store.create_or_get('req-001', {}, 1)
    second = store.create_or_get('req-002', {}, 1)

    from_generator = store.initialize_steps(first.mission_id, (key for key in KEYS))
    from_tuple = store.initialize_steps(second.mission_id, tuple(KEYS))

    assert [step.step_key for step in from_generator] == KEYS
    assert [step.step_key for step in from_tuple] == KEYS


def test_non_ascii_step_keys_round_trip(store, mission):
    store.initialize_steps(mission.mission_id, ['café', '→ base'])

    assert [step.step_key for step in store.get_steps(mission.mission_id)] == [
        'café', '→ base']


def test_identical_replay_returns_the_existing_rows(store, mission):
    first = store.initialize_steps(mission.mission_id, KEYS)

    replay = store.initialize_steps(mission.mission_id, KEYS)

    assert replay == first


def test_replay_does_not_reset_state_or_rewrite_timestamps(store, mission):
    store.initialize_steps(mission.mission_id, KEYS)
    store.prepare_step_dispatch(mission.mission_id, 'checkpoint_a', ACTION, GOAL)
    moved = store.get_step(mission.mission_id, 'checkpoint_a')
    before = store.get_steps(mission.mission_id)

    replay = store.initialize_steps(mission.mission_id, KEYS)

    assert replay == before
    assert replay[0].state is StepState.DISPATCH_PREPARED
    assert replay[0].updated_at == moved.updated_at
    assert replay[0].created_at < replay[0].updated_at


@pytest.mark.parametrize('requested', [
    ['checkpoint_a', 'somewhere_else'],
    ['checkpoint_a', 'return_to_base', 'extra'],
    ['checkpoint_a'],
    ['return_to_base', 'checkpoint_a'],
    ['extra', 'checkpoint_a', 'return_to_base'],
    ['CHECKPOINT_A', 'return_to_base'],
], ids=['changed key', 'added key', 'removed key', 'reordered', 'prepended', 'case changed'])
def test_a_different_step_list_is_a_conflict(store, mission, requested):
    original = store.initialize_steps(mission.mission_id, KEYS)

    with pytest.raises(StepDefinitionConflict) as excinfo:
        store.initialize_steps(mission.mission_id, requested)

    assert excinfo.value.mission_id == mission.mission_id
    assert excinfo.value.existing_keys == tuple(KEYS)
    assert excinfo.value.requested_keys == tuple(requested)
    # The stored definition is never replaced.
    assert store.get_steps(mission.mission_id) == original


def test_initializing_steps_for_an_unknown_mission_fails(store):
    with pytest.raises(MissionNotFound) as excinfo:
        store.initialize_steps('no-such-mission', KEYS)

    assert excinfo.value.identifier == 'no-such-mission'


@pytest.mark.parametrize('keys', [
    [],
    (),
    [''],
    ['a', ''],
    ['a', 'a'],
    ['a', 'b', 'a'],
    [1],
    ['a', 2],
    [None],
    [b'a'],
    [('a',)],
    [StrSubclass('a')],
    'checkpoint_a',
    'abc',
    'a',
    b'checkpoint_a',
    None,
    5,
], ids=['empty list', 'empty tuple', 'empty key', 'empty key second', 'duplicate',
        'duplicate apart', 'int', 'int second', 'None', 'bytes', 'tuple', 'str subclass',
        'bare string', 'bare string, no repeated letters', 'bare one-letter string',
        'bare bytes', 'None instead of list', 'int instead of list'])
def test_invalid_step_keys_are_rejected_and_create_nothing(store, mission, keys):
    with pytest.raises(InvalidRequest):
        store.initialize_steps(mission.mission_id, keys)

    assert store.get_steps(mission.mission_id) == ()


def test_a_generator_with_a_duplicate_is_rejected_after_being_read_once(store, mission):
    with pytest.raises(InvalidRequest):
        store.initialize_steps(mission.mission_id, (key for key in ['a', 'b', 'a']))

    assert store.get_steps(mission.mission_id) == ()


def test_get_steps_is_empty_before_initialization(store, mission):
    assert store.get_steps(mission.mission_id) == ()


def test_get_steps_of_an_unknown_mission_fails(store):
    with pytest.raises(MissionNotFound):
        store.get_steps('no-such-mission')


def test_get_step_returns_one_step(store, mission):
    steps = store.initialize_steps(mission.mission_id, KEYS)

    assert store.get_step(mission.mission_id, 'return_to_base') == steps[1]


def test_unknown_step_is_reported_as_a_missing_step(store, mission):
    store.initialize_steps(mission.mission_id, KEYS)

    with pytest.raises(StepNotFound) as excinfo:
        store.get_step(mission.mission_id, 'no_such_step')

    assert excinfo.value.mission_id == mission.mission_id
    assert excinfo.value.step_key == 'no_such_step'


def test_step_lookup_on_an_unknown_mission_is_a_missing_mission_not_a_missing_step(store):
    with pytest.raises(MissionNotFound):
        store.get_step('no-such-mission', 'checkpoint_a')


def test_step_of_a_mission_without_steps_is_a_missing_step(store, mission):
    with pytest.raises(StepNotFound):
        store.get_step(mission.mission_id, 'checkpoint_a')


def test_the_same_step_keys_can_be_used_by_different_missions(store):
    first = store.create_or_get('req-001', {}, 1)
    second = store.create_or_get('req-002', {}, 1)
    store.initialize_steps(first.mission_id, KEYS)
    store.initialize_steps(second.mission_id, KEYS)

    store.prepare_step_dispatch(first.mission_id, 'checkpoint_a', ACTION, GOAL)

    assert store.get_step(first.mission_id, 'checkpoint_a').state is StepState.DISPATCH_PREPARED
    assert store.get_step(second.mission_id, 'checkpoint_a').state is StepState.PENDING


def test_step_records_are_read_only(store, mission):
    step = store.initialize_steps(mission.mission_id, KEYS)[0]

    with pytest.raises(AttributeError):
        step.state = StepState.SUCCEEDED
