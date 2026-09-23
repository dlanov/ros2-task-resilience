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

from collections import OrderedDict
import contextlib
from datetime import timedelta
import enum
import hashlib
import sqlite3
import sys
import uuid

import pytest

from task_resilience.errors import IdempotencyConflict, InvalidRequest
from task_resilience.model import MissionRequest
from task_resilience.storage import MissionStore


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / 'missions.db'


@pytest.fixture
def store(db_path):
    with MissionStore(db_path) as opened:
        yield opened


def mission_count(db_path) -> int:
    with contextlib.closing(sqlite3.connect(db_path)) as conn:
        return conn.execute('SELECT COUNT(*) FROM missions').fetchone()[0]


def test_first_request_creates_one_mission(store, db_path):
    record = store.create_or_get('req-001', {'x': 1, 'y': 2}, 3)

    assert record.request_id == 'req-001'
    assert record.payload == {'x': 1, 'y': 2}
    assert record.max_attempts == 3
    assert record.attempts_used == 0
    assert uuid.UUID(record.mission_id).version == 4
    assert record.created_at == record.updated_at
    assert record.created_at.utcoffset() == timedelta(0)
    assert mission_count(db_path) == 1


def test_replay_returns_the_same_mission(store, db_path):
    first = store.create_or_get('req-001', {'x': 1, 'y': 2}, 3)
    replay = store.create_or_get('req-001', {'x': 1, 'y': 2}, 3)

    assert replay.mission_id == first.mission_id
    assert replay == first
    assert mission_count(db_path) == 1


def test_lookups_return_the_created_mission(store):
    created = store.create_or_get('req-001', {'x': 1}, 3)

    assert store.get_by_request_id('req-001') == created
    assert store.get_by_mission_id(created.mission_id) == created


def test_payload_key_order_does_not_affect_identity(store, db_path):
    first = store.create_or_get('req-001', {'x': 1, 'y': {'a': 1, 'b': 2}}, 3)
    replay = store.create_or_get('req-001', {'y': {'b': 2, 'a': 1}, 'x': 1}, 3)

    assert replay.mission_id == first.mission_id
    assert replay.request_fingerprint == first.request_fingerprint
    assert mission_count(db_path) == 1


def test_list_order_is_part_of_the_request(store):
    # Step order will matter to a mission, so [a, b] is not [b, a].
    store.create_or_get('req-001', {'steps': ['a', 'b']}, 3)

    with pytest.raises(IdempotencyConflict):
        store.create_or_get('req-001', {'steps': ['b', 'a']}, 3)


def test_changed_payload_is_an_idempotency_conflict(store, db_path):
    original = store.create_or_get('req-001', {'x': 1, 'y': 2}, 3)

    with pytest.raises(IdempotencyConflict) as excinfo:
        store.create_or_get('req-001', {'x': 2, 'y': 2}, 3)

    assert excinfo.value.request_id == 'req-001'
    # The conflicting replay must not overwrite or duplicate the accepted mission.
    assert store.get_by_request_id('req-001') == original
    assert mission_count(db_path) == 1


def test_changed_max_attempts_is_an_idempotency_conflict(store, db_path):
    original = store.create_or_get('req-001', {'x': 1, 'y': 2}, 3)

    with pytest.raises(IdempotencyConflict):
        store.create_or_get('req-001', {'x': 1, 'y': 2}, 4)

    assert store.get_by_request_id('req-001').max_attempts == 3
    assert store.get_by_request_id('req-001') == original
    assert mission_count(db_path) == 1


def test_different_request_ids_are_different_missions_even_with_equal_contents(store, db_path):
    # Identity is the explicit request ID; equal contents are not deduplicated.
    first = store.create_or_get('req-001', {'x': 1}, 3)
    second = store.create_or_get('req-002', {'x': 1}, 3)

    assert first.mission_id != second.mission_id
    assert mission_count(db_path) == 2


@pytest.mark.parametrize('max_attempts', [0, -1, True, 2.5, '3', None])
def test_invalid_max_attempts_is_rejected_and_creates_nothing(store, db_path, max_attempts):
    with pytest.raises(InvalidRequest):
        store.create_or_get('req-001', {'x': 1}, max_attempts)

    assert mission_count(db_path) == 0


@pytest.mark.parametrize('request_id', ['', None, 7])
def test_invalid_request_id_is_rejected_and_creates_nothing(store, db_path, request_id):
    with pytest.raises(InvalidRequest):
        store.create_or_get(request_id, {'x': 1}, 3)

    assert mission_count(db_path) == 0


@pytest.mark.parametrize('value', [float('nan'), float('inf'), float('-inf')])
def test_non_finite_numbers_are_rejected(store, db_path, value):
    with pytest.raises(InvalidRequest):
        store.create_or_get('req-001', {'pose': {'x': value}}, 3)
    with pytest.raises(InvalidRequest):
        store.create_or_get('req-002', [1, value], 3)

    assert mission_count(db_path) == 0


def assert_rejected(store, db_path, payload):
    with pytest.raises(InvalidRequest):
        store.create_or_get('req-001', payload, 3)

    assert mission_count(db_path) == 0


def circular_list():
    loop = []
    loop.append(loop)
    return loop


def circular_dict():
    outer = {}
    outer['inner'] = {'back': outer}
    return outer


class StrSubclass(str):
    pass


class IntSubclass(int):
    pass


class ListSubclass(list):
    pass


class DictSubclass(dict):
    pass


class Color(enum.IntEnum):
    RED = 1


@pytest.mark.parametrize('payload', [
    object(), {1, 2}, frozenset({1}), b'bytes', bytearray(b'x'), 1j, len,
    {'callback': len}, [object()],
])
def test_non_json_payload_is_rejected(store, db_path, payload):
    assert_rejected(store, db_path, payload)


@pytest.mark.parametrize('payload', [
    (1, 2),
    {'steps': ('a', 'b')},
    [1, (2, 3)],
    {'a': {'b': [{'c': (1,)}]}},
], ids=['top level', 'in dict', 'in list', 'deeply nested'])
def test_tuples_are_rejected_not_converted_to_lists(store, db_path, payload):
    assert_rejected(store, db_path, payload)


@pytest.mark.parametrize('payload', [
    {1: 'a'},
    {'a': {'b': {2: 'c'}}},
    [{'ok': 1}, {3: 'x'}],
    {'a': [{'b': [{4: 'deep'}]}]},
    {None: 1},
    {True: 1},
    {1.5: 1},
    {('a', 'b'): 1},
    {b'k': 1},
], ids=['int key', 'nested int key', 'int key in list', 'int key deep in lists',
        'None key', 'bool key', 'float key', 'tuple key', 'bytes key'])
def test_non_string_dict_keys_are_rejected_not_coerced(store, db_path, payload):
    assert_rejected(store, db_path, payload)


@pytest.mark.parametrize('payload', [
    StrSubclass('x'),
    IntSubclass(1),
    Color.RED,
    ListSubclass([1]),
    DictSubclass(a=1),
    OrderedDict(a=1),
    {StrSubclass('k'): 1},
], ids=['str', 'int', 'IntEnum', 'list', 'dict', 'OrderedDict', 'str key'])
def test_subclasses_of_json_types_are_rejected(store, db_path, payload):
    assert_rejected(store, db_path, payload)


@pytest.mark.parametrize('payload', [
    circular_list(),
    circular_dict(),
    {'steps': [circular_list()]},
], ids=['list', 'dict', 'nested'])
def test_circular_containers_are_rejected(store, db_path, payload):
    assert_rejected(store, db_path, payload)


def test_a_container_used_twice_is_not_circular(store):
    shared = [1, 2]

    created = store.create_or_get('req-001', {'a': shared, 'b': shared, 'c': [shared]}, 3)

    assert created.payload == {'a': [1, 2], 'b': [1, 2], 'c': [[1, 2]]}


def test_excessive_nesting_is_rejected(store, db_path):
    deep = []
    for _ in range(10_000):
        deep = [deep]

    assert_rejected(store, db_path, deep)


def test_integer_too_large_to_serialise_is_rejected(store, db_path):
    # The limit is process-wide and configurable, so set it rather than assume it.
    original = sys.get_int_max_str_digits()
    sys.set_int_max_str_digits(640)
    try:
        assert_rejected(store, db_path, {'n': 10 ** 1000})
    finally:
        sys.set_int_max_str_digits(original)


def test_every_json_type_is_accepted(store):
    payload = {
        'null': None,
        'flags': [True, False],
        'int': -3,
        'float': 2.5,
        'text': 'text',
        'nested': {'list': [1, [2, {'deep': None}]], 'empty': {}, 'none': []},
    }

    created = store.create_or_get('req-001', payload, 3)

    assert created.payload == payload
    assert store.get_by_request_id('req-001').payload == payload


def test_non_ascii_payload_round_trips(store):
    created = store.create_or_get('req-001', {'name': 'café → base'}, 3)

    assert store.get_by_request_id('req-001').payload == {'name': 'café → base'}
    assert store.create_or_get('req-001', {'name': 'café → base'}, 3) == created


def test_fingerprint_is_sha256_of_the_canonical_request():
    # Fingerprints are persisted. If the canonical form ever changes, legitimate
    # replays of old missions turn into conflicts, so pin it down.
    request = MissionRequest('req-001', {'y': 2, 'x': 1}, 3)

    expected = hashlib.sha256(b'{"max_attempts":3,"payload":{"x":1,"y":2}}').hexdigest()
    assert request.fingerprint == expected
    assert request.payload_json == '{"x":1,"y":2}'


def test_fingerprint_ignores_request_id_but_not_contents():
    base = MissionRequest('req-001', {'x': 1}, 3)

    assert MissionRequest('req-999', {'x': 1}, 3).fingerprint == base.fingerprint
    assert MissionRequest('req-001', {'x': 2}, 3).fingerprint != base.fingerprint
    assert MissionRequest('req-001', {'x': 1}, 4).fingerprint != base.fingerprint


def test_request_keeps_a_snapshot_not_the_callers_payload():
    payload = {'steps': ['a'], 'meta': {'x': 1}}
    request = MissionRequest('req-001', payload, 3)
    expected = MissionRequest('req-001', {'steps': ['a'], 'meta': {'x': 1}}, 3)

    payload['steps'].append('b')
    payload['meta']['x'] = 2
    payload['extra'] = True

    assert request == expected
    assert request.payload_json == expected.payload_json
    assert request.fingerprint == expected.fingerprint
    # Nothing caller-owned is retained, so there is nothing left to diverge.
    assert not hasattr(request, 'payload')
    # The mutation is real: a request built from the changed payload differs.
    assert MissionRequest('req-001', payload, 3).fingerprint != request.fingerprint


def test_requests_with_different_contents_are_not_equal():
    request = MissionRequest('req-001', {'x': 1}, 3)

    assert request == MissionRequest('req-001', {'x': 1}, 3)
    assert request != MissionRequest('req-001', {'x': 2}, 3)
    assert request != MissionRequest('req-001', {'x': 1}, 4)
    assert request != MissionRequest('req-002', {'x': 1}, 3)
