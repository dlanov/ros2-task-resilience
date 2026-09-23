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
import sqlite3
import threading

import pytest

from task_resilience.errors import AttemptLimitReached, IdempotencyConflict
from task_resilience.storage import MissionStore

# Each round is an independent race. A single race can be won "by luck" if the
# threads happen not to overlap, so repeat it.
ROUNDS = 25
TIMEOUT = 10


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / 'missions.db'


def run_concurrently(db_path, workers, rounds, operation):
    """
    Run operation(store, worker, round) in every worker, released together each round.

    Each worker opens its own store, on its own thread and before the first
    barrier, so only the operation itself is contended. Returns
    results[worker][round].
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


def consumed(store, mission_id):
    try:
        store.consume_attempt(mission_id)
    except AttemptLimitReached:
        return False
    return True


def test_two_consumers_cannot_both_take_the_final_attempt(db_path):
    with MissionStore(db_path) as store:
        mission_ids = [
            store.create_or_get(f'req-{n}', {'n': n}, 1).mission_id for n in range(ROUNDS)]

    results = run_concurrently(
        db_path, 2, ROUNDS, lambda store, worker, n: consumed(store, mission_ids[n]))

    with MissionStore(db_path) as store:
        for n, mission_id in enumerate(mission_ids):
            outcomes = [worker[n] for worker in results]
            assert outcomes.count(True) == 1, f'round {n}: {outcomes}'
            assert outcomes.count(False) == 1, f'round {n}: {outcomes}'
            assert store.get_by_mission_id(mission_id).attempts_used == 1


def test_many_consumers_never_exceed_the_limit(db_path):
    workers, max_attempts, rounds = 8, 3, 10
    with MissionStore(db_path) as store:
        mission_ids = [
            store.create_or_get(f'req-{n}', {'n': n}, max_attempts).mission_id
            for n in range(rounds)]

    results = run_concurrently(
        db_path, workers, rounds, lambda store, worker, n: consumed(store, mission_ids[n]))

    with MissionStore(db_path) as store:
        for n, mission_id in enumerate(mission_ids):
            outcomes = [worker[n] for worker in results]
            assert outcomes.count(True) == max_attempts, f'round {n}: {outcomes}'
            assert store.get_by_mission_id(mission_id).attempts_used == max_attempts


def test_concurrent_replays_of_one_request_create_one_mission(db_path):
    workers = 8
    # Create the schema up front; racing first opens have their own test.
    MissionStore(db_path).close()

    results = run_concurrently(
        db_path, workers, ROUNDS,
        lambda store, worker, n: store.create_or_get(f'req-{n}', {'n': n}, 3).mission_id)

    for n in range(ROUNDS):
        assert len({worker[n] for worker in results}) == 1, f'round {n}'
    with contextlib.closing(sqlite3.connect(db_path)) as conn:
        assert conn.execute('SELECT COUNT(*) FROM missions').fetchone()[0] == ROUNDS


def test_conflicting_concurrent_requests_accept_exactly_one(db_path):
    MissionStore(db_path).close()

    def submit(store, worker, n):
        # Same request ID, but each worker asks for something different.
        try:
            return store.create_or_get(f'req-{n}', {'winner': worker}, 3).mission_id
        except IdempotencyConflict:
            return None

    results = run_concurrently(db_path, 2, ROUNDS, submit)

    with MissionStore(db_path) as store:
        for n in range(ROUNDS):
            outcomes = [worker[n] for worker in results]
            accepted = [mission_id for mission_id in outcomes if mission_id is not None]
            assert len(accepted) == 1, f'round {n}: {outcomes}'
            assert outcomes.count(None) == 1, f'round {n}: {outcomes}'

            # What was stored is exactly what the accepted worker submitted.
            stored = store.get_by_request_id(f'req-{n}')
            assert stored.mission_id == accepted[0]
            assert stored.payload == {'winner': outcomes.index(accepted[0])}
    with contextlib.closing(sqlite3.connect(db_path)) as conn:
        assert conn.execute('SELECT COUNT(*) FROM missions').fetchone()[0] == ROUNDS


def test_concurrent_first_open_creates_the_schema_once(tmp_path):
    workers = 8
    barrier = threading.Barrier(workers, timeout=TIMEOUT)

    def open_and_close(path):
        barrier.wait()
        MissionStore(path).close()

    for n in range(10):
        path = tmp_path / f'fresh-{n}.db'
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(open_and_close, path) for _ in range(workers)]
            for future in futures:
                future.result(timeout=TIMEOUT)

        with contextlib.closing(sqlite3.connect(path)) as conn:
            assert conn.execute('PRAGMA user_version').fetchone()[0] == 1


def test_lock_timeout_surfaces_and_consumes_nothing(db_path):
    with MissionStore(db_path, busy_timeout=0.05) as store:
        mission = store.create_or_get('req-001', {'x': 1}, 1)

        blocker = sqlite3.connect(db_path, isolation_level=None)
        blocker.execute('BEGIN IMMEDIATE')
        try:
            with pytest.raises(sqlite3.OperationalError):
                store.consume_attempt(mission.mission_id)
        finally:
            blocker.execute('ROLLBACK')
            blocker.close()

        # The failed attempt must not have counted, and the store must still work.
        assert store.get_by_mission_id(mission.mission_id).attempts_used == 0
        assert store.consume_attempt(mission.mission_id).attempts_used == 1
