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

from task_resilience.errors import (
    AttemptLimitReached,
    DispatchNotFound,
    DispatchPreparationRequired,
    IdempotencyConflict,
    InvalidStateTransition,
    MissionCompletionInvariantError,
    StepDefinitionConflict,
)
from task_resilience.model import DispatchRecord
from task_resilience.reconciliation import ExpectedActionContext
from task_resilience.state_machine import MissionState, StepState
from task_resilience.storage import MissionStore

# The action every dispatch in these tests is declared to reach.
ACTION = ExpectedActionContext(
    action_name='/navigate_to_pose', action_type='nav2_msgs/action/NavigateToPose',
    robot_namespace='/robot1')

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


def running_missions(db_path, count, max_attempts, keys):
    """Create RUNNING missions whose steps are all prepared, and return their IDs."""
    mission_ids = []
    with MissionStore(db_path) as store:
        for n in range(count):
            mission = store.create_or_get(f'req-{n}', {'n': n}, max_attempts)
            store.initialize_steps(mission.mission_id, keys)
            store.transition_mission_state(mission.mission_id, MissionState.RUNNING)
            for key in keys:
                store.prepare_step_dispatch(mission.mission_id, key, ACTION)
            mission_ids.append(mission.mission_id)
    return mission_ids


def authorized(store, mission_id, step_key):
    try:
        store.authorize_step_send(mission_id, step_key)
    except AttemptLimitReached:
        return False
    return True


def test_two_callers_cannot_both_take_the_final_attempt(db_path):
    # Two different steps of one mission compete for the mission-wide budget.
    keys = ['checkpoint_a', 'return_to_base']
    mission_ids = running_missions(db_path, ROUNDS, 1, keys)

    results = run_concurrently(
        db_path, 2, ROUNDS,
        lambda store, worker, n: authorized(store, mission_ids[n], keys[worker]))

    with MissionStore(db_path) as store:
        for n, mission_id in enumerate(mission_ids):
            outcomes = [worker[n] for worker in results]
            assert outcomes.count(True) == 1, f'round {n}: {outcomes}'
            assert outcomes.count(False) == 1, f'round {n}: {outcomes}'
            assert store.get_by_mission_id(mission_id).attempts_used == 1


def test_many_callers_never_exceed_the_limit(db_path):
    workers, max_attempts, rounds = 6, 3, 10
    keys = [f'step_{n}' for n in range(workers)]
    mission_ids = running_missions(db_path, rounds, max_attempts, keys)

    results = run_concurrently(
        db_path, workers, rounds,
        lambda store, worker, n: authorized(store, mission_ids[n], keys[worker]))

    with MissionStore(db_path) as store:
        for n, mission_id in enumerate(mission_ids):
            outcomes = [worker[n] for worker in results]
            assert outcomes.count(True) == max_attempts, f'round {n}: {outcomes}'
            assert store.get_by_mission_id(mission_id).attempts_used == max_attempts
            # Each attempt that was consumed moved exactly one step.
            dispatched = [step for step in store.get_steps(mission_id)
                          if step.state is StepState.DISPATCHED]
            assert len(dispatched) == max_attempts


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
            assert conn.execute('PRAGMA user_version').fetchone()[0] == 7


def test_lock_timeout_surfaces_and_consumes_nothing(db_path):
    with MissionStore(db_path, busy_timeout=0.05) as store:
        mission = store.create_or_get('req-001', {'x': 1}, 1)
        store.initialize_steps(mission.mission_id, ['checkpoint'])
        store.transition_mission_state(mission.mission_id, MissionState.RUNNING)
        store.prepare_step_dispatch(mission.mission_id, 'checkpoint', ACTION)

        blocker = sqlite3.connect(db_path, isolation_level=None)
        blocker.execute('BEGIN IMMEDIATE')
        try:
            with pytest.raises(sqlite3.OperationalError):
                store.authorize_step_send(mission.mission_id, 'checkpoint')
        finally:
            blocker.execute('ROLLBACK')
            blocker.close()

        # The failed authorization must not have counted, and the store must still work.
        assert store.get_by_mission_id(mission.mission_id).attempts_used == 0
        assert store.get_step(mission.mission_id, 'checkpoint').state is (
            StepState.DISPATCH_PREPARED)
        assert store.authorize_step_send(mission.mission_id, 'checkpoint').attempt_number == 1


def missions_with_an_executing_step(db_path, count):
    mission_ids = []
    with MissionStore(db_path) as store:
        for n in range(count):
            mission = store.create_or_get(f'req-{n}', {'n': n}, 3)
            store.initialize_steps(mission.mission_id, ['checkpoint'])
            store.transition_mission_state(mission.mission_id, MissionState.RUNNING)
            store.prepare_step_dispatch(mission.mission_id, 'checkpoint', ACTION)
            # DISPATCHED is entered only by authorizing the send.
            store.authorize_step_send(mission.mission_id, 'checkpoint')
            for state in (StepState.ACCEPTED, StepState.EXECUTING):
                store.transition_step_state(mission.mission_id, 'checkpoint', state)
            mission_ids.append(mission.mission_id)
    return mission_ids


def test_competing_step_results_have_exactly_one_winner(db_path):
    mission_ids = missions_with_an_executing_step(db_path, ROUNDS)
    results_to_report = [StepState.SUCCEEDED, StepState.FAILED]

    def report(store, worker, n):
        try:
            store.transition_step_state(mission_ids[n], 'checkpoint', results_to_report[worker])
        except InvalidStateTransition as refusal:
            return refusal
        return results_to_report[worker]

    results = run_concurrently(db_path, 2, ROUNDS, report)

    with MissionStore(db_path) as store:
        for n, mission_id in enumerate(mission_ids):
            outcomes = [worker[n] for worker in results]
            won = [outcome for outcome in outcomes if isinstance(outcome, StepState)]
            lost = [outcome for outcome in outcomes if isinstance(outcome, InvalidStateTransition)]
            assert len(won) == 1 and len(lost) == 1, f'round {n}: {outcomes}'

            assert store.get_step(mission_id, 'checkpoint').state is won[0]
            # The loser saw the winner's persisted state and was refused by the
            # state machine, not by the database.
            (other,) = set(results_to_report) - {won[0]}
            assert lost[0].entity == 'step'
            assert lost[0].current is won[0]
            assert lost[0].target is other


def test_concurrent_identical_step_results_all_succeed(db_path):
    workers = 4
    mission_ids = missions_with_an_executing_step(db_path, ROUNDS)

    results = run_concurrently(
        db_path, workers, ROUNDS,
        lambda store, worker, n: store.transition_step_state(
            mission_ids[n], 'checkpoint', StepState.SUCCEEDED))

    for n, mission_id in enumerate(mission_ids):
        records = [worker[n] for worker in results]
        assert {record.state for record in records} == {StepState.SUCCEEDED}
        # Only the first call changed anything; the rest replayed the same state.
        assert len({record.updated_at for record in records}) == 1, f'round {n}'
    with MissionStore(db_path) as store:
        assert {store.get_step(mission_id, 'checkpoint').state
                for mission_id in mission_ids} == {StepState.SUCCEEDED}


def test_completion_never_overtakes_an_unfinished_step(db_path):
    mission_ids = missions_with_an_executing_step(db_path, ROUNDS)

    def act(store, worker, n):
        if worker == 0:
            try:
                store.transition_mission_state(mission_ids[n], MissionState.COMPLETED)
            except MissionCompletionInvariantError as refusal:
                return refusal
            return MissionState.COMPLETED
        return store.transition_step_state(mission_ids[n], 'checkpoint', StepState.SUCCEEDED).state

    results = run_concurrently(db_path, 2, ROUNDS, act)

    with MissionStore(db_path) as store:
        for n, mission_id in enumerate(mission_ids):
            mission_outcome, step_outcome = results[0][n], results[1][n]
            mission = store.get_by_mission_id(mission_id)

            # The step result always lands; only the completion depends on the order.
            assert step_outcome is StepState.SUCCEEDED
            assert store.get_step(mission_id, 'checkpoint').state is StepState.SUCCEEDED
            if mission_outcome is MissionState.COMPLETED:
                assert mission.state is MissionState.COMPLETED
            else:
                assert isinstance(mission_outcome, MissionCompletionInvariantError)
                assert mission_outcome.step_states == (StepState.EXECUTING,)
                assert mission.state is MissionState.RUNNING

    # The invariant, read straight from storage: no COMPLETED mission has zero
    # steps or any step that is not SUCCEEDED.
    with contextlib.closing(sqlite3.connect(db_path)) as conn:
        violations = conn.execute(
            """
            SELECT m.mission_id FROM missions m
            WHERE m.mission_state = 'COMPLETED' AND (
                NOT EXISTS (SELECT 1 FROM mission_steps s WHERE s.mission_id = m.mission_id)
                OR EXISTS (SELECT 1 FROM mission_steps s
                           WHERE s.mission_id = m.mission_id AND s.step_state <> 'SUCCEEDED')
            )
            """).fetchall()
    assert violations == []


def test_conflicting_step_definitions_accept_exactly_one(db_path):
    with MissionStore(db_path) as store:
        mission_ids = [
            store.create_or_get(f'req-{n}', {'n': n}, 1).mission_id for n in range(ROUNDS)]
    definitions = [['a', 'b'], ['b', 'a', 'c']]

    def define(store, worker, n):
        try:
            return store.initialize_steps(mission_ids[n], definitions[worker])
        except StepDefinitionConflict as conflict:
            return conflict

    results = run_concurrently(db_path, 2, ROUNDS, define)

    with MissionStore(db_path) as store:
        for n, mission_id in enumerate(mission_ids):
            outcomes = [worker[n] for worker in results]
            won = [outcome for outcome in outcomes if isinstance(outcome, tuple)]
            lost = [outcome for outcome in outcomes if isinstance(outcome, StepDefinitionConflict)]
            assert len(won) == 1 and len(lost) == 1, f'round {n}: {outcomes}'

            stored = store.get_steps(mission_id)
            assert stored == won[0]
            winning_keys = [step.step_key for step in stored]
            (other_keys,) = [keys for keys in definitions if keys != winning_keys]
            assert lost[0].existing_keys == tuple(winning_keys)
            assert lost[0].requested_keys == tuple(other_keys)


def test_concurrent_identical_step_definitions_create_one_list(db_path):
    with MissionStore(db_path) as store:
        mission_ids = [
            store.create_or_get(f'req-{n}', {'n': n}, 1).mission_id for n in range(ROUNDS)]

    results = run_concurrently(
        db_path, 8, ROUNDS,
        lambda store, worker, n: store.initialize_steps(mission_ids[n], ['a', 'b', 'c']))

    with MissionStore(db_path) as store:
        for n, mission_id in enumerate(mission_ids):
            assert {worker[n] for worker in results} == {store.get_steps(mission_id)}
    with contextlib.closing(sqlite3.connect(db_path)) as conn:
        assert conn.execute('SELECT COUNT(*) FROM mission_steps').fetchone()[0] == 3 * ROUNDS


def missions_with_a_pending_step(db_path, count):
    with MissionStore(db_path) as store:
        mission_ids = []
        for n in range(count):
            mission = store.create_or_get(f'req-{n}', {'n': n}, 3)
            store.initialize_steps(mission.mission_id, ['checkpoint'])
            mission_ids.append(mission.mission_id)
    return mission_ids


def test_concurrent_preparation_of_one_step_gives_everyone_the_same_dispatch(db_path):
    workers = 8
    mission_ids = missions_with_a_pending_step(db_path, ROUNDS)

    results = run_concurrently(
        db_path, workers, ROUNDS,
        lambda store, worker, n: store.prepare_step_dispatch(mission_ids[n], 'checkpoint', ACTION))

    with MissionStore(db_path) as store:
        for n, mission_id in enumerate(mission_ids):
            records = [worker[n] for worker in results]
            assert len(set(records)) == 1, f'round {n}: {records}'
            assert store.get_step_dispatch(mission_id, 'checkpoint') == records[0]
            assert store.get_step(mission_id, 'checkpoint').state is StepState.DISPATCH_PREPARED
    with contextlib.closing(sqlite3.connect(db_path)) as conn:
        assert conn.execute('SELECT COUNT(*) FROM step_dispatches').fetchone()[0] == ROUNDS
        assert conn.execute(
            'SELECT COUNT(DISTINCT dispatch_id) FROM step_dispatches').fetchone()[0] == ROUNDS


def test_a_dispatch_exists_exactly_when_preparation_won_the_race_with_a_cancellation(db_path):
    mission_ids = missions_with_a_pending_step(db_path, ROUNDS)

    def act(store, worker, n):
        if worker == 0:
            try:
                return store.prepare_step_dispatch(mission_ids[n], 'checkpoint', ACTION)
            except InvalidStateTransition as refusal:
                return refusal
        return store.transition_step_state(mission_ids[n], 'checkpoint', StepState.CANCELLED)

    results = run_concurrently(db_path, 2, ROUNDS, act)

    prepared_first = 0
    with MissionStore(db_path) as store:
        for n, mission_id in enumerate(mission_ids):
            preparation, cancellation = results[0][n], results[1][n]

            # CANCELLED is legal from PENDING and from DISPATCH_PREPARED, so the
            # cancellation always lands, before or after the preparation.
            assert cancellation.state is StepState.CANCELLED, f'round {n}'
            assert store.get_step(mission_id, 'checkpoint').state is StepState.CANCELLED

            if isinstance(preparation, DispatchRecord):
                # Preparation committed first, then the cancellation followed the
                # state machine. The dispatch row stays: it records that
                # preparation happened, and the request may have been sent.
                prepared_first += 1
                assert store.get_step_dispatch(mission_id, 'checkpoint') == preparation
            else:
                assert isinstance(preparation, InvalidStateTransition), f'round {n}'
                assert preparation.current is StepState.CANCELLED
                assert preparation.target is StepState.DISPATCH_PREPARED
                with pytest.raises(DispatchNotFound):
                    store.get_step_dispatch(mission_id, 'checkpoint')

    # The invariants, read straight from storage.
    with contextlib.closing(sqlite3.connect(db_path)) as conn:
        def count(sql):
            return conn.execute(sql).fetchone()[0]

        assert count("""
            SELECT COUNT(*) FROM mission_steps s JOIN step_dispatches d
              ON d.mission_id = s.mission_id AND d.step_key = s.step_key
            WHERE s.step_state = 'PENDING'""") == 0
        assert count("""
            SELECT COUNT(*) FROM mission_steps s
            WHERE s.step_state = 'DISPATCH_PREPARED' AND NOT EXISTS (
                SELECT 1 FROM step_dispatches d
                WHERE d.mission_id = s.mission_id AND d.step_key = s.step_key)""") == 0
        assert count('SELECT COUNT(*) FROM step_dispatches') == prepared_first


def test_a_direct_transition_racing_preparation_cannot_leave_an_orphan_prepared_step(db_path):
    workers = 4
    mission_ids = missions_with_a_pending_step(db_path, ROUNDS)

    def act(store, worker, n):
        if worker % 2 == 0:
            return store.prepare_step_dispatch(mission_ids[n], 'checkpoint', ACTION)
        try:
            store.transition_step_state(mission_ids[n], 'checkpoint', StepState.DISPATCH_PREPARED)
        except DispatchPreparationRequired as refusal:
            return refusal
        return None

    results = run_concurrently(db_path, workers, ROUNDS, act)

    with MissionStore(db_path) as store:
        for n, mission_id in enumerate(mission_ids):
            outcomes = [worker[n] for worker in results]
            prepared = [o for o in outcomes if isinstance(o, DispatchRecord)]
            refused = [o for o in outcomes if isinstance(o, DispatchPreparationRequired)]
            # Only preparation ever succeeds, and everyone who prepared got one ID.
            assert len(prepared) == 2 and len(refused) == 2, f'round {n}: {outcomes}'
            assert len(set(prepared)) == 1
            assert store.get_step(mission_id, 'checkpoint').state is StepState.DISPATCH_PREPARED
            assert store.get_step_dispatch(mission_id, 'checkpoint') == prepared[0]

    with contextlib.closing(sqlite3.connect(db_path)) as conn:
        assert conn.execute("""
            SELECT COUNT(*) FROM mission_steps s
            WHERE s.step_state = 'DISPATCH_PREPARED' AND NOT EXISTS (
                SELECT 1 FROM step_dispatches d
                WHERE d.mission_id = s.mission_id AND d.step_key = s.step_key)
            """).fetchone()[0] == 0
        assert conn.execute('SELECT COUNT(*) FROM step_dispatches').fetchone()[0] == ROUNDS
