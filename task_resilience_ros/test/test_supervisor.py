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

"""
Task 016 essential scenarios: the minimal supervisor against a real fake Nav2 action server.

Every scenario runs against FakeNav2Server (fake_nav2_server.py), a real rclpy.ActionServer in
an isolated per-test action namespace -- never a physical robot or the real Nav2 stack. The one
genuine supervisor-process restart test invokes the real, installed CLI
(`python3 -m task_resilience_ros.supervisor_cli`) via ordinary subprocess.Popen, mirroring
test_nav2_adapter.py's own small subprocess pattern -- no new general process harness.
"""

from datetime import datetime, timezone
import hashlib
import os
import sqlite3
import subprocess
import sys

from fake_nav2_server import FakeNav2Server, SpinningNode, unique_action_name
import pytest
import rclpy

import task_resilience as _task_resilience_module
from task_resilience.errors import DispatchNotFound, IdempotencyConflict, MissionNotFound
from task_resilience.model import StepRecord
from task_resilience.state_machine import MissionState, StepState
from task_resilience.storage import MissionStore

from task_resilience_ros.errors import ExecutionOwnerAlreadyHeld, InvalidGoalPayload
from task_resilience_ros.execution_owner import ExecutionOwner
from task_resilience_ros.nav2_adapter import Nav2Adapter
import task_resilience_ros.supervisor as sup
import task_resilience_ros.supervisor_cli as sup_cli

BOUNDED = 10.0
SHORT = 0.5

CHECKPOINT_PAYLOAD = {
    'frame_id': 'map',
    'position': {'x': 0.5, 'y': 0.5, 'z': 0.0},
    'orientation': {'x': 0.0, 'y': 0.0, 'z': 0.0, 'w': 1.0},
    'behavior_tree': '',
}
BASE_PAYLOAD = {
    'frame_id': 'map',
    'position': {'x': -2.0, 'y': -0.5, 'z': 0.0},
    'orientation': {'x': 0.0, 'y': 0.0, 'z': 0.0, 'w': 1.0},
    'behavior_tree': '',
}
OTHER_CHECKPOINT_PAYLOAD = {
    'frame_id': 'map',
    'position': {'x': 1.0, 'y': 1.0, 'z': 0.0},
    'orientation': {'x': 0.0, 'y': 0.0, 'z': 0.0, 'w': 1.0},
    'behavior_tree': '',
}


# --------------------------------------------------------------------------
# Fixtures
# --------------------------------------------------------------------------

@pytest.fixture
def ros():
    rclpy.init()
    yield
    rclpy.try_shutdown()


@pytest.fixture
def server(ros):
    spinner = SpinningNode('t016_fake_server_node')
    fake = FakeNav2Server(spinner.node, unique_action_name('t016'))
    try:
        yield fake
    finally:
        fake.destroy()
        spinner.stop(BOUNDED)


@pytest.fixture
def client_node(ros):
    node = rclpy.create_node('t016_supervisor_test_client')
    try:
        yield node
    finally:
        node.destroy_node()


@pytest.fixture
def adapter(client_node):
    a = Nav2Adapter(client_node)
    try:
        yield a
    finally:
        a.close()


@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / 'mission.db')


@pytest.fixture
def owner(db_path):
    o = ExecutionOwner(db_path)
    o.acquire()
    try:
        yield o
    finally:
        o.close()


@pytest.fixture
def store(db_path, owner):
    with MissionStore(owner.canonical_db_path) as opened:
        yield opened


def run_it(owner, store, adapter, server, **kwargs):
    kwargs.setdefault('poll_attempts', 5)
    kwargs.setdefault('poll_result_timeout_s', BOUNDED)
    return sup.run_mission(
        owner=owner, store=store, adapter=adapter, action_name=server.action_name,
        checkpoint_goal_payload=kwargs.pop('checkpoint_goal_payload', CHECKPOINT_PAYLOAD),
        base_goal_payload=kwargs.pop('base_goal_payload', BASE_PAYLOAD), **kwargs)


def resume_it(owner, store, adapter, **kwargs):
    kwargs.setdefault('poll_attempts', 5)
    kwargs.setdefault('poll_result_timeout_s', BOUNDED)
    return sup.resume_mission(owner=owner, store=store, adapter=adapter, **kwargs)


# --------------------------------------------------------------------------
# 1. Ordered two-step success; attempts; identical replay; changed-definition conflict
# --------------------------------------------------------------------------

def test_ordered_two_step_mission_succeeds_with_two_attempts(owner, store, adapter, server):
    report = run_it(owner, store, adapter, server)
    assert report.outcome == 'COMPLETED'
    assert [s.state for s in report.steps] == [StepState.SUCCEEDED, StepState.SUCCEEDED]
    assert report.mission.attempts_used == 2
    assert report.mission.max_attempts == 2
    assert server.goal_request_count == 2
    assert server.send_count == 2


def test_identical_completed_replay_produces_no_additional_requests(
        owner, store, adapter, server):
    first = run_it(owner, store, adapter, server)
    assert first.outcome == 'COMPLETED'
    assert server.goal_request_count == 2

    second = run_it(owner, store, adapter, server)
    assert second.outcome == 'COMPLETED'
    assert second.mission.mission_id == first.mission.mission_id
    assert second.mission.attempts_used == 2
    # A replay of an already-COMPLETED mission submits nothing further.
    assert server.goal_request_count == 2
    assert server.send_count == 2


def test_changed_definition_under_same_request_identity_conflicts(
        owner, store, adapter, server):
    run_it(owner, store, adapter, server)
    with pytest.raises(IdempotencyConflict):
        run_it(owner, store, adapter, server, checkpoint_goal_payload=OTHER_CHECKPOINT_PAYLOAD)
    # The conflicting replay must not have reached the server at all.
    assert server.goal_request_count == 2


# --------------------------------------------------------------------------
# 2. A genuine, separate supervisor OS process restarts after an authorized submission
# --------------------------------------------------------------------------

PACKAGE_PARENT_DIR = os.path.dirname(os.path.dirname(sup.__file__))
CORE_PARENT_DIR = os.path.dirname(os.path.dirname(_task_resilience_module.__file__))


def _subprocess_env(extra=None):
    env = dict(os.environ)
    parts = [PACKAGE_PARENT_DIR, CORE_PARENT_DIR]
    existing = env.get('PYTHONPATH', '')
    if existing:
        parts.append(existing)
    env['PYTHONPATH'] = os.pathsep.join(parts)
    if extra:
        env.update(extra)
    return env


def _run_cli_bounded(args, timeout_s, extra_env=None):
    proc = subprocess.Popen(
        [sys.executable, '-m', 'task_resilience_ros.supervisor_cli', *args],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        env=_subprocess_env(extra_env))
    try:
        stdout, stderr = proc.communicate(timeout=timeout_s)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.communicate(timeout=5)
        raise AssertionError(f'supervisor CLI {args} did not complete within {timeout_s}s')
    return proc.pid, proc.returncode, stdout, stderr


def test_genuine_supervisor_process_restart_recovers_result(db_path, server):
    """
    Recover a retained result across a genuine, separate supervisor process restart.

    A genuinely separate supervisor PROCESS submits checkpoint_a and pauses after send (the
    documented TASK016_PAUSE_AFTER_SEND demo/test checkpoint, logged in its own stdout), before
    ever polling for the result. A second, separate process then resumes against the SAME
    database and recovers that exact dispatch's retained result without resending it.
    """
    first_pid, first_rc, first_out, first_err = _run_cli_bounded(
        ['run', '--db', db_path, '--action-name', server.action_name,
         '--checkpoint-x', '0.5', '--checkpoint-y', '0.5',
         '--base-x', '-2.0', '--base-y', '-0.5'],
        BOUNDED, extra_env={sup.PAUSE_AFTER_SEND_ENV: '1'})
    assert first_rc == 7, (first_out, first_err)  # PAUSED_FOR_DEMO
    assert 'OUTCOME PAUSED_FOR_DEMO' in first_out
    assert f'STEP checkpoint_a state={StepState.DISPATCHED.value}' in first_out

    assert server.goal_request_count == 1
    assert server.send_count == 1

    second_pid, second_rc, second_out, second_err = _run_cli_bounded(
        ['resume', '--db', db_path], BOUNDED)
    assert second_rc == 0, (second_out, second_err)  # COMPLETED
    assert 'OUTCOME COMPLETED' in second_out
    assert f'STEP checkpoint_a state={StepState.SUCCEEDED.value}' in second_out
    assert f'STEP return_to_base state={StepState.SUCCEEDED.value}' in second_out

    assert len({first_pid, second_pid, os.getpid()}) == 3
    # Exactly one request reached the server for EACH of the two steps -- recovery of
    # checkpoint_a's retained result, and the fresh send of return_to_base, never a resend.
    assert server.goal_request_count == 2
    assert server.send_count == 2


# --------------------------------------------------------------------------
# 3. A native DISPATCH_PREPARED step with no authorization survives a restart (fresh
#    objects) and receives its first authorization/send exactly once
# --------------------------------------------------------------------------

def test_dispatch_prepared_without_authorization_is_authorized_and_sent_exactly_once(
        store, owner, server, client_node):
    # Prepare checkpoint_a's dispatch directly through the core's own public API, exactly as
    # an interrupted run_mission would have left it: a dispatch, context and goal are bound,
    # but authorize_step_send was never reached. Fresh objects stand in for "restart" here
    # (see test_genuine_supervisor_process_restart_recovers_result above for a real process).
    mission = store.create_or_get(
        sup.PREVIEW_REQUEST_ID,
        sup.build_mission_payload(
            action_name=server.action_name, robot_namespace='',
            checkpoint_goal_payload=CHECKPOINT_PAYLOAD, base_goal_payload=BASE_PAYLOAD),
        sup.DEFAULT_MAX_ATTEMPTS)
    store.initialize_steps(mission.mission_id, sup.STEP_ORDER)
    store.transition_mission_state(mission.mission_id, MissionState.RUNNING)
    definitions = sup._parse_definition(mission.payload)
    store.prepare_step_dispatch(
        mission.mission_id, sup.CHECKPOINT_STEP,
        sup._action_context(definitions[sup.CHECKPOINT_STEP]),
        sup._goal_spec(definitions[sup.CHECKPOINT_STEP]))
    before = store.get_step(mission.mission_id, sup.CHECKPOINT_STEP)
    assert before.state is StepState.DISPATCH_PREPARED
    assert server.goal_request_count == 0

    fresh_adapter = Nav2Adapter(client_node)
    try:
        report = resume_it(owner, store, fresh_adapter)
    finally:
        fresh_adapter.close()

    assert report.outcome == 'COMPLETED'
    assert server.goal_request_count == 2  # checkpoint_a authorized+sent once, then return_to_base
    assert server.send_count == 2


# --------------------------------------------------------------------------
# A non-terminal report always reflects a FRESH read, never a stale snapshot taken
# earlier in the same invocation (regression: attempts_used was once read before the
# authorize_step_send call that incremented it)
# --------------------------------------------------------------------------

def test_paused_report_reflects_fresh_attempts_used_not_a_stale_snapshot(
        owner, store, adapter, server, monkeypatch):
    monkeypatch.setenv(sup.PAUSE_AFTER_SEND_ENV, '1')
    report = run_it(owner, store, adapter, server)
    assert report.outcome == sup.OUTCOME_PAUSED_FOR_DEMO
    # authorize_step_send for checkpoint_a already committed (it happens before adapter.send,
    # which is what TASK016_PAUSE_AFTER_SEND pauses after) -- the reported mission must show
    # that attempt, not a snapshot taken before it.
    assert report.mission.attempts_used == 1


# --------------------------------------------------------------------------
# 4. Authorization commits but the process exits before send is ever invoked
# --------------------------------------------------------------------------

def test_authorization_without_send_recovers_as_unresolved_and_blocks_further_work(
        owner, store, adapter, server, monkeypatch):
    monkeypatch.setenv(sup.PAUSE_AFTER_AUTHORIZE_ENV, '1')
    paused = run_it(owner, store, adapter, server)
    assert paused.outcome == sup.OUTCOME_PAUSED_FOR_DEMO
    step = store.get_step(paused.mission.mission_id, sup.CHECKPOINT_STEP)
    assert step.state is StepState.DISPATCHED
    # authorize_step_send committed, but adapter.send was never called: nothing reached the
    # server at all.
    assert server.goal_request_count == 0
    assert server.send_count == 0
    monkeypatch.delenv(sup.PAUSE_AFTER_AUTHORIZE_ENV)

    first_resume = resume_it(owner, store, adapter)
    assert first_resume.outcome == sup.OUTCOME_UNRESOLVED
    step_after = store.get_step(paused.mission.mission_id, sup.CHECKPOINT_STEP)
    # No resend, no fabricated durable OUTCOME_UNKNOWN, no next-step authorization.
    assert step_after.state is StepState.DISPATCHED
    assert server.goal_request_count == 0
    assert server.send_count == 0
    return_step = store.get_step(paused.mission.mission_id, sup.RETURN_STEP)
    assert return_step.state is StepState.PENDING

    # A second resume remains held at exactly the same place.
    second_resume = resume_it(owner, store, adapter)
    assert second_resume.outcome == sup.OUTCOME_UNRESOLVED
    assert store.get_step(
        paused.mission.mission_id, sup.CHECKPOINT_STEP).state is StepState.DISPATCHED
    assert server.goal_request_count == 0


# --------------------------------------------------------------------------
# 5. Timeout recovery leaves durable state unchanged and blocks advancement; later
#    genuine terminal evidence permits supported progress
# --------------------------------------------------------------------------

def test_timeout_recovery_leaves_state_unchanged_then_later_evidence_permits_progress(
        owner, store, adapter, server):
    server.set_result_delay(1.5)
    timed_out = run_it(owner, store, adapter, server, poll_attempts=1, poll_result_timeout_s=SHORT)
    assert timed_out.outcome == sup.OUTCOME_UNRESOLVED
    step = store.get_step(timed_out.mission.mission_id, sup.CHECKPOINT_STEP)
    assert step.state is StepState.DISPATCHED
    assert server.send_count == 1  # the timed-out poll never triggered a resend

    resumed = resume_it(owner, store, adapter, poll_attempts=3, poll_result_timeout_s=BOUNDED)
    assert resumed.outcome == 'COMPLETED'
    assert server.send_count == 2  # checkpoint_a's one send, return_to_base's one send
    assert server.goal_request_count == 2


# --------------------------------------------------------------------------
# 6. A second supervisor is refused while the first holds ownership; a known
#    rejection/failure also prevents sending the next step
# --------------------------------------------------------------------------

def test_second_supervisor_refused_while_first_holds_ownership(db_path, owner):
    second = ExecutionOwner(db_path)
    with pytest.raises(ExecutionOwnerAlreadyHeld):
        second.acquire()
    assert not second.acquired
    # No execution-capable activity (not even opening MissionStore) ever happened for the
    # refused second attempt; this is simply confirmed by second.acquired staying False and
    # no exception other than ExecutionOwnerAlreadyHeld being raised above.


def test_rejected_send_prevents_authorizing_next_step(owner, store, adapter, server):
    server.set_accept(False)
    report = run_it(owner, store, adapter, server)
    assert report.outcome == 'FAILED'
    assert report.mission.state is MissionState.FAILED
    checkpoint = store.get_step(report.mission.mission_id, sup.CHECKPOINT_STEP)
    assert checkpoint.state is StepState.FAILED
    return_step = store.get_step(report.mission.mission_id, sup.RETURN_STEP)
    assert return_step.state is StepState.PENDING
    assert server.goal_request_count == 1
    assert server.send_count == 0


# --------------------------------------------------------------------------
# Compact assertion: final-step success persisted, mission completion not yet recorded
# --------------------------------------------------------------------------

def test_recovers_mission_completion_after_final_step_success_but_before_completed_recorded(
        owner, store, adapter, server, monkeypatch):
    real_transition = MissionStore.transition_mission_state

    def crash_before_completed(self, mission_id, target):
        if target is MissionState.COMPLETED:
            raise RuntimeError('injected crash: exit after the final step, before COMPLETED')
        return real_transition(self, mission_id, target)

    monkeypatch.setattr(MissionStore, 'transition_mission_state', crash_before_completed)
    with pytest.raises(RuntimeError, match='injected crash'):
        run_it(owner, store, adapter, server)
    monkeypatch.undo()

    mission = store.get_by_request_id(sup.PREVIEW_REQUEST_ID)
    assert mission.state is MissionState.RUNNING
    assert store.get_step(mission.mission_id, sup.CHECKPOINT_STEP).state is StepState.SUCCEEDED
    assert store.get_step(mission.mission_id, sup.RETURN_STEP).state is StepState.SUCCEEDED
    assert server.goal_request_count == 2

    recovered = resume_it(owner, store, adapter)
    assert recovered.outcome == 'COMPLETED'
    assert recovered.mission.state is MissionState.COMPLETED
    # No additional submission was needed or made to recover mission completion.
    assert server.goal_request_count == 2
    assert server.send_count == 2


# --------------------------------------------------------------------------
# Correction item 1: resume recovers from an interrupted step initialization
# (create_or_get committed; initialize_steps never ran)
# --------------------------------------------------------------------------

def test_resume_completes_interrupted_step_initialization(db_path, owner, adapter, server):
    payload = sup.build_mission_payload(
        action_name=server.action_name, robot_namespace='',
        checkpoint_goal_payload=CHECKPOINT_PAYLOAD, base_goal_payload=BASE_PAYLOAD)
    first_store = MissionStore(owner.canonical_db_path)
    mission = first_store.create_or_get(sup.PREVIEW_REQUEST_ID, payload, sup.DEFAULT_MAX_ATTEMPTS)
    original_mission_id = mission.mission_id
    # Simulate a crash here: initialize_steps is deliberately never called.
    assert first_store.get_steps(mission.mission_id) == ()
    first_store.close()

    # A genuinely separate MissionStore connection -- close/reopen, not a stale reference.
    second_store = MissionStore(owner.canonical_db_path)
    try:
        report = resume_it(owner, second_store, adapter)
    finally:
        second_store.close()

    assert report.outcome == 'COMPLETED'
    assert report.mission.mission_id == original_mission_id
    assert [s.step_key for s in report.steps] == list(sup.STEP_ORDER)
    assert report.mission.attempts_used == 2
    assert server.goal_request_count == 2


# --------------------------------------------------------------------------
# Correction item 2: preflight validation before any durable write, authorization, or ROS call
# --------------------------------------------------------------------------

@pytest.mark.parametrize('bad_poll_attempts', [0, -1, True, False])
def test_invalid_poll_attempts_on_run_causes_zero_ros_calls(
        owner, store, adapter, server, bad_poll_attempts):
    with pytest.raises((TypeError, ValueError)):
        run_it(owner, store, adapter, server, poll_attempts=bad_poll_attempts)
    assert server.goal_request_count == 0


@pytest.mark.parametrize('bad_timeout', [0, -1.0, float('nan'), float('inf'), True])
def test_invalid_poll_timeout_on_run_causes_zero_ros_calls(
        owner, store, adapter, server, bad_timeout):
    with pytest.raises((TypeError, ValueError)):
        run_it(owner, store, adapter, server, poll_result_timeout_s=bad_timeout)
    assert server.goal_request_count == 0


def test_invalid_poll_attempts_on_resume_causes_zero_additional_ros_calls(
        owner, store, adapter, server):
    run_it(owner, store, adapter, server)
    assert server.goal_request_count == 2
    with pytest.raises((TypeError, ValueError)):
        resume_it(owner, store, adapter, poll_attempts=0)
    assert server.goal_request_count == 2


def test_invalid_return_payload_rejected_before_checkpoint_navigation(
        owner, store, adapter, server):
    bad_base_payload = {
        'frame_id': 'map', 'position': {'x': 'not-a-number', 'y': 0.0, 'z': 0.0},
        'orientation': {'x': 0.0, 'y': 0.0, 'z': 0.0, 'w': 1.0}, 'behavior_tree': '',
    }
    with pytest.raises(InvalidGoalPayload):
        run_it(owner, store, adapter, server, base_goal_payload=bad_base_payload)
    # Rejected before create_or_get itself: no mission exists at all, and EITHER step
    # (including checkpoint_a) was never created or sent.
    assert server.goal_request_count == 0
    with pytest.raises(MissionNotFound):
        store.get_by_request_id(sup.PREVIEW_REQUEST_ID)


def _make_step(step_key: str, step_index: int, state: StepState = StepState.PENDING):
    now = datetime.now(timezone.utc)
    return StepRecord(
        mission_id='m', step_key=step_key, step_index=step_index, state=state,
        created_at=now, updated_at=now)


def test_step_list_order_validation_rejects_out_of_order_or_unexpected_steps():
    sup._validate_step_list_order(())
    sup._validate_step_list_order(
        (_make_step(sup.CHECKPOINT_STEP, 0), _make_step(sup.RETURN_STEP, 1)))
    with pytest.raises(sup.MissionDefinitionError):
        sup._validate_step_list_order(
            (_make_step(sup.RETURN_STEP, 0), _make_step(sup.CHECKPOINT_STEP, 1)))


def test_cli_rejects_invalid_poll_attempts_with_usage_error_not_traceback(db_path, capsys):
    with pytest.raises(SystemExit) as exc_info:
        sup_cli.build_parser().parse_args(['run', '--db', db_path, '--poll-attempts', '0'])
    assert exc_info.value.code == 2
    err = capsys.readouterr().err
    assert 'poll-attempts' in err
    # A usage error at parse time: no database was ever touched.
    assert not os.path.exists(db_path)


# --------------------------------------------------------------------------
# Correction item 3: the dedicated-database boundary is enforced, not merely documented
# --------------------------------------------------------------------------

def test_incompatible_database_is_refused_and_left_unchanged(db_path, owner, server):
    unrelated_store = MissionStore(owner.canonical_db_path)
    unrelated_store.create_or_get('some-unrelated-request', {'unrelated': True}, 3)
    unrelated_store.close()

    before = hashlib.sha256(open(db_path, 'rb').read()).hexdigest()
    with pytest.raises(sup.IncompatibleDatabase):
        sup.ensure_database_admissible(owner.canonical_db_path)
    after = hashlib.sha256(open(db_path, 'rb').read()).hexdigest()

    assert before == after
    assert server.goal_request_count == 0


def test_fresh_and_already_owned_databases_remain_admissible(
        db_path, owner, store, adapter, server):
    # A brand new database (nothing written yet): admissible.
    sup.ensure_database_admissible(owner.canonical_db_path)
    # Run the real preview mission, then confirm the now-owned database stays admissible.
    run_it(owner, store, adapter, server)
    sup.ensure_database_admissible(owner.canonical_db_path)


# --------------------------------------------------------------------------
# Correction item 4: status never creates or migrates, and always exits 0 on success
# --------------------------------------------------------------------------

def test_status_on_missing_path_does_not_create_it(tmp_path):
    missing_path = str(tmp_path / 'does-not-exist.db')
    with pytest.raises(sup.MissingPreviewDatabase):
        sup.open_preview_store_for_status(missing_path)
    assert not os.path.exists(missing_path)


def test_status_on_unsupported_schema_leaves_contents_unchanged(tmp_path):
    bad_path = str(tmp_path / 'old-schema.db')
    conn = sqlite3.connect(bad_path)
    conn.execute('PRAGMA user_version = 3')
    conn.execute('CREATE TABLE sentinel (x INTEGER)')
    conn.execute('INSERT INTO sentinel VALUES (42)')
    conn.commit()
    conn.close()
    before = open(bad_path, 'rb').read()

    with pytest.raises(sup.UnsupportedPreviewSchema):
        sup.open_preview_store_for_status(bad_path)

    after = open(bad_path, 'rb').read()
    assert before == after


def test_status_exit_code_is_zero_for_running_and_completed_missions(
        db_path, owner, store, adapter, server, monkeypatch, capsys):
    monkeypatch.setenv(sup.PAUSE_AFTER_SEND_ENV, sup.RETURN_STEP)
    paused = run_it(owner, store, adapter, server)
    assert paused.outcome == sup.OUTCOME_PAUSED_FOR_DEMO
    monkeypatch.delenv(sup.PAUSE_AFTER_SEND_ENV)

    rc = sup_cli.main(['status', '--db', db_path])
    assert rc == 0
    running_out = capsys.readouterr().out
    assert 'OUTCOME RUNNING' in running_out
    assert f'STEP {sup.CHECKPOINT_STEP} state={StepState.SUCCEEDED.value}' in running_out

    resume_it(owner, store, adapter)

    rc = sup_cli.main(['status', '--db', db_path])
    assert rc == 0
    completed_out = capsys.readouterr().out
    assert 'OUTCOME COMPLETED' in completed_out


# --------------------------------------------------------------------------
# Correction-2 item 1: stored PROGRESSION, not only step-key order, is checked
# --------------------------------------------------------------------------

def test_out_of_order_progression_is_refused_before_any_new_send(
        db_path, owner, adapter, server):
    """
    Refuse resume when return_to_base is already DISPATCHED while checkpoint_a is PENDING.

    Built through the core's own public API alone (prepare_step_dispatch/authorize_step_send
    enforce no cross-step ordering of their own, so this fixture needs no SQL bypass). Without
    the progression check, resume's per-step loop would reach checkpoint_a first, find it PENDING
    and eligible, and authorize/send it -- ahead of a later step that is already in flight.
    """
    payload = sup.build_mission_payload(
        action_name=server.action_name, robot_namespace='',
        checkpoint_goal_payload=CHECKPOINT_PAYLOAD, base_goal_payload=BASE_PAYLOAD)
    first_store = MissionStore(owner.canonical_db_path)
    mission = first_store.create_or_get(sup.PREVIEW_REQUEST_ID, payload, sup.DEFAULT_MAX_ATTEMPTS)
    first_store.initialize_steps(mission.mission_id, sup.STEP_ORDER)
    definitions = sup._parse_definition(mission.payload)
    first_store.transition_mission_state(mission.mission_id, MissionState.RUNNING)
    first_store.prepare_step_dispatch(
        mission.mission_id, sup.RETURN_STEP, sup._action_context(definitions[sup.RETURN_STEP]),
        sup._goal_spec(definitions[sup.RETURN_STEP]))
    first_store.authorize_step_send(mission.mission_id, sup.RETURN_STEP)
    before_attempts = first_store.get_by_mission_id(mission.mission_id).attempts_used
    assert before_attempts == 1
    first_store.close()

    second_store = MissionStore(owner.canonical_db_path)
    try:
        with pytest.raises(sup.MissionDefinitionError):
            resume_it(owner, second_store, adapter)
        after = second_store.get_by_mission_id(mission.mission_id)
        assert after.attempts_used == before_attempts
        checkpoint = second_store.get_step(mission.mission_id, sup.CHECKPOINT_STEP)
        assert checkpoint.state is StepState.PENDING
        with pytest.raises(DispatchNotFound):
            second_store.get_step_dispatch(mission.mission_id, sup.CHECKPOINT_STEP)
    finally:
        second_store.close()
    # Nothing was ever sent for either step through this flow (return_to_base was only
    # prepared/authorized directly, never adapter.send'd; checkpoint_a was never reached).
    assert server.goal_request_count == 0


@pytest.mark.parametrize('states', [
    (StepState.PENDING, StepState.PENDING),
    (StepState.DISPATCH_PREPARED, StepState.PENDING),
    (StepState.SUCCEEDED, StepState.DISPATCHED),
    (StepState.SUCCEEDED, StepState.SUCCEEDED),
])
def test_step_progression_validation_accepts_normal_sequences(states):
    checkpoint_state, return_state = states
    sup._validate_step_progression((
        _make_step(sup.CHECKPOINT_STEP, 0, checkpoint_state),
        _make_step(sup.RETURN_STEP, 1, return_state)))


@pytest.mark.parametrize('states', [
    (StepState.PENDING, StepState.DISPATCHED),
    (StepState.DISPATCH_PREPARED, StepState.DISPATCHED),
    (StepState.PENDING, StepState.FAILED),
    (StepState.PENDING, StepState.CANCEL_REQUESTED),
    (StepState.PENDING, StepState.OUTCOME_UNKNOWN),
])
def test_step_progression_validation_rejects_a_later_step_ahead_of_an_earlier_one(states):
    checkpoint_state, return_state = states
    with pytest.raises(sup.MissionDefinitionError):
        sup._validate_step_progression((
            _make_step(sup.CHECKPOINT_STEP, 0, checkpoint_state),
            _make_step(sup.RETURN_STEP, 1, return_state)))


# --------------------------------------------------------------------------
# Correction-2 item 2: admission/status read-only connections use one correctly
# escaped, absolute file: URI; version 0 is not proof of an empty database
# --------------------------------------------------------------------------

def test_admission_and_status_inspect_the_actual_hash_path_not_a_neighbor(tmp_path):
    # mission#old.db: an OLD, incompatible schema version -- the file that must actually be
    # inspected.
    hash_path = str(tmp_path / 'mission#old.db')
    conn = sqlite3.connect(hash_path)
    conn.execute('PRAGMA user_version = 3')
    conn.execute('CREATE TABLE sentinel (x INTEGER)')
    conn.commit()
    conn.close()

    # mission: a DECOY neighboring file with the CURRENT schema and no unrelated mission --
    # exactly what an unescaped '#' (which truncates a URI at the fragment) would cause the
    # old, buggy code to inspect instead, wrongly admitting the '#' path as compatible.
    neighbor_path = str(tmp_path / 'mission')
    MissionStore(neighbor_path).close()

    with pytest.raises(sup.IncompatibleDatabase):
        sup.ensure_database_admissible(hash_path)
    with pytest.raises(sup.UnsupportedPreviewSchema):
        sup.open_preview_store_for_status(hash_path)

    # The neighbor itself is untouched and still admissible on its own -- confirming the
    # check above inspected mission#old.db, not this file.
    sup.ensure_database_admissible(neighbor_path)


def test_admission_and_status_allow_a_valid_preview_path_containing_hash(
        tmp_path, owner, adapter, server):
    # A real, compatible preview database whose OWN path contains a URI-reserved character
    # must not be wrongly refused either.
    hash_db_path = str(tmp_path / 'trial#1' / 'mission.db')
    os.makedirs(os.path.dirname(hash_db_path))
    hash_owner = ExecutionOwner(hash_db_path)
    hash_owner.acquire()
    try:
        sup.ensure_database_admissible(hash_owner.canonical_db_path)
        hash_store = MissionStore(hash_owner.canonical_db_path)
        try:
            report = run_it(hash_owner, hash_store, adapter, server)
        finally:
            hash_store.close()
    finally:
        hash_owner.close()
    assert report.outcome == 'COMPLETED'

    status_store = sup.open_preview_store_for_status(hash_owner.canonical_db_path)
    try:
        status_report = sup.mission_status(store=status_store)
    finally:
        status_store.close()
    assert status_report.outcome == 'COMPLETED'


def test_admission_refuses_version_zero_database_holding_unrelated_data(tmp_path):
    path = str(tmp_path / 'version-zero-with-data.db')
    conn = sqlite3.connect(path)
    conn.execute('CREATE TABLE sentinel (x INTEGER)')
    conn.execute('INSERT INTO sentinel VALUES (42)')
    conn.commit()
    conn.close()
    # PRAGMA user_version is still 0 here -- SQLite's own default, never explicitly set --
    # even though the file plainly already holds unrelated data.
    before = open(path, 'rb').read()

    with pytest.raises(sup.IncompatibleDatabase):
        sup.ensure_database_admissible(path)

    after = open(path, 'rb').read()
    assert before == after


def test_admission_allows_a_truly_empty_version_zero_database(tmp_path):
    path = str(tmp_path / 'truly-empty.db')
    sqlite3.connect(path).close()
    sup.ensure_database_admissible(path)  # must not raise
