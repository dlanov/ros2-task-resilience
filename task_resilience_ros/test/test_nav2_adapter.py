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
Task 015 essential scenarios: real ROS communication against a test-owned fake server.

Every scenario runs against FakeNav2Server (fake_nav2_server.py), a real rclpy.ActionServer
in an isolated per-test action namespace -- never a physical robot, the real Nav2 stack, or
a server shared with another test.
"""

import os
import subprocess
import sys

from action_msgs.msg import GoalStatus
from fake_nav2_server import FakeNav2Server, SpinningNode, unique_action_name
import pytest
import rclpy
from rclpy.client import Client

import task_resilience as _task_resilience_module
from task_resilience.model import ExpectedGoalSpecification
from task_resilience.reconciliation import ActionStatus, ExpectedActionContext, ObservationKind
from task_resilience.state_machine import MissionState, StepState
from task_resilience.storage import MissionStore

from task_resilience_ros.errors import (
    ActionEndpointMismatch,
    AdapterSendAlreadyAttempted,
    AuthorizationMismatch,
    InvalidGoalPayload,
    OwnerNotHeld,
    UnsupportedGoalFormat,
)
from task_resilience_ros.execution_owner import ExecutionOwner
import task_resilience_ros.nav2_adapter as _nav2_adapter_module
from task_resilience_ros.nav2_adapter import Nav2Adapter, SUPPORTED_ACTION_TYPE
from task_resilience_ros.nav2_goal_v1 import GOAL_FORMAT

BOUNDED = 10.0
SHORT = 0.5  # deliberately shorter than any configured server-side delay in these tests

SUBPROCESS_HELPER = os.path.join(os.path.dirname(__file__), '_nav2_adapter_subprocess.py')

GOOD_PAYLOAD = {
    'frame_id': 'map',
    'position': {'x': 1.5, 'y': -2.5, 'z': 0.0},
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
    spinner = SpinningNode('fake_nav2_server_node')
    fake = FakeNav2Server(spinner.node, unique_action_name())
    try:
        yield fake
    finally:
        fake.destroy()
        spinner.stop(BOUNDED)


@pytest.fixture
def client_node(ros):
    node = rclpy.create_node('nav2_adapter_test_client')
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
def store(db_path):
    with MissionStore(db_path) as opened:
        yield opened


@pytest.fixture
def owner(db_path):
    o = ExecutionOwner(db_path)
    o.acquire()
    try:
        yield o
    finally:
        o.close()


def prepare_authorized_dispatch(
    store, action_name, *, payload=None, goal_format=GOAL_FORMAT, robot_namespace='',
    action_type=SUPPORTED_ACTION_TYPE, mission_id_suffix='1', step_key='step1',
):
    """Prepare one step through the real public API and authorize its one send."""
    mission = store.create_or_get(f'req-{mission_id_suffix}', {'demo': True}, 3)
    store.initialize_steps(mission.mission_id, [step_key])
    action = ExpectedActionContext(
        action_name=action_name, action_type=action_type, robot_namespace=robot_namespace)
    goal = ExpectedGoalSpecification(goal_format, GOOD_PAYLOAD if payload is None else payload)
    store.prepare_step_dispatch(mission.mission_id, step_key, action, goal)
    store.transition_mission_state(mission.mission_id, MissionState.RUNNING)
    authorization = store.authorize_step_send(mission.mission_id, step_key)
    return mission.mission_id, step_key, authorization


# --------------------------------------------------------------------------
# 1. Authorized send: fake server receives the exact UUID and goal fields
# --------------------------------------------------------------------------

def test_authorized_send_reaches_server_with_exact_uuid_and_goal(store, owner, adapter, server):
    mission_id, step_key, authorization = prepare_authorized_dispatch(store, server.action_name)
    outcome = adapter.send(
        owner=owner, store=store, authorization=authorization,
        mission_id=mission_id, step_key=step_key)
    assert outcome.accepted is True
    assert outcome.reason == 'ACCEPTED'

    dispatch = store.get_step_dispatch(mission_id, step_key)
    assert outcome.dispatch_id == dispatch.dispatch_id
    assert server.send_count == 1
    received_uuid, goal_msg = server.received[0]
    assert received_uuid == dispatch.dispatch_id
    assert goal_msg.pose.header.frame_id == GOOD_PAYLOAD['frame_id']
    assert goal_msg.pose.pose.position.x == GOOD_PAYLOAD['position']['x']
    assert goal_msg.pose.pose.position.y == GOOD_PAYLOAD['position']['y']
    assert goal_msg.pose.pose.orientation.w == GOOD_PAYLOAD['orientation']['w']
    assert goal_msg.behavior_tree == ''
    assert goal_msg.pose.header.stamp.sec == 0
    assert goal_msg.pose.header.stamp.nanosec == 0


# --------------------------------------------------------------------------
# 2. Success and explicit rejection are distinguished
# --------------------------------------------------------------------------

def test_success_result_is_reported_as_succeeded(store, owner, adapter, server):
    mission_id, step_key, authorization = prepare_authorized_dispatch(store, server.action_name)
    dispatch = store.get_step_dispatch(mission_id, step_key)
    server.plan_result(dispatch.dispatch_id, GoalStatus.STATUS_SUCCEEDED)
    outcome = adapter.send(
        owner=owner, store=store, authorization=authorization,
        mission_id=mission_id, step_key=step_key)
    assert outcome.accepted is True

    observation = adapter.query_result(store=store, mission_id=mission_id, step_key=step_key)
    assert observation.kind is ObservationKind.RESULT_RESPONSE
    assert observation.status is ActionStatus.SUCCEEDED


def test_explicit_rejection_is_distinguished_from_success(store, owner, adapter, server):
    server.set_accept(False)
    mission_id, step_key, authorization = prepare_authorized_dispatch(store, server.action_name)
    outcome = adapter.send(
        owner=owner, store=store, authorization=authorization,
        mission_id=mission_id, step_key=step_key)
    assert outcome.accepted is False
    assert outcome.reason == 'REJECTED'
    assert server.send_count == 0  # rejected before execute_callback ever ran


# --------------------------------------------------------------------------
# 3. Delayed acceptance -> unresolved local timeout, never a resend
# --------------------------------------------------------------------------

def test_delayed_acceptance_is_unresolved_timeout_not_resend(store, owner, adapter, server):
    server.set_accept_delay(3.0)
    mission_id, step_key, authorization = prepare_authorized_dispatch(store, server.action_name)
    outcome = adapter.send(
        owner=owner, store=store, authorization=authorization,
        mission_id=mission_id, step_key=step_key, accept_timeout_s=SHORT)
    assert outcome.accepted is None
    assert outcome.reason == 'ACCEPT_TIMEOUT'

    # a repeat local call for the SAME dispatch is refused, not retried -- marked as
    # attempted before the timeout was even known, matching the documented guard.
    with pytest.raises(AdapterSendAlreadyAttempted):
        adapter.send(
            owner=owner, store=store, authorization=authorization,
            mission_id=mission_id, step_key=step_key)


# --------------------------------------------------------------------------
# 4. Delayed result -> query timeout; a later query gets the eventual result,
#    without another submission
# --------------------------------------------------------------------------

def test_delayed_result_query_times_out_then_a_later_query_succeeds(
        store, owner, adapter, server):
    server.set_result_delay(1.5)
    mission_id, step_key, authorization = prepare_authorized_dispatch(store, server.action_name)
    outcome = adapter.send(
        owner=owner, store=store, authorization=authorization,
        mission_id=mission_id, step_key=step_key)
    assert outcome.accepted is True

    first = adapter.query_result(
        store=store, mission_id=mission_id, step_key=step_key, result_timeout_s=SHORT)
    assert first.kind is ObservationKind.LOCAL_TIMEOUT

    second = adapter.query_result(
        store=store, mission_id=mission_id, step_key=step_key, result_timeout_s=BOUNDED)
    assert second.kind is ObservationKind.RESULT_RESPONSE
    assert second.status is ActionStatus.SUCCEEDED
    assert server.send_count == 1  # the delayed-result query never triggered a resend


# --------------------------------------------------------------------------
# 5. A fresh client (new store, new adapter/node) retrieves the retained
#    result from the still-live fake server after the original exits
# --------------------------------------------------------------------------

def test_same_process_fresh_objects_recover_result_without_resend(
        db_path, store, owner, adapter, server, client_node):
    """
    Fresh OBJECTS (same OS process) recover a result without resending.

    This is object recreation, not a process exit: everything here still runs inside this
    one pytest process. It is a fast, useful check that Nav2Adapter/MissionStore hold no
    hidden same-process state across instances, but it does NOT by itself demonstrate
    surviving a real process restart. See
    test_genuine_fresh_process_recovery_via_real_subprocesses for that (real, separate OS
    processes, via ordinary subprocess execution).
    """
    mission_id, step_key, authorization = prepare_authorized_dispatch(store, server.action_name)
    outcome = adapter.send(
        owner=owner, store=store, authorization=authorization,
        mission_id=mission_id, step_key=step_key)
    assert outcome.accepted is True
    assert server.send_count == 1

    # Destroy the original adapter/client node and reopen the store as a genuinely separate
    # MissionStore instance -- not a second call reusing the original client or store object
    # -- but still within this same process.
    adapter.close()
    client_node.destroy_node()
    store.close()

    fresh_store = MissionStore(db_path)
    fresh_node = rclpy.create_node('nav2_adapter_test_fresh_client')
    fresh_adapter = Nav2Adapter(fresh_node)
    try:
        observation = fresh_adapter.query_result(
            store=fresh_store, mission_id=mission_id, step_key=step_key)
        assert observation.kind is ObservationKind.RESULT_RESPONSE
        assert observation.status is ActionStatus.SUCCEEDED
    finally:
        fresh_adapter.close()
        fresh_node.destroy_node()
        fresh_store.close()

    assert server.send_count == 1  # the fresh client's query never resent anything


def _subprocess_env():
    """
    Build a subprocess environment resolving to this process's own package origin.

    PYTHONPATH is derived from THIS process's own already-imported modules, mirroring Task
    014's own _child_env() reasoning: a subprocess resolves task_resilience/
    task_resilience_ros to the exact same source/installed artifact this pytest process
    itself already loaded, never a different or assumed copy.
    """
    env = dict(os.environ)
    ros_pkg_parent = os.path.dirname(os.path.dirname(_nav2_adapter_module.__file__))
    core_pkg_parent = os.path.dirname(os.path.dirname(_task_resilience_module.__file__))
    parts = [ros_pkg_parent, core_pkg_parent]
    existing = env.get('PYTHONPATH', '')
    if existing:
        parts.append(existing)
    env['PYTHONPATH'] = os.pathsep.join(parts)
    return env


def _run_subprocess_bounded(role_args, timeout_s):
    """Ordinary subprocess execution (never os.fork()), with a bounded wait and direct cleanup."""
    proc = subprocess.Popen(
        [sys.executable, SUBPROCESS_HELPER, *role_args],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=_subprocess_env())
    try:
        stdout, stderr = proc.communicate(timeout=timeout_s)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.communicate(timeout=5)
        raise AssertionError(f'subprocess {role_args} did not complete within {timeout_s}s')
    return proc.pid, proc.returncode, stdout, stderr


def _parse_lines(stdout):
    parsed = {}
    for line in stdout.strip().splitlines():
        parts = line.split(' ', 1)
        parsed[parts[0]] = parts[1] if len(parts) > 1 else ''
    return parsed


def test_genuine_fresh_process_recovery_via_real_subprocesses(db_path, server):
    """
    A genuinely separate sender process, then a genuinely separate query process.

    The fake ActionServer stays alive in THIS test controller throughout. Neither
    subprocess is os.fork()ed, uses Task 014's process harness, or is given a
    SendAuthorization/goal handle beyond what its own role legitimately constructs
    (sender) or is explicitly denied (query, which receives only db_path/mission_id/
    step_key and reopens the database on its own via a plain MissionStore -- no
    ExecutionOwner, no authorization, no goal handle).
    """
    sender_pid, sender_rc, sender_out, sender_err = _run_subprocess_bounded(
        ['sender', db_path, server.action_name, 'req-fresh-process-1', 'step1'], BOUNDED)
    assert sender_rc == 0, (sender_out, sender_err)
    sender_lines = _parse_lines(sender_out)
    mission_id = sender_lines['MISSION-ID']
    accepted, reason = sender_lines['SEND-OUTCOME'].split(' ', 1)
    assert accepted == 'True' and reason == 'ACCEPTED', sender_out

    # The sender has already exited (communicate() returned) before this point: its exit is
    # asserted above, strictly before the query subprocess is even started below.
    assert server.goal_request_count == 1
    assert server.send_count == 1

    query_pid, query_rc, query_out, query_err = _run_subprocess_bounded(
        ['query', db_path, mission_id, 'step1'], BOUNDED)
    assert query_rc == 0, (query_out, query_err)

    # Distinct PIDs: the sender, the query process, and this test controller are three
    # different OS processes, not the same interpreter reused.
    assert len({sender_pid, query_pid, os.getpid()}) == 3

    query_lines = _parse_lines(query_out)
    result_parts = query_lines['RESULT'].split(' ', 3)
    kind, status, code = result_parts[0], result_parts[1], result_parts[2]
    assert kind == 'RESULT_RESPONSE', query_out
    assert status == 'SUCCEEDED', query_out
    assert code == '0', query_out

    # The query subprocess never triggered an additional SendGoal request: both counters,
    # checked at the fake server's own goal_callback (all requests) and execute callback
    # (accepted requests only), are unchanged from right after the sender's own send.
    assert server.goal_request_count == 1
    assert server.send_count == 1


# --------------------------------------------------------------------------
# 6. Server UNKNOWN and an unavailable service remain distinct
# --------------------------------------------------------------------------

def test_server_unknown_result_is_distinct_from_unavailable_service(store, owner, server, adapter):
    # A dispatch this fake server never received a goal for at all: the real
    # ActionServer's own generated get_result handler reports UNKNOWN for a goal ID it has
    # no record of -- nothing here fabricates that answer.
    mission_id, step_key, _ = prepare_authorized_dispatch(store, server.action_name)
    observation = adapter.query_result(store=store, mission_id=mission_id, step_key=step_key)
    assert observation.kind is ObservationKind.RESULT_RESPONSE
    assert observation.status is ActionStatus.UNKNOWN
    # error_code=0 is NOT treated as success: the outcome is judged by status, and the raw
    # Nav2 result fields are still preserved for inspection either way.
    assert observation.result_code == 0


def test_unavailable_service_is_reported_distinctly(store, client_node, ros):
    # No FakeNav2Server was started at all for this action name -- nothing is listening.
    adapter = Nav2Adapter(client_node)
    try:
        mission_id, step_key, _ = prepare_authorized_dispatch(
            store, unique_action_name('no_server_here'))
        observation = adapter.query_result(
            store=store, mission_id=mission_id, step_key=step_key,
            service_wait_timeout_s=SHORT)
        assert observation.kind is ObservationKind.SERVICE_UNAVAILABLE
    finally:
        adapter.close()


# --------------------------------------------------------------------------
# 7. ABORTED/CANCELED classification is correct
# --------------------------------------------------------------------------

def test_aborted_result_is_classified_correctly(store, owner, adapter, server):
    mission_id, step_key, authorization = prepare_authorized_dispatch(store, server.action_name)
    dispatch = store.get_step_dispatch(mission_id, step_key)
    server.plan_result(
        dispatch.dispatch_id, GoalStatus.STATUS_ABORTED, error_code=42,
        error_msg='planner failed')
    adapter.send(
        owner=owner, store=store, authorization=authorization,
        mission_id=mission_id, step_key=step_key)
    observation = adapter.query_result(store=store, mission_id=mission_id, step_key=step_key)
    assert observation.status is ActionStatus.ABORTED
    assert observation.result_code == 42
    assert observation.result_message == 'planner failed'


def test_canceled_result_is_classified_correctly(store, owner, adapter, server):
    # Mapping-level fixture, per Task 015's own scope: the fake server marks the goal
    # canceled directly; no real cancel-request flow is implemented or exercised.
    mission_id, step_key, authorization = prepare_authorized_dispatch(store, server.action_name)
    dispatch = store.get_step_dispatch(mission_id, step_key)
    server.plan_result(dispatch.dispatch_id, GoalStatus.STATUS_CANCELED)
    adapter.send(
        owner=owner, store=store, authorization=authorization,
        mission_id=mission_id, step_key=step_key)
    observation = adapter.query_result(store=store, mission_id=mission_id, step_key=step_key)
    assert observation.status is ActionStatus.CANCELED


# --------------------------------------------------------------------------
# 8. Missing/mismatched receipt, owner not held, invalid binding/format, and
#    repeated local send all fail BEFORE an additional ROS submission
# --------------------------------------------------------------------------

def test_owner_not_held_is_refused_before_any_ros_submission(db_path, store, adapter, server):
    not_acquired = ExecutionOwner(db_path)
    mission_id, step_key, authorization = prepare_authorized_dispatch(store, server.action_name)
    with pytest.raises(OwnerNotHeld):
        adapter.send(
            owner=not_acquired, store=store, authorization=authorization,
            mission_id=mission_id, step_key=step_key)
    assert server.send_count == 0
    not_acquired.close()  # never acquired; safe no-op


def test_mismatched_authorization_is_refused_before_any_ros_submission(
        store, owner, adapter, server):
    mission_id_a, step_key_a, _ = prepare_authorized_dispatch(
        store, server.action_name, mission_id_suffix='a', step_key='step-a')
    mission_id_b, step_key_b, authorization_b = prepare_authorized_dispatch(
        store, server.action_name, mission_id_suffix='b', step_key='step-b')
    with pytest.raises(AuthorizationMismatch):
        adapter.send(
            owner=owner, store=store, authorization=authorization_b,
            mission_id=mission_id_a, step_key=step_key_a)
    assert server.send_count == 0


def test_stored_authorization_record_is_not_accepted_as_a_fresh_authorization(
        store, owner, adapter, server):
    mission_id, step_key, _ = prepare_authorized_dispatch(store, server.action_name)
    stored_record = store.get_dispatch_authorization(mission_id, step_key)
    with pytest.raises(TypeError):
        adapter.send(
            owner=owner, store=store, authorization=stored_record,
            mission_id=mission_id, step_key=step_key)
    assert server.send_count == 0


def test_unsupported_goal_format_is_refused_before_any_ros_submission(
        store, owner, adapter, server):
    mission_id, step_key, authorization = prepare_authorized_dispatch(
        store, server.action_name, goal_format='some/other/v1')
    with pytest.raises(UnsupportedGoalFormat):
        adapter.send(
            owner=owner, store=store, authorization=authorization,
            mission_id=mission_id, step_key=step_key)
    assert server.send_count == 0


def test_invalid_goal_payload_is_refused_before_any_ros_submission(store, owner, adapter, server):
    # A non-unit quaternion: finite numbers, so the CORE's own strict-JSON-domain check
    # (which does reject non-finite values like NaN/inf at ExpectedGoalSpecification
    # construction, before this ever reaches storage) accepts and stores it -- it is the
    # ADAPTER's own, stricter nav2/navigate_to_pose/v1 schema that must reject it.
    bad_payload = dict(GOOD_PAYLOAD, orientation={'x': 1.0, 'y': 1.0, 'z': 0.0, 'w': 1.0})
    mission_id, step_key, authorization = prepare_authorized_dispatch(
        store, server.action_name, payload=bad_payload)
    with pytest.raises(InvalidGoalPayload):
        adapter.send(
            owner=owner, store=store, authorization=authorization,
            mission_id=mission_id, step_key=step_key)
    assert server.send_count == 0


def test_invalid_endpoint_binding_is_refused_before_any_ros_submission(store, owner, adapter):
    # An absolute action_name combined with a non-empty robot_namespace: this narrow
    # convention does not resolve that combination, so it is refused rather than guessed.
    mission_id, step_key, authorization = prepare_authorized_dispatch(
        store, '/already/absolute/navigate_to_pose', robot_namespace='/robot1')
    with pytest.raises(ActionEndpointMismatch):
        adapter.send(
            owner=owner, store=store, authorization=authorization,
            mission_id=mission_id, step_key=step_key)


def test_relative_action_name_with_empty_namespace_is_refused_on_a_namespaced_node(
        db_path, store, owner, server, ros):
    # A relative action_name with an empty stored robot_namespace must be refused even
    # against a CALLER node that happens to have its own namespace -- durable stored
    # identity has no way to know that unstored configuration, so it must never silently
    # inherit it. Uses a namespaced node specifically to prove this isn't accidentally
    # "working" by coincidence when robot_namespace happens to match the node's namespace.
    namespaced_node = rclpy.create_node('nav2_adapter_test_namespaced', namespace='/robot9')
    adapter = Nav2Adapter(namespaced_node)
    try:
        mission_id, step_key, authorization = prepare_authorized_dispatch(
            store, 'navigate_to_pose', robot_namespace='')
        with pytest.raises(ActionEndpointMismatch):
            adapter.send(
                owner=owner, store=store, authorization=authorization,
                mission_id=mission_id, step_key=step_key)
        assert server.goal_request_count == 0
    finally:
        adapter.close()
        namespaced_node.destroy_node()


def test_explicit_namespaced_binding_resolves_and_works_for_send_and_query(
        store, owner, adapter, server):
    # The mirror-image success case for the endpoint-resolution table: robot_namespace
    # (absolute) + action_name (relative) composing to the EXACT same fully qualified name
    # as server.action_name, which is itself already absolute.
    assert server.action_name.count('/') >= 2
    namespace, _, relative_name = server.action_name.rpartition('/')
    mission_id, step_key, authorization = prepare_authorized_dispatch(
        store, relative_name, robot_namespace=namespace)
    outcome = adapter.send(
        owner=owner, store=store, authorization=authorization,
        mission_id=mission_id, step_key=step_key)
    assert outcome.accepted is True
    assert server.goal_request_count == 1

    observation = adapter.query_result(store=store, mission_id=mission_id, step_key=step_key)
    assert observation.kind is ObservationKind.RESULT_RESPONSE
    assert observation.status is ActionStatus.SUCCEEDED


def test_remapped_send_goal_endpoint_is_refused_before_send(db_path, store, owner, server):
    # A per-node remap rule redirects THIS specific service, out from under the stored
    # action identity -- exactly the redirection this adapter must never silently follow.
    decoy_name = server.action_name + '_decoy'
    remap_arg = f'{server.action_name}/_action/send_goal:={decoy_name}/_action/send_goal'
    remapped_node = rclpy.create_node(
        'nav2_adapter_test_remapped_send', cli_args=['--ros-args', '-r', remap_arg])
    adapter = Nav2Adapter(remapped_node)
    try:
        mission_id, step_key, authorization = prepare_authorized_dispatch(
            store, server.action_name)
        with pytest.raises(ActionEndpointMismatch):
            adapter.send(
                owner=owner, store=store, authorization=authorization,
                mission_id=mission_id, step_key=step_key)
        # Rejected before any external send: neither the real endpoint nor the
        # remap-redirected decoy ever received a request.
        assert server.goal_request_count == 0
    finally:
        adapter.close()
        remapped_node.destroy_node()


def test_remapped_get_result_endpoint_is_refused_before_query(store, owner, adapter, server):
    mission_id, step_key, authorization = prepare_authorized_dispatch(store, server.action_name)
    outcome = adapter.send(
        owner=owner, store=store, authorization=authorization,
        mission_id=mission_id, step_key=step_key)
    assert outcome.accepted is True
    assert server.goal_request_count == 1

    decoy_name = server.action_name + '_decoy'
    remap_arg = f'{server.action_name}/_action/get_result:={decoy_name}/_action/get_result'
    remapped_node = rclpy.create_node(
        'nav2_adapter_test_remapped_query', cli_args=['--ros-args', '-r', remap_arg])
    remapped_adapter = Nav2Adapter(remapped_node)
    try:
        with pytest.raises(ActionEndpointMismatch):
            remapped_adapter.query_result(store=store, mission_id=mission_id, step_key=step_key)
    finally:
        remapped_adapter.close()
        remapped_node.destroy_node()
    # Never attach the original stored context to a response knowingly obtained from a
    # different (here: nonexistent, since nothing listens at the decoy name) endpoint --
    # rejected before any query went out, so no additional request was ever counted.
    assert server.goal_request_count == 1


def test_remapped_base_action_name_is_refused_before_send(db_path, store, owner, server):
    """
    Reject a remap rule written against the BASE action name, before any send.

    Exactly as upstream Jazzy rcl_action (rcl_action/action_client.c) itself
    resolves/remaps it before constructing any of
    ActionClient's generated service names -- distinct from, and not caught by,
    test_remapped_send_goal_endpoint_is_refused_before_send above, whose per-service probe is
    never given the base action name to remap in the first place. Two independent fake
    servers (the intended one and the decoy the remap would otherwise redirect to) confirm
    this is refused before anything reaches EITHER of them, not merely before it reaches the
    intended one.
    """
    decoy_spinner = SpinningNode('nav2_adapter_test_base_remap_decoy_server')
    decoy_server = FakeNav2Server(decoy_spinner.node, server.action_name + '_decoy')
    try:
        remap_arg = f'{server.action_name}:={decoy_server.action_name}'
        remapped_node = rclpy.create_node(
            'nav2_adapter_test_remapped_base_send', cli_args=['--ros-args', '-r', remap_arg])
        adapter = Nav2Adapter(remapped_node)
        try:
            mission_id, step_key, authorization = prepare_authorized_dispatch(
                store, server.action_name)
            with pytest.raises(ActionEndpointMismatch):
                adapter.send(
                    owner=owner, store=store, authorization=authorization,
                    mission_id=mission_id, step_key=step_key)
            # Rejected before any external send: neither the intended endpoint nor the
            # remap-redirected decoy ever received a request.
            assert server.goal_request_count == 0
            assert decoy_server.goal_request_count == 0
        finally:
            adapter.close()
            remapped_node.destroy_node()
    finally:
        decoy_server.destroy()
        decoy_spinner.stop(BOUNDED)


def test_remapped_base_action_name_is_refused_before_query(
        store, owner, adapter, server, monkeypatch):
    """
    Reject the same base-action remap gap, on the query_result path.

    query_result has no "goal" and no decoy server to count against (a GetResult client would
    be the only thing created), so this directly counts calls to Client.call_async -- the one
    ROS call that would carry the GetResult request -- instead of a server-side counter.
    """
    mission_id, step_key, authorization = prepare_authorized_dispatch(store, server.action_name)
    outcome = adapter.send(
        owner=owner, store=store, authorization=authorization,
        mission_id=mission_id, step_key=step_key)
    assert outcome.accepted is True
    assert server.goal_request_count == 1

    decoy_name = server.action_name + '_decoy'
    remap_arg = f'{server.action_name}:={decoy_name}'
    remapped_node = rclpy.create_node(
        'nav2_adapter_test_remapped_base_query', cli_args=['--ros-args', '-r', remap_arg])
    remapped_adapter = Nav2Adapter(remapped_node)

    call_count = {'n': 0}
    real_call_async = Client.call_async

    def counting_call_async(self, *args, **kwargs):
        call_count['n'] += 1
        return real_call_async(self, *args, **kwargs)

    monkeypatch.setattr(Client, 'call_async', counting_call_async)
    try:
        with pytest.raises(ActionEndpointMismatch):
            remapped_adapter.query_result(store=store, mission_id=mission_id, step_key=step_key)
    finally:
        remapped_adapter.close()
        remapped_node.destroy_node()
    # Configuration rejected before the GetResult service was ever called -- not merely before
    # a result was received.
    assert call_count['n'] == 0


def test_repeated_local_send_fails_before_any_additional_ros_submission(
        store, owner, adapter, server):
    mission_id, step_key, authorization = prepare_authorized_dispatch(store, server.action_name)
    outcome = adapter.send(
        owner=owner, store=store, authorization=authorization,
        mission_id=mission_id, step_key=step_key)
    assert outcome.accepted is True
    assert server.send_count == 1
    with pytest.raises(AdapterSendAlreadyAttempted):
        adapter.send(
            owner=owner, store=store, authorization=authorization,
            mission_id=mission_id, step_key=step_key)
    assert server.send_count == 1  # unchanged: no additional submission was made


# --------------------------------------------------------------------------
# Timeout parameters: finite, positive, numeric, or rejected before anything is submitted
# --------------------------------------------------------------------------

INVALID_TIMEOUTS = [
    (None, TypeError),
    (True, TypeError),
    (False, TypeError),
    ('1.0', TypeError),
    (0, ValueError),
    (0.0, ValueError),
    (-1.0, ValueError),
    (float('nan'), ValueError),
    (float('inf'), ValueError),
    (float('-inf'), ValueError),
]


@pytest.mark.parametrize('bad_value,expected_exc', INVALID_TIMEOUTS)
def test_invalid_server_wait_timeout_is_rejected_before_any_send_goal_call(
        store, owner, adapter, server, bad_value, expected_exc):
    mission_id, step_key, authorization = prepare_authorized_dispatch(store, server.action_name)
    with pytest.raises(expected_exc):
        adapter.send(
            owner=owner, store=store, authorization=authorization,
            mission_id=mission_id, step_key=step_key, server_wait_timeout_s=bad_value)
    assert server.goal_request_count == 0


@pytest.mark.parametrize('bad_value,expected_exc', INVALID_TIMEOUTS)
def test_invalid_accept_timeout_is_rejected_before_any_send_goal_call(
        store, owner, adapter, server, bad_value, expected_exc):
    # The exact concern named in the instructions: an invalid accept_timeout_s must never
    # be discovered only after send_goal_async has already been called.
    mission_id, step_key, authorization = prepare_authorized_dispatch(store, server.action_name)
    with pytest.raises(expected_exc):
        adapter.send(
            owner=owner, store=store, authorization=authorization,
            mission_id=mission_id, step_key=step_key, accept_timeout_s=bad_value)
    assert server.goal_request_count == 0


@pytest.mark.parametrize('bad_value,expected_exc', INVALID_TIMEOUTS)
def test_invalid_service_wait_timeout_is_rejected_before_any_get_result_call(
        store, owner, adapter, server, bad_value, expected_exc):
    # query_result needs no prior send (no fresh receipt, no goal row) -- validated directly.
    mission_id, step_key, _ = prepare_authorized_dispatch(store, server.action_name)
    with pytest.raises(expected_exc):
        adapter.query_result(
            store=store, mission_id=mission_id, step_key=step_key,
            service_wait_timeout_s=bad_value)
    assert server.goal_request_count == 0


@pytest.mark.parametrize('bad_value,expected_exc', INVALID_TIMEOUTS)
def test_invalid_result_timeout_is_rejected_before_any_get_result_call(
        store, owner, adapter, server, bad_value, expected_exc):
    mission_id, step_key, _ = prepare_authorized_dispatch(store, server.action_name)
    with pytest.raises(expected_exc):
        adapter.query_result(
            store=store, mission_id=mission_id, step_key=step_key, result_timeout_s=bad_value)
    assert server.goal_request_count == 0


# --------------------------------------------------------------------------
# A narrow, injected transport exception is reported as TRANSPORT_ERROR
# --------------------------------------------------------------------------

def test_transport_exception_during_query_is_reported_distinctly(
        store, owner, adapter, server, monkeypatch):
    mission_id, step_key, authorization = prepare_authorized_dispatch(store, server.action_name)
    adapter.send(
        owner=owner, store=store, authorization=authorization,
        mission_id=mission_id, step_key=step_key)

    real_spin = rclpy.spin_until_future_complete

    def faulty_spin(*args, **kwargs):
        raise RuntimeError('injected transport failure')

    monkeypatch.setattr(rclpy, 'spin_until_future_complete', faulty_spin)
    try:
        observation = adapter.query_result(store=store, mission_id=mission_id, step_key=step_key)
    finally:
        monkeypatch.setattr(rclpy, 'spin_until_future_complete', real_spin)
    assert observation.kind is ObservationKind.TRANSPORT_ERROR


def test_completed_future_with_injected_exception_yields_transport_error(
        store, owner, adapter, server, monkeypatch):
    """
    A future that is already DONE, but done with an exception set rather than a value.

    Distinct from test_transport_exception_during_query_is_reported_distinctly above
    (where spin_until_future_complete itself raises): here spin succeeds normally and the
    future reports done() == True, so the earlier `if not future.done(): ...` timeout
    branch is never taken either -- the exception is only ever observed at future.result(),
    which must be inside the same TRANSPORT_ERROR boundary for this to be caught.
    """
    mission_id, step_key, authorization = prepare_authorized_dispatch(store, server.action_name)
    adapter.send(
        owner=owner, store=store, authorization=authorization,
        mission_id=mission_id, step_key=step_key)

    def faulty_call_async(request):
        future = rclpy.task.Future()
        future.set_exception(RuntimeError('injected completed-future exception'))
        return future

    # query_result creates its GetResult client lazily on first use; patch the bound method
    # on whichever client object it ends up creating this call by wrapping create_client.
    real_create_client = adapter._node.create_client

    def wrapped_create_client(*args, **kwargs):
        client = real_create_client(*args, **kwargs)
        monkeypatch.setattr(client, 'call_async', faulty_call_async)
        return client

    monkeypatch.setattr(adapter._node, 'create_client', wrapped_create_client)
    try:
        observation = adapter.query_result(store=store, mission_id=mission_id, step_key=step_key)
    finally:
        monkeypatch.setattr(adapter._node, 'create_client', real_create_client)
    assert observation.kind is ObservationKind.TRANSPORT_ERROR


# --------------------------------------------------------------------------
# 9. Adapter operations leave durable state and attempts unchanged
# --------------------------------------------------------------------------

def test_query_result_never_mutates_durable_state_or_attempts(store, owner, adapter, server):
    mission_id, step_key, authorization = prepare_authorized_dispatch(store, server.action_name)
    dispatch = store.get_step_dispatch(mission_id, step_key)
    server.plan_result(dispatch.dispatch_id, GoalStatus.STATUS_SUCCEEDED)
    adapter.send(
        owner=owner, store=store, authorization=authorization,
        mission_id=mission_id, step_key=step_key)

    mission_before = store.get_by_mission_id(mission_id)
    step_before = store.get_step(mission_id, step_key)

    for _ in range(3):
        adapter.query_result(store=store, mission_id=mission_id, step_key=step_key)

    mission_after = store.get_by_mission_id(mission_id)
    step_after = store.get_step(mission_id, step_key)
    assert step_after.state is StepState.DISPATCHED
    assert step_after.state == step_before.state
    assert mission_after.attempts_used == mission_before.attempts_used
    # apply_recovery_observation was never called by the adapter: no evidence exists.
    assert store.get_recovery_evidence(mission_id, step_key) == ()
