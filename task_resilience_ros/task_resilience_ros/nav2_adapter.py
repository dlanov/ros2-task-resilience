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
A thin Nav2 adapter: one authorized submission, one read-only recovery query.

Nav2Adapter is a bounded, SYNCHRONOUS facade over a caller-supplied rclpy.Node. It is not a
mission supervisor: it never creates or prepares missions, never obtains send authorization
itself, never mutates MissionStore (including recording recovery evidence, changing an
attempt, or transitioning a step or mission), and never acquires or releases execution
ownership. All of that is the caller's -- and, later, the supervisor's -- job. See
docs/nav2_adapter.md for the full contract this class implements and the caller sequence it
requires.

SUPPORTED EXECUTOR/SPIN MODEL: exactly one. The caller constructs and owns an rclpy.Node
(and, if it wants one, an executor); this class only creates clients on it. Every method
here is a bounded synchronous call: internally, it calls rclpy.spin_until_future_complete
on THIS SAME node up to an explicit timeout, then returns. Do not call these methods from
inside a callback already being served by a spin on this node (spin_until_future_complete
does not support nested/re-entrant spinning of the same node) and do not spin this node
concurrently from another thread while a call here is in progress. There is no async
variant, no background executor, and this module never calls rclpy.init()/shutdown() --
that remains entirely the caller's responsibility.

SEND GUARD IS LOCAL, NOT DURABLE: _attempted is an in-process set on this one instance. It
stops an accidental second local call for a dispatch this exact object already tried, but a
new Nav2Adapter, a new process, or a crash and restart has no memory of it and is not
guarded by this at all. It is a courtesy against a programming mistake within one call site,
not exactly-once enforcement and not an unforgeable capability -- durable exactly-once-send
enforcement is exactly the problem execution ownership (Task 014) and durable send
authorization (Task 008) exist to bound in the ways they document, not something this class
adds on top.
"""

from dataclasses import dataclass
import math
import uuid

from action_msgs.msg import GoalStatus
from nav2_msgs.action import NavigateToPose
import rclpy
from rclpy.action import ActionClient

from task_resilience.model import DispatchAuthorizationRecord, SendAuthorization
from task_resilience.reconciliation import ActionStatus, ObservationKind, RecoveryObservation
from task_resilience.storage import MissionStore

from . import nav2_goal_v1
from .errors import (
    ActionEndpointMismatch,
    AdapterSendAlreadyAttempted,
    AuthorizationMismatch,
    OwnerNotHeld,
)
from .execution_owner import ExecutionOwner

# The one action type this adapter implements. Stored dispatch context's action_type must
# match this exactly, or the endpoint cannot be resolved -- never silently substituted or
# ignored.
SUPPORTED_ACTION_TYPE = 'nav2_msgs/action/NavigateToPose'

DEFAULT_SERVER_WAIT_TIMEOUT_S = 10.0
DEFAULT_ACCEPT_TIMEOUT_S = 30.0
DEFAULT_SERVICE_WAIT_TIMEOUT_S = 5.0
DEFAULT_RESULT_TIMEOUT_S = 30.0


@dataclass(frozen=True)
class SendOutcome:
    """
    What happened when this adapter tried to submit one authorized dispatch.

    accepted is True/False only for an actual server response; None means the outcome is
    UNRESOLVED (a local timeout waiting for accept/reject, or the action server was never
    discovered) -- not a rejection, and not proof the request was never received. No
    automatic retry, replacement UUID, or attempt refund follows from any value here; that
    is a caller/supervisor policy decision, not this adapter's.
    """

    dispatch_id: uuid.UUID
    accepted: bool | None
    reason: str


def _resolve_action_name(action_name: str, robot_namespace: str) -> str:
    """
    Resolve the stored action_name/robot_namespace into one fully qualified ROS action name.

    A narrow, explicit, FAIL-FAST convention, not general ROS remapping:
      - robot_namespace == '' and action_name is ABSOLUTE (starts with '/'): the resolved
        name is action_name, unchanged. Supported.
      - robot_namespace != '': action_name must be RELATIVE (must not start with '/') and
        robot_namespace must be ABSOLUTE; the resolved name is robot_namespace with any
        trailing '/' stripped, then '/', then action_name. Supported.
      - robot_namespace == '' and action_name is RELATIVE: REFUSED. An earlier version
        passed a relative action_name straight to ActionClient in this case, letting it
        silently resolve against whatever namespace the CALLER's node happens to have --
        durable stored identity has no way to know that unstored configuration, so this is
        refused rather than inherited.
      - An absolute action_name combined with a non-empty robot_namespace, or a relative
        robot_namespace, is refused the same way it always was.
    The result is always an absolute name: nothing downstream needs to guess at, or expand,
    a relative name against any node's configuration.
    """
    if robot_namespace == '':
        if not action_name.startswith('/'):
            raise ActionEndpointMismatch(
                action_name, '', robot_namespace,
                'action_name is relative and robot_namespace is empty; this narrow '
                "convention does not resolve a relative name against the caller's own "
                'node configuration -- store an absolute action_name, or a robot_namespace')
        return action_name
    if action_name.startswith('/'):
        raise ActionEndpointMismatch(
            action_name, '', robot_namespace,
            'action_name is absolute but robot_namespace is non-empty; this narrow '
            'convention does not resolve that combination')
    if not robot_namespace.startswith('/'):
        raise ActionEndpointMismatch(
            action_name, '', robot_namespace,
            'robot_namespace must be absolute (start with "/") when non-empty')
    return robot_namespace.rstrip('/') + '/' + action_name


def _check_action_type(action_name: str, action_type: str, robot_namespace: str) -> None:
    if action_type != SUPPORTED_ACTION_TYPE:
        raise ActionEndpointMismatch(
            action_name, action_type, robot_namespace,
            f'this adapter only implements {SUPPORTED_ACTION_TYPE!r}')


def _verify_unremapped(
    node, service_type, intended_service_name: str, endpoint_label: str,
    action_name: str, action_type: str, robot_namespace: str,
) -> None:
    """
    Confirm intended_service_name is not being silently redirected by ROS remapping.

    _resolve_action_name already guarantees intended_service_name is fully qualified, so
    resolve_service_name(..., only_expand=True) is a no-op on it EXCEPT that it also applies
    no remap rules -- it is exactly "the name we intended, with no remapping." A throwaway
    client's own .service_name, by contrast, reflects whatever rcl actually resolved once
    remap rules (e.g. --ros-args -r) were applied at creation time. If the two differ, some
    remap rule redirected this specific endpoint out from under the stored identity; this
    adapter implements no general remapping support, so that is refused before the real
    client this method uses is ever allowed to talk to whatever the remap pointed at instead.
    """
    intended = node.resolve_service_name(intended_service_name, only_expand=True)
    probe = node.create_client(service_type, intended_service_name)
    try:
        actual = probe.service_name
    finally:
        node.destroy_client(probe)
    if actual != intended:
        raise ActionEndpointMismatch(
            action_name, action_type, robot_namespace,
            f'{endpoint_label} {intended!r} was remapped to {actual!r}; this adapter does '
            'not support remapping and refuses rather than silently using a redirected '
            'endpoint')


def _verify_base_action_unremapped(
    node, resolved_name: str, action_name: str, action_type: str, robot_namespace: str,
) -> None:
    """
    Confirm the BASE action name itself is not being redirected by ROS remapping.

    _verify_unremapped (above) catches a remap written against one GENERATED service name
    (e.g. '.../navigate_to_pose/_action/send_goal:=...'), but ActionClient does not resolve
    its service names independently: upstream Jazzy rcl_action (rcl_action/action_client.c)
    first resolves/remaps the BASE action name via rcl_node_resolve_name, THEN constructs
    every one of its service/topic names from whatever that resolved to. A remap rule written
    against the base name instead (e.g. '/robot_a/navigate_to_pose:=/robot_b/navigate_to_pose')
    redirects all of them at once, yet a probe client created directly for one already-generated
    service name is never given the base name to remap -- it is simply never redirected, and
    _verify_unremapped alone sees no mismatch at all. This is a distinct gap, caught here,
    before any ActionClient for this name is created.

    node.resolve_topic_name(name, only_expand=True) performs namespace expansion only, no
    remap rules -- "the name we intended, with no remapping." only_expand=False performs the
    same expansion AND applies remap rules -- "the name ROS will actually resolve the base
    action to." If the two differ, this adapter's narrow, no-remapping convention refuses
    rather than silently following every service ActionClient would otherwise derive from the
    redirected base name.
    """
    intended = node.resolve_topic_name(resolved_name, only_expand=True)
    actual = node.resolve_topic_name(resolved_name, only_expand=False)
    if actual != intended:
        raise ActionEndpointMismatch(
            action_name, action_type, robot_namespace,
            f'action {intended!r} was remapped to {actual!r}; this adapter does not support '
            'remapping and refuses rather than silently using a redirected endpoint')


def _validate_timeout(value: object, name: str) -> None:
    """Require a finite, positive, non-boolean numeric timeout."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f'{name} must be a finite positive number, got {value!r}')
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f'{name} must be finite, got {value!r}')
    if number <= 0:
        raise ValueError(f'{name} must be positive, got {value!r}')


class Nav2Adapter:
    """See the module docstring for the full contract."""

    def __init__(self, node) -> None:
        self._node = node
        self._attempted: set[uuid.UUID] = set()
        self._action_clients: dict[str, object] = {}
        self._result_service_clients: dict[str, object] = {}

    def send(
        self, *, owner: ExecutionOwner, store: MissionStore,
        authorization: SendAuthorization, mission_id: str, step_key: str,
        server_wait_timeout_s: float = DEFAULT_SERVER_WAIT_TIMEOUT_S,
        accept_timeout_s: float = DEFAULT_ACCEPT_TIMEOUT_S,
    ) -> SendOutcome:
        """
        Submit the one stored, authorized goal for mission_id/step_key.

        Requires: owner.acquired (a currently held ExecutionOwner -- this method does not
        acquire or release it); authorization, the actual SendAuthorization object
        authorize_step_send returned to THIS caller for this exact mission/step (a
        DispatchAuthorizationRecord read back from storage is rejected: it is provenance of
        a past commit, never a fresh permit -- see docs/execution_ownership_model.md and
        docs/send_authorization_model.md). Dispatch UUID, action context and goal binding
        are reloaded from `store`, never accepted as separate arguments that could disagree
        with those durable records.
        """
        _validate_timeout(server_wait_timeout_s, 'server_wait_timeout_s')
        _validate_timeout(accept_timeout_s, 'accept_timeout_s')

        if not isinstance(owner, ExecutionOwner):
            raise TypeError(f'owner must be an ExecutionOwner, got {owner!r}')
        if not owner.acquired:
            raise OwnerNotHeld(mission_id, step_key)
        if isinstance(authorization, DispatchAuthorizationRecord) or not isinstance(
                authorization, SendAuthorization):
            raise TypeError(
                'authorization must be the fresh SendAuthorization object returned by '
                'authorize_step_send to this caller, not a DispatchAuthorizationRecord or '
                f'any other object; got {authorization!r}')

        dispatch = store.get_step_dispatch(mission_id, step_key)
        if authorization.mission_id != mission_id or authorization.step_key != step_key:
            raise AuthorizationMismatch(
                mission_id, step_key, dispatch.dispatch_id,
                f'authorization identifies {authorization.mission_id!r}/'
                f'{authorization.step_key!r}, not {mission_id!r}/{step_key!r}')
        if authorization.dispatch_id != dispatch.dispatch_id:
            raise AuthorizationMismatch(
                mission_id, step_key, dispatch.dispatch_id,
                f'authorization dispatch_id {authorization.dispatch_id} does not match the '
                f'stored dispatch {dispatch.dispatch_id}')

        if dispatch.dispatch_id in self._attempted:
            raise AdapterSendAlreadyAttempted(dispatch.dispatch_id)
        # Marked BEFORE the ROS call, not after: a repeat must be refused even if this very
        # attempt goes on to raise or time out.
        self._attempted.add(dispatch.dispatch_id)

        context = store.get_dispatch_context(mission_id, step_key)
        goal_record = store.get_dispatch_goal(mission_id, step_key)
        _check_action_type(context.action_name, context.action_type, context.robot_namespace)
        resolved_name = _resolve_action_name(context.action_name, context.robot_namespace)
        _verify_base_action_unremapped(
            self._node, resolved_name, context.action_name, context.action_type,
            context.robot_namespace)
        _verify_unremapped(
            self._node, NavigateToPose.Impl.SendGoalService, f'{resolved_name}/_action/send_goal',
            'the send_goal service', context.action_name, context.action_type,
            context.robot_namespace)
        goal_msg = nav2_goal_v1.build_goal_message(goal_record.goal_format, goal_record.payload)

        client = self._action_clients.get(resolved_name)
        if client is None:
            client = ActionClient(self._node, NavigateToPose, resolved_name)
            self._action_clients[resolved_name] = client

        if not client.wait_for_server(timeout_sec=server_wait_timeout_s):
            return SendOutcome(dispatch.dispatch_id, None, 'SERVER_UNAVAILABLE')

        goal_uuid = nav2_goal_v1.uuid_to_ros_uuid(dispatch.dispatch_id)
        send_future = client.send_goal_async(goal_msg, goal_uuid=goal_uuid)
        rclpy.spin_until_future_complete(self._node, send_future, timeout_sec=accept_timeout_s)
        if not send_future.done():
            return SendOutcome(dispatch.dispatch_id, None, 'ACCEPT_TIMEOUT')

        handle = send_future.result()
        if not handle.accepted:
            return SendOutcome(dispatch.dispatch_id, False, 'REJECTED')
        return SendOutcome(dispatch.dispatch_id, True, 'ACCEPTED')

    def query_result(
        self, *, store: MissionStore, mission_id: str, step_key: str,
        service_wait_timeout_s: float = DEFAULT_SERVICE_WAIT_TIMEOUT_S,
        result_timeout_s: float = DEFAULT_RESULT_TIMEOUT_S,
    ) -> RecoveryObservation:
        """
        Ask the resolved action's GetResult service what it knows, without sending anything.

        Read-only: never sends a goal, never fabricates a ClientGoalHandle or an acceptance
        response, and never requires a fresh send receipt or a bound goal (get_dispatch_goal
        is never called here) -- only the dispatch identity and its action context, which is
        all historical-result inspection needs. Does not require execution ownership. Never
        calls store.apply_recovery_observation: the returned RecoveryObservation is hand-back
        evidence for the caller (or a future supervisor) to apply through the core's own
        API, never applied here.
        """
        _validate_timeout(service_wait_timeout_s, 'service_wait_timeout_s')
        _validate_timeout(result_timeout_s, 'result_timeout_s')

        dispatch = store.get_step_dispatch(mission_id, step_key)
        context = store.get_dispatch_context(mission_id, step_key)
        _check_action_type(context.action_name, context.action_type, context.robot_namespace)
        resolved_name = _resolve_action_name(context.action_name, context.robot_namespace)
        _verify_base_action_unremapped(
            self._node, resolved_name, context.action_name, context.action_type,
            context.robot_namespace)

        correlation = {
            'dispatch_id': dispatch.dispatch_id, 'action_name': context.action_name,
            'action_type': context.action_type, 'robot_namespace': context.robot_namespace,
        }

        service_type = NavigateToPose.Impl.GetResultService
        service_name = f'{resolved_name}/_action/get_result'
        _verify_unremapped(
            self._node, service_type, service_name, 'the get_result service',
            context.action_name, context.action_type, context.robot_namespace)
        client = self._result_service_clients.get(service_name)
        if client is None:
            client = self._node.create_client(service_type, service_name)
            self._result_service_clients[service_name] = client

        if not client.wait_for_service(timeout_sec=service_wait_timeout_s):
            return RecoveryObservation(kind=ObservationKind.SERVICE_UNAVAILABLE, **correlation)

        request = service_type.Request()
        request.goal_id = nav2_goal_v1.uuid_to_ros_uuid(dispatch.dispatch_id)
        future = None
        try:
            future = client.call_async(request)
            rclpy.spin_until_future_complete(self._node, future, timeout_sec=result_timeout_s)
            if not future.done():
                client.remove_pending_request(future)
                return RecoveryObservation(kind=ObservationKind.LOCAL_TIMEOUT, **correlation)
            # future.result() is INSIDE this try on purpose: a future can complete with an
            # exception set on it (not only a value), and that exception only actually
            # raises here, at .result() -- outside this boundary it would escape as an
            # uncaught exception instead of the correlated TRANSPORT_ERROR this method
            # promises for every ROS-level failure.
            response = future.result()
        except Exception:
            # A narrow catch around exactly the ROS call this method makes (never around the
            # contract validation above, which must still raise as itself): any transport-
            # level failure here becomes a typed, correlated TRANSPORT_ERROR observation
            # rather than an uncaught exception reaching the caller. Clean up a still-pending
            # request (never a ROS goal -- there is no goal here, only this GetResult service
            # call) if the exception came from call_async/spin rather than from the future's
            # own stored result.
            if future is not None and not future.done():
                client.remove_pending_request(future)
            return RecoveryObservation(kind=ObservationKind.TRANSPORT_ERROR, **correlation)

        status = _STATUS_MAP.get(response.status)
        if status is None:
            # An action-status code this adapter does not recognize is itself a transport-
            # level surprise, not a status to guess at or silently drop.
            return RecoveryObservation(kind=ObservationKind.TRANSPORT_ERROR, **correlation)
        return RecoveryObservation(
            kind=ObservationKind.RESULT_RESPONSE, status=status,
            result_code=int(response.result.error_code),
            result_message=response.result.error_msg, **correlation)

    def close(self) -> None:
        """Deterministically destroy every ROS client this adapter instance created."""
        for client in self._action_clients.values():
            client.destroy()
        self._action_clients.clear()
        for client in self._result_service_clients.values():
            self._node.destroy_client(client)
        self._result_service_clients.clear()


# ROS spells CANCELED with one L; reconciliation.ActionStatus matches that spelling too
# (see task_resilience/reconciliation.py's own comment on this).
_STATUS_MAP = {
    GoalStatus.STATUS_UNKNOWN: ActionStatus.UNKNOWN,
    GoalStatus.STATUS_ACCEPTED: ActionStatus.ACCEPTED,
    GoalStatus.STATUS_EXECUTING: ActionStatus.EXECUTING,
    GoalStatus.STATUS_CANCELING: ActionStatus.CANCELING,
    GoalStatus.STATUS_SUCCEEDED: ActionStatus.SUCCEEDED,
    GoalStatus.STATUS_CANCELED: ActionStatus.CANCELED,
    GoalStatus.STATUS_ABORTED: ActionStatus.ABORTED,
}
