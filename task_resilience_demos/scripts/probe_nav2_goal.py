#!/usr/bin/env python3
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
Ask a running Nav2 what it knows about a NavigateToPose goal UUID. Never resends it.

Experiment tooling for the ROS action recovery spike (docs/ros_action_recovery_spike.md).
A fresh process that knows only the UUID. Sections, by mechanism:

  A. status topic          ROS ACTION PROTOCOL  (/navigate_to_pose/_action/status)
  B. public API surface    PUBLIC HIGH-LEVEL API (inspection only; nothing is constructed)
  C. get-result service    ROS ACTION PROTOCOL  (/navigate_to_pose/_action/get_result)
  D. private handle        PRIVATE RCLPY INTERNALS (only with --private-handle; comparison)

Every wait is bounded. A local timeout is reported as such and is not the same thing as
the server returning UNKNOWN.
"""

import argparse
import datetime
import inspect
import json
import sys
import time

from action_msgs.msg import GoalStatus, GoalStatusArray
from nav2_msgs.action import NavigateToPose
import rclpy
from rclpy.action import ActionClient
from rclpy.qos import qos_profile_action_status_default

from ros_uuid import hex_bytes, msg_to_uuid, uuid_arg, uuid_to_msg

ACTION = '/navigate_to_pose'
STATUS_TOPIC = f'{ACTION}/_action/status'
GET_RESULT_SERVICE = f'{ACTION}/_action/get_result'

STATUS_NAMES = {
    GoalStatus.STATUS_UNKNOWN: 'UNKNOWN',
    GoalStatus.STATUS_ACCEPTED: 'ACCEPTED',
    GoalStatus.STATUS_EXECUTING: 'EXECUTING',
    GoalStatus.STATUS_CANCELING: 'CANCELING',
    GoalStatus.STATUS_SUCCEEDED: 'SUCCEEDED',
    GoalStatus.STATUS_CANCELED: 'CANCELED',
    GoalStatus.STATUS_ABORTED: 'ABORTED',
}


def wall():
    return datetime.datetime.now().astimezone().isoformat(timespec='milliseconds')


def say(text):
    print(f'{wall()} {text}', flush=True)


def parse_args(argv):
    p = argparse.ArgumentParser(description=__doc__.strip().split('\n')[0])
    p.add_argument('--uuid', type=uuid_arg, required=True,
                   help='goal UUID, canonical text (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)')
    p.add_argument('--status-window', type=float, default=3.0,
                   help='seconds to listen on the status topic')
    p.add_argument('--result-timeout', type=float, default=15.0,
                   help='seconds to wait for the get-result response before giving up locally')
    p.add_argument('--server-timeout', type=float, default=5.0,
                   help='seconds to wait for the get-result service to be discovered')
    p.add_argument('--skip-status', action='store_true', help='skip section A')
    p.add_argument('--skip-result', action='store_true', help='skip section C')
    p.add_argument('--private-handle', action='store_true',
                   help='EXPERIMENTAL, section D: build a ClientGoalHandle by hand and use '
                        'rclpy private internals to ask for the result')
    p.add_argument('--json', action='store_true',
                   help='also print one machine-readable "SUMMARY {...}" line')
    return p.parse_args(argv)


def entries_of(msg):
    return [(msg_to_uuid(e.goal_info.goal_id), STATUS_NAMES.get(e.status, str(e.status)),
             e.goal_info.stamp) for e in msg.status_list]


def probe_status(node, requested, window):
    """Section A. Listen on the status topic and look for the UUID."""
    say(f'[A: status topic] {STATUS_TOPIC} (ROS action protocol), listening {window}s')
    received = []
    started = time.monotonic()

    def on_status(msg):
        received.append((time.monotonic() - started, entries_of(msg)))

    node.create_subscription(GoalStatusArray, STATUS_TOPIC, on_status,
                             qos_profile_action_status_default)
    while time.monotonic() - started < window:
        rclpy.spin_once(node, timeout_sec=0.05)

    # Asked after the window: right after node creation the graph is still undiscovered.
    publishers = [
        {'node': f'{p.node_namespace.rstrip("/")}/{p.node_name}',
         'reliability': p.qos_profile.reliability.name,
         'durability': p.qos_profile.durability.name,
         'history': f'{p.qos_profile.history.name} depth={p.qos_profile.depth}'}
        for p in node.get_publishers_info_by_topic(STATUS_TOPIC)]
    for pub in publishers:
        say(f'  publisher {pub["node"]}: {pub["reliability"]}, {pub["durability"]}, '
            f'{pub["history"]}')
    if not publishers:
        say('  no publisher is visible on the status topic')

    summary = {'publishers': publishers, 'messages': len(received),
               'first_message_after_s': round(received[0][0], 3) if received else None}
    if not received:
        say('  no status message received in the window; that says nothing about whether '
            'the goal ever existed')
        summary.update(present_in_latest=None, latest_status=None, present_ever=False,
                       latest_entries=None)
        return summary

    for offset, entries in received:
        listing = ', '.join(
            f'{u}={s} (goal_info.stamp {st.sec}.{st.nanosec:09d})' for u, s, st in entries
        ) or '(empty list)'
        say(f'  +{offset:.3f}s status array: {listing}')
    latest = received[-1][1]
    in_latest = [s for u, s, _ in latest if u == requested]
    ever = [s for _, entries in received for u, s, _ in entries if u == requested]
    summary.update(present_in_latest=bool(in_latest),
                   latest_status=in_latest[0] if in_latest else None,
                   present_ever=bool(ever),
                   latest_entries=[f'{u}={s}' for u, s, _ in latest])
    if in_latest:
        say(f'  status UUID present: yes; status: {in_latest[0]}')
    else:
        say('  status UUID present: no (not in the latest status array; this is not proof '
            'that the goal never existed)')
    return summary


def report_public_api():
    """
    Section B. What the public high-level API offers a process that holds only a UUID.

    Inspection only. Nothing is constructed and nothing is sent.
    """
    from rclpy.action import client as action_client

    say('[B: public high-level API] inspecting rclpy.action.client (no goal is sent)')
    takes_uuid = {}
    for name, member in inspect.getmembers(ActionClient, callable):
        if name.startswith('_'):
            continue
        params = inspect.signature(member).parameters
        if any('uuid' in p or 'goal_id' in p for p in params):
            takes_uuid[name] = str(inspect.signature(member))
    handle_public = sorted(n for n in dir(action_client.ClientGoalHandle) if not n.startswith('_'))
    exported = 'ClientGoalHandle' in dir(__import__('rclpy.action', fromlist=['x']))
    summary = {
        'public_ActionClient_methods_taking_a_uuid': takes_uuid,
        'ClientGoalHandle_public_members': handle_public,
        'ClientGoalHandle_exported_from_rclpy.action': exported,
        'ClientGoalHandle_init_signature': str(inspect.signature(
            action_client.ClientGoalHandle.__init__)),
        'ClientGoalHandle_documented_as_user_constructed':
            'goal handle' in (action_client.ClientGoalHandle.__doc__ or '').lower()
            and 'construct' in (action_client.ClientGoalHandle.__doc__ or '').lower(),
    }
    for key, value in summary.items():
        say(f'  {key}: {value}')
    say('  finding: the only public ActionClient method that takes a goal UUID is '
        'send_goal_async(goal, ..., goal_uuid=...), and it SENDS a goal request. No public '
        'method turns a UUID into a ClientGoalHandle or fetches a result by UUID.')
    return summary


def probe_result_protocol(node, requested, server_timeout, result_timeout):
    """Section C. Call the generated get_result service directly with the UUID."""
    say(f'[C: get-result service] {GET_RESULT_SERVICE} (ROS action protocol; not the '
        f'high-level ActionClient API)')
    service_type = NavigateToPose.Impl.GetResultService
    client = node.create_client(service_type, GET_RESULT_SERVICE)
    found = client.wait_for_service(timeout_sec=server_timeout)
    listed = [types for name, types in node.get_service_names_and_types()
              if name == GET_RESULT_SERVICE]
    summary = {'service_type': f'{service_type.__module__}.{service_type.__name__}',
               'service_discovered': found, 'graph_lists_service_types': listed}
    say(f'  service discovered: {found}; type listed by the graph: {listed}')
    if not found:
        say(f'  service not available after {server_timeout}s; nothing was requested')
        summary['outcome'] = 'service_unavailable'
        node.destroy_client(client)
        return summary

    request = service_type.Request()
    request.goal_id = uuid_to_msg(requested)
    started = time.monotonic()
    requested_at = wall()
    say(f'  request  uuid={requested} at {requested_at} (waiting up to {result_timeout}s)')
    future = client.call_async(request)
    rclpy.spin_until_future_complete(node, future, timeout_sec=result_timeout)
    elapsed = time.monotonic() - started
    summary.update(request_time=requested_at, elapsed_s=round(elapsed, 3))

    if not future.done():
        client.remove_pending_request(future)
        say(f'  RESULT REQUEST TIMED OUT LOCALLY after {elapsed:.3f}s: no response arrived. '
            'The server may still be holding the request (goal active), or the response may '
            'have been lost; this is not a server answer.')
        summary['outcome'] = 'local_timeout'
    else:
        response = future.result()
        status = STATUS_NAMES.get(response.status, str(response.status))
        say(f'  response uuid={requested} at {wall()} after {elapsed:.3f}s')
        say(f'  returned action status: {status} ({response.status})')
        say(f'  NavigateToPose result.error_code={response.result.error_code} '
            f'result.error_msg={response.result.error_msg!r}')
        summary.update(outcome='response', response_time=wall(), status=status,
                       error_code=int(response.result.error_code),
                       error_msg=response.result.error_msg)
    node.destroy_client(client)
    return summary


def probe_private_handle(node, requested, server_timeout, result_timeout):
    """
    Section D. EXPERIMENTAL, PRIVATE INTERNALS. A comparison, not a supported method.

    Builds a ClientGoalHandle by hand with a FABRICATED accepted response, then calls the
    public-looking get_result_async(), which reaches into ActionClient._get_result_async and
    the private _client_handle. Nothing here is a documented way to recover a goal.
    """
    from rclpy.action.client import ClientGoalHandle

    say('[D: private rclpy internals, EXPERIMENTAL] hand-built ClientGoalHandle with a '
        'fabricated accepted response')
    client = ActionClient(node, NavigateToPose, 'navigate_to_pose')
    summary = {'mechanism': 'private rclpy internals (ClientGoalHandle(...) built by hand)'}
    if not client.wait_for_server(timeout_sec=server_timeout):
        say('  action server not available; nothing was requested')
        summary['outcome'] = 'server_unavailable'
        client.destroy()
        return summary
    fabricated = NavigateToPose.Impl.SendGoalService.Response()
    fabricated.accepted = True
    handle = ClientGoalHandle(client, uuid_to_msg(requested), fabricated)
    started = time.monotonic()
    future = handle.get_result_async()
    rclpy.spin_until_future_complete(node, future, timeout_sec=result_timeout)
    elapsed = time.monotonic() - started
    summary['elapsed_s'] = round(elapsed, 3)
    if not future.done():
        say(f'  RESULT REQUEST TIMED OUT LOCALLY after {elapsed:.3f}s (no response)')
        summary['outcome'] = 'local_timeout'
    else:
        response = future.result()
        status = STATUS_NAMES.get(response.status, str(response.status))
        say(f'  returned action status: {status}; error_code={response.result.error_code} '
            f'error_msg={response.result.error_msg!r} after {elapsed:.3f}s')
        summary.update(outcome='response', status=status,
                       error_code=int(response.result.error_code),
                       error_msg=response.result.error_msg)
    client.destroy()
    return summary


def main(argv=None):
    args = parse_args(argv)
    rclpy.init()
    node = rclpy.create_node('probe_nav2_goal')
    summary = {'uuid': str(args.uuid)}
    try:
        say(f'uuid: {args.uuid}')
        say(f'uuid bytes (hex, ROS order): {hex_bytes(args.uuid)}')
        say('probe only: this process never sends a goal')
        if not args.skip_status:
            summary['status_topic'] = probe_status(node, args.uuid, args.status_window)
        summary['public_api'] = report_public_api()
        if not args.skip_result:
            summary['get_result_protocol'] = probe_result_protocol(
                node, args.uuid, args.server_timeout, args.result_timeout)
        if args.private_handle:
            summary['private_handle'] = probe_private_handle(
                node, args.uuid, args.server_timeout, args.result_timeout)
        if args.json:
            print('SUMMARY ' + json.dumps(summary, default=str), flush=True)
        return 0
    finally:
        node.destroy_node()
        rclpy.try_shutdown()


if __name__ == '__main__':
    sys.exit(main())
