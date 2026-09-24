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
Send one NavigateToPose goal to Nav2 under a caller-chosen goal UUID.

Experiment tooling for the ROS action recovery spike (docs/ros_action_recovery_spike.md).
Uses only the public rclpy API: ActionClient.send_goal_async(goal, goal_uuid=...).

Do not reuse a UUID. Sending a UUID the server still tracks aborted the whole Nav2 container in
the spike's planned trials, and once the server has forgotten it the goal is accepted and runs
again. Generate a new uuid4 for every independent experiment. A new UUID is a new request, not
a retry of an unresolved one. --exit-before-response is a controlled experiment that needs a
UUID that has never been used: a status entry left by an earlier goal with the same UUID would
satisfy its check at once.

Exit codes: 0 in the default mode means accepted and SUCCEEDED, 1 means rejected or an
unsuccessful terminal status, 2 means timeout or no server. In the early-exit modes a 0 is not
confirmation of acceptance or success. --exit-after-accept returns 0 after an accept response
without asking for the result, and --exit-before-response and --exit-immediately return 0 without
any accept/reject response. Read the printed lines, not the exit code, to see what was observed.
"""

import argparse
import datetime
import math
import os
import sys
import time

from action_msgs.msg import GoalStatus, GoalStatusArray
from nav2_msgs.action import NavigateToPose
import rclpy
from rclpy.action import ActionClient
from rclpy.qos import qos_profile_action_status_default

from ros_uuid import hex_bytes, msg_to_uuid, uuid_arg, uuid_to_msg

STATUS_NAMES = {
    GoalStatus.STATUS_UNKNOWN: 'UNKNOWN',
    GoalStatus.STATUS_ACCEPTED: 'ACCEPTED',
    GoalStatus.STATUS_EXECUTING: 'EXECUTING',
    GoalStatus.STATUS_CANCELING: 'CANCELING',
    GoalStatus.STATUS_SUCCEEDED: 'SUCCEEDED',
    GoalStatus.STATUS_CANCELED: 'CANCELED',
    GoalStatus.STATUS_ABORTED: 'ABORTED',
}


def now():
    return datetime.datetime.now().astimezone().isoformat(timespec='milliseconds')


def say(text):
    print(f'{now()} {text}', flush=True)


def parse_args(argv):
    p = argparse.ArgumentParser(description=__doc__.strip().split('\n')[0])
    p.add_argument('--uuid', type=uuid_arg, required=True,
                   help='goal UUID, canonical text (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)')
    p.add_argument('--x', type=float, default=0.5, help='goal x in map frame (m)')
    p.add_argument('--y', type=float, default=0.5, help='goal y in map frame (m)')
    p.add_argument('--yaw', type=float, default=0.0, help='goal yaw (rad)')
    p.add_argument('--server-timeout', type=float, default=10.0,
                   help='seconds to wait for the action server')
    p.add_argument('--timeout', type=float, default=180.0,
                   help='overall seconds to wait for acceptance and result')
    mode = p.add_mutually_exclusive_group()
    mode.add_argument('--exit-after-accept', action='store_true',
                      help='exit as soon as the goal is accepted; do not ask for the result '
                           '(simulates the original client disappearing)')
    mode.add_argument('--exit-before-response', action='store_true',
                      help='send the goal, wait until the goal shows up on the status topic '
                           '(read through a separate node), then die WITHOUT ever taking the '
                           'accept/reject response. Needs a never-before-used UUID: a status '
                           'entry left by an earlier goal with the same UUID ends the wait '
                           'at once')
    mode.add_argument('--exit-immediately', action='store_true',
                      help='send the goal and die at once, without waiting for anything')
    p.add_argument('--evidence-timeout', type=float, default=10.0,
                   help='with --exit-before-response: seconds to wait for status evidence')
    return p.parse_args(argv)


def make_goal(x, y, yaw):
    goal = NavigateToPose.Goal()
    goal.pose.header.frame_id = 'map'
    # Stamp stays 0 on purpose: Nav2 runs on sim time, our clock does not.
    goal.pose.pose.position.x = x
    goal.pose.pose.position.y = y
    goal.pose.pose.orientation.z = math.sin(yaw / 2.0)
    goal.pose.pose.orientation.w = math.cos(yaw / 2.0)
    return goal


def die(code=0):
    # No destroy, no shutdown: the point is a client that vanishes.
    sys.stdout.flush()
    os._exit(code)


def watch_status_for(requested, timeout):
    """
    Wait, on a separate node, until the status topic lists the UUID.

    Returns (status name, seconds waited) or (None, seconds waited). The node that owns
    the ActionClient is deliberately not spun, so its send_goal response is never taken.
    """
    watcher = rclpy.create_node('send_goal_status_watcher')
    seen = {}

    def on_status(msg):
        for entry in msg.status_list:
            if msg_to_uuid(entry.goal_info.goal_id) == requested:
                seen['status'] = STATUS_NAMES.get(entry.status, str(entry.status))

    watcher.create_subscription(
        GoalStatusArray, '/navigate_to_pose/_action/status', on_status,
        qos_profile_action_status_default)
    started = time.monotonic()
    while 'status' not in seen and time.monotonic() - started < timeout:
        rclpy.spin_once(watcher, timeout_sec=0.1)
    return seen.get('status'), time.monotonic() - started


def run(node, client, args):
    requested = args.uuid
    deadline = time.monotonic() + args.timeout

    def remaining():
        return max(0.0, deadline - time.monotonic())

    say(f'uuid: {requested}')
    say(f'uuid bytes (hex, ROS order): {hex_bytes(requested)}')

    if not client.wait_for_server(timeout_sec=args.server_timeout):
        say(f'ERROR: /navigate_to_pose not available after {args.server_timeout}s')
        return 2

    goal = make_goal(args.x, args.y, args.yaw)
    sent_at = time.monotonic()
    send_future = client.send_goal_async(goal, goal_uuid=uuid_to_msg(requested))
    say(f'goal submitted: uuid={requested} x={args.x} y={args.y} yaw={args.yaw} frame=map')

    if args.exit_immediately:
        say('exiting immediately after send (nothing was awaited; response never taken)')
        die(0)

    if args.exit_before_response:
        status, waited = watch_status_for(requested, args.evidence_timeout)
        if status is None:
            say(f'no status entry for the uuid within {waited:.1f}s; '
                'accept/reject unknown, exiting without the response')
        else:
            say(f'status topic listed the uuid as {status} after {waited:.1f}s (a status '
                'observation only, not an acceptance response); exiting without consuming the '
                'send_goal response')
        die(0)

    rclpy.spin_until_future_complete(node, send_future, timeout_sec=remaining())
    if not send_future.done():
        say('ERROR: timed out waiting for goal accept/reject; acceptance outcome UNKNOWN '
            '(the goal may or may not have been received; no cancel was sent)')
        return 2

    handle = send_future.result()
    waited = time.monotonic() - sent_at
    stamp = handle.stamp
    same = msg_to_uuid(handle.goal_id) == requested
    if not handle.accepted:
        say(f'goal REJECTED (response after {waited:.3f}s, server stamp '
            f'{stamp.sec}.{stamp.nanosec:09d}, handle uuid matches request: {same})')
        return 1
    say(f'goal ACCEPTED (response after {waited:.3f}s, server stamp '
        f'{stamp.sec}.{stamp.nanosec:09d}, handle uuid matches request: {same})')

    if args.exit_after_accept:
        say('exiting after accept, without asking for the result')
        return 0

    result_future = handle.get_result_async()
    rclpy.spin_until_future_complete(node, result_future, timeout_sec=remaining())
    if not result_future.done():
        say(f'ERROR: timed out waiting for a terminal result (overall limit {args.timeout}s); '
            'current goal state unknown (acceptance was received; no cancel was sent)')
        return 2

    wrapped = result_future.result()
    status = STATUS_NAMES.get(wrapped.status, str(wrapped.status))
    say(f'terminal status: {status} '
        f'(error_code={wrapped.result.error_code} error_msg={wrapped.result.error_msg!r})')
    return 0 if wrapped.status == GoalStatus.STATUS_SUCCEEDED else 1


def main(argv=None):
    args = parse_args(argv)
    rclpy.init()
    node = rclpy.create_node('send_nav2_goal_with_uuid')
    client = ActionClient(node, NavigateToPose, 'navigate_to_pose')
    try:
        return run(node, client, args)
    finally:
        client.destroy()
        node.destroy_node()
        rclpy.try_shutdown()


if __name__ == '__main__':
    sys.exit(main())
