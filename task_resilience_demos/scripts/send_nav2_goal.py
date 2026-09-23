#!/usr/bin/env python3
"""Send one NavigateToPose goal to Nav2 and wait for a terminal result.

Exit codes: 0 succeeded, 1 rejected or unsuccessful terminal status,
2 timeout (no server, no response, or no result in time).
"""

import argparse
import math
import sys
import time

import rclpy
from action_msgs.msg import GoalStatus
from action_msgs.srv import CancelGoal
from nav2_msgs.action import NavigateToPose
from rclpy.action import ActionClient

STATUS_NAMES = {
    GoalStatus.STATUS_UNKNOWN: 'UNKNOWN',
    GoalStatus.STATUS_ACCEPTED: 'ACCEPTED',
    GoalStatus.STATUS_EXECUTING: 'EXECUTING',
    GoalStatus.STATUS_CANCELING: 'CANCELING',
    GoalStatus.STATUS_SUCCEEDED: 'SUCCEEDED',
    GoalStatus.STATUS_CANCELED: 'CANCELED',
    GoalStatus.STATUS_ABORTED: 'ABORTED',
}

CANCEL_ERROR_NAMES = {
    CancelGoal.Response.ERROR_REJECTED: 'ERROR_REJECTED',
    CancelGoal.Response.ERROR_UNKNOWN_GOAL_ID: 'ERROR_UNKNOWN_GOAL_ID',
    CancelGoal.Response.ERROR_GOAL_TERMINATED: 'ERROR_GOAL_TERMINATED',
}


def parse_args(argv):
    p = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    p.add_argument('--x', type=float, default=0.5, help='goal x in map frame (m)')
    p.add_argument('--y', type=float, default=0.5, help='goal y in map frame (m)')
    p.add_argument('--yaw', type=float, default=0.0, help='goal yaw (rad)')
    p.add_argument('--server-timeout', type=float, default=10.0,
                   help='seconds to wait for the action server')
    p.add_argument('--timeout', type=float, default=120.0,
                   help='overall seconds to wait for acceptance and result')
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


def describe_cancel(cancel_future):
    """Summarize the cancel response. It does not say the goal reached CANCELED."""
    if not cancel_future.done() or cancel_future.exception() is not None:
        return 'cancel request outcome UNKNOWN (no usable response)'
    response = cancel_future.result()
    code = response.return_code
    if code != CancelGoal.Response.ERROR_NONE:
        return f'cancel request NOT ACCEPTED ({CANCEL_ERROR_NAMES.get(code, code)})'
    # An empty list means no goal entered CANCELING, regardless of the return code.
    if not response.goals_canceling:
        return 'cancel request NOT ACCEPTED (ERROR_NONE but no goal CANCELING)'
    return 'cancel request ACCEPTED (goal CANCELING; CANCELED not confirmed)'


def run(node, client, args):
    deadline = time.monotonic() + args.timeout

    def remaining():
        return max(0.0, deadline - time.monotonic())

    if not client.wait_for_server(timeout_sec=args.server_timeout):
        print(f'ERROR: /navigate_to_pose not available after {args.server_timeout}s')
        return 2

    goal = make_goal(args.x, args.y, args.yaw)
    print(f'goal submitted: x={args.x} y={args.y} yaw={args.yaw} frame=map')
    send_future = client.send_goal_async(goal)
    rclpy.spin_until_future_complete(node, send_future, timeout_sec=remaining())
    if not send_future.done():
        print('ERROR: timed out waiting for goal accept/reject; acceptance outcome UNKNOWN '
              '(goal may or may not have been received; no cancel was sent)')
        return 2

    handle = send_future.result()
    if not handle.accepted:
        print('goal REJECTED')
        return 1
    print('goal ACCEPTED')

    result_future = handle.get_result_async()
    rclpy.spin_until_future_complete(node, result_future, timeout_sec=remaining())
    if not result_future.done():
        print(f'ERROR: no terminal result within {args.timeout}s, requesting cancel')
        cancel_future = handle.cancel_goal_async()
        rclpy.spin_until_future_complete(node, cancel_future, timeout_sec=5.0)
        print(describe_cancel(cancel_future))
        return 2

    wrapped = result_future.result()
    status = STATUS_NAMES.get(wrapped.status, str(wrapped.status))
    print(f'terminal status: {status} '
          f'(error_code={wrapped.result.error_code} error_msg={wrapped.result.error_msg!r})')
    return 0 if wrapped.status == GoalStatus.STATUS_SUCCEEDED else 1


def main(argv=None):
    args = parse_args(argv)
    rclpy.init()
    node = rclpy.create_node('send_nav2_goal')
    client = ActionClient(node, NavigateToPose, 'navigate_to_pose')
    try:
        return run(node, client, args)
    finally:
        client.destroy()
        node.destroy_node()
        rclpy.try_shutdown()


if __name__ == '__main__':
    sys.exit(main())
