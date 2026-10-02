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
Command-line entry point for supervisor.py: `run` / `resume` / `status`.

EXPERIMENTAL DEVELOPER PREVIEW. See docs/task016_supervisor.md for the full runbook, exit
codes, and the dedicated-database restriction. This module owns the one supported resource
lifecycle for `run`/`resume`:

    construct/validate ExecutionOwner -> acquire()
      -> MissionStore(owner.canonical_db_path)
      -> rclpy.init() -> create_node() -> Nav2Adapter(node)
      -> supervisor.run_mission / resume_mission
    (reverse order on the way out: adapter.close()/node.destroy_node(), rclpy.try_shutdown(),
    store.close(), owner.close() -- ownership released LAST, including on initialization
    failure)

`status` is read-only and does not acquire ownership or touch ROS at all (MissionStore(db_path)
directly) -- see supervisor.py's own docstring.

A second supervisor pointed at the same database fails here, in ExecutionOwner.acquire(),
before MissionStore is ever constructed -- this module nests the existing resources in the
documented order; it does not modify ExecutionOwner to enforce that.
"""

import argparse
import math
import sys

import rclpy

from task_resilience.errors import MissionNotFound
from task_resilience.storage import MissionStore

from . import supervisor as sup
from .errors import ExecutionOwnerAlreadyHeld
from .execution_owner import ExecutionOwner
from .nav2_adapter import Nav2Adapter

DEFAULT_ACTION_NAME = '/navigate_to_pose'
DEFAULT_ROBOT_NAMESPACE = ''
DEFAULT_FRAME_ID = 'map'
# Verified free, against the committed hardware-free Nav2 loopback baseline's own map
# (docs/hardware_free_demo.md): (-2.0, -0.5) is that baseline's documented initial pose, and
# (0.5, 0.5) is its documented default goal -- both already checked there as free cells with
# 0.5 m clearance at a 0.3 m robot radius. checkpoint_a reuses the verified goal; return_to_base
# reuses the verified start pose, so "returning to base" is literally returning to it.
DEFAULT_CHECKPOINT_XY = (0.5, 0.5)
DEFAULT_BASE_XY = (-2.0, -0.5)

_EXIT_BY_OUTCOME = {
    'COMPLETED': 0,
    'FAILED': 4,
    'CANCELLED': 4,
    sup.OUTCOME_UNRESOLVED: 3,
    sup.OUTCOME_NEEDS_RECONCILIATION: 5,
    sup.OUTCOME_CANCEL_PRESERVED: 6,
    sup.OUTCOME_PAUSED_FOR_DEMO: 7,
}


def _goal_payload(x: float, y: float, frame_id: str) -> dict:
    return {
        'frame_id': frame_id,
        'position': {'x': x, 'y': y, 'z': 0.0},
        'orientation': {'x': 0.0, 'y': 0.0, 'z': 0.0, 'w': 1.0},
        'behavior_tree': '',
    }


def _print_report(report: sup.SupervisorReport) -> None:
    print(f'OUTCOME {report.outcome}')
    print(f'DETAIL {report.detail}')
    if report.mission is not None:
        m = report.mission
        print(
            f'MISSION mission_id={m.mission_id} state={m.state.value} '
            f'attempts_used={m.attempts_used}/{m.max_attempts}')
    for step in report.steps:
        print(f'STEP {step.step_key} state={step.state.value} index={step.step_index}')
    for detail in report.step_details:
        print(
            f'STEP-DETAIL {detail.step.step_key} dispatch_id={detail.dispatch_id} '
            f'has_context={detail.has_context} has_goal={detail.has_goal} '
            f'has_authorization={detail.has_authorization} '
            f'evidence_count={detail.evidence_count}')


def _run_with_resources(db_path: str, node_name: str, body):
    """
    Run the one supported resource lifecycle: owner -> store -> (rclpy node -> adapter) -> body.

    Teardown in reverse, ownership released last, including when any construction step
    raises. Returns body's SupervisorReport, or re-raises ExecutionOwnerAlreadyHeld for the
    caller to report distinctly (no MissionStore is ever touched in that case).
    """
    owner = ExecutionOwner(db_path)
    owner.acquire()
    try:
        sup.ensure_database_admissible(owner.canonical_db_path)
        store = MissionStore(owner.canonical_db_path)
        try:
            rclpy.init()
            try:
                node = rclpy.create_node(node_name)
                adapter = Nav2Adapter(node)
                try:
                    return body(owner, store, adapter)
                finally:
                    adapter.close()
                    node.destroy_node()
            finally:
                rclpy.try_shutdown()
        finally:
            store.close()
    finally:
        owner.close()


def cmd_run(args: argparse.Namespace) -> sup.SupervisorReport:
    checkpoint_payload = _goal_payload(args.checkpoint_x, args.checkpoint_y, args.frame_id)
    base_payload = _goal_payload(args.base_x, args.base_y, args.frame_id)

    def body(owner, store, adapter):
        return sup.run_mission(
            owner=owner, store=store, adapter=adapter, action_name=args.action_name,
            robot_namespace=args.robot_namespace, checkpoint_goal_payload=checkpoint_payload,
            base_goal_payload=base_payload, max_attempts=args.max_attempts,
            poll_attempts=args.poll_attempts, poll_result_timeout_s=args.poll_timeout)

    return _run_with_resources(args.db, args.node_name, body)


def cmd_resume(args: argparse.Namespace) -> sup.SupervisorReport:
    def body(owner, store, adapter):
        return sup.resume_mission(
            owner=owner, store=store, adapter=adapter, poll_attempts=args.poll_attempts,
            poll_result_timeout_s=args.poll_timeout)

    return _run_with_resources(args.db, args.node_name, body)


def cmd_status(args: argparse.Namespace) -> sup.SupervisorReport:
    store = sup.open_preview_store_for_status(args.db)
    try:
        return sup.mission_status(store=store)
    finally:
        store.close()


def _positive_int(raw: str) -> int:
    try:
        value = int(raw)
    except ValueError:
        raise argparse.ArgumentTypeError(f'must be a positive integer, got {raw!r}') from None
    if value <= 0:
        raise argparse.ArgumentTypeError(f'must be positive, got {value!r}')
    return value


def _positive_finite_float(raw: str) -> float:
    try:
        value = float(raw)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f'must be a finite positive number, got {raw!r}') from None
    if not math.isfinite(value) or value <= 0:
        raise argparse.ArgumentTypeError(f'must be a finite positive number, got {value!r}')
    return value


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog='task016_supervisor',
        description='Minimal two-step (checkpoint_a, return_to_base) Nav2 mission supervisor '
                    '-- experimental developer preview, see docs/task016_supervisor.md')
    sub = p.add_subparsers(dest='command', required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        '--db', required=True,
        help="path to this mission's own DEDICATED database file (see the runbook -- never "
             'point this at a database shared with unrelated missions)')

    run_resume_common = argparse.ArgumentParser(add_help=False, parents=[common])
    run_resume_common.add_argument(
        '--node-name', default='task016_supervisor',
        help='rclpy node name this invocation creates (default: %(default)s)')
    run_resume_common.add_argument(
        '--poll-attempts', type=_positive_int, default=sup.DEFAULT_POLL_ATTEMPTS,
        help='bounded read-only query_result polls per in-flight step before reporting '
             'UNRESOLVED (default: %(default)s). Each poll also waits for the GetResult '
             'service to be discovered, so the total time bound is not simply '
             'poll-attempts x poll-timeout -- see the runbook')
    run_resume_common.add_argument(
        '--poll-timeout', type=_positive_finite_float, default=sup.DEFAULT_POLL_RESULT_TIMEOUT_S,
        help='result_timeout_s passed to each poll (default: %(default)s)')

    run_p = sub.add_parser(
        'run', parents=[run_resume_common],
        help='create or replay the one preview mission, then recover and advance it')
    run_p.add_argument('--action-name', default=DEFAULT_ACTION_NAME)
    run_p.add_argument('--robot-namespace', default=DEFAULT_ROBOT_NAMESPACE)
    run_p.add_argument('--frame-id', default=DEFAULT_FRAME_ID)
    run_p.add_argument('--checkpoint-x', type=float, default=DEFAULT_CHECKPOINT_XY[0])
    run_p.add_argument('--checkpoint-y', type=float, default=DEFAULT_CHECKPOINT_XY[1])
    run_p.add_argument('--base-x', type=float, default=DEFAULT_BASE_XY[0])
    run_p.add_argument('--base-y', type=float, default=DEFAULT_BASE_XY[1])
    run_p.add_argument(
        '--max-attempts', type=int, default=sup.DEFAULT_MAX_ATTEMPTS,
        help='default %(default)s: exactly one authorized send per step')
    run_p.set_defaults(func=cmd_run)

    resume_p = sub.add_parser(
        'resume', parents=[run_resume_common],
        help='load the accepted mission definition from the database and recover/advance it '
             '-- no new goals')
    resume_p.set_defaults(func=cmd_resume)

    status_p = sub.add_parser(
        'status', parents=[common],
        help='print durable mission/step/dispatch/attempt state; submits nothing')
    status_p.set_defaults(func=cmd_status)

    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        report = args.func(args)
    except ExecutionOwnerAlreadyHeld as error:
        print(f'ERROR {error}', file=sys.stderr)
        return 2
    except MissionNotFound:
        print(
            f'ERROR no preview mission exists yet in {args.db!r} -- run "run" first',
            file=sys.stderr)
        return 2
    except (sup.IncompatibleDatabase, sup.MissingPreviewDatabase,
            sup.UnsupportedPreviewSchema, sup.MissionDefinitionError) as error:
        print(f'ERROR {error}', file=sys.stderr)
        return 2
    _print_report(report)
    if args.command == 'status':
        # status's own exit code reflects whether INSPECTION succeeded, never the inspected
        # mission's current state -- a FAILED or UNRESOLVED mission is still a successful,
        # complete status read.
        return 0
    return _EXIT_BY_OUTCOME.get(report.outcome, 1)


if __name__ == '__main__':
    sys.exit(main())
