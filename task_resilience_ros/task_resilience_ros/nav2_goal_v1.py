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
The nav2/navigate_to_pose/v1 goal format, and the UUID <-> ROS UUID bridge.

goal_format identifies which adapter format/version a stored DispatchGoalRecord's payload
is meant for (see task_resilience.model.ExpectedGoalSpecification); it is opaque to the
core and interpreted only here. This module owns the ONE payload schema Task 015
implements, converts it to the generated nav2_msgs.action.NavigateToPose.Goal message
(never the other way -- the stored payload is the source of truth, never regenerated from
current configuration), and owns the generated-message/uuid_to_msg boundary so nav2_adapter
itself does not need to import unique_identifier_msgs or nav2_msgs field layouts directly
for this part.

The payload is validated exactly, not repaired: extra or missing keys, wrong types,
non-finite numbers, or a non-unit quaternion are all rejected before anything is built or
sent. header.stamp is always left at zero -- Nav2 typically runs on sim time, unrelated to
this process's wall clock, and a zero stamp is this format's explicit v1 convention, not an
oversight (matching task_resilience_demos/scripts/send_nav2_goal_with_uuid.py's existing
working convention).
"""

import math
import uuid

from .errors import InvalidGoalPayload, UnsupportedGoalFormat

GOAL_FORMAT = 'nav2/navigate_to_pose/v1'

_REQUIRED_TOP_KEYS = frozenset({'frame_id', 'position', 'orientation', 'behavior_tree'})
_REQUIRED_POSITION_KEYS = frozenset({'x', 'y', 'z'})
_REQUIRED_ORIENTATION_KEYS = frozenset({'x', 'y', 'z', 'w'})

# A unit quaternion's squared norm is exactly 1; this is the tolerance allowed for a value
# that has round-tripped through JSON text as a float.
_QUATERNION_NORM_TOLERANCE = 1e-6


def _require_dict(value, path):
    if not isinstance(value, dict):
        raise InvalidGoalPayload(
            GOAL_FORMAT, f'{path} must be an object, got {type(value).__name__}')
    return value


def _require_str(value, path):
    if not isinstance(value, str):
        raise InvalidGoalPayload(
            GOAL_FORMAT, f'{path} must be a string, got {type(value).__name__}')
    return value


def _require_finite_number(value, path):
    # bool is an int subclass, but True/False are not coordinates.
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise InvalidGoalPayload(
            GOAL_FORMAT, f'{path} must be a number, got {type(value).__name__}')
    number = float(value)
    if not math.isfinite(number):
        raise InvalidGoalPayload(GOAL_FORMAT, f'{path} is {number!r}, which is not finite')
    return number


def _require_exact_keys(mapping, expected, path):
    actual = set(mapping.keys())
    if actual != expected:
        missing = expected - actual
        extra = actual - expected
        detail = []
        if missing:
            detail.append(f'missing {sorted(missing)}')
        if extra:
            detail.append(f'unexpected {sorted(extra)}')
        raise InvalidGoalPayload(GOAL_FORMAT, f'{path} keys are wrong: {", ".join(detail)}')


def validate_payload(payload: object) -> dict:
    """
    Validate a nav2/navigate_to_pose/v1 payload exactly, returning it unchanged.

    Raises InvalidGoalPayload for anything not satisfying the exact schema: extra or
    missing fields, wrong types, non-finite numbers, or a position/orientation whose
    quaternion is not (within a small floating-point tolerance) a unit quaternion. Never
    repairs, normalizes, or fills in a default for anything the payload itself omits or
    gets wrong -- the one explicit exception is that an empty-string behavior_tree is a
    valid, meaningful value (Nav2's own default behavior tree), not something rejected.
    """
    top = _require_dict(payload, 'payload')
    _require_exact_keys(top, _REQUIRED_TOP_KEYS, 'payload')

    frame_id = _require_str(top['frame_id'], 'payload.frame_id')
    if not frame_id:
        raise InvalidGoalPayload(GOAL_FORMAT, 'payload.frame_id must not be empty')

    position = _require_dict(top['position'], 'payload.position')
    _require_exact_keys(position, _REQUIRED_POSITION_KEYS, 'payload.position')
    px = _require_finite_number(position['x'], 'payload.position.x')
    py = _require_finite_number(position['y'], 'payload.position.y')
    pz = _require_finite_number(position['z'], 'payload.position.z')

    orientation = _require_dict(top['orientation'], 'payload.orientation')
    _require_exact_keys(orientation, _REQUIRED_ORIENTATION_KEYS, 'payload.orientation')
    ox = _require_finite_number(orientation['x'], 'payload.orientation.x')
    oy = _require_finite_number(orientation['y'], 'payload.orientation.y')
    oz = _require_finite_number(orientation['z'], 'payload.orientation.z')
    ow = _require_finite_number(orientation['w'], 'payload.orientation.w')
    norm_sq = ox * ox + oy * oy + oz * oz + ow * ow
    if abs(norm_sq - 1.0) > _QUATERNION_NORM_TOLERANCE:
        raise InvalidGoalPayload(
            GOAL_FORMAT,
            f'payload.orientation is not a unit quaternion (squared norm {norm_sq!r}, '
            f'expected 1.0 within {_QUATERNION_NORM_TOLERANCE})')

    behavior_tree = _require_str(top['behavior_tree'], 'payload.behavior_tree')

    return {
        'frame_id': frame_id,
        'position': (px, py, pz),
        'orientation': (ox, oy, oz, ow),
        'behavior_tree': behavior_tree,
    }


def build_goal_message(goal_format: str, payload: object):
    """
    Build a nav2_msgs.action.NavigateToPose.Goal from a stored dispatch goal.

    goal_format must be exactly GOAL_FORMAT; anything else raises UnsupportedGoalFormat
    before the payload is even inspected. Imports nav2_msgs lazily so importing this module
    (and, transitively, nav2_adapter.py) does not itself require nav2_msgs to be installed
    for code paths that only need goal validation.
    """
    if goal_format != GOAL_FORMAT:
        raise UnsupportedGoalFormat(goal_format, (GOAL_FORMAT,))
    validated = validate_payload(payload)

    from nav2_msgs.action import NavigateToPose

    goal = NavigateToPose.Goal()
    goal.pose.header.frame_id = validated['frame_id']
    # Explicit v1 convention: the stamp is always zero, never the current time (Nav2
    # commonly runs on sim time, unrelated to this process's wall clock). This is already
    # a fresh message's default; set explicitly so the convention is not merely accidental.
    goal.pose.header.stamp.sec = 0
    goal.pose.header.stamp.nanosec = 0
    px, py, pz = validated['position']
    goal.pose.pose.position.x = px
    goal.pose.pose.position.y = py
    goal.pose.pose.position.z = pz
    ox, oy, oz, ow = validated['orientation']
    goal.pose.pose.orientation.x = ox
    goal.pose.pose.orientation.y = oy
    goal.pose.pose.orientation.z = oz
    goal.pose.pose.orientation.w = ow
    goal.behavior_tree = validated['behavior_tree']
    return goal


def uuid_to_ros_uuid(value: uuid.UUID):
    """
    Return the unique_identifier_msgs/msg/UUID for a uuid.UUID, byte for byte.

    Mirrors task_resilience_demos/scripts/ros_uuid.py's own uuid_to_msg exactly (the
    message holds the same 16 bytes, in the same order, as uuid.UUID.bytes); duplicated
    here rather than imported, since that script lives under demos/scripts, not an
    importable package.
    """
    from unique_identifier_msgs.msg import UUID

    return UUID(uuid=list(value.bytes))


def ros_uuid_to_uuid(msg) -> uuid.UUID:
    """Return the uuid.UUID for a unique_identifier_msgs/msg/UUID, byte for byte."""
    return uuid.UUID(bytes=bytes(msg.uuid))
