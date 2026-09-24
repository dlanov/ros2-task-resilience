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
Convert between uuid.UUID and unique_identifier_msgs/msg/UUID, byte for byte.

The ROS message holds 16 bytes in the same order as uuid.UUID.bytes, so the first
byte is the first two hex digits of the text form. rclpy builds its own random goal
IDs the same way (UUID(uuid=list(uuid.uuid4().bytes))). Nothing here reverses bytes
or reinterprets fields.

Experiment tooling for the ROS action recovery spike. Not part of task_resilience.

    python3 ros_uuid.py --self-test
"""

import argparse
import sys
import uuid


def parse_uuid(text):
    """Parse canonical UUID text (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)."""
    parsed = uuid.UUID(text)
    # uuid.UUID also accepts braces, urn: prefixes and bare hex. Only the canonical
    # form is accepted here so the same text is used everywhere.
    if str(parsed) != text.lower():
        raise ValueError(f'not canonical UUID text: {text!r} (expected {parsed})')
    return parsed


def uuid_arg(text):
    """Parse a canonical UUID for argparse, raising ArgumentTypeError on bad text."""
    try:
        return parse_uuid(text)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def uuid_to_msg(value):
    """Return the unique_identifier_msgs/msg/UUID for a uuid.UUID."""
    from unique_identifier_msgs.msg import UUID
    return UUID(uuid=list(value.bytes))


def msg_to_uuid(msg):
    """Return the uuid.UUID for a unique_identifier_msgs/msg/UUID."""
    return uuid.UUID(bytes=bytes(msg.uuid))


def hex_bytes(value):
    """Return the 16 bytes as hex, for showing the ROS-side representation."""
    return value.bytes.hex()


# Fixed vectors, so the check is deterministic. The sequential one makes any byte
# reversal or field swap visible.
SELF_TEST_VECTORS = [
    '00000000-0000-0000-0000-000000000000',
    'ffffffff-ffff-ffff-ffff-ffffffffffff',
    '00010203-0405-0607-0809-0a0b0c0d0e0f',
    '12345678-9abc-4def-8012-3456789abcde',
    'f47ac10b-58cc-4372-a567-0e02b2c3d479',
]


def self_test():
    """
    Check uuid.UUID -> ROS UUID -> uuid.UUID on the fixed vectors.

    Returns a list of (vector, ok, detail) tuples.
    """
    results = []
    for text in SELF_TEST_VECTORS:
        original = parse_uuid(text)
        msg = uuid_to_msg(original)
        problems = []
        if list(msg.uuid) != list(original.bytes):
            problems.append(f'bytes differ: {list(msg.uuid)} != {list(original.bytes)}')
        if int(msg.uuid[0]) != int(text[:2], 16):
            problems.append('first ROS byte is not the first hex pair of the text')
        if int(msg.uuid[15]) != int(text[-2:], 16):
            problems.append('last ROS byte is not the last hex pair of the text')
        back = msg_to_uuid(msg)
        if back != original or str(back) != text:
            problems.append(f'round trip gave {back}')
        results.append((text, not problems, '; '.join(problems) or 'exact'))
    return results


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.strip().split('\n')[0])
    parser.add_argument('--self-test', action='store_true', help='run the round-trip check')
    parser.add_argument('uuid', nargs='?', type=uuid_arg,
                        help='print the canonical text and ROS bytes of this UUID')
    args = parser.parse_args(argv)

    if args.uuid is not None:
        print(f'uuid: {args.uuid}\nROS uuid[16]: {list(args.uuid.bytes)}')
    if args.self_test or args.uuid is None:
        results = self_test()
        for text, ok, detail in results:
            print(f'{"ok  " if ok else "FAIL"} {text}  {detail}')
        return 0 if all(ok for _, ok, _ in results) else 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
