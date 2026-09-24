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

"""Round-trip check for the experiment UUID helper. Needs ROS sourced."""

import pathlib
import sys

import pytest

pytest.importorskip('unique_identifier_msgs')
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / 'scripts'))

import ros_uuid  # noqa: E402


@pytest.mark.parametrize('text', ros_uuid.SELF_TEST_VECTORS)
def test_uuid_survives_the_ros_message_unchanged(text):
    original = ros_uuid.parse_uuid(text)

    msg = ros_uuid.uuid_to_msg(original)

    assert list(msg.uuid) == list(original.bytes)
    assert ros_uuid.msg_to_uuid(msg) == original
    assert str(ros_uuid.msg_to_uuid(msg)) == text


def test_ros_bytes_follow_the_text_order_with_no_reversal():
    msg = ros_uuid.uuid_to_msg(ros_uuid.parse_uuid('00010203-0405-0607-0809-0a0b0c0d0e0f'))

    assert list(msg.uuid) == list(range(16))


@pytest.mark.parametrize('text', [
    '{00010203-0405-0607-0809-0a0b0c0d0e0f}',
    'urn:uuid:00010203-0405-0607-0809-0a0b0c0d0e0f',
    '000102030405060708090a0b0c0d0e0f',
    '00010203-0405-0607-0809-0a0b0c0d0e0',
    '',
])
def test_only_canonical_text_is_accepted(text):
    with pytest.raises(ValueError):
        ros_uuid.parse_uuid(text)


def test_the_self_test_passes():
    assert all(ok for _, ok, _ in ros_uuid.self_test())
