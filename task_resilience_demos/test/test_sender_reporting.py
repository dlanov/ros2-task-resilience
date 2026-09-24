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
Offline checks of what the goal sender says it knows. Needs ROS sourced for the message types.

The sender's run() is driven with fake clients and futures. Nothing here initializes rclpy,
creates a node, talks to a server or waits: the waiting call and the process-exit call are
replaced, so a test cannot block or end the pytest process.
"""

import pathlib
import sys
import types

import pytest

for _module in ('rclpy', 'nav2_msgs', 'action_msgs', 'unique_identifier_msgs'):
    pytest.importorskip(_module)
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / 'scripts'))

import ros_uuid  # noqa: E402
import send_nav2_goal_with_uuid as sender  # noqa: E402

UUID_TEXT = '12345678-9abc-4def-8012-3456789abcde'


class Died(Exception):
    """Raised in place of os._exit, so a test can see an intentional exit."""

    def __init__(self, code):
        super().__init__(code)
        self.code = code


class FakeFuture:
    def __init__(self, done, result=None):
        self._done = done
        self._result = result
        self.result_calls = 0

    def done(self):
        return self._done

    def result(self):
        self.result_calls += 1
        return self._result


class FakeHandle:
    """Stands in for a ClientGoalHandle after an accept response."""

    def __init__(self, goal_id, result_future):
        self.accepted = True
        self.goal_id = goal_id
        self.stamp = types.SimpleNamespace(sec=7, nanosec=8)
        self.cancel_calls = 0
        self._result_future = result_future

    def get_result_async(self):
        return self._result_future

    def cancel_goal_async(self):
        self.cancel_calls += 1


class FakeClient:
    def __init__(self, send_future):
        self.submissions = []
        self._send_future = send_future

    def wait_for_server(self, timeout_sec=None):
        return True

    def send_goal_async(self, goal, feedback_callback=None, goal_uuid=None):
        self.submissions.append(goal_uuid)
        return self._send_future


@pytest.fixture
def env(monkeypatch):
    """Collect what the sender prints, and replace its waiting and exiting calls."""
    env = types.SimpleNamespace(lines=[], spins=0)

    def fake_spin(node, future, timeout_sec=None):
        env.spins += 1

    def fake_exit(code):
        raise Died(code)

    monkeypatch.setattr(sender, 'say', env.lines.append)
    monkeypatch.setattr(sender.rclpy, 'spin_until_future_complete', fake_spin)
    monkeypatch.setattr(sender.os, '_exit', fake_exit)
    return env


def test_unfinished_result_is_reported_as_unknown_state_not_as_running(env):
    requested = ros_uuid.parse_uuid(UUID_TEXT)
    result_future = FakeFuture(done=False)
    handle = FakeHandle(ros_uuid.uuid_to_msg(requested), result_future)
    client = FakeClient(FakeFuture(done=True, result=handle))
    args = sender.parse_args(['--uuid', UUID_TEXT])

    code = sender.run(object(), client, args)

    assert code == 2
    assert len(client.submissions) == 1
    assert ros_uuid.msg_to_uuid(client.submissions[0]) == requested
    assert handle.cancel_calls == 0
    assert result_future.result_calls == 0
    assert len([line for line in env.lines if 'goal ACCEPTED' in line]) == 1
    error = env.lines[-1]
    assert error.startswith('ERROR: timed out waiting for a terminal result')
    assert 'current goal state unknown' in error
    assert 'no cancel was sent' in error
    assert 'running' not in error


def test_status_observation_is_not_reported_as_acceptance_or_execution(env, monkeypatch):
    requested = ros_uuid.parse_uuid(UUID_TEXT)
    send_future = FakeFuture(done=False)
    client = FakeClient(send_future)
    watched = []

    def fake_watch(uuid_value, timeout):
        watched.append((uuid_value, timeout))
        return 'EXECUTING', 0.25

    monkeypatch.setattr(sender, 'watch_status_for', fake_watch)
    args = sender.parse_args(['--uuid', UUID_TEXT, '--exit-before-response'])

    with pytest.raises(Died) as exited:
        sender.run(object(), client, args)

    assert exited.value.code == 0
    assert len(client.submissions) == 1
    assert watched == [(requested, args.evidence_timeout)]
    assert env.spins == 0
    assert send_future.result_calls == 0
    observed = [line for line in env.lines if 'status topic listed' in line]
    assert len(observed) == 1
    assert 'listed the uuid as EXECUTING' in observed[0]
    assert 'exiting without consuming the send_goal response' in observed[0]
    output = '\n'.join(env.lines)
    for claim in ('received the goal', 'goal ACCEPTED', 'accepted', 'is executing', 'running'):
        assert claim not in output
