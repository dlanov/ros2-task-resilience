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

import sqlite3

from task_resilience import storage
from task_resilience.storage import MissionStore


class FaultyConnection:
    """Wrap a connection, log its statements, and fail the ones chosen."""

    def __init__(self, real):
        object.__setattr__(self, '_real', real)
        object.__setattr__(self, 'statements', [])
        object.__setattr__(self, 'fail_when', None)

    def execute(self, sql, *args):
        self.statements.append(' '.join(sql.split()))
        if self.fail_when is not None and self.fail_when(sql):
            raise sqlite3.OperationalError('injected failure')
        return self._real.execute(sql, *args)

    def __getattr__(self, name):
        return getattr(self._real, name)

    def __setattr__(self, name, value):
        if name == 'fail_when':
            object.__setattr__(self, name, value)
        else:
            setattr(self._real, name, value)


def open_faulty_store(monkeypatch, db_path, fail_when=None):
    """Open a MissionStore whose connection is a FaultyConnection, and return both."""
    real_connect = sqlite3.connect
    opened = []

    def connect(*args, **kwargs):
        opened.append(FaultyConnection(real_connect(*args, **kwargs)))
        opened[-1].fail_when = fail_when
        return opened[-1]

    with monkeypatch.context() as patch:
        patch.setattr(storage.sqlite3, 'connect', connect)
        return MissionStore(db_path), opened[0]
