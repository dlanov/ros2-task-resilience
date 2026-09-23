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

from collections.abc import Iterator
import contextlib
from datetime import datetime, timezone
import json
import os
import sqlite3
from typing import Any
import uuid

from .errors import (
    AttemptLimitReached,
    IdempotencyConflict,
    MissionNotFound,
    UnsupportedSchemaVersion,
)
from .model import MissionRecord, MissionRequest

SCHEMA_VERSION = 1

# request_json is the canonical payload only; max_attempts has its own column.
_SCHEMA = """
CREATE TABLE missions (
    mission_id          TEXT PRIMARY KEY,
    request_id          TEXT NOT NULL UNIQUE,
    request_fingerprint TEXT NOT NULL,
    request_json        TEXT NOT NULL,
    max_attempts        INTEGER NOT NULL CHECK (max_attempts > 0),
    attempts_used       INTEGER NOT NULL DEFAULT 0 CHECK (attempts_used >= 0),
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL,
    CHECK (attempts_used <= max_attempts)
)
"""

_BY_MISSION_ID = 'SELECT * FROM missions WHERE mission_id = ?'
_BY_REQUEST_ID = 'SELECT * FROM missions WHERE request_id = ?'

_INSERT = """
INSERT INTO missions (
    mission_id, request_id, request_fingerprint, request_json,
    max_attempts, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?)
"""

# The limit is checked in the WHERE clause, so a mission that is already at
# its limit matches no row and its counter and timestamp are left alone.
_CONSUME = """
UPDATE missions
SET attempts_used = attempts_used + 1, updated_at = ?
WHERE mission_id = ? AND attempts_used < max_attempts
"""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec='microseconds')


def _to_record(row: sqlite3.Row) -> MissionRecord:
    return MissionRecord(
        mission_id=row['mission_id'],
        request_id=row['request_id'],
        request_fingerprint=row['request_fingerprint'],
        payload=json.loads(row['request_json']),
        max_attempts=row['max_attempts'],
        attempts_used=row['attempts_used'],
        created_at=datetime.fromisoformat(row['created_at']),
        updated_at=datetime.fromisoformat(row['updated_at']),
    )


class MissionStore:
    """
    Durable mission store backed by a SQLite file.

    An instance owns one connection and must stay on the thread that created it.
    Concurrent users, in threads or processes, each open their own instance on
    the same file. In-memory databases are not supported: they cannot be shared
    between instances and would not survive a restart.
    """

    def __init__(self, path: str | os.PathLike[str], busy_timeout: float = 5.0) -> None:
        # isolation_level=None: transactions are ours (see _transaction), not the
        # sqlite3 module's implicit ones.
        self._conn = sqlite3.connect(
            os.fspath(path), timeout=busy_timeout, isolation_level=None)
        try:
            self._conn.row_factory = sqlite3.Row
            # Favor SQLite's strongest normal durability setting for committed
            # attempts.
            self._conn.execute('PRAGMA synchronous = FULL')
            self._init_schema()
        except BaseException:
            self._conn.close()
            raise

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> 'MissionStore':
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def create_or_get(self, request_id: str, payload: Any, max_attempts: int) -> MissionRecord:
        request = MissionRequest(request_id, payload, max_attempts)
        with self._transaction():
            existing = self._fetch(_BY_REQUEST_ID, request_id)
            if existing is not None:
                if existing.request_fingerprint != request.fingerprint:
                    raise IdempotencyConflict(request_id)
                return existing

            mission_id = str(uuid.uuid4())
            now = _utc_now()
            self._conn.execute(
                _INSERT,
                (mission_id, request_id, request.fingerprint, request.payload_json,
                 max_attempts, now, now))
            return self._require(_BY_MISSION_ID, mission_id)

    def get_by_mission_id(self, mission_id: str) -> MissionRecord:
        return self._require(_BY_MISSION_ID, mission_id)

    def get_by_request_id(self, request_id: str) -> MissionRecord:
        return self._require(_BY_REQUEST_ID, request_id)

    def consume_attempt(self, mission_id: str) -> MissionRecord:
        with self._transaction():
            updated = self._conn.execute(_CONSUME, (_utc_now(), mission_id)).rowcount
            record = self._require(_BY_MISSION_ID, mission_id)
            if not updated:
                raise AttemptLimitReached(mission_id, record.max_attempts)
            return record

    @contextlib.contextmanager
    def _transaction(self) -> Iterator[None]:
        # IMMEDIATE takes the write lock up front. A deferred transaction that
        # reads first and upgrades later can fail with SQLITE_BUSY straight
        # away, and would let two writers act on the same stale read.
        self._conn.execute('BEGIN IMMEDIATE')
        try:
            yield
            self._conn.execute('COMMIT')
        except BaseException:
            # COMMIT can itself fail on a lock timeout and leave the
            # transaction open.
            if self._conn.in_transaction:
                self._conn.execute('ROLLBACK')
            raise

    def _init_schema(self) -> None:
        # Inside one write transaction so concurrent first opens cannot both
        # create the table.
        with self._transaction():
            version = self._conn.execute('PRAGMA user_version').fetchone()[0]
            if version == 0:
                self._conn.execute(_SCHEMA)
                self._conn.execute(f'PRAGMA user_version = {SCHEMA_VERSION}')
            elif version != SCHEMA_VERSION:
                raise UnsupportedSchemaVersion(version)

    def _fetch(self, query: str, value: str) -> MissionRecord | None:
        row = self._conn.execute(query, (value,)).fetchone()
        return None if row is None else _to_record(row)

    def _require(self, query: str, value: str) -> MissionRecord:
        record = self._fetch(query, value)
        if record is None:
            raise MissionNotFound(value)
        return record
