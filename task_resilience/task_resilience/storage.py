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

from collections.abc import Iterable, Iterator
import contextlib
from datetime import datetime, timedelta, timezone
import enum
import json
import os
import sqlite3
from typing import Any, TypeVar
import uuid

from .errors import (
    AttemptLimitReached,
    CorruptDispatchAuthorization,
    CorruptDispatchContext,
    CorruptDispatchRecord,
    CorruptStoredState,
    DispatchAuthorizationMissing,
    DispatchContextConflict,
    DispatchContextMissing,
    DispatchNotFound,
    DispatchPreparationRequired,
    IdempotencyConflict,
    MissionNotFound,
    MissionNotRunning,
    MissionReconciliationRequired,
    SendAuthorizationRequired,
    StepDefinitionConflict,
    StepNotAwaitingSend,
    StepNotFound,
    UnsupportedSchemaVersion,
)
from .model import (
    DispatchAuthorizationRecord,
    DispatchContextRecord,
    DispatchRecord,
    MissionRecord,
    MissionRequest,
    SendAuthorization,
    StepRecord,
    validate_step_keys,
)
from .reconciliation import ExpectedActionContext
from .state_machine import MissionState, StepState, transition_mission, transition_step

SCHEMA_VERSION = 5

_State = TypeVar('_State', bound=enum.Enum)

# States in which a step's external request may exist, so each needs a dispatch
# identity. The finished and unknown states are not listed: a version 2
# database legitimately holds those without one.
_IN_FLIGHT = frozenset({
    StepState.DISPATCH_PREPARED,
    StepState.DISPATCHED,
    StepState.ACCEPTED,
    StepState.EXECUTING,
    StepState.CANCEL_REQUESTED,
})

# The state lists are spelled out rather than built from the enums: this is
# what the schema is. A new state needs a new schema version, and a test
# compares these lists with the enums.
#
# request_json is the canonical payload only; max_attempts has its own column.
_MISSIONS_DDL = """
CREATE TABLE missions (
    mission_id          TEXT PRIMARY KEY,
    request_id          TEXT NOT NULL UNIQUE,
    request_fingerprint TEXT NOT NULL,
    request_json        TEXT NOT NULL,
    max_attempts        INTEGER NOT NULL CHECK (max_attempts > 0),
    attempts_used       INTEGER NOT NULL DEFAULT 0 CHECK (attempts_used >= 0),
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL,
    mission_state       TEXT NOT NULL CHECK (mission_state IN (
        'ACCEPTED', 'RUNNING', 'CANCELLING', 'COMPLETED', 'FAILED', 'CANCELLED',
        'NEEDS_RECONCILIATION'
    )),
    CHECK (attempts_used <= max_attempts)
)
"""

_MISSION_STEPS_DDL = """
CREATE TABLE mission_steps (
    mission_id  TEXT NOT NULL REFERENCES missions (mission_id) ON DELETE CASCADE,
    step_key    TEXT NOT NULL CHECK (step_key <> ''),
    step_index  INTEGER NOT NULL CHECK (step_index >= 0),
    step_state  TEXT NOT NULL CHECK (step_state IN (
        'PENDING', 'DISPATCH_PREPARED', 'DISPATCHED', 'ACCEPTED', 'EXECUTING',
        'SUCCEEDED', 'FAILED', 'CANCEL_REQUESTED', 'CANCELLED', 'OUTCOME_UNKNOWN'
    )),
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL,
    PRIMARY KEY (mission_id, step_key),
    UNIQUE (mission_id, step_index)
)
"""

# The dispatch identity is kept apart from mission_steps so that version 2
# steps keep their schema and migrated steps are not given made-up IDs. The
# step's own state stays the lifecycle; this table only reserves the ID.
_STEP_DISPATCHES_DDL = """
CREATE TABLE step_dispatches (
    dispatch_id TEXT NOT NULL PRIMARY KEY,
    mission_id  TEXT NOT NULL,
    step_key    TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    FOREIGN KEY (mission_id, step_key)
        REFERENCES mission_steps (mission_id, step_key)
        ON DELETE CASCADE,
    UNIQUE (mission_id, step_key)
)
"""

# The action a dispatch was declared to reach, kept apart from step_dispatches for the same
# reason that table is kept apart from mission_steps: dispatches that already exist keep their
# schema and are not given made-up contexts. No row means no context is bound. The primary key
# allows one row per dispatch. It does not make a row immutable: only the first preparation of
# a step inserts a row, and no code updates or deletes one. Nothing else stops a writer that
# bypasses the code.
_DISPATCH_CONTEXTS_DDL = """
CREATE TABLE dispatch_contexts (
    dispatch_id     TEXT NOT NULL PRIMARY KEY
                    REFERENCES step_dispatches (dispatch_id) ON DELETE CASCADE,
    action_name     TEXT NOT NULL CHECK (action_name <> ''),
    action_type     TEXT NOT NULL CHECK (action_type <> ''),
    robot_namespace TEXT NOT NULL
)
"""

# One row per dispatch for which authorize_step_send committed a send authorization. Under the
# supported-store contract the row is written in that transaction and by nothing else, so a valid
# row is durable provenance that the authorization committed, not that a request was sent. The
# absence of a row is a lack of proof, not proof that no authorization happened: a dispatch
# authorized before this table existed has none. The primary key allows one row per dispatch. It
# does not make a row immutable: only authorize_step_send inserts one, and no code updates or
# deletes one. The table is not tamper-evident: nothing stops a writer that bypasses the code
# from forging or altering even a canonically formatted row, and such writes are outside the
# contract. authorized_at is the store's own wall-clock time for that transaction. The attempt
# number is deliberately not kept.
_DISPATCH_AUTHORIZATIONS_DDL = """
CREATE TABLE dispatch_authorizations (
    dispatch_id   TEXT NOT NULL PRIMARY KEY
                  REFERENCES step_dispatches (dispatch_id) ON DELETE CASCADE,
    authorized_at TEXT NOT NULL
)
"""

_RENAME_V1_MISSIONS = 'ALTER TABLE missions RENAME TO missions_v1'

_COPY_V1_MISSIONS = """
INSERT INTO missions (
    mission_id, request_id, request_fingerprint, request_json,
    max_attempts, attempts_used, created_at, updated_at, mission_state
)
SELECT
    mission_id, request_id, request_fingerprint, request_json,
    max_attempts, attempts_used, created_at, updated_at, ?
FROM missions_v1
ORDER BY rowid
"""

_DROP_V1_MISSIONS = 'DROP TABLE missions_v1'

_BY_MISSION_ID = 'SELECT * FROM missions WHERE mission_id = ?'
_BY_REQUEST_ID = 'SELECT * FROM missions WHERE request_id = ?'
_MISSION_EXISTS = 'SELECT 1 FROM missions WHERE mission_id = ?'

_INSERT = """
INSERT INTO missions (
    mission_id, request_id, request_fingerprint, request_json,
    max_attempts, mission_state, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""

# The limit is checked in the WHERE clause, so a mission that is already at
# its limit matches no row and its counter and timestamp are left alone. Only
# authorize_step_send runs this: an attempt is one committed send authorization.
_CONSUME = """
UPDATE missions
SET attempts_used = attempts_used + 1, updated_at = ?
WHERE mission_id = ? AND attempts_used < max_attempts
"""

_SET_MISSION_STATE = 'UPDATE missions SET mission_state = ?, updated_at = ? WHERE mission_id = ?'

_STEPS_OF_MISSION = 'SELECT * FROM mission_steps WHERE mission_id = ? ORDER BY step_index'
_STEP_BY_KEY = 'SELECT * FROM mission_steps WHERE mission_id = ? AND step_key = ?'

_INSERT_STEP = """
INSERT INTO mission_steps (
    mission_id, step_key, step_index, step_state, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?)
"""

_SET_STEP_STATE = """
UPDATE mission_steps SET step_state = ?, updated_at = ?
WHERE mission_id = ? AND step_key = ?
"""

_DISPATCHES_OF_STEP = 'SELECT * FROM step_dispatches WHERE mission_id = ? AND step_key = ?'
_DISPATCH_BY_ID = 'SELECT * FROM step_dispatches WHERE dispatch_id = ?'

_INSERT_DISPATCH = """
INSERT INTO step_dispatches (dispatch_id, mission_id, step_key, created_at)
VALUES (?, ?, ?, ?)
"""

_CONTEXT_OF_DISPATCH = 'SELECT * FROM dispatch_contexts WHERE dispatch_id = ?'

# The only statement that writes dispatch_contexts, and only the first preparation of a step
# runs it. There is no later binding, backfill or repair.
_INSERT_CONTEXT = """
INSERT INTO dispatch_contexts (dispatch_id, action_name, action_type, robot_namespace)
VALUES (?, ?, ?, ?)
"""

_CONTEXT_FIELDS = ('action_name', 'action_type', 'robot_namespace')

_AUTHORIZATION_OF_DISPATCH = 'SELECT * FROM dispatch_authorizations WHERE dispatch_id = ?'

# The only statement that writes dispatch_authorizations, and only authorize_step_send runs it.
_INSERT_AUTHORIZATION = """
INSERT INTO dispatch_authorizations (dispatch_id, authorized_at)
VALUES (?, ?)
"""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec='microseconds')


def _require_kind(value: object, kind: type[enum.Enum], name: str) -> None:
    if not isinstance(value, kind):
        raise TypeError(f'{name} must be a {kind.__name__}, got {value!r}')


def _parse_state(kind: type[_State], value: object, entity: str, mission_id: str) -> _State:
    # The CHECK constraints should make this unreachable. If it is reached,
    # fail loudly rather than guess which state was meant.
    try:
        return kind(value)
    except ValueError:
        raise CorruptStoredState(entity, mission_id, value) from None


def _to_record(row: sqlite3.Row) -> MissionRecord:
    return MissionRecord(
        mission_id=row['mission_id'],
        request_id=row['request_id'],
        request_fingerprint=row['request_fingerprint'],
        payload=json.loads(row['request_json']),
        max_attempts=row['max_attempts'],
        attempts_used=row['attempts_used'],
        state=_parse_state(MissionState, row['mission_state'], 'mission', row['mission_id']),
        created_at=datetime.fromisoformat(row['created_at']),
        updated_at=datetime.fromisoformat(row['updated_at']),
    )


def _to_step(row: sqlite3.Row) -> StepRecord:
    return StepRecord(
        mission_id=row['mission_id'],
        step_key=row['step_key'],
        step_index=row['step_index'],
        state=_parse_state(StepState, row['step_state'], 'step', row['mission_id']),
        created_at=datetime.fromisoformat(row['created_at']),
        updated_at=datetime.fromisoformat(row['updated_at']),
    )


def _to_dispatch(row: sqlite3.Row) -> DispatchRecord:
    stored = row['dispatch_id']
    # Only the canonical text this code writes is accepted. Anything else means
    # the row was changed behind our back, and a lookup by ID would not find it.
    try:
        dispatch_id = uuid.UUID(stored)
    except (ValueError, TypeError, AttributeError):
        dispatch_id = None
    if dispatch_id is None or str(dispatch_id) != stored:
        raise CorruptDispatchRecord(
            row['mission_id'], row['step_key'], 'stored dispatch ID is not a canonical UUID',
            stored)
    return DispatchRecord(
        dispatch_id=dispatch_id,
        mission_id=row['mission_id'],
        step_key=row['step_key'],
        created_at=datetime.fromisoformat(row['created_at']),
    )


def _to_context(row: sqlite3.Row, dispatch: DispatchRecord) -> DispatchContextRecord:
    # The schema's checks should make this unreachable. If a row was changed behind our
    # back, fail loudly rather than treat it as the action the dispatch was declared for.
    for column in _CONTEXT_FIELDS:
        if not isinstance(row[column], str):
            raise CorruptDispatchContext(
                dispatch.mission_id, dispatch.step_key, f'{column} is not text', row[column])
    if not row['action_name'] or not row['action_type']:
        raise CorruptDispatchContext(
            dispatch.mission_id, dispatch.step_key, 'action_name and action_type are empty')
    return DispatchContextRecord(
        dispatch_id=dispatch.dispatch_id,
        mission_id=dispatch.mission_id,
        step_key=dispatch.step_key,
        action_name=row['action_name'],
        action_type=row['action_type'],
        robot_namespace=row['robot_namespace'],
    )


def _to_authorization(
    row: sqlite3.Row, dispatch: DispatchRecord,
) -> DispatchAuthorizationRecord:
    # Only the exact text _utc_now writes is accepted, like the canonical UUID in _to_dispatch:
    # anything else means the row was changed behind our back. The value is never compared with
    # another timestamp, because a wall clock can step backwards.
    stored = row['authorized_at']
    parsed = None
    if isinstance(stored, str):
        try:
            parsed = datetime.fromisoformat(stored)
        except ValueError:
            pass
    if (parsed is None or parsed.utcoffset() != timedelta(0)
            or parsed.isoformat(timespec='microseconds') != stored):
        raise CorruptDispatchAuthorization(
            dispatch.mission_id, dispatch.step_key,
            'authorized_at is not a canonical UTC timestamp', stored)
    return DispatchAuthorizationRecord(
        dispatch_id=dispatch.dispatch_id,
        mission_id=dispatch.mission_id,
        step_key=dispatch.step_key,
        authorized_at=parsed,
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
            # Off by default in SQLite, and set per connection.
            self._conn.execute('PRAGMA foreign_keys = ON')
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
                 max_attempts, MissionState.ACCEPTED.value, now, now))
            return self._require(_BY_MISSION_ID, mission_id)

    def get_by_mission_id(self, mission_id: str) -> MissionRecord:
        return self._require(_BY_MISSION_ID, mission_id)

    def get_by_request_id(self, request_id: str) -> MissionRecord:
        return self._require(_BY_REQUEST_ID, request_id)

    def initialize_steps(
        self, mission_id: str, step_keys: Iterable[str],
    ) -> tuple[StepRecord, ...]:
        keys = validate_step_keys(step_keys)
        with self._transaction():
            self._require_mission(mission_id)
            existing = self._fetch_steps(mission_id)
            if existing:
                # A replay must match exactly, order included. The step list is
                # never silently replaced.
                existing_keys = tuple(step.step_key for step in existing)
                if existing_keys != keys:
                    raise StepDefinitionConflict(mission_id, existing_keys, keys)
                return existing

            now = _utc_now()
            self._conn.executemany(
                _INSERT_STEP,
                [(mission_id, key, index, StepState.PENDING.value, now, now)
                 for index, key in enumerate(keys)])
            return self._fetch_steps(mission_id)

    def get_steps(self, mission_id: str) -> tuple[StepRecord, ...]:
        self._require_mission(mission_id)
        return self._fetch_steps(mission_id)

    def get_step(self, mission_id: str, step_key: str) -> StepRecord:
        return self._require_step(mission_id, step_key)

    def transition_mission_state(self, mission_id: str, target: MissionState) -> MissionRecord:
        _require_kind(target, MissionState, 'target')
        with self._transaction():
            mission = self._require(_BY_MISSION_ID, mission_id)
            steps = self._fetch_steps(mission_id)
            # The state machine decides, including the completion invariant
            # over the persisted steps. Nothing is written unless it agrees.
            new_state = transition_mission(
                mission.state, target, [step.state for step in steps])
            if new_state is mission.state:
                return mission

            self._conn.execute(_SET_MISSION_STATE, (new_state.value, _utc_now(), mission_id))
            return self._require(_BY_MISSION_ID, mission_id)

    def transition_step_state(
        self, mission_id: str, step_key: str, target: StepState,
    ) -> StepRecord:
        _require_kind(target, StepState, 'target')
        # DISPATCH_PREPARED only ever comes with a dispatch ID, written in the
        # same transaction: that is prepare_step_dispatch. Refuse it for every
        # current state, so the mistake shows up whatever the timing.
        if target is StepState.DISPATCH_PREPARED:
            raise DispatchPreparationRequired(mission_id, step_key)
        # DISPATCHED is where an attempt is consumed and the one send is authorized.
        # Refused for every current state, a same-state replay included, and before any
        # lookup, for the same reason.
        if target is StepState.DISPATCHED:
            raise SendAuthorizationRequired(mission_id, step_key)
        with self._transaction():
            step = self._require_step(mission_id, step_key)
            # An in-flight step migrated from version 2 has no dispatch identity,
            # and none is made up for it. Nothing moves it on, whatever the
            # target, until a later reconciliation resolves it.
            if step.state in _IN_FLIGHT and self._fetch_dispatch(mission_id, step_key) is None:
                raise CorruptDispatchRecord(
                    mission_id, step_key, f'step is {step.state.value} but has no dispatch')
            new_state = transition_step(step.state, target)
            if new_state is step.state:
                return step

            self._conn.execute(
                _SET_STEP_STATE, (new_state.value, _utc_now(), mission_id, step_key))
            return self._require_step(mission_id, step_key)

    def prepare_step_dispatch(
        self, mission_id: str, step_key: str, action: ExpectedActionContext,
    ) -> DispatchRecord:
        """
        Reserve the step's dispatch identity, and bind the action it is declared to reach.

        The identity, the action context and DISPATCH_PREPARED commit together, and this
        first preparation is the only thing that ever binds a context. The context is declared
        intent, not proof of where a request is sent, and preparing authorizes no send.
        Replaying with the same context changes nothing; a different one raises
        DispatchContextConflict; a dispatch with no context raises DispatchContextMissing.
        A replay writes nothing, and a context is never replaced or added afterwards.
        """
        # Before any database access, like the other argument checks.
        if not isinstance(action, ExpectedActionContext):
            raise TypeError(f'action must be an ExpectedActionContext, got {action!r}')
        with self._transaction():
            step = self._require_step(mission_id, step_key)
            # The state machine decides. The same state means this is a replay,
            # a legal move is the first preparation, and anything else (a step
            # already past preparation, or finished) is refused here too.
            new_state = transition_step(step.state, StepState.DISPATCH_PREPARED)
            existing = self._fetch_dispatch(mission_id, step_key)

            if new_state is step.state:
                if existing is None:
                    raise CorruptDispatchRecord(
                        mission_id, step_key, f'step is {step.state.value} but has no dispatch')
                self._require_matching_context(existing, action)
                return existing
            if existing is not None:
                raise CorruptDispatchRecord(
                    mission_id, step_key, f'step is {step.state.value} but already has a dispatch',
                    str(existing.dispatch_id))

            # The ID, the context and DISPATCH_PREPARED commit together. That only reserves the
            # identity and declares the action, and both stay whether or not anything is ever
            # sent. It does not authorize an external send: authorize_step_send is the
            # separate operation that does. This is the only place a context is inserted.
            dispatch_id = uuid.uuid4()
            now = _utc_now()
            self._conn.execute(_INSERT_DISPATCH, (str(dispatch_id), mission_id, step_key, now))
            self._conn.execute(_INSERT_CONTEXT, (
                str(dispatch_id), action.action_name, action.action_type,
                action.robot_namespace))
            self._conn.execute(_SET_STEP_STATE, (new_state.value, now, mission_id, step_key))
            return self._require_dispatch(mission_id, step_key)

    def authorize_step_send(self, mission_id: str, step_key: str) -> SendAuthorization:
        """
        Authorize the one send of a prepared step, consuming one attempt for it.

        The step's dispatch must also have a bound action context. The checks and all three
        writes, the attempt, the durable authorization record and DISPATCHED, are a single
        transaction, and the receipt is returned only once it has committed. Nothing external
        happens in here: the caller sends afterwards, and only a caller that got a receipt
        may. A replay is refused and is never answered with the earlier receipt. A committed
        authorization is not undone, so a crash after the commit can mean no send at all, but
        never a second authorization.
        """
        with self._transaction():
            mission = self._require(_BY_MISSION_ID, mission_id)
            if mission.state is not MissionState.RUNNING:
                raise MissionNotRunning(mission_id, mission.state)

            step = self._require_step(mission_id, step_key)
            if step.state is not StepState.DISPATCH_PREPARED:
                raise StepNotAwaitingSend(mission_id, step_key, step.state)

            # A step that is already unknown comes first, even if the mission has not been
            # moved to NEEDS_RECONCILIATION yet.
            unknown = tuple(
                other.step_key for other in self._fetch_steps(mission_id)
                if other.state is StepState.OUTCOME_UNKNOWN)
            if unknown:
                raise MissionReconciliationRequired(mission_id, unknown)

            dispatch = self._fetch_dispatch(mission_id, step_key)
            if dispatch is None:
                raise CorruptDispatchRecord(
                    mission_id, step_key, f'step is {step.state.value} but has no dispatch')

            # The action the dispatch is declared to reach must be bound before any send can be
            # authorized. Like the checks above, this comes before anything is written.
            self._require_context(dispatch)

            # A step that is still DISPATCH_PREPARED cannot already have an authorization
            # record. If it does, the row is not ours and not a receipt to hand back: it is
            # corruption, so it is neither replaced nor deleted and no attempt is consumed.
            self._authorization_of(step, dispatch)

            # The state machine still decides that this edge exists.
            new_state = transition_step(step.state, StepState.DISPATCHED)
            now = _utc_now()
            if not self._conn.execute(_CONSUME, (now, mission_id)).rowcount:
                raise AttemptLimitReached(mission_id, mission.max_attempts)
            # The attempt, this record and DISPATCHED are one atomic fact, all with the one
            # timestamp. The record says an authorization was committed, not that anything
            # was sent. It is the only place a record is inserted, and the step is written
            # last.
            self._conn.execute(_INSERT_AUTHORIZATION, (str(dispatch.dispatch_id), now))
            self._conn.execute(_SET_STEP_STATE, (new_state.value, now, mission_id, step_key))

            # Built here, but the caller gets it only when this block ends: if COMMIT
            # fails, the caller gets that error and no receipt. The time is the persisted
            # value read back, not a second reading of the clock.
            authorization = self._require_authorization(dispatch)
            return SendAuthorization(
                mission_id=mission_id,
                step_key=step_key,
                dispatch_id=dispatch.dispatch_id,
                attempt_number=mission.attempts_used + 1,
                max_attempts=mission.max_attempts,
                authorized_at=authorization.authorized_at,
            )

    def get_step_dispatch(self, mission_id: str, step_key: str) -> DispatchRecord:
        self._require_step(mission_id, step_key)
        return self._require_dispatch(mission_id, step_key)

    def get_dispatch(self, dispatch_id: uuid.UUID) -> DispatchRecord:
        if not isinstance(dispatch_id, uuid.UUID):
            raise TypeError(f'dispatch_id must be a uuid.UUID, got {dispatch_id!r}')
        row = self._conn.execute(_DISPATCH_BY_ID, (str(dispatch_id),)).fetchone()
        if row is None:
            raise DispatchNotFound(dispatch_id=dispatch_id)
        return _to_dispatch(row)

    def get_dispatch_context(self, mission_id: str, step_key: str) -> DispatchContextRecord:
        """Return the action context bound to the step's dispatch. Never binds or repairs one."""
        self._require_step(mission_id, step_key)
        return self._require_context(self._require_dispatch(mission_id, step_key))

    def get_dispatch_authorization(
        self, mission_id: str, step_key: str,
    ) -> DispatchAuthorizationRecord:
        """
        Return the durable record that authorize_step_send committed a send authorization.

        Under the supported-store contract a valid record is provenance of that commit. It is
        not tamper-evident: a writer that bypasses MissionStore can forge or alter even a
        canonically formatted row. Never creates, repairs or backfills one, and executes no
        write. A record is not proof
        that anything was sent. No record is not proof that no authorization happened: a step
        that progressed past DISPATCH_PREPARED under schema version 4 has none, and that raises
        DispatchAuthorizationMissing, not corruption. A DISPATCH_PREPARED step that has one
        raises CorruptDispatchAuthorization.
        """
        # One read transaction: the step's state and the record are compared, and a
        # concurrent authorization between the reads would make them contradict each other.
        with self._snapshot():
            step = self._require_step(mission_id, step_key)
            dispatch = self._require_dispatch(mission_id, step_key)
            authorization = self._authorization_of(step, dispatch)
            if authorization is None:
                raise DispatchAuthorizationMissing(mission_id, step_key)
            return authorization

    @contextlib.contextmanager
    def _snapshot(self) -> Iterator[None]:
        # A deferred transaction for reads that must agree with each other. The first read
        # takes a shared lock that is held to the end, so a writer cannot commit in between.
        # Nothing is written, so it ends with ROLLBACK, which only releases the lock.
        self._conn.execute('BEGIN')
        try:
            yield
        finally:
            if self._conn.in_transaction:
                self._conn.execute('ROLLBACK')

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
        # One write transaction covers the version check and any migration:
        # concurrent opens serialise, and a failure leaves the old version and
        # schema exactly as they were.
        with self._transaction():
            version = self._conn.execute('PRAGMA user_version').fetchone()[0]
            if version == SCHEMA_VERSION:
                return
            if version == 0:
                self._conn.execute(_MISSIONS_DDL)
                self._conn.execute(_MISSION_STEPS_DDL)
                self._conn.execute(_STEP_DISPATCHES_DDL)
                self._conn.execute(_DISPATCH_CONTEXTS_DDL)
                self._conn.execute(_DISPATCH_AUTHORIZATIONS_DDL)
            elif version in (1, 2, 3, 4):
                # An older database goes through every step up to the current version inside
                # this one transaction, so no version in between is ever committed.
                if version == 1:
                    self._migrate_v1_to_v2()
                if version <= 2:
                    self._migrate_v2_to_v3()
                if version <= 3:
                    self._migrate_v3_to_v4()
                self._migrate_v4_to_v5()
            else:
                raise UnsupportedSchemaVersion(version)
            self._conn.execute(f'PRAGMA user_version = {SCHEMA_VERSION}')

    def _migrate_v1_to_v2(self) -> None:
        # SQLite cannot add a NOT NULL column without a default, and a default
        # would leave migrated databases different from fresh ones. So rebuild
        # the table. Version 1 kept no lifecycle state, but every mission it
        # stored had been accepted.
        self._conn.execute(_RENAME_V1_MISSIONS)
        self._conn.execute(_MISSIONS_DDL)
        self._conn.execute(_COPY_V1_MISSIONS, (MissionState.ACCEPTED.value,))
        self._conn.execute(_DROP_V1_MISSIONS)
        self._conn.execute(_MISSION_STEPS_DDL)

    def _migrate_v2_to_v3(self) -> None:
        # Existing steps get no dispatch: none of them was prepared through this
        # code, and an ID would be invented.
        self._conn.execute(_STEP_DISPATCHES_DDL)

    def _migrate_v3_to_v4(self) -> None:
        # Only a table is added. Existing dispatches get no context: none was declared,
        # and an action name, type or namespace would be invented.
        self._conn.execute(_DISPATCH_CONTEXTS_DDL)

    def _migrate_v4_to_v5(self) -> None:
        # Only a table is added. Existing dispatches get no record, however far they have
        # progressed: whether an authorization was committed under version 4 cannot be told
        # from DISPATCHED, the attempt counter or a timestamp, so none is invented.
        self._conn.execute(_DISPATCH_AUTHORIZATIONS_DDL)

    def _fetch(self, query: str, value: str) -> MissionRecord | None:
        row = self._conn.execute(query, (value,)).fetchone()
        return None if row is None else _to_record(row)

    def _require(self, query: str, value: str) -> MissionRecord:
        record = self._fetch(query, value)
        if record is None:
            raise MissionNotFound(value)
        return record

    def _require_mission(self, mission_id: str) -> None:
        if self._conn.execute(_MISSION_EXISTS, (mission_id,)).fetchone() is None:
            raise MissionNotFound(mission_id)

    def _fetch_steps(self, mission_id: str) -> tuple[StepRecord, ...]:
        rows = self._conn.execute(_STEPS_OF_MISSION, (mission_id,)).fetchall()
        return tuple(_to_step(row) for row in rows)

    def _require_step(self, mission_id: str, step_key: str) -> StepRecord:
        row = self._conn.execute(_STEP_BY_KEY, (mission_id, step_key)).fetchone()
        if row is not None:
            return _to_step(row)
        # Tell an unknown mission apart from an unknown step.
        self._require_mission(mission_id)
        raise StepNotFound(mission_id, step_key)

    def _fetch_dispatch(self, mission_id: str, step_key: str) -> DispatchRecord | None:
        rows = self._conn.execute(_DISPATCHES_OF_STEP, (mission_id, step_key)).fetchall()
        if len(rows) > 1:
            raise CorruptDispatchRecord(
                mission_id, step_key, 'more than one dispatch for the step', len(rows))
        return _to_dispatch(rows[0]) if rows else None

    def _require_dispatch(self, mission_id: str, step_key: str) -> DispatchRecord:
        dispatch = self._fetch_dispatch(mission_id, step_key)
        if dispatch is None:
            raise DispatchNotFound(mission_id=mission_id, step_key=step_key)
        return dispatch

    def _fetch_context(self, dispatch: DispatchRecord) -> DispatchContextRecord | None:
        row = self._conn.execute(_CONTEXT_OF_DISPATCH, (str(dispatch.dispatch_id),)).fetchone()
        return None if row is None else _to_context(row, dispatch)

    def _require_context(self, dispatch: DispatchRecord) -> DispatchContextRecord:
        context = self._fetch_context(dispatch)
        if context is None:
            raise DispatchContextMissing(dispatch.mission_id, dispatch.step_key)
        return context

    def _fetch_authorization(self, dispatch: DispatchRecord) -> DispatchAuthorizationRecord | None:
        row = self._conn.execute(
            _AUTHORIZATION_OF_DISPATCH, (str(dispatch.dispatch_id),)).fetchone()
        return None if row is None else _to_authorization(row, dispatch)

    def _require_authorization(self, dispatch: DispatchRecord) -> DispatchAuthorizationRecord:
        authorization = self._fetch_authorization(dispatch)
        if authorization is None:
            raise DispatchAuthorizationMissing(dispatch.mission_id, dispatch.step_key)
        return authorization

    def _authorization_of(
        self, step: StepRecord, dispatch: DispatchRecord,
    ) -> DispatchAuthorizationRecord | None:
        # The one judgement of a dispatch's authorization record against its step, shared by
        # the reader and by authorize_step_send. Reads only: no path here writes a record, and
        # a missing one is never guessed to mean anything.
        if step.state is StepState.PENDING:
            # A step that was never prepared has no dispatch. That is reported as the
            # dispatch record's corruption, not hidden behind the authorization.
            raise CorruptDispatchRecord(
                step.mission_id, step.step_key, f'step is {step.state.value} but already has a '
                'dispatch', str(dispatch.dispatch_id))
        authorization = self._fetch_authorization(dispatch)
        if authorization is not None and step.state is StepState.DISPATCH_PREPARED:
            raise CorruptDispatchAuthorization(
                step.mission_id, step.step_key,
                f'step is {step.state.value} but its dispatch already has an authorization',
                authorization.authorized_at.isoformat(timespec='microseconds'))
        # Past DISPATCH_PREPARED a record is possible and its absence is not corruption: a
        # step that was authorized under schema version 4 has none.
        return authorization

    def _require_matching_context(
        self, dispatch: DispatchRecord, action: ExpectedActionContext,
    ) -> None:
        # Reads only. A missing context is never inserted here, and never guessed to be a
        # legacy row: version 4 has nothing that tells a version 3 dispatch that never had a
        # context from one whose context was removed by something outside this code, so both
        # fail closed with DispatchContextMissing (a malformed one, CorruptDispatchContext).
        bound = self._require_context(dispatch)
        differing = tuple(
            name for name in _CONTEXT_FIELDS if getattr(bound, name) != getattr(action, name))
        if differing:
            raise DispatchContextConflict(dispatch.mission_id, dispatch.step_key, differing)
