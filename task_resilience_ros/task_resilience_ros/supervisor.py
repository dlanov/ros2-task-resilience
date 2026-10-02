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
A minimal, synchronous two-step mission supervisor over Nav2Adapter.

EXPERIMENTAL DEVELOPER PREVIEW, not a production supervisor: see docs/task016_supervisor.md.
This module makes the handful of ordering/policy decisions Task 015's own adapter explicitly
leaves to its caller (docs/nav2_adapter.md's "What this is not") -- which step to authorize
next, when a recovered result means progression may continue, when it must not -- using only
the existing public task_resilience/task_resilience_ros APIs. It creates no schema, applies no
new durable uncertainty policy, and sends no cancellation.

ONE NATIVE PREVIEW MISSION PER DEDICATED DATABASE: every function here operates on the one
mission identified by PREVIEW_REQUEST_ID, a stable idempotency key, not a caller-supplied
request ID. There is no queue, multi-mission scheduler, or "new request ID" shortcut that could
bypass an unresolved mission in that database -- point a fresh, empty, or already-task016-owned
database at these functions, never a database shared with unrelated missions (see the module
docstring's "Dedicated-database restriction" in docs/task016_supervisor.md). A changed mission
definition replayed under PREVIEW_REQUEST_ID is refused by the existing IdempotencyConflict/
StepDefinitionConflict machinery, not repaired.

RESOURCE LIFETIME IS THE CALLER'S JOB, same as Nav2Adapter's own convention: run_mission and
resume_mission take an already-acquired ExecutionOwner, an already-open MissionStore (opened
from owner.canonical_db_path), and an already-constructed Nav2Adapter (one instance for the
owner's whole lifetime). This module never calls ExecutionOwner(...)/acquire()/close(),
MissionStore(...)/close(), rclpy.init()/shutdown(), or Nav2Adapter(...)/close() -- see
supervisor_cli.py for the one supported construction/teardown order. mission_status takes only
a MissionStore: it is read-only and needs no ownership, no node, and no adapter.

SYNCHRONOUS, SINGLE-THREADED, BOUNDED: each call walks the mission's steps in order, recovers
any in-flight dispatch through bounded, read-only query_result polling (never a resend), and
authorizes/sends at most the one next eligible step before polling it too. There is no daemon,
no background thread, no async orchestration, and no cancellation -- interrupting the calling
process is not a cancellation request (see docs/task016_supervisor.md).
"""

from dataclasses import dataclass, field
import math
import os
import sqlite3
from urllib.parse import quote
import uuid

from task_resilience.errors import (
    DispatchAuthorizationMissing,
    DispatchContextMissing,
    DispatchGoalMissing,
)
from task_resilience.model import (
    ExpectedGoalSpecification,
    MissionRecord,
    StepRecord,
    TERMINAL_RESULT_STATUSES,
)
from task_resilience.reconciliation import ExpectedActionContext, ObservationKind
from task_resilience.state_machine import MissionState, requires_reconciliation, StepState
from task_resilience.storage import MissionStore, SCHEMA_VERSION

from . import nav2_adapter
from .errors import UnsupportedGoalFormat
from .execution_owner import ExecutionOwner
from .nav2_adapter import Nav2Adapter
from .nav2_goal_v1 import GOAL_FORMAT, validate_payload

# The one stable idempotency key for this task's one native preview mission. Not a caller
# argument: see the module docstring's dedicated-database restriction.
PREVIEW_REQUEST_ID = 'task016-nav2-loopback-preview-v1'

CHECKPOINT_STEP = 'checkpoint_a'
RETURN_STEP = 'return_to_base'
STEP_ORDER = (CHECKPOINT_STEP, RETURN_STEP)

MISSION_SCHEMA_TAG = 'task016-mission-definition-v1'

DEFAULT_MAX_ATTEMPTS = 2  # one authorized send per step, two steps: the normal budget.
DEFAULT_POLL_ATTEMPTS = 6
DEFAULT_POLL_RESULT_TIMEOUT_S = 20.0

# Test/demo-only pause checkpoints (see docs/task016_supervisor.md). Unset in ordinary use.
# Each, when set to '1' (any step) or to a specific step_key, makes the invocation stop and
# report PAUSED_FOR_DEMO the next time it reaches that point FOR THAT STEP, after the named
# durable write has already committed, before the next ROS call. Never read anywhere outside
# this module.
PAUSE_AFTER_AUTHORIZE_ENV = 'TASK016_PAUSE_AFTER_AUTHORIZE'
PAUSE_AFTER_SEND_ENV = 'TASK016_PAUSE_AFTER_SEND'

OUTCOME_UNRESOLVED = 'UNRESOLVED'
OUTCOME_NEEDS_RECONCILIATION = 'NEEDS_RECONCILIATION'
OUTCOME_CANCEL_PRESERVED = 'CANCEL_PRESERVED'
OUTCOME_PAUSED_FOR_DEMO = 'PAUSED_FOR_DEMO'


@dataclass(frozen=True)
class StepDefinition:
    """One step of the preview mission's versioned definition, as persisted in the request."""

    step_key: str
    action_name: str
    action_type: str
    robot_namespace: str
    goal_format: str
    goal_payload: object


@dataclass(frozen=True)
class StepDetail:
    """Durable dispatch/attempt detail for one step, for mission_status only."""

    step: StepRecord
    dispatch_id: uuid.UUID | None
    has_context: bool
    has_goal: bool
    has_authorization: bool
    evidence_count: int


@dataclass(frozen=True)
class SupervisorReport:
    """
    What one supervisor invocation did, or found, and why it stopped.

    outcome is either a MissionState value (COMPLETED/FAILED/CANCELLED/RUNNING/ACCEPTED) --
    RUNNING/ACCEPTED meaning the mission is still open but this invocation made no further
    progress for a reason detail explains -- or one of the OUTCOME_* sentinels above for a
    non-mission-state halt (an inconclusive recovery observation, a preserved cancellation
    intent, or a demo/test pause checkpoint). It is a local invocation outcome, never a new
    persisted core enum.
    """

    outcome: str
    detail: str
    mission: MissionRecord | None
    steps: tuple[StepRecord, ...] = ()
    step_details: tuple[StepDetail, ...] = field(default=())


def build_mission_payload(
    *, action_name: str, robot_namespace: str,
    checkpoint_goal_payload: object, base_goal_payload: object,
) -> dict:
    """
    Build the complete versioned mission definition to pass as create_or_get's payload.

    Both steps share one action_name/action_type/robot_namespace (the same NavigateToPose
    endpoint) and the existing nav2/navigate_to_pose/v1 goal format; only the goal payload
    (the pose) differs between them. Validated by ExpectedGoalSpecification at prepare time,
    not here -- this function only shapes the plain, JSON-safe dict MissionRequest canonicalizes.
    """
    steps = []
    for step_key, payload in ((CHECKPOINT_STEP, checkpoint_goal_payload),
                              (RETURN_STEP, base_goal_payload)):
        steps.append({
            'step_key': step_key,
            'action_name': action_name,
            'action_type': nav2_adapter.SUPPORTED_ACTION_TYPE,
            'robot_namespace': robot_namespace,
            'goal_format': GOAL_FORMAT,
            'goal_payload': payload,
        })
    return {'schema': MISSION_SCHEMA_TAG, 'steps': steps}


class MissionDefinitionError(Exception):
    """The mission's persisted request payload is not a definition this module understands."""


class IncompatibleDatabase(Exception):
    """The database is not a fresh database or one already holding only this preview mission."""


class MissingPreviewDatabase(Exception):
    """status was asked to inspect a database path that does not exist. Never auto-created."""

    def __init__(self, db_path: str) -> None:
        super().__init__(f'{db_path!r} does not exist; status never creates a database')
        self.db_path = db_path


class UnsupportedPreviewSchema(Exception):
    """status refuses to migrate an older (or otherwise unrecognized) schema just to look."""

    def __init__(self, db_path: str, version: int) -> None:
        super().__init__(
            f'{db_path!r} holds schema version {version}, not the current {SCHEMA_VERSION}; '
            'status refuses to migrate it merely to inspect it')
        self.db_path = db_path
        self.version = version


def _validate_poll_attempts(value: object) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f'poll_attempts must be a positive int, got {value!r}')
    if value <= 0:
        raise ValueError(f'poll_attempts must be positive, got {value!r}')


def _validate_poll_result_timeout(value: object) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f'poll_result_timeout_s must be a finite positive number, got {value!r}')
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f'poll_result_timeout_s must be finite, got {value!r}')
    if number <= 0:
        raise ValueError(f'poll_result_timeout_s must be positive, got {value!r}')


def _validate_definitions(definitions: dict[str, 'StepDefinition']) -> None:
    """
    Validate a complete mission definition before any durable write, never after.

    Checks exactly what the core/adapter would at prepare/send time, refused up front rather
    than repaired. Reuses nav2_adapter's own action-type check and endpoint-resolution
    convention directly (never a separately maintained copy of that fail-fast rule) so this
    preflight can never drift from what send() actually enforces later; the adapter's own
    final endpoint/remap checks (which additionally require a live rclpy.Node) are unchanged
    and still run then.
    """
    for definition in definitions.values():
        nav2_adapter._check_action_type(
            definition.action_name, definition.action_type, definition.robot_namespace)
        nav2_adapter._resolve_action_name(definition.action_name, definition.robot_namespace)
        if definition.goal_format != GOAL_FORMAT:
            raise UnsupportedGoalFormat(definition.goal_format, (GOAL_FORMAT,))
        validate_payload(definition.goal_payload)


def _validate_step_list_order(steps: tuple[StepRecord, ...]) -> None:
    # Empty is fine (not yet initialized -- see resume_mission). Anything else must match
    # STEP_ORDER exactly: a later step must never be inspected, let alone sent, ahead of an
    # earlier one that is still PENDING or otherwise unresolved.
    actual = tuple(step.step_key for step in steps)
    if actual and actual != STEP_ORDER:
        raise MissionDefinitionError(
            f'mission steps are {list(actual)}, not the expected {list(STEP_ORDER)} in order')


def _validate_step_progression(steps: tuple[StepRecord, ...]) -> None:
    """
    Reject a stored step set whose progression skips ahead of an unresolved predecessor.

    Checking step KEYS/order alone (_validate_step_list_order) is not enough: a store built
    directly through the core's own public API -- prepare_step_dispatch/authorize_step_send
    enforce no cross-step ordering of their own -- can legally produce checkpoint_a=PENDING
    with return_to_base already DISPATCHED. _advance's per-step loop walks steps in order and
    would then authorize/send checkpoint_a without ever first inspecting that return_to_base
    is already ahead of it. This is a narrow, sequential-preview-specific check (exactly two
    hardcoded, ordered steps), not a general scheduler: once an earlier required step is found
    not yet SUCCEEDED, every later step must still be PENDING -- genuinely untouched, never
    authorized, sent, or settled -- or this refuses outright. Nothing is reordered, resent, or
    given invented evidence to make an incompatible stored state fit.

    Compatible with the native DISPATCH_PREPARED-without-authorization case (an earlier step
    that is DISPATCH_PREPARED still counts as "not yet SUCCEEDED", but a later step that is
    still PENDING raises nothing): the steps given are assumed already confirmed to be in
    STEP_ORDER by _validate_step_list_order.
    """
    blocked = False
    for step in steps:
        if blocked and step.state is not StepState.PENDING:
            raise MissionDefinitionError(
                f'step {step.step_key!r} is {step.state.value}, but an earlier required step '
                'has not yet SUCCEEDED; this stored progression is not one this supervisor '
                'can safely advance -- nothing is reordered, resent, or repaired')
        if step.state is not StepState.SUCCEEDED:
            blocked = True


def _readonly_connection(db_path: str) -> sqlite3.Connection:
    """
    Open one short-lived, read-only sqlite3 connection via a correctly escaped file: URI.

    db_path is made absolute first (a file: URI does not reliably support a relative path)
    and then percent-encoded with urllib.parse.quote (default safe='/', so only the path
    separator is left unescaped): a character that is otherwise significant in a URI -- '#'
    (starts a fragment), '?' (starts a query), '%' itself -- would otherwise silently truncate
    or redirect which file is actually opened, making this check inspect a different,
    neighboring file than the one actually given. Every read-only inspection in this module
    goes through this one helper; the caller is responsible for closing the connection.
    """
    absolute = os.path.abspath(db_path)
    return sqlite3.connect(f'file:{quote(absolute)}?mode=ro', uri=True)


def _read_schema_version_readonly(db_path: str) -> int | None:
    """Return PRAGMA user_version via a read-only connection, or None if the file is absent."""
    if not os.path.exists(db_path) or os.path.getsize(db_path) == 0:
        return None
    conn = _readonly_connection(db_path)
    try:
        return conn.execute('PRAGMA user_version').fetchone()[0]
    finally:
        conn.close()


def _is_truly_empty_sqlite_file(db_path: str) -> bool:
    """
    Return whether a version-0 SQLite file has no schema objects at all.

    PRAGMA user_version == 0 is SQLite's own default for a connection that never explicitly
    set it -- it is NOT proof the file is otherwise empty: a file can hold an arbitrary
    existing table and still report version 0. This checks sqlite_master directly, the one
    way to tell "genuinely nothing in here yet" apart from "something is in here, just never
    versioned."
    """
    conn = _readonly_connection(db_path)
    try:
        return conn.execute('SELECT COUNT(*) FROM sqlite_master').fetchone()[0] == 0
    finally:
        conn.close()


def ensure_database_admissible(canonical_db_path: str) -> None:
    """
    Refuse a database that is not a fresh file or one already holding only this preview.

    Read-only: every connection here is short-lived, always closed before returning, and no
    row is ever written. Must run AFTER ExecutionOwner.acquire() (so a concurrent writer is
    excluded) but BEFORE MissionStore is ever constructed on this path -- constructing
    MissionStore on an older schema version migrates it as a side effect, which this check
    exists to prevent for any database this supervisor does not already own as its one
    preview mission. A nonexistent or empty file is always admitted (MissionStore's own
    fresh-create path is not a migration of existing data); a version-0 file that already
    holds unrelated schema objects is not -- version 0 alone is not proof of an empty
    database. No queue, scheduler, or repair path is added: an incompatible database is
    refused outright, never adopted or merged into.
    """
    version = _read_schema_version_readonly(canonical_db_path)
    if version is None:
        return
    if version == 0:
        if _is_truly_empty_sqlite_file(canonical_db_path):
            return
        raise IncompatibleDatabase(
            f'{canonical_db_path!r} has schema version 0 but already holds existing schema '
            'objects; version 0 alone is not proof of an empty database, and this supervisor '
            'refuses to treat unrelated existing data as a fresh preview database')
    if version != SCHEMA_VERSION:
        raise IncompatibleDatabase(
            f'{canonical_db_path!r} holds schema version {version}, not the current '
            f'{SCHEMA_VERSION}; refusing to let MissionStore migrate a database this '
            'supervisor does not already own as its one preview mission')
    conn = _readonly_connection(canonical_db_path)
    try:
        rows = conn.execute('SELECT DISTINCT request_id FROM missions').fetchall()
    finally:
        conn.close()
    unrelated = sorted(row[0] for row in rows if row[0] != PREVIEW_REQUEST_ID)
    if unrelated:
        raise IncompatibleDatabase(
            f'{canonical_db_path!r} holds unrelated mission(s) {unrelated!r}; this supervisor '
            'only ever acts on its own one preview mission in a dedicated database and '
            'refuses to touch one holding anything else')


def open_preview_store_for_status(db_path: str) -> MissionStore:
    """
    Open MissionStore for `status` only after confirming it is safe to do so, read-only.

    Never creates a missing database (raises MissingPreviewDatabase instead) and never
    migrates an older/unrecognized schema merely to look at it (raises
    UnsupportedPreviewSchema instead). Once past both checks, constructing MissionStore is
    known to trigger neither a creation nor a migration, so the ordinary constructor is used
    for the actual read.
    """
    version = _read_schema_version_readonly(db_path)
    if version is None:
        raise MissingPreviewDatabase(db_path)
    if version != SCHEMA_VERSION:
        raise UnsupportedPreviewSchema(db_path, version)
    return MissionStore(db_path)


def _parse_definition(payload: object) -> dict[str, StepDefinition]:
    # The payload was already accepted once by MissionRequest's own strict-JSON-domain check;
    # this only checks the shape THIS module relies on. Nothing here repairs a bad payload --
    # a mismatch means the database holds a mission this version of the supervisor cannot
    # safely act on, and it is refused, not guessed at.
    if not isinstance(payload, dict) or payload.get('schema') != MISSION_SCHEMA_TAG:
        raise MissionDefinitionError(
            f'request payload is not a {MISSION_SCHEMA_TAG!r} mission definition: {payload!r}')
    steps = payload.get('steps')
    if not isinstance(steps, list) or [s['step_key'] for s in steps] != list(STEP_ORDER):
        raise MissionDefinitionError(
            f'mission definition steps must be exactly {list(STEP_ORDER)} in order, '
            f'got {payload!r}')
    parsed = {}
    for entry in steps:
        parsed[entry['step_key']] = StepDefinition(
            step_key=entry['step_key'], action_name=entry['action_name'],
            action_type=entry['action_type'], robot_namespace=entry['robot_namespace'],
            goal_format=entry['goal_format'], goal_payload=entry['goal_payload'])
    return parsed


def _action_context(definition: StepDefinition) -> ExpectedActionContext:
    return ExpectedActionContext(
        action_name=definition.action_name, action_type=definition.action_type,
        robot_namespace=definition.robot_namespace)


def _goal_spec(definition: StepDefinition) -> ExpectedGoalSpecification:
    return ExpectedGoalSpecification(definition.goal_format, definition.goal_payload)


def _pause(env_var: str, step_key: str) -> bool:
    configured = os.environ.get(env_var)
    return configured == '1' or configured == step_key


def _poll_for_result(
    adapter: Nav2Adapter, store: MissionStore, mission_id: str, step_key: str,
    poll_attempts: int, poll_result_timeout_s: float,
) -> tuple[bool, str]:
    """
    Poll query_result up to poll_attempts times. Return (resolved, detail).

    Each call is an ordinary bounded, read-only Nav2Adapter.query_result -- never a resend.
    A terminal RESULT_RESPONSE is applied through MissionStore.apply_recovery_observation
    (a fresh evidence_id each call, since each poll is a new observation attempt) and the
    function returns True immediately. Every other observation (a non-terminal RESULT_RESPONSE
    status, a local timeout, an unavailable service, or a transport error) changes no durable
    state and is simply retried, up to the budget; exhausting the budget is not evidence of
    anything and returns False -- the caller reports UNRESOLVED and authorizes nothing further.
    """
    last_detail = 'no poll attempt made'
    for attempt in range(1, poll_attempts + 1):
        observation = adapter.query_result(
            store=store, mission_id=mission_id, step_key=step_key,
            result_timeout_s=poll_result_timeout_s)
        if observation.kind is ObservationKind.RESULT_RESPONSE and (
                observation.status in TERMINAL_RESULT_STATUSES):
            store.apply_recovery_observation(
                uuid.uuid4(), mission_id, step_key, observation)
            return True, f'terminal result observed on poll {attempt}/{poll_attempts}'
        status_name = observation.status.name if observation.status else None
        last_detail = (
            f'poll {attempt}/{poll_attempts}: {observation.kind.name}'
            + (f' status={status_name}' if status_name else ''))
    return False, f'polling budget exhausted ({poll_attempts} attempts); last: {last_detail}'


def _report(store: MissionStore, mission_id: str, detail: str, outcome: str | None = None,
            ) -> SupervisorReport:
    """
    Build a SupervisorReport from a FRESH read of durable state, never a carried-over snapshot.

    Every return point in _advance goes through this, specifically so a report (including a
    non-terminal one such as UNRESOLVED or PAUSED_FOR_DEMO) always reflects whatever the most
    recent write in this invocation actually committed -- attempts_used included -- rather than
    a `mission`/`step` variable captured earlier in the function, before that write. outcome
    defaults to the freshly read mission's own state value; pass one of the OUTCOME_* sentinels
    explicitly only where the local invocation outcome is not simply the mission's state.
    """
    mission = store.get_by_mission_id(mission_id)
    return SupervisorReport(
        mission.state.value if outcome is None else outcome, detail, mission,
        store.get_steps(mission_id))


def _advance(
    *, owner: ExecutionOwner, store: MissionStore, adapter: Nav2Adapter, mission_id: str,
    definitions: dict[str, StepDefinition], poll_attempts: int, poll_result_timeout_s: float,
) -> SupervisorReport:
    mission = store.get_by_mission_id(mission_id)

    if mission.state is MissionState.NEEDS_RECONCILIATION:
        if requires_reconciliation([s.state for s in store.get_steps(mission_id)]):
            return _report(
                store, mission_id,
                'mission has an OUTCOME_UNKNOWN step; only Task 011/012 evidence/reconciliation '
                'paths can resolve it -- nothing is repaired or overridden here',
                OUTCOME_NEEDS_RECONCILIATION)
        mission = store.resolve_mission_reconciliation(mission_id)

    if mission.state in (MissionState.COMPLETED, MissionState.FAILED, MissionState.CANCELLED):
        return _report(store, mission_id, 'mission is already terminal; no submission attempted')

    if mission.state is MissionState.ACCEPTED:
        mission = store.transition_mission_state(mission_id, MissionState.RUNNING)

    # Inspect the full, actual persisted step list before touching any single step: a later
    # step must never be newly sent, or even inspected for that matter, ahead of discovering
    # that an earlier one is unresolved, out of the expected order, or already ahead of it.
    steps_now = store.get_steps(mission_id)
    _validate_step_list_order(steps_now)
    _validate_step_progression(steps_now)

    may_authorize_new = mission.state is MissionState.RUNNING

    for step_key in STEP_ORDER:
        step = store.get_step(mission_id, step_key)

        if step.state is StepState.SUCCEEDED:
            continue

        if step.state in (StepState.FAILED, StepState.CANCELLED):
            if mission.state is MissionState.RUNNING and step.state is StepState.FAILED:
                store.transition_mission_state(mission_id, MissionState.FAILED)
            return _report(
                store, mission_id, f'step {step_key!r} is {step.state.value}; progression '
                'stopped, no later step is authorized')

        if step.state is StepState.CANCEL_REQUESTED:
            return _report(
                store, mission_id,
                f'step {step_key!r} is CANCEL_REQUESTED; cancellation intent is preserved, no '
                'new navigation or automatic return-to-base', OUTCOME_CANCEL_PRESERVED)

        if step.state is StepState.OUTCOME_UNKNOWN:
            if mission.state in (MissionState.RUNNING, MissionState.CANCELLING):
                store.enter_mission_reconciliation(mission_id)
            return _report(
                store, mission_id,
                f'step {step_key!r} is OUTCOME_UNKNOWN; mission moved to NEEDS_RECONCILIATION',
                OUTCOME_NEEDS_RECONCILIATION)

        if step.state in (StepState.DISPATCHED, StepState.ACCEPTED, StepState.EXECUTING):
            resolved, detail = _poll_for_result(
                adapter, store, mission_id, step_key, poll_attempts, poll_result_timeout_s)
            if not resolved:
                return _report(
                    store, mission_id, f'step {step_key!r}: {detail}', OUTCOME_UNRESOLVED)
            step = store.get_step(mission_id, step_key)
            if step.state is StepState.SUCCEEDED:
                continue
            if mission.state is MissionState.RUNNING and step.state is StepState.FAILED:
                store.transition_mission_state(mission_id, MissionState.FAILED)
            return _report(
                store, mission_id, f'step {step_key!r} resolved to {step.state.value}; '
                'progression stopped')

        # PENDING or DISPATCH_PREPARED: the next eligible step (every prior one, if any,
        # already SUCCEEDED by this loop's own order).
        if not may_authorize_new:
            return _report(
                store, mission_id, f'mission is {mission.state.value}; no new navigation is '
                f'authorized for {step_key!r}', OUTCOME_CANCEL_PRESERVED)

        definition = definitions[step_key]
        store.prepare_step_dispatch(
            mission_id, step_key, _action_context(definition), _goal_spec(definition))
        authorization = store.authorize_step_send(mission_id, step_key)

        if _pause(PAUSE_AFTER_AUTHORIZE_ENV, step_key):
            return _report(
                store, mission_id,
                f'{PAUSE_AFTER_AUTHORIZE_ENV} set: paused for step {step_key!r} after '
                'authorize_step_send committed, before adapter.send was ever called',
                OUTCOME_PAUSED_FOR_DEMO)

        outcome = adapter.send(
            owner=owner, store=store, authorization=authorization, mission_id=mission_id,
            step_key=step_key)

        if _pause(PAUSE_AFTER_SEND_ENV, step_key):
            return _report(
                store, mission_id,
                f'{PAUSE_AFTER_SEND_ENV} set: paused for step {step_key!r} after adapter.send '
                f'returned accepted={outcome.accepted!r} reason={outcome.reason!r}, before any '
                'result polling', OUTCOME_PAUSED_FOR_DEMO)

        if outcome.accepted is False:
            # An explicit rejection observed on this fresh send: the existing supported
            # ordinary failure transition, never apply_recovery_observation (there is no
            # action-server result here, only a SendGoal-level fact this process just
            # observed directly).
            store.transition_step_state(mission_id, step_key, StepState.FAILED)
            store.transition_mission_state(mission_id, MissionState.FAILED)
            return _report(
                store, mission_id, f'step {step_key!r} was REJECTED on send; progression '
                'stopped, no later step is authorized')

        # accepted True or None (SERVER_UNAVAILABLE/ACCEPT_TIMEOUT -- not proof the request was
        # never received): either way the step is already durably DISPATCHED. Poll exactly as
        # recovery of an existing DISPATCHED step would.
        resolved, detail = _poll_for_result(
            adapter, store, mission_id, step_key, poll_attempts, poll_result_timeout_s)
        if not resolved:
            return _report(
                store, mission_id,
                f'step {step_key!r}: send outcome accepted={outcome.accepted!r} '
                f'reason={outcome.reason!r}; {detail}', OUTCOME_UNRESOLVED)
        step = store.get_step(mission_id, step_key)
        if step.state is StepState.SUCCEEDED:
            continue
        if mission.state is MissionState.RUNNING and step.state is StepState.FAILED:
            store.transition_mission_state(mission_id, MissionState.FAILED)
        return _report(
            store, mission_id, f'step {step_key!r} resolved to {step.state.value}; '
            'progression stopped')

    # Every step is SUCCEEDED.
    if mission.state is MissionState.RUNNING:
        store.transition_mission_state(mission_id, MissionState.COMPLETED)
    return _report(store, mission_id, 'every step SUCCEEDED')


def run_mission(
    *, owner: ExecutionOwner, store: MissionStore, adapter: Nav2Adapter,
    action_name: str, robot_namespace: str = '',
    checkpoint_goal_payload: object, base_goal_payload: object,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS, poll_attempts: int = DEFAULT_POLL_ATTEMPTS,
    poll_result_timeout_s: float = DEFAULT_POLL_RESULT_TIMEOUT_S,
) -> SupervisorReport:
    """
    Create or replay the one preview mission, then recover and advance it.

    Uses the core's own create_or_get/initialize_steps semantics directly: an identical
    replay (same action/goal payloads, same max_attempts) preserves mission identity,
    progress and attempts and continues recovery exactly as resume_mission would; a changed
    definition under PREVIEW_REQUEST_ID raises IdempotencyConflict/StepDefinitionConflict,
    unchanged from the core's existing contract -- never silently repaired or replaced.

    Every check below runs before create_or_get: an invalid poll setting or an invalid goal
    for EITHER step (including return_to_base) is refused before anything is created,
    including before checkpoint_a's own dispatch.
    """
    _validate_poll_attempts(poll_attempts)
    _validate_poll_result_timeout(poll_result_timeout_s)
    payload = build_mission_payload(
        action_name=action_name, robot_namespace=robot_namespace,
        checkpoint_goal_payload=checkpoint_goal_payload, base_goal_payload=base_goal_payload)
    _validate_definitions(_parse_definition(payload))

    mission = store.create_or_get(PREVIEW_REQUEST_ID, payload, max_attempts)
    store.initialize_steps(mission.mission_id, STEP_ORDER)
    definitions = _parse_definition(mission.payload)
    return _advance(
        owner=owner, store=store, adapter=adapter, mission_id=mission.mission_id,
        definitions=definitions, poll_attempts=poll_attempts,
        poll_result_timeout_s=poll_result_timeout_s)


def resume_mission(
    *, owner: ExecutionOwner, store: MissionStore, adapter: Nav2Adapter,
    poll_attempts: int = DEFAULT_POLL_ATTEMPTS,
    poll_result_timeout_s: float = DEFAULT_POLL_RESULT_TIMEOUT_S,
) -> SupervisorReport:
    """
    Load the previously accepted preview mission and recover/advance it. No new goals.

    The mission definition (action context and both exact goal payloads) is read back from
    the mission's own persisted request payload, never recomputed or re-supplied by this
    call -- return_to_base is never recomputed from a current pose after a restart. The
    definition is validated before anything else; if step initialization never completed (a
    crash between create_or_get and initialize_steps), it is finished here through the same
    existing, idempotent initialize_steps -- a replay against an already-initialized mission
    changes nothing (no reset, no overwrite, no UUID replacement, no attempt change).
    """
    _validate_poll_attempts(poll_attempts)
    _validate_poll_result_timeout(poll_result_timeout_s)
    mission = store.get_by_request_id(PREVIEW_REQUEST_ID)
    definitions = _parse_definition(mission.payload)
    _validate_definitions(definitions)
    store.initialize_steps(mission.mission_id, STEP_ORDER)
    return _advance(
        owner=owner, store=store, adapter=adapter, mission_id=mission.mission_id,
        definitions=definitions, poll_attempts=poll_attempts,
        poll_result_timeout_s=poll_result_timeout_s)


def mission_status(*, store: MissionStore) -> SupervisorReport:
    """
    Report durable mission/step/dispatch/attempt state. Submits no goal, applies no evidence.

    Read-only: needs no ExecutionOwner and no Nav2Adapter, and may be called at any time,
    including while another supervisor process holds ownership of the same database.
    """
    mission = store.get_by_request_id(PREVIEW_REQUEST_ID)
    steps = store.get_steps(mission.mission_id)
    details = []
    for step in steps:
        dispatch_id = None
        has_context = has_goal = has_authorization = False
        evidence_count = 0
        if step.state is not StepState.PENDING:
            dispatch = store.get_step_dispatch(mission.mission_id, step.step_key)
            dispatch_id = dispatch.dispatch_id
            has_context = _safe_has(
                DispatchContextMissing, store.get_dispatch_context,
                mission.mission_id, step.step_key)
            has_goal = _safe_has(
                DispatchGoalMissing, store.get_dispatch_goal, mission.mission_id, step.step_key)
            has_authorization = _safe_has(
                DispatchAuthorizationMissing, store.get_dispatch_authorization,
                mission.mission_id, step.step_key)
            evidence_count = len(store.get_recovery_evidence(mission.mission_id, step.step_key))
        details.append(StepDetail(
            step=step, dispatch_id=dispatch_id, has_context=has_context, has_goal=has_goal,
            has_authorization=has_authorization, evidence_count=evidence_count))
    return SupervisorReport(
        mission.state.value, 'status only; no goal submitted, no evidence applied', mission,
        steps, tuple(details))


def _safe_has(missing_error: type[Exception], getter, mission_id: str, step_key: str) -> bool:
    # Only the typed "missing" exception each getter raises for an absent-but-not-corrupt
    # binding is swallowed here; a Corrupt* error (a malformed row) still propagates, since
    # status must surface corruption honestly, not hide it behind a plain False.
    try:
        getter(mission_id, step_key)
        return True
    except missing_error:
        return False
