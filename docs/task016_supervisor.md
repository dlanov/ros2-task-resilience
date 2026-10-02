# Minimal Nav2 mission supervisor (Task 016) — EXPERIMENTAL DEVELOPER PREVIEW

This is an **experimental developer preview**, not a production supervisor and not a
physical-safety certification. Linux/Jazzy only, exercised only against the hardware-free Nav2
loopback simulator (`docs/hardware_free_demo.md`), never a physical robot. There is **no
cancellation integration and no automatic retry** anywhere in this task: interrupting the
supervisor process is not a cancellation request and is never reported as proof that navigation
stopped. "SUCCEEDED" means the Nav2 action server *reported* success (`error_code=0`) within its
own goal tolerance — this is action-reported success, not independently verified physical
safety.

Code: `task_resilience_ros/task_resilience_ros/supervisor.py` (the logic) and
`supervisor_cli.py` (the `run`/`resume`/`status` CLI, installed as the `task016_supervisor`
console script). Tests: `task_resilience_ros/test/test_supervisor.py`. Demo:
`task_resilience_demos/scripts/task016_loopback_demo.py`.

## What this is

A synchronous, single-threaded supervisor for exactly **one** two-step mission
(`checkpoint_a`, then `return_to_base`) against exactly **one** Nav2 `NavigateToPose` action
server, using Task 015's `Nav2Adapter` and the existing `task_resilience` core's own public API
for everything else. It makes the ordering decisions `Nav2Adapter` itself explicitly leaves to
its caller: which step to authorize next, when a recovered result means progression may
continue, and when it must not.

## The dedicated-database restriction

**One native preview mission per database, for good.** There is no request-ID argument, no
queue, and no "new request ID" shortcut: every invocation acts on the one mission under a
single, fixed, internal idempotency key. Point `--db` at a fresh path, or at a database this
supervisor already created — **never** at a database shared with an unrelated mission or tool.
`run`, called again with a *changed* mission definition (different poses, action name, or
namespace) under that same key, is refused (`IdempotencyConflict`/`StepDefinitionConflict`,
unchanged core behavior) — the existing data is never repaired or silently replaced.

## Resource lifecycle and the second-supervisor refusal

`run`/`resume` acquire resources in exactly this order, and release them in reverse, ownership
released last even on initialization failure:

```text
ExecutionOwner(db) -> acquire()
  -> MissionStore(owner.canonical_db_path)
    -> rclpy.init() -> create_node() -> Nav2Adapter(node)
      -> supervisor.run_mission / resume_mission
```

A second `run`/`resume` pointed at the same database fails in `ExecutionOwner.acquire()`,
**before `MissionStore` is ever constructed** — this is the existing Task 014 `ExecutionOwner`
contract, not anything new here. Immediately after acquiring ownership (still before
`MissionStore` is constructed), `run`/`resume` also run a small, read-only admission check
(`supervisor.ensure_database_admissible`): a database holding anything other than a fresh,
empty file or this one preview mission is refused outright — a stable idempotency key alone
does not prove the database is actually dedicated, so this is enforced, not just documented.
`status` is read-only and does not acquire ownership or touch ROS; it opens the database only
after its own narrow check confirms the file exists and matches the current schema version, so
it never creates a missing database and never migrates an older one merely to look at it. It may
be run at any time, including while another supervisor process holds ownership.

## Startup recovery, every time, before any new work

Every `run`/`resume` invocation re-inspects the whole mission before considering a new
authorization — there is no cached "resume point" between invocations:

- A terminal mission (`COMPLETED`/`FAILED`/`CANCELLED`) reports immediately; nothing is sent.
- `NEEDS_RECONCILIATION` is left alone unless every step has settled (the existing Task 011/012
  evidence/reconciliation paths); nothing here repairs or overrides it.
- A step already `DISPATCHED`/`ACCEPTED`/`EXECUTING` is only ever **queried** (bounded,
  read-only `query_result` polling) — never resent.
- Only the one next eligible step (every required step before it already `SUCCEEDED`) receives
  a fresh `authorize_step_send` + `adapter.send`.
- An explicit `SendOutcome` rejection follows the existing ordinary step `FAILED` transition. A
  timeout, transport exception, or unavailable server leaves the durable step untouched and is
  reported as `UNRESOLVED` — a **local invocation outcome**, never a new persisted core enum and
  never a fabricated `OUTCOME_UNKNOWN`.
- A terminal result is applied only through the existing `MissionStore.apply_recovery_observation`.

## Exit codes

| Code | Meaning |
| --- | --- |
| 0 | `COMPLETED` (or `status` printed successfully) |
| 2 | Usage/configuration/ownership error (an invalid `--poll-attempts`/`--poll-timeout`, a second supervisor refused, an inadmissible database, or no mission exists yet for `status`/`resume`) |
| 3 | `UNRESOLVED` — an in-flight step's result could not be confirmed within the polling budget; **run `resume` again** |
| 4 | `FAILED`/`CANCELLED` — a supported terminal failure stopped progression |
| 5 | `NEEDS_RECONCILIATION` |
| 6 | Cancellation intent (or a `CANCELLING` mission) preserved; no new navigation |
| 7 | `PAUSED_FOR_DEMO` — a demo/test pause checkpoint fired (see below) |

## Demo/test pause checkpoints (not for ordinary use)

Two environment variables make the restart scenarios reproducible without timing races:
`TASK016_PAUSE_AFTER_AUTHORIZE` and `TASK016_PAUSE_AFTER_SEND`. Set to `1` to pause on the next
step reaching that point, or to a specific step key (e.g. `return_to_base`) to pause only for
that one. Each pause is a **clean, logged, self-reported exit** (`OUTCOME PAUSED_FOR_DEMO`,
naming exactly which checkpoint and step) after the named durable write has already committed —
it is not a crash and not a cancellation, and resuming afterward is ordinary startup recovery,
not a special code path.

## Runbook

All commands below run from the repository root, against a **source checkout directly** (no
prior `colcon build` required — the demo wrapper adds `task_resilience`/`task_resilience_ros`
to `sys.path` itself). An isolated colcon build is a separate, self-contained alternative, not
mixed into the steps below — see "Isolated build" at the end of this section.

### 1. Create a unique, persistent trial directory

Every trial gets its own directory and its own database file — never a shared or reused path,
and never deleted mid-trial with `rm -f`:

```bash
TRIAL_DIR=".local-artifacts/task016/runs/manual-$(date +%Y%m%dT%H%M%S)"
mkdir -p "$TRIAL_DIR"
DB="$TRIAL_DIR/mission.db"
```

### 2. Start the loopback baseline (terminal 1)

```bash
source /opt/ros/jazzy/setup.bash
export ROS_DOMAIN_ID=42   # any isolated, test-owned domain -- use the SAME value in terminal 2
./task_resilience_demos/scripts/run_loopback_baseline.sh
```

### 3. Activate it (terminal 2, within ~60s of launch)

```bash
source /opt/ros/jazzy/setup.bash
export ROS_DOMAIN_ID=42
ros2 topic pub --once /initialpose geometry_msgs/msg/PoseWithCovarianceStamped \
  '{header: {frame_id: map}, pose: {pose: {position: {x: -2.0, y: -0.5, z: 0.0}, orientation: {x: 0.0, y: 0.0, z: 0.0, w: 1.0}}}}'
# wait for terminal 1 to print "[lifecycle_manager_navigation]: Managed nodes are active"
```

### 4. Normal run (terminal 2) — reaches COMPLETED with real navigation both ways

```bash
source /opt/ros/jazzy/setup.bash
export ROS_DOMAIN_ID=42
python3 task_resilience_demos/scripts/task016_loopback_demo.py run --db "$DB"
# OUTCOME COMPLETED, STEP checkpoint_a/return_to_base both SUCCEEDED, attempts_used=2/2
```

### 5. Status at any time (does not touch ROS or ownership)

```bash
python3 task_resilience_demos/scripts/task016_loopback_demo.py status --db "$DB"
```

### 6. Interrupt and resume

This is a SEPARATE trial from step 4 above (that one's `$DB` already reached `COMPLETED`;
`run`/`resume` against a completed mission just reports `COMPLETED` again and submits nothing).
Start a new, fresh trial directory first, exactly as in step 1:

```bash
TRIAL_DIR=".local-artifacts/task016/runs/manual-$(date +%Y%m%dT%H%M%S)-restart"
mkdir -p "$TRIAL_DIR"
DB="$TRIAL_DIR/mission.db"
```

This example reproduces exactly the restart scenario actually demonstrated against the real
loopback stack: `checkpoint_a` completes, `return_to_base` is sent, and the supervisor pauses —
cleanly, by its own choice, not a crash — before polling for that result. Nav2 (from steps 2–3)
is left running throughout, for this same trial's `$DB`:

```bash
TASK016_PAUSE_AFTER_SEND=return_to_base \
  python3 task_resilience_demos/scripts/task016_loopback_demo.py run --db "$DB"
echo "exit code: $?"   # 7 (PAUSED_FOR_DEMO) -- a clean, logged, self-chosen exit, not a crash.
                        # Compare: an actual crash (Ctrl-C, kill -9, a power loss) leaves no
                        # such line at all, but is equally safe to resume -- see below.

python3 task_resilience_demos/scripts/task016_loopback_demo.py resume --db "$DB"
# A genuinely separate process, against the SAME database. Recovers the retained result for
# return_to_base; Nav2's own log shows no second "Begin navigating" for it.
```

An ordinary `Ctrl-C`/`kill`/crash of the supervisor process at any point is also safe and
recoverable the same way, via `resume` against that same database — the pause checkpoint above
exists only to make one exact, specific timing reproducible on demand for this example, not
because an ordinary interrupt behaves any differently. **Resuming is not guaranteed to reach
`COMPLETED`**, though: if the interrupted step's result was never retained by the action server
(or can no longer be retrieved), `resume` honestly reports `UNRESOLVED` rather than inventing a
result — see below.

### 7. Interpreting `UNRESOLVED`

`UNRESOLVED` (exit 3) means the in-flight step's result could not be confirmed within the
configured polling budget — it is **not** a failure and **not** proof the goal didn't run.
Durable state is unchanged. Just run `resume` again; it repeats startup recovery (another
bounded round of read-only polling) before considering anything else. It is possible for a
mission to stay `UNRESOLVED` indefinitely across repeated `resume` calls — for example if the
action server has already discarded the goal's retained result — and that is the honest,
correct report in that case, not a bug to work around.

The polling budget is **not** simply `--poll-attempts` × `--poll-timeout`: each individual poll
is one full `Nav2Adapter.query_result` call, which first waits (up to its own internal
`service_wait_timeout_s`) for the `GetResult` service to be discovered at all, and only then
waits up to `--poll-timeout` for an actual response. A slow-to-discover service adds to every
single poll's wall-clock time on top of `--poll-timeout`, so the true worst-case total is
somewhat larger than the simple product.

### Isolated build (optional, self-contained alternative to the source-checkout steps above)

```bash
source /opt/ros/jazzy/setup.bash
WS=/tmp/t016-ws && rm -rf "$WS" && mkdir -p "$WS/src"
cp -r task_resilience task_resilience_ros "$WS/src/"
(cd "$WS" && colcon build --symlink-install)
source "$WS/install/setup.bash"
# From here on, replace every
#   python3 task_resilience_demos/scripts/task016_loopback_demo.py <command> ...
# above with:
ros2 run task_resilience_ros task016_supervisor <command> ...
```

## Known limitations (backlog, not expanded here)

- No cancellation integration, anywhere — not even to clean up after a `FAILED` step.
- No automatic retry and no operator override for a blocked (`UNRESOLVED`/
  `NEEDS_RECONCILIATION`) mission; a human runs `resume`, or (for `NEEDS_RECONCILIATION`)
  waits for Task 011/012's existing evidence paths to apply.
- Exactly two hardcoded steps, one action server, one goal format (`nav2/navigate_to_pose/v1`)
  — not a general multi-step/multi-robot mission language.
- Task 011's own limitation is unchanged: an inconclusive probe against a `DISPATCHED` step
  still cannot become a durable `OUTCOME_UNKNOWN` here either.
- The loopback simulator itself is "frictionless, inertialess, and collisionless" (see
  `docs/hardware_free_demo.md`) — this validates software execution/recovery semantics only.
