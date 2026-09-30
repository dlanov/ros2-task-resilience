# Single-host execution ownership (Task 014)

This note defines `ExecutionOwner`, the one-host execution-ownership primitive in the new
`task_resilience_ros` package, and the package boundary it establishes. Code:
`task_resilience_ros/task_resilience_ros/execution_owner.py` and `errors.py`. Tests:
`task_resilience_ros/test/test_execution_owner.py` (single-process) and
`test_execution_owner_processes.py` (real multi-process scenarios, using the private worker
roles in `_execution_owner_process.py`).

The contract in one sentence: **for one canonical database path on the tested local Linux
filesystem, at most one process — and, within it, at most one `ExecutionOwner` instance — can
hold execution ownership at a time; acquisition is immediate and never blocks, waits, or
retries.**

## The gap this closes

Tasks 008–013 give a durable dispatch UUID, a durable action context, a durable exact goal
binding, and a durably committed send authorization. None of that is a process-ownership
primitive: two independent processes (or threads) can each open their own `MissionStore`
connection and both read the same committed, authorized facts — but neither can correctly
conclude from that alone that it should send. A stored authorization is historical provenance,
not a reusable permit: it exists so a *successor* can recover `DISPATCHED` work without resending
it, not so any reader may treat it as license to call ROS now. Acting correctly requires three
independent things together: fresh authorization returned to the caller making that specific
request, durable action-context/goal binding, and being the one process that ownership excludes
every other cooperating reader — including another cooperating supervisor — from believing it may
also act. `ExecutionOwner` supplies that last requirement: not "was this authorized?" but "am I
the one process allowed to act on committed authorizations by calling ROS?"

Of the mechanisms considered (a documented convention alone, ROS graph discovery, a PID file, an
in-process mutex, a SQLite row/lease, a lease with a fencing token, an OS advisory file lock, and
a bound Unix socket), an OS advisory file lock (`fcntl.flock`, `LOCK_EX | LOCK_NB`, on a sidecar
file held for the process's lifetime) was chosen: the kernel releases it automatically and
immediately on normal exit, `os._exit`, or `SIGKILL`, with no heartbeat or staleness window to
get wrong, and it needs no ROS or SQLite dependency.

## Package boundary

`task_resilience_ros` is a new `ament_python` package, a sibling of `task_resilience`:

```text
task_resilience_ros/
    package.xml, setup.py, setup.cfg, resource/task_resilience_ros
    task_resilience_ros/__init__.py       (empty; no package-level re-exports)
    task_resilience_ros/errors.py         (ExecutionOwnerAlreadyHeld)
    task_resilience_ros/execution_owner.py
    test/test_execution_owner.py
    test/test_execution_owner_processes.py
    test/_execution_owner_process.py      (private test-only worker roles, not installed)
    test/test_copyright.py, test_flake8.py, test_pep257.py
```

`execution_owner.py` imports nothing from `task_resilience`, `sqlite3`, `rclpy`, or any ROS
package — it operates on a path string alone. It has no runtime dependency at all beyond the
standard library; `package.xml` declares the same four `test_depend` entries as `task_resilience`
(`ament_copyright`, `ament_flake8`, `ament_pep257`, `python3-pytest`) and no `<depend>` tag. The
core `task_resilience` package is unchanged by this task and must never import
`task_resilience_ros`, `rclpy`, `nav2_msgs`, `fcntl`, `subprocess`, or `multiprocessing` — an
enduring, permanent-scope invariant, checked by AST (not substring grep) in
`test_execution_owner.py`. The narrower fact that `execution_owner.py`/`errors.py` currently
import nothing from `task_resilience` is checked separately and is not itself a permanent ban on
this package's own future growth: Tasks 015/016 will legitimately add ROS/adapter/supervisor
modules here that do import `task_resilience`.

## `ExecutionOwner` API

```python
class ExecutionOwner:
    def __init__(self, db_path: str | os.PathLike[str]) -> None: ...
    def acquire(self) -> None: ...
    def close(self) -> None: ...
    def __enter__(self) -> 'ExecutionOwner': ...
    def __exit__(self, *exc_info: object) -> None: ...

    db_path: str             # read-only; exactly as given, only os.fspath'd
    canonical_db_path: str   # read-only; resolved once at construction
    lock_path: str           # read-only; canonical_db_path + '.lock'
    acquired: bool           # read-only; True only after this instance's own acquire() succeeded
```

Construction validates and snapshots paths; it creates no file, acquires no lock, and opens no
database connection. `acquire()` is the sole point that touches the filesystem lock. One
successful lifecycle per instance: `acquire()` while already held raises `RuntimeError`; a
*failed* acquisition (contention or otherwise) leaves the instance available for an explicit
retry — nothing here retries automatically; `close()` permanently retires the instance, including
when called before any `acquire()`; `acquire()` after `close()` raises `RuntimeError`; repeated
`close()` is always a no-op. `__enter__` calls `acquire()` and returns `self`; `__exit__` calls
`close()` and never suppresses a body exception. Nested entry on the *same* object
(`with owner: with owner: ...`) is rejected by the same "already held" `RuntimeError`: if that
exception is caught inside the outer block, the outer object is still held; if it escapes the
outer block, the outer `__exit__` still runs and releases it. If `close()` itself raises (see
below) while a body exception is also in flight, ordinary Python `with`-statement chaining
applies — the `close()` error is what propagates, with the body's error attached as
`__context__` — this is not a special guarantee, just undisclaimed default behavior.

The instance is single-thread-use (construct, `acquire()`, `close()` from one thread — the same
restriction `MissionStore` documents for its own connection). Copying, deep-copying, or pickling
it is **unsupported, not prevented**: there is no `__copy__`/`__deepcopy__`/`__reduce__` override
and nothing raises if code does this anyway; a plain attribute copy would just duplicate the
`_fd` integer bookkeeping onto a second object referring to the same underlying descriptor, which
is exactly the kind of misuse this class simply does not guard against, not a case it forbids.
Retaining a reference to the object is necessary to manage or release ownership through it --
there is no destructor or `atexit` hook, so `close()` can only ever be called on an object code
still has a handle to. It is not what keeps the underlying lock held, though: the kernel-level
flock and its open file description persist independently of any Python reference to this object.
Dropping the last Python reference without calling `close()` does not close the integer file
descriptor or release the lock; it only removes any way for this process to call `close()` on it
later, leaving the descriptor open and the lock held until the process itself exits.
`acquired` is this object's own local bookkeeping; it is not a liveness probe of anything else and
not evidence about durable send authority.

## Path contract

`db_path` accepts `str` and `os.PathLike[str]`, matching `MissionStore`'s own type contract
exactly, converted once via `os.fspath()`. Rejected at construction, before any filesystem
mutation: bytes or a `bytes`-returning path-like (`TypeError`); an empty string, a string
containing a NUL byte, the literal `':memory:'`, or a `'file:'`-prefixed string (`ValueError`); an
existing directory at the database path (`IsADirectoryError`); any other existing non-regular file
there (`ValueError`).

The canonical path is resolved exactly once, at construction, using `os.path.realpath(raw,
strict=True)` (available since Python 3.10; this package targets base 3.12.3 and does not depend
on `os.path.ALLOW_MISSING`, a later addition). If that raises `FileNotFoundError`, the parent is
resolved and validated strictly on its own (propagating `NotADirectoryError`/`PermissionError`/
`OSError(ELOOP)` as themselves), and the exact final candidate entry is inspected with `lstat`
(never following a dangling symlink): a genuinely absent entry is accepted — the normal "database
not created yet" case — but an entry that exists there despite the failed strict resolution (an
unresolvable, dangling symlink) is rejected with `FileNotFoundError` rather than silently returned
as an unresolved name. This closes a real defect found in an earlier draft: without the `lstat`
check, a symlink whose target does not exist yet would resolve to one (wrong, unresolved)
canonical string before the target's first creation and to a different (correct) one afterward —
two processes racing across that boundary could each acquire a different, non-contending lock for
what was actually one logical database. Existing resolvable database and parent-directory symlink
aliases, and a plain not-yet-created database name under an existing directory, remain fully
supported and collapse to the identical canonical string every time. No lexical normalization
(e.g. `os.path.normpath`) is ever applied before resolution, since it could change what a path
containing `..` or a symlink actually means.

`lock_path` is always `canonical_db_path + '.lock'`. A future caller must construct `MissionStore`
from **this owner's `canonical_db_path`**, never the original `db_path` re-interpreted later: an
intervening `os.chdir()` between the two constructions can otherwise make a relative path resolve
to two different files even though the same string was used both times.

## Sidecar open, contention, and cleanup contract

`acquire()` opens the sidecar with exactly one flag expression:

```python
os.O_CREAT | os.O_RDWR | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK
```

at mode `0o644` (actual mode `0o644 & ~umask`; an existing sidecar's mode is never changed).
`O_NOFOLLOW` rejects a symlinked sidecar (`OSError`, `ELOOP`) before any lock attempt. `O_NONBLOCK`
guards the open itself against blocking on an unexpected non-regular file (e.g. a FIFO); after an
`fstat`/`S_ISREG` check rejects anything non-regular, `O_NONBLOCK` is cleared via `fcntl`
`F_SETFL` so the long-lived descriptor behaves normally for the rest of its life. `O_CLOEXEC` is
explicit, defense-in-depth intent — `os.open()` already produces a non-inheritable descriptor by
default on this Python/host — not a claim that Python's own default was ever inheritable.

Only `fcntl.flock(fd, LOCK_EX | LOCK_NB)` itself is wrapped for contention translation: an
`OSError` there with `errno` `EAGAIN`/`EWOULDBLOCK` becomes `ExecutionOwnerAlreadyHeld(
canonical_db_path, lock_path)`; every other error from that call, or from the `fstat`/`fcntl`
steps before it, propagates unmodified. One outer cleanup guard closes the just-opened descriptor
on *any* failure; `self._fd` is only ever set after every step has succeeded, so a failed
`acquire()` never leaks a descriptor and leaves the instance cleanly retryable. If that cleanup
`os.close()` call itself fails (a real kernel close still happens first), ordinary Python exception
chaining applies exactly as it does for `close()` below: the cleanup failure is what propagates,
with the original triggering error attached as its `__context__` — not a special guarantee this
class adds, just undisclaimed default behavior a caller should expect either way.

`close()` detaches the stored descriptor and marks the instance retired *before* attempting the
one `os.close()` call, under this class's single-thread-use contract — not as a claim of atomic
thread synchronization, but so that bookkeeping stays truthful even if `close()` itself raises
(`close(2)` can report a deferred error while still releasing the descriptor), and so a repeated
`close()` never retries a stale, possibly-since-reused descriptor number. There is no explicit
`LOCK_UN`: plain `close()` already releases the lock when it is the last descriptor referencing
that open file description, and — unlike an explicit unlock — it never drops the lock early just
because one of several duplicate descriptors (e.g. from an unsupported fork) happens to close
first.

No content is ever written to the sidecar (a newly created one is empty; a pre-existing one's
bytes and mode are left exactly as found), no `O_TRUNC`, no unlink on close or on any failure
path, no stale-file deletion, and no PID-file authority of any kind: the only fact this class ever
trusts is whether `flock(LOCK_EX | LOCK_NB)` just succeeded, in this process, right now.

## Process and descriptor-inheritance contract

Supported and tested: ordinary `subprocess.Popen`/`.run` with default arguments (`close_fds=True`,
no `pass_fds` naming this descriptor, no stdio redirection of it, no `preexec_fn` touching it) —
confirmed by device/inode identity, not by assuming the fd number is simply unused in the child:
under the default `close_fds=True`, an exec'd child was observed either unable to `fstat()` the
descriptor at all, or, if that integer happened to be reused for some unrelated file of the
child's own, never the actual lock file by device/inode; the same identity check holds under
`close_fds=False`. This observed non-inheritance is not attributed solely to this class's own
explicit `O_CLOEXEC` flag: `os.open()` already returns a non-inheritable descriptor by default
(PEP 446, Python 3.4+), so a plain exec-boundary check like this cannot separate the two
mechanisms from each other.

Source-understood, **not separately validated** for this task: `os.posix_spawn()` called directly,
and `multiprocessing`'s `'spawn'` start method, both of which are genuinely exec-based end to end,
the same category as ordinary `subprocess`. `'forkserver'` is different and is called out
separately rather than grouped with them: a single persistent server process is started once via
exec, but each individual *worker* after that is created by the server calling `os.fork()`
internally, not by a fresh exec per worker — so a worker inherits open descriptors from the
*server* by fork semantics, not exec semantics. This only matters if the fork server process
itself ever held an acquired `ExecutionOwner` at the moment it forks a worker, which is the same
unsupported fork case described below, not a new one; it is noted here only so `'forkserver'` is
not mistakenly read as sharing `'spawn'`'s exec-only safety. `subprocess`'s own internal
`posix_spawn` fast path is not even reachable under the `close_fds=True` this project requires.

Explicitly **unsupported**, not defended against: calling `os.fork()` directly, or using
`multiprocessing`'s `'fork'` start method, from a process holding an acquired `ExecutionOwner`.
`exec()` never runs in that path, so `O_CLOEXEC` gives no protection at all — the subreaper
attribute a process may set on itself is not inherited by fork/clone children either, so there is
no OS mechanism here that helps. Measured directly with a real `os.fork()` (performed only by
test code, never production code): a child that immediately closes its inherited duplicate cannot
revoke the parent's ownership, but a child that keeps its duplicate open can keep the lock held
even after the nominal owner process has fully exited — a real, demonstrated *availability*
problem, not a hypothetical one. It is not split-brain in the sense of two processes each
correctly believing they hold independent, divergent ownership: both descriptors still refer to
the exact same underlying open file description and the exact same single `flock`, so there is
never more than one lock. The actual failure is that a legitimate successor is blocked from ever
acquiring, because an orphaned duplicate it knows nothing about is still keeping that one lock
held — a liveness/availability defect, not a correctness split.

Explicit descriptor passing or duplication (`pass_fds` naming this descriptor,
`os.set_inheritable` on it, stdio redirection of it, a `preexec_fn` that manipulates it) is
likewise unsupported. An owner process that replaces its own image via `exec()` keeps the same PID
but loses the `O_CLOEXEC`'d descriptor as a side effect of that call; the resulting program holds
no ownership and must construct a fresh `ExecutionOwner` and acquire/recover from scratch.

## Unsupported deployment cases

Not claimed, in addition to the above: hard-link aliases of the database or sidecar (`realpath`
collapses symlinks, not hard links); renaming, replacing, or retargeting a live database, sidecar,
or an intervening symlink while ownership is held; bind mounts or differing mount namespaces;
network filesystems and untested Windows-backed mounts (flock semantics there are inconsistent and
were not exercised); and any non-cooperating process that simply never calls `flock` at all — this
is an advisory lock, not enforcement. No inode registry, PID registry, `os.register_at_fork`
handler, or other general lifecycle machinery is built to work around any of this; each case is a
documented boundary instead.

## Future integration ordering (documented contract only — not implemented here)

```text
construct/validate ExecutionOwner(configured_path)
  -> acquire()
  -> MissionStore(owner.canonical_db_path)      # never the original configured_path again
  -> startup recovery
  -> permit new authorization
```

Ownership must outlive every execution-capable component built on top of it and is released last
during orderly shutdown, including cleanup after a partial initialization failure (if
`MissionStore` construction or startup recovery fails, the owner lock must still be released so a
successor is not blocked by a half-started supervisor). A context manager does not itself enforce
this: `with ExecutionOwner(path) as owner:` only guarantees `close()` runs when that specific
`with` block exits, not that a supervisor built on top keeps everything nested correctly inside
it. This task implements none of that ordering, wraps no `MissionStore` writer, and adds no
supervisor; Task 015 (thin ROS adapter) and Task 016 (minimal supervisor) remain future work.

## Semantics this task does not change

One durable send authorization is still not one physical sender; execution ownership, a fresh
send authorization returned by the current authorization call, and durable action-context/goal
binding remain three independent requirements, and reading an existing authorization row is not a
new permit. Acquiring ownership after a crash says nothing about whether an already-`DISPATCHED`
request was physically sent — startup recovery must still precede any new authorization, and an
authorized dispatch must never be resent as a recovery probe. A local timeout is still not proof
of activity, and server `UNKNOWN` is still ambiguous. Task 011's limitation — an inconclusive
restart probe against a `DISPATCHED` step cannot currently become a durable `OUTCOME_UNKNOWN` — is
unchanged; this task adds no new durable uncertainty policy. `task_resilience` remains ROS- and
process-lifecycle-independent.

Every process-kill test in this task's suite (normal exit, `os._exit`, `SIGKILL`, and the raw-fork
boundary tests) is a process-crash test, not a power-loss test, and flock support is claimed only
for the tested local Linux filesystem — not for physical safety, and not for any untested
filesystem or namespace.
