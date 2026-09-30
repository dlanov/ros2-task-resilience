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

import errno
import fcntl
import os
import stat

from .errors import ExecutionOwnerAlreadyHeld


class ExecutionOwner:
    """
    One-host execution ownership for a task_resilience database.

    Backed by an advisory fcntl.flock() on a sidecar file next to the
    database, never on the database file itself. Construction validates
    and canonicalizes db_path and computes the sidecar name; it creates no
    file, acquires no lock, and opens no database connection. acquire()
    must be called, once, before any ROS send, cancel, or durable
    execution-state write against the same database. Exactly one process
    -- and, within it, exactly one instance -- may hold the lock for a
    given canonical database path at a time; a second attempt fails
    immediately and never blocks, retries, or waits.

    A future MissionStore for the same database must be constructed from
    THIS object's canonical_db_path, never from the original db_path
    re-interpreted later: an intervening os.chdir() between the two
    constructions can otherwise make them resolve to two different files
    even though the caller passed "the same" path string both times.

    Not thread-safe: construct, acquire(), and close() one instance from a
    single thread, the same restriction MissionStore documents for its own
    connection.

    Not fork-safe: do not call os.fork() (directly, or via
    multiprocessing's 'fork' start method) from a process that holds an
    acquired ExecutionOwner. A forked child shares this object's
    underlying kernel lock (the same open file description) regardless of
    how carefully it behaves, and can affect or outlive this process's
    ownership. Ordinary subprocess execution with default arguments
    (close_fds=True, no pass_fds naming this fd, no stdio redirection of
    it, no preexec_fn manipulating it) is unaffected: the lock fd is
    close-on-exec and is never visible to an exec'd child. multiprocessing's
    'spawn' start method is exec-based by the same underlying mechanism,
    but is not separately validated for this task; treat it as understood
    from source, not as a tested guarantee. 'forkserver' is NOT the same
    category: only the one persistent server process is started via exec,
    and every worker after that is produced by the server calling
    os.fork() internally, so a worker inherits descriptors from the server
    by fork semantics, not exec semantics -- this matters only if the
    server process itself ever held an acquired ExecutionOwner, which is
    the same unsupported fork case above, not a separate guarantee.
    Explicit descriptor passing (pass_fds naming this fd, os.set_inheritable
    on it, stdio redirection of it, a preexec_fn that manipulates it) is
    unsupported.

    Copying, deep-copying, or pickling this instance is unsupported, not
    prevented: there is no __copy__/__deepcopy__/__reduce__ override, and
    nothing raises if calling code does this anyway -- a plain attribute
    copy would simply duplicate the _fd bookkeeping onto a second object
    referring to the same underlying descriptor. Retaining a reference is
    necessary to manage or release ownership through it (there is no
    destructor or atexit hook, so close() can only be called on an object
    still reachable) but is not what keeps the lock held: the kernel-level
    flock and its open file description persist independently of any
    Python reference. Dropping the object without calling close() (or
    exiting a `with` block) does not close the integer descriptor or
    release the lock; it only removes any way to call close() on it later,
    leaving the descriptor open and the lock held until the process exits.
    acquired reflects only this object's own bookkeeping; it is not a
    liveness probe and not evidence about durable send authority.
    """

    def __init__(self, db_path: str | os.PathLike[str]) -> None:
        raw = os.fspath(db_path)
        if isinstance(raw, bytes):
            raise TypeError(
                f'ExecutionOwner requires a str path, not bytes: {db_path!r}')
        if raw == '':
            raise ValueError('ExecutionOwner requires a non-empty path')
        if '\x00' in raw:
            raise ValueError('ExecutionOwner path must not contain a NUL byte')
        if raw == ':memory:':
            raise ValueError("ExecutionOwner does not support ':memory:'")
        if raw.startswith('file:'):
            raise ValueError('ExecutionOwner does not support SQLite URI paths')

        canonical_db_path = self._canonicalize(raw)

        try:
            db_stat = os.stat(canonical_db_path)
        except FileNotFoundError:
            db_stat = None
        if db_stat is not None:
            if stat.S_ISDIR(db_stat.st_mode):
                raise IsADirectoryError(
                    f'database path is a directory: {canonical_db_path!r}')
            if not stat.S_ISREG(db_stat.st_mode):
                raise ValueError(
                    'database path exists and is not a regular file: '
                    f'{canonical_db_path!r} (mode={db_stat.st_mode:o})')

        self._db_path = raw
        self._canonical_db_path = canonical_db_path
        self._lock_path = canonical_db_path + '.lock'
        self._fd: int | None = None
        self._closed = False

    @staticmethod
    def _canonicalize(raw: str) -> str:
        # strict=True (3.10+) only: this package targets base Python 3.12.3,
        # so os.path.ALLOW_MISSING (a later addition) is not used here. One
        # fallback level: if the full path doesn't resolve, the parent must
        # be, and is itself validated strictly; only a genuinely absent
        # final entry is then accepted -- an existing but unresolvable
        # (dangling) final symlink is rejected instead of silently
        # returning its unresolved name.
        try:
            return os.path.realpath(raw, strict=True)
        except FileNotFoundError:
            parent = os.path.dirname(raw) or os.curdir
            base = os.path.basename(raw)
            canonical_parent = os.path.realpath(parent, strict=True)
            parent_stat = os.stat(canonical_parent)
            if not stat.S_ISDIR(parent_stat.st_mode):
                raise NotADirectoryError(
                    f'not a directory: {canonical_parent!r}') from None
            candidate = os.path.join(canonical_parent, base)
            try:
                os.lstat(candidate)
            except FileNotFoundError:
                return candidate
            raise FileNotFoundError(
                'database path is an unresolvable symlink (dangling target): '
                f'{raw!r}') from None

    @property
    def db_path(self) -> str:
        return self._db_path

    @property
    def canonical_db_path(self) -> str:
        return self._canonical_db_path

    @property
    def lock_path(self) -> str:
        return self._lock_path

    @property
    def acquired(self) -> bool:
        return self._fd is not None

    def acquire(self) -> None:
        if self._fd is not None:
            raise RuntimeError('ExecutionOwner.acquire() called while already acquired')
        if self._closed:
            raise RuntimeError('ExecutionOwner.acquire() called after close()')
        fd = os.open(
            self._lock_path,
            os.O_CREAT | os.O_RDWR | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK,
            0o644)
        try:
            st = os.fstat(fd)
            if not stat.S_ISREG(st.st_mode):
                raise ValueError(
                    'sidecar path is not a regular file: '
                    f'{self._lock_path!r} (mode={st.st_mode:o})')
            flags = fcntl.fcntl(fd, fcntl.F_GETFL)
            fcntl.fcntl(fd, fcntl.F_SETFL, flags & ~os.O_NONBLOCK)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as e:
                if e.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
                    raise ExecutionOwnerAlreadyHeld(
                        self._canonical_db_path, self._lock_path) from None
                raise
        except BaseException:
            os.close(fd)
            raise
        self._fd = fd

    def close(self) -> None:
        # Detach fd/closed state BEFORE attempting the close syscall, under
        # this class's documented single-thread lifecycle, so bookkeeping
        # stays truthful even if close() itself raises (close(2) can report
        # a deferred error while still releasing the descriptor), and a
        # repeated close() never retries a stale, possibly-reused fd.
        fd, self._fd = self._fd, None
        self._closed = True
        if fd is not None:
            os.close(fd)

    def __enter__(self) -> 'ExecutionOwner':
        self.acquire()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()
