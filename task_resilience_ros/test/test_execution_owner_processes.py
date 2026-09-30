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
Real multi-process scenarios for ExecutionOwner.

Covers independent-process refusal, crash/exit release semantics, release
while the original process stays alive, real exec-child descriptor
identity, the raw-fork boundary (documented as unsupported), the
first-acquisition race, and the surviving-helper scenario. Every worker is
a real process, spawned via _execution_owner_process.py; every wait is
bounded; every descendant is reaped or its absence is otherwise checked
before the test returns.

Process-kill scenarios here are process-crash tests, not power-loss tests:
a killed process still lets the kernel run its normal fd/lock cleanup.
"""

import fcntl
import os
import select
import signal
import stat
import subprocess
import sys
import time

import pytest

from task_resilience_ros.errors import ExecutionOwnerAlreadyHeld
import task_resilience_ros.execution_owner as _execution_owner_module
from task_resilience_ros.execution_owner import ExecutionOwner

HELPER_SCRIPT = os.path.join(os.path.dirname(__file__), '_execution_owner_process.py')
BOUNDED = 10.0  # seconds, the default deadline for a well-behaved worker

# The directory that, added to sys.path, resolves task_resilience_ros to the
# EXACT source or installed artifact THIS pytest process already loaded --
# derived from the already-imported module's own __file__, never assumed or
# hard-coded as a source-tree-relative path. Every child that needs to use
# ExecutionOwner is handed this same directory via PYTHONPATH, so a run
# against an installed package exercises the installed copy end to end
# instead of silently falling back to the source tree in every child.
PACKAGE_PARENT_DIR = os.path.dirname(os.path.dirname(_execution_owner_module.__file__))


def _child_env():
    env = dict(os.environ)
    existing = env.get('PYTHONPATH', '')
    env['PYTHONPATH'] = (
        PACKAGE_PARENT_DIR + os.pathsep + existing if existing else PACKAGE_PARENT_DIR)
    return env


def spawn(*role_args, stdin=True):
    return subprocess.Popen(
        [sys.executable, HELPER_SCRIPT, *role_args],
        stdin=subprocess.PIPE if stdin else None,
        stdout=subprocess.PIPE, text=True, env=_child_env())


def read_line_bounded(proc: subprocess.Popen, deadline_s: float, label: str) -> str:
    """
    Read a single line from proc.stdout, or raise TimeoutError.

    A prior version of this helper ran proc.stdout.readline() in a daemon
    thread bounded only by Thread.join(deadline_s). join() returning on
    timeout does not cancel the underlying blocking read: the thread stays
    alive, blocked in readline(), and if the peer eventually writes, that
    abandoned thread silently consumes the line no caller ever sees. This
    version never starts a thread at all: it reads the RAW file descriptor
    directly (bypassing proc.stdout's own TextIOWrapper/BufferedReader
    entirely, so there is no hidden Python-level buffer for select() to
    disagree with) using select() plus non-blocking os.read() calls against
    one monotonic deadline, accumulating bytes in an explicit buffer stored
    on the Popen object itself so a partial line received in one call is
    not discarded before the next call completes it.
    """
    if not hasattr(proc, '_bounded_read_buf'):
        proc._bounded_read_buf = bytearray()
    buf = proc._bounded_read_buf
    fd = proc.stdout.fileno()
    deadline = time.monotonic() + deadline_s
    while b'\n' not in buf:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(
                f'{label}: no line within {deadline_s}s (partial: {bytes(buf)!r})')
        ready, _, _ = select.select([fd], [], [], remaining)
        if not ready:
            continue
        chunk = os.read(fd, 4096)
        if not chunk:
            raise AssertionError(
                f'{label}: process closed stdout (EOF) while waiting; '
                f'partial: {bytes(buf)!r}')
        buf.extend(chunk)
    line, _, rest = buf.partition(b'\n')
    proc._bounded_read_buf = bytearray(rest)
    return line.decode()


def wait_with_deadline(proc: subprocess.Popen, deadline_s: float, label: str) -> int:
    """
    Wait for proc to exit, killing and reaping it on timeout.

    subprocess.Popen.wait() already polls internally; this just gives a
    clear, labeled failure instead of letting a misbehaving worker hang
    the whole test session.
    """
    try:
        return proc.wait(timeout=deadline_s)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)
        raise AssertionError(f'{label}: did not exit within {deadline_s}s; killed and reaped')


def acquire_with_deadline(owner: ExecutionOwner, deadline_s: float) -> None:
    """
    Retry owner.acquire() until it succeeds or a bounded deadline passes.

    Signalling or commanding a process to release does not make its exit
    (and the kernel's release of its fd) instantaneous from an outside
    observer; poll for the condition with a bounded deadline rather than
    assuming a single acquire() attempt lands after the right amount of
    real time has passed -- the same style Task 013's own SIGKILL probe
    used (a bounded polling deadline, not a fixed sleep as readiness
    proof).
    """
    deadline = time.monotonic() + deadline_s
    while True:
        try:
            owner.acquire()
            return
        except ExecutionOwnerAlreadyHeld:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.02)


def send(proc: subprocess.Popen, command: str) -> None:
    proc.stdin.write(command + '\n')
    proc.stdin.flush()


def _ensure_dead(proc: subprocess.Popen) -> None:
    """
    Best-effort cleanup for a spawned process in a finally block.

    Used where cleanup ownership must be established immediately at spawn
    time (so any exception between spawn and a test's own graceful
    shutdown sequence still results in the process being killed and
    reaped), and also as a defensive fallback after a graceful shutdown
    attempt whose own send()/read() could itself raise -- that failure
    must not skip reaping the process it was trying to shut down.
    """
    if proc.poll() is None:
        proc.kill()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        pass


def try_lock_externally(lock_path: str):
    """
    Attempt a raw flock from THIS process, independent of ExecutionOwner.

    Used where the test needs to probe contention without constructing a
    full owner (e.g. against a path whose parent may not exist).
    """
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return fd
    except BlockingIOError:
        os.close(fd)
        return None


class _FifoOpenTimedOut(Exception):
    """Internal signal-handler-raised marker; never escapes open_fifo_bounded."""


class UnsupportedCallingContext(Exception):
    """Raised by open_fifo_bounded when its single-caller precondition is violated."""


def open_fifo_bounded(path: str, flags: int, deadline_s: float, label: str) -> int:
    """
    Open a FIFO with a genuine bound, preserving real blocking semantics.

    Two prior versions of this helper were each wrong in a different way.
    (1) A daemon thread running a plain blocking os.open(), bounded only by
    Thread.join(deadline_s): join() timing out does not cancel the
    underlying open(); the thread stays blocked in the kernel, and if a
    peer arrives later, that thread obtains a real fd no caller ever
    receives or closes -- a genuine leak, not a timeout. (2) Adding
    O_NONBLOCK and retrying: an O_RDONLY|O_NONBLOCK open of a FIFO
    succeeds immediately REGARDLESS of whether a writer is connected yet,
    which means a reader that opens this way, then reads, can observe zero
    current writers and get a spurious immediate EOF instead of the
    genuine wait for a writer this protocol depends on -- a real, measured
    race, not just an unlikely timing coincidence.

    This version keeps the plain, correct BLOCKING open() call (which
    natively waits for a peer exactly the way this protocol needs) and
    bounds it with a real, interrupting SIGALRM timer instead of a thread:
    if no peer arrives in time, the pending open() is interrupted and
    fails cleanly, with no descriptor ever allocated to leak, and no
    partner-side race introduced. A delayed real peer still connects
    successfully well within the deadline, and the elapsed time matches
    the peer's actual delay, not a busy-poll interval (see
    test_open_fifo_bounded_success_matches_actual_peer_delay).

    SUPPORTED CALLING CONTEXT, ENFORCED, NOT JUST DOCUMENTED: single-
    threaded, one in-flight call at a time, from a process with no OTHER
    active ITIMER_REAL of its own. An earlier version merely documented
    this as a limitation while still silently overwriting -- and then
    CANCELLING, not pausing and resuming -- any pre-existing caller timer:
    measured directly, a caller's own 60s itimer became (0.0, 0.0) after
    a call to this function returned, with only the signal HANDLER
    restored, not the itimer's remaining time. This version instead
    checks for an active pre-existing itimer FIRST, before touching any
    signal state at all, and refuses with UnsupportedCallingContext if one
    is found -- so every case this function actually accepts genuinely
    has both its handler and its itimer state fully restored afterward,
    rather than silently claiming that for a case where it wasn't true.
    """
    if signal.getitimer(signal.ITIMER_REAL) != (0.0, 0.0):
        raise UnsupportedCallingContext(
            f'{label}: a SIGALRM ITIMER_REAL is already active in this '
            f'process; open_fifo_bounded would cancel it (not pause and '
            f'resume it) -- refusing before touching any signal state, '
            f'rather than silently clobbering an unrelated caller timer')

    def on_alarm(signum, frame):
        raise _FifoOpenTimedOut()

    old_handler = signal.signal(signal.SIGALRM, on_alarm)
    signal.setitimer(signal.ITIMER_REAL, deadline_s)
    try:
        return os.open(path, flags)
    except _FifoOpenTimedOut:
        raise TimeoutError(
            f'{label}: os.open({path!r}) did not connect within {deadline_s}s') from None
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, old_handler)


# --------------------------------------------------------------------------
# open_fifo_bounded's own properties: the signal-timeout restriction is
# enforced (not merely documented), and the success/timeout/late-peer
# behavior this whole design depends on is a real, rerunnable test here,
# not only a one-off manual probe from an earlier review pass.
# --------------------------------------------------------------------------

def test_open_fifo_bounded_rejects_active_pre_existing_timer():
    signal.setitimer(signal.ITIMER_REAL, 60.0)
    before_itimer = signal.getitimer(signal.ITIMER_REAL)
    before_handler = signal.getsignal(signal.SIGALRM)
    try:
        with pytest.raises(UnsupportedCallingContext):
            open_fifo_bounded(
                '/nonexistent/path/does/not/matter', os.O_RDONLY, 1.0, 'precondition')
        # refusal happens BEFORE any signal state is touched: the
        # caller's own timer and handler must be completely unchanged,
        # not merely "restored to some value" -- still counting down from
        # close to 60s, never reset or cancelled.
        after_itimer = signal.getitimer(signal.ITIMER_REAL)
        assert signal.getsignal(signal.SIGALRM) == before_handler
        assert after_itimer[0] > 0, f'caller itimer must not be cancelled: {after_itimer}'
        assert after_itimer[0] <= before_itimer[0], after_itimer
        assert before_itimer[0] - after_itimer[0] < 5.0, (
            f'unexpectedly large gap, suggesting something else ran: '
            f'before={before_itimer} after={after_itimer}')
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, signal.SIG_DFL)


def test_open_fifo_bounded_success_matches_actual_peer_delay(tmp_path):
    path = str(tmp_path / 'f.fifo')
    os.mkfifo(path)
    pid = os.fork()
    if pid == 0:
        time.sleep(0.3)
        fd = os.open(path, os.O_WRONLY)
        os.write(fd, b'x')
        os.close(fd)
        os._exit(0)
    try:
        t0 = time.monotonic()
        fd = open_fifo_bounded(path, os.O_RDONLY, BOUNDED, 'delayed real peer')
        elapsed = time.monotonic() - t0
        try:
            assert os.read(fd, 1) == b'x'
        finally:
            os.close(fd)
        # matches the peer's ACTUAL ~0.3s delay -- not near-instant (which
        # would mean a busy-poll or non-blocking race), and not anywhere
        # near the full BOUNDED deadline (which would mean it only
        # returned because of a timeout, not a genuine connection).
        assert 0.2 <= elapsed <= 2.0, elapsed
    finally:
        os.waitpid(pid, 0)


def test_open_fifo_bounded_missing_peer_times_out(tmp_path):
    path = str(tmp_path / 'f.fifo')
    os.mkfifo(path)
    t0 = time.monotonic()
    with pytest.raises(TimeoutError):
        open_fifo_bounded(path, os.O_RDONLY, 0.3, 'no peer ever arrives')
    elapsed = time.monotonic() - t0
    assert 0.25 <= elapsed <= 2.0, elapsed


def test_open_fifo_bounded_late_peer_after_timeout_does_not_leak_fd(tmp_path):
    path = str(tmp_path / 'f.fifo')
    os.mkfifo(path)
    before = _open_fd_count()
    with pytest.raises(TimeoutError):
        open_fifo_bounded(path, os.O_RDONLY, 0.3, 'no peer, about to time out')
    assert _open_fd_count() == before, 'no descriptor may be allocated on timeout'

    # a genuine late peer, arriving after this caller already gave up:
    # a real blocking writer, in a forked child, connecting only once we
    # open a reader ourselves below.
    pid = os.fork()
    if pid == 0:
        fd = os.open(path, os.O_WRONLY)  # blocks until a reader connects
        os.write(fd, b'late')
        os.close(fd)
        os._exit(0)
    try:
        time.sleep(0.3)  # let the late writer actually be blocked, waiting
        assert _open_fd_count() == before, (
            'this process must not have leaked an fd just because a late '
            'peer is blocked waiting -- we never opened anything for it')
        fd = open_fifo_bounded(path, os.O_RDONLY, BOUNDED, 'cleanup read')
        try:
            assert os.read(fd, 64) == b'late'
        finally:
            os.close(fd)
        assert _open_fd_count() == before
    finally:
        os.waitpid(pid, 0)


# --------------------------------------------------------------------------
# Import provenance: a real ownership-using child resolves to the SAME
# execution_owner artifact as the pytest parent, measured, not assumed
# --------------------------------------------------------------------------

def test_child_worker_imports_the_same_execution_owner_artifact_as_pytest(tmp_path):
    db_path = str(tmp_path / 'mission.db')
    child = subprocess.Popen(
        [sys.executable, HELPER_SCRIPT, 'hold-wait', db_path],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, env=_child_env())
    try:
        assert read_line_bounded(child, BOUNDED, 'child ready') == 'READY'
    finally:
        send(child, 'EXIT')
        wait_with_deadline(child, BOUNDED, 'child cleanup')

    _, stderr = child.communicate(timeout=BOUNDED)
    line = next(
        (ln for ln in stderr.splitlines() if ln.startswith('EXECUTION-OWNER-FILE ')), None)
    assert line is not None, f'child never reported its import origin; stderr={stderr!r}'
    child_file = line[len('EXECUTION-OWNER-FILE '):]
    assert child_file == _execution_owner_module.__file__, (
        'an ownership-using child must resolve execution_owner to the exact '
        'same artifact this pytest process itself loaded, not a different '
        f'source/installed copy: parent={_execution_owner_module.__file__!r} '
        f'child={child_file!r}')


# --------------------------------------------------------------------------
# Immediate independent-process refusal
# --------------------------------------------------------------------------

def test_second_real_process_refused_immediately(tmp_path):
    db_path = str(tmp_path / 'mission.db')
    holder = spawn('hold-wait', db_path)
    try:
        assert read_line_bounded(holder, BOUNDED, 'holder ready') == 'READY'

        contender = spawn('hold-wait', db_path)
        try:
            line = read_line_bounded(contender, BOUNDED, 'contender outcome')
            parts = line.split()
            assert parts[0] == 'ALREADY-HELD', line
            canonical_db_path, lock_path = parts[1], parts[2]
            assert canonical_db_path == str((tmp_path / 'mission.db').resolve())
            assert lock_path == canonical_db_path + '.lock'
            rc = wait_with_deadline(contender, BOUNDED, 'contender exit')
            assert rc == 1
        finally:
            _ensure_dead(contender)
    finally:
        try:
            send(holder, 'EXIT')
            wait_with_deadline(holder, BOUNDED, 'holder cleanup')
        finally:
            _ensure_dead(holder)


# --------------------------------------------------------------------------
# Crash/exit release semantics (process-crash tests, not power-loss tests)
# --------------------------------------------------------------------------

def test_normal_process_exit_releases(tmp_path):
    db_path = str(tmp_path / 'mission.db')
    holder = spawn('hold-wait', db_path)
    try:
        assert read_line_bounded(holder, BOUNDED, 'holder ready') == 'READY'
        send(holder, 'EXIT')  # no explicit CLOSE first: release via ordinary exit
        assert wait_with_deadline(holder, BOUNDED, 'holder exit') == 0
    finally:
        _ensure_dead(holder)

    fresh = ExecutionOwner(db_path)
    fresh.acquire()
    fresh.close()


def test_os_exit_releases(tmp_path):
    db_path = str(tmp_path / 'mission.db')
    holder = spawn('hold-os-exit', db_path, stdin=False)
    try:
        assert read_line_bounded(holder, BOUNDED, 'holder ready') == 'READY'
        assert wait_with_deadline(holder, BOUNDED, 'holder os._exit') == 0
    finally:
        _ensure_dead(holder)

    fresh = ExecutionOwner(db_path)
    fresh.acquire()
    fresh.close()


def test_sigkill_releases(tmp_path):
    db_path = str(tmp_path / 'mission.db')
    holder = spawn('hold-wait', db_path)
    try:
        assert read_line_bounded(holder, BOUNDED, 'holder ready') == 'READY'
        holder.send_signal(signal.SIGKILL)
        rc = wait_with_deadline(holder, BOUNDED, 'holder SIGKILL')
        assert rc == -signal.SIGKILL
    finally:
        _ensure_dead(holder)

    fresh = ExecutionOwner(db_path)
    fresh.acquire()
    fresh.close()


# --------------------------------------------------------------------------
# Release while the ORIGINAL process remains alive (a real second process)
# --------------------------------------------------------------------------

def test_release_while_original_process_remains_alive(tmp_path):
    db_path = str(tmp_path / 'mission.db')
    holder = spawn('hold-wait', db_path)
    try:
        assert read_line_bounded(holder, BOUNDED, 'holder ready') == 'READY'
        send(holder, 'CLOSE')
        assert read_line_bounded(holder, BOUNDED, 'holder closed') == 'CLOSED'
        # the holder PROCESS is still alive and blocked on its next command;
        # a real, independent SECOND process must be able to acquire now.
        assert holder.poll() is None, 'holder must still be alive at this point'

        second = spawn('hold-wait', db_path)
        try:
            assert read_line_bounded(second, BOUNDED, 'second ready') == 'READY'
            send(second, 'EXIT')
            wait_with_deadline(second, BOUNDED, 'second cleanup')
        finally:
            _ensure_dead(second)
    finally:
        try:
            send(holder, 'EXIT')
            wait_with_deadline(holder, BOUNDED, 'holder cleanup')
        finally:
            _ensure_dead(holder)


# --------------------------------------------------------------------------
# Real exec-child descriptor identity (fd identity, not "assume unused")
# --------------------------------------------------------------------------

def test_exec_child_default_close_fds_cannot_fstat_parent_fd(tmp_path):
    """
    An exec'd child with default close_fds=True cannot use the parent's fd.

    Checked by device/inode identity, not by assuming the fd number is
    simply unused in the child: close_fds=True does not guarantee no
    OTHER file the child opens for its own unrelated purposes ever lands
    on that same integer (an interpreter's own early file opens, for
    instance, could plausibly claim it). The observed non-inheritance is
    not attributed solely to the explicit O_CLOEXEC flag this module
    passes -- os.open() already returns a non-inheritable descriptor by
    default (PEP 446, Python 3.4+), so this check cannot separate the two
    mechanisms; see test_exec_child_close_fds_false_still_cannot_use_
    parent_fd_by_identity, which isolates that question directly.
    """
    db_path = str(tmp_path / 'mission.db')
    owner = ExecutionOwner(db_path)
    owner.acquire()
    try:
        fd = owner._fd
        real_dev, real_ino = os.fstat(fd).st_dev, os.fstat(fd).st_ino
        code = (
            'import os, sys\n'
            f'fd = {fd}\n'
            'try:\n'
            '    st = os.fstat(fd)\n'
            '    print("FD_VISIBLE", st.st_dev, st.st_ino)\n'
            'except OSError as e:\n'
            '    print("FD_NOT_VISIBLE", str(e))\n'
        )
        result = subprocess.run(
            [sys.executable, '-c', code], capture_output=True, text=True, timeout=BOUNDED)
        assert result.returncode == 0, (result.stdout, result.stderr)
        parts = result.stdout.split()
        assert parts[0] in ('FD_VISIBLE', 'FD_NOT_VISIBLE'), result.stdout
        if parts[0] == 'FD_VISIBLE':
            seen_dev, seen_ino = int(parts[1]), int(parts[2])
            assert (seen_dev, seen_ino) != (real_dev, real_ino), (
                'an unrelated file may legitimately reuse this fd NUMBER '
                'in the child, but it must never be OUR actual lock file')

        # the child's OWN independent attempt on the same path is refused --
        # ordinary contention, unrelated to descriptor inheritance. Import
        # provenance comes from PYTHONPATH (via _child_env(), derived from
        # this pytest process's own already-imported module), never from an
        # inline sys.path.insert hard-coded to the source tree.
        contention_code = (
            f'from task_resilience_ros.execution_owner import ExecutionOwner\n'
            f'from task_resilience_ros.errors import ExecutionOwnerAlreadyHeld\n'
            f'owner = ExecutionOwner({db_path!r})\n'
            f'try:\n'
            f'    owner.acquire()\n'
            f'    print("ACQUIRED")\n'
            f'except ExecutionOwnerAlreadyHeld:\n'
            f'    print("REFUSED")\n'
        )
        result2 = subprocess.run(
            [sys.executable, '-c', contention_code], env=_child_env(),
            capture_output=True, text=True, timeout=BOUNDED)
        assert result2.returncode == 0, (result2.stdout, result2.stderr)
        assert result2.stdout.strip() == 'REFUSED', result2.stdout + result2.stderr
    finally:
        owner.close()


def test_exec_child_close_fds_false_still_cannot_use_parent_fd_by_identity(tmp_path):
    db_path = str(tmp_path / 'mission.db')
    owner = ExecutionOwner(db_path)
    owner.acquire()
    try:
        fd = owner._fd
        real_dev, real_ino = os.fstat(fd).st_dev, os.fstat(fd).st_ino
        # close_fds=False disables subprocess's OWN blanket protection for
        # every other fd, isolating what THIS descriptor's own inheritance
        # state actually is. This verifies observed non-inheritance -- the
        # child must show the fd is EITHER not visible, or visible as a
        # DIFFERENT file (never our actual lock file) by device/inode -- and
        # does not attribute that non-inheritance to any specific mechanism:
        # os.open() itself, independent of the explicit os.O_CLOEXEC flag
        # this module also happens to pass, already returns non-inheritable
        # descriptors by default (PEP 446, Python 3.4+), so the two are not
        # separable by this observation alone.
        code = (
            'import os, sys\n'
            f'fd = {fd}\n'
            'try:\n'
            '    st = os.fstat(fd)\n'
            '    print("FD_VISIBLE", st.st_dev, st.st_ino)\n'
            'except OSError as e:\n'
            '    print("FD_NOT_VISIBLE", str(e))\n'
        )
        result = subprocess.run(
            [sys.executable, '-c', code], close_fds=False,
            capture_output=True, text=True, timeout=BOUNDED)
        assert result.returncode == 0, (result.stdout, result.stderr)
        parts = result.stdout.split()
        assert parts[0] in ('FD_VISIBLE', 'FD_NOT_VISIBLE'), result.stdout
        if parts[0] == 'FD_VISIBLE':
            seen_dev, seen_ino = int(parts[1]), int(parts[2])
            assert (seen_dev, seen_ino) != (real_dev, real_ino), (
                'the child must never see OUR lock file through this fd '
                'number, even under close_fds=False')
    finally:
        owner.close()


# --------------------------------------------------------------------------
# The raw-fork boundary (documented as UNSUPPORTED, not a new promise)
# --------------------------------------------------------------------------

def test_raw_fork_well_behaved_child_does_not_release_parents_lock(tmp_path):
    """
    A well-behaved forked child must not revoke the parent's ownership.

    Uses a REAL os.fork(), performed by a dedicated disposable test
    process (never production code), acting on the ACTUAL ExecutionOwner
    class -- not a raw-flock stand-in -- so this is a regression test of
    this implementation's own descriptor, not of flock() in general: a
    child that immediately closes its inherited fd duplicate cannot revoke
    the parent's ownership.

    The forked child reports its OWN pid alongside its readiness line: if
    a failure here (e.g. the contention check below) leads to THIS
    outer role process (proc) being killed before its own internal reap
    loop ever runs, the forked child -- a real, separate process, not
    automatically terminated when its parent dies -- would otherwise be
    orphaned, still blocked reading done_fifo forever. The reported pid is
    what lets the finally block below fall back to a direct, ancestry-real
    PID signal in that case, never a command-line-substring guess.
    """
    db_path = str(tmp_path / 'mission.db')
    ready_fifo = str(tmp_path / 'ready.fifo')
    done_fifo = str(tmp_path / 'done.fifo')
    os.mkfifo(ready_fifo)
    os.mkfifo(done_fifo)

    proc = spawn('raw-fork-well-behaved', db_path, ready_fifo, done_fifo, stdin=False)
    child_pid = None
    try:
        assert read_line_bounded(proc, BOUNDED, 'parent ready') == 'PARENT-READY'
        ready_fd = open_fifo_bounded(ready_fifo, os.O_RDONLY, BOUNDED, 'well-behaved child ready')
        try:
            child_says = os.read(ready_fd, 64).decode().strip()
        finally:
            os.close(ready_fd)
        label, child_pid_str = child_says.split()
        assert label == 'CHILD-READY', child_says
        child_pid = int(child_pid_str)

        contender = ExecutionOwner(db_path)
        with pytest.raises(ExecutionOwnerAlreadyHeld):
            contender.acquire()
        contender.close()  # never acquired; safe no-op

        done_fd = open_fifo_bounded(done_fifo, os.O_WRONLY, BOUNDED, 'done fifo')
        try:
            os.write(done_fd, b'go\n')
        finally:
            os.close(done_fd)
        assert read_line_bounded(proc, BOUNDED, 'parent done') == 'PARENT-DONE'
        assert wait_with_deadline(proc, BOUNDED, 'raw-fork-well-behaved exit') == 0

        fresh = ExecutionOwner(db_path)
        fresh.acquire()  # lock must be free once both parent and child are gone
        fresh.close()
    finally:
        child_reaped_by_parent = False
        if proc.poll() is None:
            # Best-effort: unblock the forked child through the normal
            # protocol first, giving proc's own bounded internal reap loop
            # (the only thing that can actually waitpid() its own forked
            # child) a chance to complete normally.
            try:
                done_fd = open_fifo_bounded(done_fifo, os.O_WRONLY, 2.0, 'cleanup done fifo')
                try:
                    os.write(done_fd, b'go\n')
                finally:
                    os.close(done_fd)
            except (TimeoutError, OSError):
                pass
            try:
                wait_with_deadline(proc, BOUNDED, 'raw-fork-well-behaved cleanup exit')
                child_reaped_by_parent = True
            except AssertionError:
                proc.kill()
                proc.wait(timeout=5)
        else:
            # proc already exited normally; it already reaped its own child.
            child_reaped_by_parent = True

        if not child_reaped_by_parent and child_pid is not None:
            # proc was killed before it could reap its own forked child:
            # that child is now an orphan pytest cannot waitpid() (not its
            # parent) -- fall back to a direct, PID-based signal, never a
            # command-line match, and confirm departure by polling
            # kill(pid, 0), the same checked pattern the raw-fork
            # "keeps fd" scenario already uses for an orphan it cannot
            # waitpid() either.
            try:
                os.kill(child_pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            deadline = time.monotonic() + BOUNDED
            while time.monotonic() < deadline:
                try:
                    os.kill(child_pid, 0)
                    time.sleep(0.02)
                except ProcessLookupError:
                    break
            else:
                raise AssertionError(f'forked child {child_pid} was not confirmed gone')


def test_raw_fork_keeps_fd_documents_the_unsupported_direction(tmp_path):
    """
    Document, not newly promise, that a fork-kept fd outlives the owner.

    Uses a REAL os.fork(), performed by a dedicated disposable test
    process, acting on the ACTUAL ExecutionOwner class: this documents a
    known limitation -- raw fork while holding ownership is unsupported --
    it is not a new support promise. A child that keeps its inherited
    duplicate open can keep the lock held after the nominal owner process
    has exited.
    """
    db_path = str(tmp_path / 'mission.db')
    ready_fifo = str(tmp_path / 'ready.fifo')
    os.mkfifo(ready_fifo)

    proc = spawn('raw-fork-keeps-fd', db_path, ready_fifo, stdin=False)
    child_pid = None
    try:
        assert read_line_bounded(proc, BOUNDED, 'parent exiting') == 'PARENT-EXITING'
        assert wait_with_deadline(proc, BOUNDED, 'raw-fork-keeps-fd parent exit') == 0

        ready_fd = open_fifo_bounded(ready_fifo, os.O_RDONLY, BOUNDED, 'keeps-fd child ready')
        try:
            child_pid = int(os.read(ready_fd, 64).decode().strip())
        finally:
            os.close(ready_fd)

        contender = ExecutionOwner(db_path)
        with pytest.raises(ExecutionOwnerAlreadyHeld):
            contender.acquire()
        contender.close()  # never acquired; safe no-op
        # documents the unsupported boundary: the orphaned child still holds
        # a live duplicate of the lock even though its nominal owner
        # process has already exited.
    finally:
        _ensure_dead(proc)  # covers an early failure above, before proc's own exit
        if child_pid is not None:
            try:
                os.kill(child_pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            # not our child (it was never reparented to us): we cannot
            # waitpid() it, so confirm its departure by polling kill(pid, 0)
            # instead, which is the checked, bounded equivalent available here.
            deadline = time.monotonic() + BOUNDED
            gone = False
            while time.monotonic() < deadline:
                try:
                    os.kill(child_pid, 0)
                    time.sleep(0.05)
                except ProcessLookupError:
                    gone = True
                    break
            assert gone, f'orphaned raw-fork child {child_pid} was not confirmed gone'

    fresh = ExecutionOwner(db_path)
    fresh.acquire()
    fresh.close()


# --------------------------------------------------------------------------
# First-acquisition race: initially unheld, K contenders released together
# --------------------------------------------------------------------------

def test_first_acquisition_race_exactly_one_winner(tmp_path):
    db_path = str(tmp_path / 'mission.db')
    k = 8
    # Built incrementally, not via a single list-comprehension expression:
    # if spawn() itself raised partway through (e.g. the 5th of 8), a
    # one-shot comprehension would never assign `workers` at all, leaking
    # every already-spawned process with nothing left to clean them up --
    # cleanup ownership must be established immediately at every spawn,
    # not only once the whole batch has succeeded.
    workers = []
    try:
        for _ in range(k):
            workers.append(spawn('race-worker', db_path))
        for w in workers:
            assert read_line_bounded(w, BOUNDED, 'worker ready') == 'READY'

        # release every contender before any of them has tried to acquire
        for w in workers:
            send(w, 'GO')

        outcomes = [read_line_bounded(w, BOUNDED, 'worker outcome') for w in workers]
        wins = outcomes.count('WON')
        losses = outcomes.count('LOST')
        assert wins == 1, f'expected exactly one winner, got {wins}: {outcomes}'
        assert losses == k - 1, f'expected {k - 1} losers, got {losses}: {outcomes}'

        winner = workers[outcomes.index('WON')]
        # the winner is still holding right now: verify contention before release
        contender = try_lock_externally(str((tmp_path / 'mission.db').resolve()) + '.lock')
        assert contender is None, 'the winner must still hold the lock at this point'

        send(winner, 'RELEASE')
        assert read_line_bounded(winner, BOUNDED, 'winner released') == 'RELEASED'
        for w in workers:
            assert wait_with_deadline(w, BOUNDED, 'race worker exit') == 0

        # verify successful acquisition AFTER every outcome was collected and released
        fresh = try_lock_externally(str((tmp_path / 'mission.db').resolve()) + '.lock')
        assert fresh is not None
        os.close(fresh)
    finally:
        for w in workers:
            _ensure_dead(w)


# --------------------------------------------------------------------------
# Surviving-helper scenario, subreaper strictly in a dedicated controller
# --------------------------------------------------------------------------

def _shutdown_controller(controller, deadline_s):
    """
    Ask the controller to wind down, and wait for it to actually exit.

    The controller -- not this function, and not pytest -- has real
    process-ancestry authority over the owner and helper it spawned: it is
    the owner's direct parent, and becomes the helper's parent by
    subreaper adoption if the owner exits first. Its own reap sweep
    (role_subreaper_controller) already terminates and reaps every
    descendant it has a handle on before it exits, even on its own
    internal timeout (see that role's docstring). So the correct action
    here is to trigger that behavior and wait for it, never to guess at
    PIDs via command-line matching. Sending 'CONTROLLER-EXIT' and closing
    stdin both work (the controller treats EOF the same way, since a
    caller in trouble may not be able to write a clean command first).
    Returns (exited: bool, returncode: int | None).
    """
    if controller.poll() is None:
        try:
            controller.stdin.write('CONTROLLER-EXIT\n')
            controller.stdin.flush()
        except (BrokenPipeError, ValueError, OSError):
            pass
        try:
            controller.stdin.close()
        except (BrokenPipeError, ValueError, OSError):
            pass
    try:
        rc = controller.wait(timeout=deadline_s)
        return True, rc
    except subprocess.TimeoutExpired:
        return False, None


def _kill_chain_best_effort(controller, owner_pid, helper_pid, search_path=None):
    """
    Emergency cleanup for a chain that failed partway through setup or use.

    Primary path: ask the controller to wind down and wait (bounded) for
    it to exit -- its own internal logic guarantees no descendant of its
    own is left running by the time it does, regardless of whether THIS
    caller ever learned owner_pid/helper_pid itself (an earlier version of
    this function needed those PIDs to act at all, which fails exactly
    when a setup failure unwinds through the very read call that would
    have captured one of them). Only if the controller itself does not
    exit within the bound does this function escalate: SIGKILL whatever
    PIDs this caller already knows (never a pgrep/command-line-substring
    guess), then kill the controller directly. search_path, if given, is
    used ONLY for an independent, informational post-check via pgrep --
    never as authority for deciding what to signal -- so a caller can
    still notice and report a stray survivor even in the escalated path.

    Returns a dict describing what actually happened, so a caller can
    distinguish cleanup performed by the tested harness (the controller
    itself, 'escalated': False) from emergency cleanup performed by this
    guard ('escalated': True) -- the two are not the same outcome and
    should not be reported as if they were.
    """
    exited, rc = _shutdown_controller(controller, deadline_s=15.0)
    if exited:
        return {'controller_rc': rc, 'escalated': False, 'stray_after_escalation': None}

    for pid in (helper_pid, owner_pid):
        if pid is not None:
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
    controller.kill()
    try:
        rc = controller.wait(timeout=5)
    except subprocess.TimeoutExpired:
        rc = None

    stray = None
    if search_path is not None:
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            result = subprocess.run(
                ['pgrep', '-f', str(search_path)], capture_output=True, text=True)
            stray = result.stdout.split()
            if not stray:
                break
            time.sleep(0.05)
    return {'controller_rc': rc, 'escalated': True, 'stray_after_escalation': stray}


def _start_chain(tmp_path):
    """
    Spawn the subreaper-controller -> owner-with-helper -> helper chain.

    Every setup step is wrapped so that a failure partway through (the
    exact case a reviewer reproduced: an exception raised right after the
    helper PID was received, before this function had returned anything to
    the caller) still cleans up whatever of the chain already exists --
    the CALLER's own try/finally cannot protect against a failure that
    happens inside this function, since it has not been entered yet.
    """
    db_path = str(tmp_path / 'mission.db')
    ping_fifo = str(tmp_path / 'ping.fifo')
    pong_fifo = str(tmp_path / 'pong.fifo')
    os.mkfifo(ping_fifo)
    os.mkfifo(pong_fifo)
    controller = spawn('subreaper-controller', db_path, ping_fifo, pong_fifo)
    owner_pid = None
    helper_pid = None
    try:
        assert read_line_bounded(controller, BOUNDED, 'subreaper set').startswith('SUBREAPER-SET')
        line = read_line_bounded(controller, BOUNDED, 'owner pid')
        assert 'OWNER-PID' in line
        owner_pid = int(line.split()[-1])
        line = read_line_bounded(controller, BOUNDED, 'owner ready')
        assert 'OWNER-READY' in line
        line = read_line_bounded(controller, BOUNDED, 'helper pid')
        assert 'HELPER-PID' in line
        helper_pid = int(line.split()[-1])
    except BaseException:
        _kill_chain_best_effort(controller, owner_pid, helper_pid, search_path=tmp_path)
        raise
    return controller, db_path, ping_fifo, pong_fifo, owner_pid, helper_pid


def _read_reap_report(controller, deadline_s):
    """
    Read the reap report through its final status line.

    Returns (reaped, owner_exit_code, status).
    """
    reaped = []
    owner_exit_code = None
    while True:
        line = read_line_bounded(controller, deadline_s, 'reap report')
        parts = line.split()
        if parts[0] == 'CONTROLLER-REAPED-PID':
            reaped.append((int(parts[1]), int(parts[2])))
        elif parts[0] == 'CONTROLLER-OWNER-EXIT-STATUS':
            owner_exit_code = int(parts[1])
        elif parts[0] == 'CONTROLLER-REAP-STATUS':
            return reaped, owner_exit_code, parts[1]
        else:
            raise AssertionError(f'unexpected line while reading reap report: {line!r}')


def _rendezvous_with_helper(controller, ping_fifo, pong_fifo):
    """
    Open both rendezvous FIFOs, or leak neither.

    Each fd is protected immediately after its own open() call; ownership
    is transferred to the caller (by returning it) only once BOTH opens
    and the readiness read have succeeded. A prior version returned the
    pair only at the very end, so a failure in the readiness read (after
    ping_fd was already open) or in the second open() (after ping_fd was
    already open and the readiness line already consumed) leaked ping_fd
    with no reference anywhere left to close it.
    """
    # The helper's own os.open(ping_fifo, O_RDONLY) is already blocked;
    # opening it for writing here is what unblocks it, and is therefore a
    # prerequisite for it ever printing its own READY line -- this must
    # happen BEFORE reading that relayed line, not after.
    ping_fd = open_fifo_bounded(ping_fifo, os.O_WRONLY, BOUNDED, 'ping fifo rendezvous')
    try:
        line = read_line_bounded(controller, BOUNDED, 'helper says ready')
        assert 'READY' in line
        pong_fd = open_fifo_bounded(pong_fifo, os.O_RDONLY, BOUNDED, 'pong fifo rendezvous')
    except BaseException:
        os.close(ping_fd)
        raise
    return ping_fd, pong_fd


def _open_fd_count() -> int:
    return len(os.listdir('/proc/self/fd'))


def test_rendezvous_failure_after_first_open_does_not_leak_ping_fd(tmp_path, monkeypatch):
    controller, db_path, ping_fifo, pong_fifo, owner_pid, helper_pid = _start_chain(tmp_path)
    try:
        real_read_line_bounded = read_line_bounded

        def fake_read_line_bounded(proc, deadline_s, label):
            if label == 'helper says ready':
                raise RuntimeError('injected failure after ping_fd is already open')
            return real_read_line_bounded(proc, deadline_s, label)

        monkeypatch.setattr(sys.modules[__name__], 'read_line_bounded', fake_read_line_bounded)
        before = _open_fd_count()
        with pytest.raises(RuntimeError, match='injected failure after ping_fd is already open'):
            _rendezvous_with_helper(controller, ping_fifo, pong_fifo)
        assert _open_fd_count() == before, 'ping_fd must not leak when the readiness read fails'
    finally:
        monkeypatch.undo()
        result = _kill_chain_best_effort(controller, owner_pid, helper_pid, search_path=tmp_path)
        assert not result['escalated'], result


def test_rendezvous_failure_during_second_open_does_not_leak_ping_fd(tmp_path, monkeypatch):
    controller, db_path, ping_fifo, pong_fifo, owner_pid, helper_pid = _start_chain(tmp_path)
    try:
        real_open_fifo_bounded = open_fifo_bounded

        def fake_open_fifo_bounded(path, flags, deadline_s, label):
            if path == pong_fifo:
                raise TimeoutError('injected failure opening the second (pong) fifo')
            return real_open_fifo_bounded(path, flags, deadline_s, label)

        monkeypatch.setattr(sys.modules[__name__], 'open_fifo_bounded', fake_open_fifo_bounded)
        before = _open_fd_count()
        with pytest.raises(TimeoutError, match='injected failure opening the second'):
            _rendezvous_with_helper(controller, ping_fifo, pong_fifo)
        assert _open_fd_count() == before, 'ping_fd must not leak when the second open fails'
    finally:
        monkeypatch.undo()
        # The helper is left genuinely stuck in its own blocking
        # os.open(pong_fifo, O_WRONLY) (our injected failure happened
        # before anyone ever opened pong_fifo for reading), which is
        # exactly the "unresponsive descendant" case the controller's own
        # emergency termination path (role_subreaper_controller) exists
        # for -- no graceful pytest-side handshake is possible here, and
        # none is attempted; the assertion below confirms the controller's
        # OWN cleanup was sufficient without this guard needing to escalate.
        result = _kill_chain_best_effort(controller, owner_pid, helper_pid, search_path=tmp_path)
        assert not result['escalated'], result


def _ping_pong(ping_fd, pong_fd, nonce: str):
    os.write(ping_fd, f'PING:{nonce}\n'.encode())
    r, _, _ = select.select([pong_fd], [], [], BOUNDED)
    assert r, f'no PONG:{nonce} within {BOUNDED}s'
    resp = os.read(pong_fd, 64).decode().strip()
    assert resp == f'PONG:{nonce}', resp


def test_surviving_helper_owner_close_while_alive(tmp_path):
    controller, db_path, ping_fifo, pong_fifo, owner_pid, helper_pid = _start_chain(tmp_path)
    try:
        ping_fd, pong_fd = _rendezvous_with_helper(controller, ping_fifo, pong_fifo)
        try:
            send(controller, 'CLOSE')
            assert 'OWNER-CLOSED' in read_line_bounded(controller, BOUNDED, 'owner closed')

            # do NOT wait for the owner process to exit -- it stays alive.
            fresh = ExecutionOwner(db_path)
            fresh.acquire()  # must succeed immediately after close(), not exit
            fresh.close()

            # the helper must not have had to exit for this to work: prove
            # it with a fresh nonce-matched PING/PONG taken AFTER acquisition.
            _ping_pong(ping_fd, pong_fd, 'nonce-close-while-alive')

            os.write(ping_fd, b'EXIT\n')
            # subcase order: the OWNER reaps its own helper before exiting.
            send(controller, 'REAP-HELPER')
            assert 'HELPER-REAPED' in read_line_bounded(controller, BOUNDED, 'helper reaped')
            send(controller, 'EXIT')
        finally:
            os.close(ping_fd)
            os.close(pong_fd)

        send(controller, 'CONTROLLER-EXIT')
        reaped, owner_exit_code, status = _read_reap_report(controller, BOUNDED)
        assert status == 'COMPLETE', (status, reaped)
        assert owner_exit_code == 0, owner_exit_code
        assert reaped == [], (
            f'the owner should already have reaped its own helper: {reaped}')
        assert wait_with_deadline(controller, BOUNDED, 'controller exit') == 0
    finally:
        result = _kill_chain_best_effort(controller, owner_pid, helper_pid, search_path=tmp_path)
        assert not result['escalated'], (
            f'the controller should already have exited cleanly on its own: {result}')


def test_surviving_helper_owner_process_exit(tmp_path):
    controller, db_path, ping_fifo, pong_fifo, owner_pid, helper_pid = _start_chain(tmp_path)
    try:
        ping_fd, pong_fd = _rendezvous_with_helper(controller, ping_fifo, pong_fifo)
        try:
            # subcase order: the owner exits directly, WITHOUT reaping its
            # helper first -- that is the scenario under test.
            send(controller, 'EXIT')

            fresh = ExecutionOwner(db_path)
            # the EXIT command was just forwarded through the controller;
            # the owner's actual process exit is not instantaneous from here.
            acquire_with_deadline(fresh, BOUNDED)
            fresh.close()

            _ping_pong(ping_fd, pong_fd, 'nonce-process-exit')

            os.write(ping_fd, b'EXIT\n')
        finally:
            os.close(ping_fd)
            os.close(pong_fd)

        send(controller, 'CONTROLLER-EXIT')
        reaped, owner_exit_code, status = _read_reap_report(controller, BOUNDED)
        assert status == 'COMPLETE', (status, reaped)
        assert owner_exit_code == 0, owner_exit_code
        # not "any nonempty list" -- the SPECIFIC expected helper, cleanly exited.
        reaped_pids = dict(reaped)
        assert helper_pid in reaped_pids, (
            f'expected helper pid {helper_pid} in the reap report, got {reaped}')
        assert reaped_pids[helper_pid] == 0, (
            f'helper pid {helper_pid} exited nonzero: {reaped_pids[helper_pid]}')
        assert wait_with_deadline(controller, BOUNDED, 'controller exit') == 0
    finally:
        result = _kill_chain_best_effort(controller, owner_pid, helper_pid, search_path=tmp_path)
        assert not result['escalated'], (
            f'the controller should already have exited cleanly on its own: {result}')


# --------------------------------------------------------------------------
# Abnormal cleanup through the ACTUAL chain (subreaper-controller ->
# owner-with-helper -> helper). The harness-only tests further below prove
# read_line_bounded/select timeouts fail loudly, but they use a bare
# sleeping process or a fake helper stand-in; neither one exercises this
# implementation's own controller/owner/helper cleanup paths. These do.
# --------------------------------------------------------------------------

def test_chain_setup_failure_cleans_up_partial_chain(tmp_path, monkeypatch):
    """
    Regression test for reviewer-reproduced bug A.

    Injecting a failure inside _start_chain right after the helper PID line
    was received (but before _start_chain had returned anything to its
    caller, so the CALLER's own try/finally could not yet be in scope) used
    to leave the controller, owner, and helper all still running. Confirm
    that _start_chain's OWN internal cleanup now leaves nothing behind, by
    checking -- via an independent, PID-free mechanism -- that no process
    anywhere still has this test's unique throwaway tmp_path open in its
    command line.
    """
    # _start_chain's 4 reads, in order, are: subreaper-set, owner-pid,
    # owner-ready, helper-pid. The 4th call is the one whose successful
    # return is what lets _start_chain assign helper_pid -- firing after
    # THAT one (not the 3rd, 'owner ready') is what makes this a genuine
    # regression test of bug A: an earlier, off-by-one version of this
    # fault injection fired one call too early, before helper_pid was ever
    # captured, so _kill_chain_best_effort had no PID to SIGKILL directly
    # and had to rely on the owner being killed before it finished spawning
    # the helper -- a real race that intermittently left an orphaned helper
    # process behind, caught only by rerunning this test repeatedly.
    real_read_line_bounded = read_line_bounded
    calls = {'n': 0}

    def fake_read_line_bounded(proc, deadline_s, label):
        line = real_read_line_bounded(proc, deadline_s, label)
        calls['n'] += 1
        if calls['n'] == 4:  # right after the helper-pid line is read
            raise RuntimeError('injected startup observation failure')
        return line

    monkeypatch.setattr(sys.modules[__name__], 'read_line_bounded', fake_read_line_bounded)

    with pytest.raises(RuntimeError, match='injected startup observation failure'):
        _start_chain(tmp_path)

    # An independent check, scoped to this test's own unique tmp_path (never
    # a broad process-name kill/search): no controller, owner, or helper
    # process may still reference it after _start_chain's own cleanup ran.
    deadline = time.monotonic() + BOUNDED
    survivors = ''
    while time.monotonic() < deadline:
        result = subprocess.run(
            ['pgrep', '-f', str(tmp_path)], capture_output=True, text=True)
        survivors = result.stdout.strip()
        if not survivors:
            break
        time.sleep(0.05)
    assert not survivors, (
        f'process(es) still reference {tmp_path} after a setup failure: {survivors!r}')


def test_missing_fifo_peer_through_real_chain_times_out_and_cleans_up(tmp_path):
    """
    A rendezvous FIFO peer that never arrives must time out, not hang.

    The chain must still be fully cleanable afterward -- exercised through
    the real controller/owner/helper chain, not a standalone FIFO probe.
    """
    controller, db_path, ping_fifo, pong_fifo, owner_pid, helper_pid = _start_chain(tmp_path)
    result = None
    try:
        # The real helper is already blocked in os.open(ping_fifo, O_RDONLY)
        # (see role_helper). Deliberately never open ping_fifo for writing:
        # the "missing peer" is this test itself withholding the rendezvous.
        with pytest.raises(TimeoutError):
            open_fifo_bounded(pong_fifo, os.O_RDONLY, 0.5, 'pong fifo, no peer')
    finally:
        result = _kill_chain_best_effort(controller, owner_pid, helper_pid, search_path=tmp_path)

    # The controller's own cleanup should suffice here -- this guard should
    # not need to escalate -- but if it did, the escalation path's own
    # post-check (already performed inside _kill_chain_best_effort) is what
    # is trusted, distinguishing which cleanup actually ran.
    assert not result['escalated'], (
        f'the controller should already have exited cleanly on its own: {result}')
    deadline = time.monotonic() + BOUNDED
    for pid in (helper_pid, owner_pid):
        while time.monotonic() < deadline:
            try:
                os.kill(pid, 0)
                time.sleep(0.02)
            except ProcessLookupError:
                break
        else:
            raise AssertionError(f'pid {pid} was not confirmed gone after cleanup')
    assert controller.poll() is not None, 'controller must not still be running'


def test_withholding_helper_exit_makes_controller_reap_incomplete(tmp_path):
    """
    Regression test for reviewer-reproduced bug B, revised on a second round.

    Letting the owner exit but withholding the helper's own EXIT command
    used to make the ORIGINAL controller print an empty, successful-
    looking reap report and exit 0, even though the helper was still alive
    and unreaped. An intermediate fix made the controller correctly report
    INCOMPLETE/nonzero, but then REQUIRED the helper to survive the
    controller's own exit so pytest could independently observe and clean
    it up -- which itself just relocated the escaped-orphan problem one
    level up. The corrected controller now does both at once: it reports
    the correct diagnosis (INCOMPLETE -- the natural, bounded reap sweep
    genuinely did not finish in time) AND actively terminates and reaps
    every descendant it still has a handle on before it ever exits, so
    there is nothing left over for pytest to discover or clean up
    afterward. This test now asserts BOTH halves: the honest failure
    diagnosis, and that the specific expected helper is confirmed gone by
    the time the controller itself has exited.
    """
    controller, db_path, ping_fifo, pong_fifo, owner_pid, helper_pid = _start_chain(tmp_path)
    try:
        ping_fd, pong_fd = _rendezvous_with_helper(controller, ping_fifo, pong_fifo)
        try:
            send(controller, 'EXIT')  # owner exits; helper is orphaned, unreaped
            fresh = ExecutionOwner(db_path)
            acquire_with_deadline(fresh, BOUNDED)
            fresh.close()

            # the exact withheld step the original bug depended on: no EXIT
            # is ever sent to the helper before the controller is told to
            # finish up.
            send(controller, 'CONTROLLER-EXIT')
            reaped, owner_exit_code, status = _read_reap_report(controller, BOUNDED)
            assert status == 'INCOMPLETE', (status, reaped)
            assert owner_exit_code == 0, owner_exit_code
            rc = wait_with_deadline(controller, BOUNDED, 'controller exit (expected failure)')
            assert rc == 1, rc

            # the controller's OWN emergency termination+reap must already
            # have caught the helper by the time it exits: the SPECIFIC
            # expected helper pid, not "any nonempty list", appears in the
            # reap report, and a direct kill(pid, 0) check finds it
            # genuinely gone -- not merely reported gone.
            reaped_pids = dict(reaped)
            assert helper_pid in reaped_pids, (
                f'expected helper pid {helper_pid} in the reap report after '
                f"the controller's own emergency cleanup, got {reaped}")
            deadline = time.monotonic() + BOUNDED
            while time.monotonic() < deadline:
                try:
                    os.kill(helper_pid, 0)
                    time.sleep(0.02)
                except ProcessLookupError:
                    break
            else:
                raise AssertionError(f'helper pid {helper_pid} was not confirmed gone')
        finally:
            os.close(ping_fd)
            os.close(pong_fd)
    finally:
        # the controller should already have exited cleanly (with its own
        # cleanup already done) by this point; this guard should not need
        # to escalate, and asserts that distinction rather than silently
        # accepting either outcome as the same "cleaned up" result.
        result = _kill_chain_best_effort(controller, owner_pid, helper_pid, search_path=tmp_path)
        assert not result['escalated'], (
            f'the controller should already have exited cleanly on its own: {result}')


# --------------------------------------------------------------------------
# Abnormal harness paths: an intentional readiness failure, and a
# deliberately unresponsive helper -- proving the harness itself fails
# loudly rather than quietly releasing ownership / reporting a false pass.
# --------------------------------------------------------------------------

def test_harness_detects_a_worker_that_never_reports_ready(tmp_path):
    # A worker that sleeps well past the bounded deadline before ever
    # printing anything: read_line_bounded must raise, not hang or pass.
    proc = subprocess.Popen(
        [sys.executable, '-c', 'import time; time.sleep(30)'],
        stdout=subprocess.PIPE, text=True)
    try:
        with pytest.raises(TimeoutError):
            read_line_bounded(proc, 0.5, 'deliberately silent worker')
    finally:
        proc.kill()
        proc.wait(timeout=5)


def test_harness_detects_an_unresponsive_helper(tmp_path):
    # A "helper" that opens both FIFOs correctly (so the rendezvous itself
    # succeeds) but never answers PING: the PONG wait must time out and be
    # treated as a failure, never silently accepted as success.
    ping_fifo = str(tmp_path / 'ping.fifo')
    pong_fifo = str(tmp_path / 'pong.fifo')
    os.mkfifo(ping_fifo)
    os.mkfifo(pong_fifo)
    code = (
        'import os, time, sys\n'
        f'ping_fd = os.open({ping_fifo!r}, os.O_RDONLY)\n'
        'print("READY", flush=True)\n'
        f'pong_fd = os.open({pong_fifo!r}, os.O_WRONLY)\n'
        'time.sleep(30)\n'  # never reads or answers a PING
    )
    proc = subprocess.Popen([sys.executable, '-c', code], stdout=subprocess.PIPE, text=True)
    try:
        ping_fd = open_fifo_bounded(ping_fifo, os.O_WRONLY, BOUNDED, 'unresponsive helper ping')
        assert read_line_bounded(proc, BOUNDED, 'unresponsive helper ready') == 'READY'
        pong_fd = open_fifo_bounded(pong_fifo, os.O_RDONLY, BOUNDED, 'unresponsive helper pong')
        os.write(ping_fd, b'PING:should-time-out\n')
        r, _, _ = select.select([pong_fd], [], [], 0.5)
        assert not r, 'the deliberately unresponsive helper must NOT produce a PONG'
        os.close(ping_fd)
        os.close(pong_fd)
    finally:
        proc.kill()
        proc.wait(timeout=5)


# --------------------------------------------------------------------------
# No SQLite/database side effects from any of the above (spot check)
# --------------------------------------------------------------------------

def test_owner_with_helper_role_never_creates_a_database_file(tmp_path):
    db_path = str(tmp_path / 'mission.db')
    ping_fifo = str(tmp_path / 'ping.fifo')
    pong_fifo = str(tmp_path / 'pong.fifo')
    os.mkfifo(ping_fifo)
    os.mkfifo(pong_fifo)
    proc = spawn('owner-with-helper', db_path, ping_fifo, pong_fifo)
    try:
        assert 'OWNER-PID' in read_line_bounded(proc, BOUNDED, 'owner pid')
        assert read_line_bounded(proc, BOUNDED, 'owner ready') == 'OWNER-READY'
        line = read_line_bounded(proc, BOUNDED, 'helper pid')
        assert 'HELPER-PID' in line
        ping_fd = open_fifo_bounded(ping_fifo, os.O_WRONLY, BOUNDED, 'db-check ping')
        try:
            assert 'READY' in read_line_bounded(proc, BOUNDED, 'helper says')
            pong_fd = open_fifo_bounded(pong_fifo, os.O_RDONLY, BOUNDED, 'db-check pong')
            try:
                os.write(ping_fd, b'EXIT\n')
            finally:
                os.close(pong_fd)
        finally:
            os.close(ping_fd)
        send(proc, 'REAP-HELPER')
        assert 'HELPER-REAPED' in read_line_bounded(proc, BOUNDED, 'helper reaped')
        assert not os.path.exists(db_path)
    finally:
        send(proc, 'EXIT')
        wait_with_deadline(proc, BOUNDED, 'owner-with-helper cleanup')
        assert not os.path.exists(db_path)
        assert stat.S_ISREG(os.stat(db_path + '.lock').st_mode)
