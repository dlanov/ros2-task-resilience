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
Private, test-only worker roles for test_execution_owner_processes.py.

Not a test module itself (the leading underscore keeps pytest from
collecting it) and not installed runtime code: it is invoked only as
`python3 _execution_owner_process.py <role> ...` by the test suite, via
subprocess.Popen with its own stdin/stdout kept as pipes.

Every role speaks a line-based protocol over its own stdin/stdout
(complete, newline-terminated, flushed messages -- a message is never
considered sent until the newline and an explicit flush). An unexpected
EOF on stdin, or a command outside a role's known set, is a PROTOCOL
FAILURE: every role reports it distinctly and exits nonzero rather than
looping forever or treating it as an ordinary shutdown. Roles that must be
reachable independently of their process ancestry (the "helper" role,
which may outlive the process that spawned it) instead use named FIFOs
given on the command line, since a FIFO's path -- unlike a pipe fd -- is
addressable by any process that knows it, regardless of who its current
parent is.
"""

import ctypes
import os
import select
import signal
import subprocess
import sys
import threading
import time

# No sys.path.insert here: an earlier version unconditionally pointed at the
# source tree ('..' relative to this file), which would silently resolve to
# the SOURCE copy of task_resilience_ros even during a validation run meant
# to exercise an INSTALLED artifact. Import resolution instead depends
# entirely on PYTHONPATH as inherited from this process's own parent (see
# _child_env() in test_execution_owner_processes.py), so every
# ownership-using child -- this process, and any grandchild it in turn
# spawns -- resolves to the exact same origin the top-level pytest process
# itself already selected.
from task_resilience_ros.errors import ExecutionOwnerAlreadyHeld
from task_resilience_ros.execution_owner import ExecutionOwner

# Unconditional, self-reported import provenance: every invocation of this
# script -- regardless of role -- announces the actual file it resolved
# execution_owner to, on stderr (never stdout, which carries the line
# protocol the test suite parses). This is what makes source/installed
# child provenance a measurement rather than an assumption.
_owner_module = sys.modules[ExecutionOwner.__module__]
sys.stderr.write(f'EXECUTION-OWNER-FILE {_owner_module.__file__}\n')
sys.stderr.flush()

PR_SET_CHILD_SUBREAPER = 36
PROTOCOL_ERROR = 2  # distinct from 0 (ok) and 1 (expected-refusal outcomes)


def _say(*parts: object) -> None:
    print(*parts, flush=True)


class _ProtocolError(Exception):
    """Raised, then turned into a distinct exit code, on EOF or an unknown command."""


def _read_command() -> str:
    line = sys.stdin.readline()
    if line == '':
        raise _ProtocolError('unexpected EOF on stdin')
    return line.rstrip('\n')


def _read_line_bounded_or_none(pipe, deadline_s: float):
    """
    Read one line from a subprocess pipe within a bound, or return None.

    Mirrors the pytest-side read_line_bounded's own reasoning (raw fd +
    select + os.read, bypassing the TextIOWrapper's hidden buffer) for the
    same reason: a plain, unbounded pipe.readline() here is exactly the
    abandoned-blocking-operation pattern already fixed on the pytest side.
    Without this, a helper that never becomes ready (e.g. its own FIFO
    peer never arrives) leaves THIS owner process blocked indefinitely
    waiting for a readiness line that will never come -- which in turn
    forces the controller above to fall back to its own internal timeout
    and SIGKILL, burning most of a caller's bounded shutdown budget on a
    wait that could have failed fast instead. Returns None on timeout or
    EOF; never raises for either of those two ordinary outcomes.
    """
    fd = pipe.fileno()
    buf = bytearray()
    deadline = time.monotonic() + deadline_s
    while b'\n' not in buf:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return None
        ready, _, _ = select.select([fd], [], [], remaining)
        if not ready:
            continue
        chunk = os.read(fd, 4096)
        if not chunk:
            return None
        buf.extend(chunk)
    line, _, _ = buf.partition(b'\n')
    return line.decode()


def _command_loop(handlers: dict) -> int:
    """
    Read one command at a time and dispatch it.

    EOF or an unrecognized command is a protocol failure (distinct exit
    code), never an endless loop and never treated as a successful
    shutdown. A handler returning a non-None value ends the loop with that
    value as the exit code.
    """
    try:
        while True:
            command = _read_command()
            handler = handlers.get(command)
            if handler is None:
                _say('UNKNOWN-COMMAND', command)
                return PROTOCOL_ERROR
            result = handler()
            if result is not None:
                return result
    except _ProtocolError as e:
        _say('PROTOCOL-ERROR', str(e))
        return PROTOCOL_ERROR


def role_hold_wait(db_path: str) -> int:
    """Acquire, report READY, then act on CLOSE/EXIT commands from stdin."""
    owner = ExecutionOwner(db_path)
    try:
        owner.acquire()
    except ExecutionOwnerAlreadyHeld as e:
        _say('ALREADY-HELD', e.canonical_db_path, e.lock_path)
        return 1
    _say('READY')

    def on_close():
        owner.close()
        _say('CLOSED')
        return None

    def on_exit():
        return 0

    return _command_loop({'CLOSE': on_close, 'EXIT': on_exit})


def role_hold_os_exit(db_path: str) -> int:
    """Acquire, report READY, then terminate via os._exit (no cleanup)."""
    owner = ExecutionOwner(db_path)
    owner.acquire()
    _say('READY')
    sys.stdout.flush()
    os._exit(0)


def role_race_worker(db_path: str) -> int:
    """Report READY, wait for a validated GO, race to acquire, report WON/LOST."""
    _say('READY')
    go = sys.stdin.readline()
    if go.rstrip('\n') != 'GO':
        _say('PROTOCOL-ERROR', f'expected GO, got {go!r}')
        return PROTOCOL_ERROR
    owner = ExecutionOwner(db_path)
    try:
        owner.acquire()
    except ExecutionOwnerAlreadyHeld:
        _say('LOST')
        return 0
    _say('WON')
    release = sys.stdin.readline()
    if release.rstrip('\n') != 'RELEASE':
        _say('PROTOCOL-ERROR', f'expected RELEASE, got {release!r}')
        owner.close()
        return PROTOCOL_ERROR
    owner.close()
    _say('RELEASED')
    return 0


def role_helper(ping_fifo: str, pong_fifo: str) -> int:
    """
    Answer PING:<nonce> with PONG:<nonce> until told EXIT.

    Spawned BY an owner but addressable independently of it via the given
    FIFOs. Proves genuine execution liveness (a real request/response
    round trip), not merely that its PID still exists.
    """
    ping_fd = os.open(ping_fifo, os.O_RDONLY)  # blocks until a writer opens it
    _say('READY')
    pong_fd = os.open(pong_fifo, os.O_WRONLY)  # blocks until a reader opens it
    ping_file = os.fdopen(ping_fd, 'r')
    try:
        while True:
            line = ping_file.readline()
            if not line:
                _say('PROTOCOL-ERROR', 'unexpected EOF on ping_fifo')
                return PROTOCOL_ERROR
            line = line.rstrip('\n')
            if line == 'EXIT':
                break
            if line.startswith('PING:'):
                nonce = line[len('PING:'):]
                os.write(pong_fd, f'PONG:{nonce}\n'.encode())
            else:
                _say('PROTOCOL-ERROR', f'unknown ping_fifo command: {line!r}')
                return PROTOCOL_ERROR
    finally:
        ping_file.close()
        os.close(pong_fd)
    return 0


def role_owner_with_helper(db_path: str, ping_fifo: str, pong_fifo: str) -> int:
    """
    Acquire, spawn a 'helper' child, and act on CLOSE/REAP-HELPER/EXIT.

    The helper is spawned as this process's OWN child (so it reparents to
    whatever subreaper is above this process if this process exits first),
    and its stdout lines are relayed upward so a caller can observe the
    helper's OWN readiness signal rather than a claim made on its behalf.
    This process's own PID is reported first (OWNER-PID) so a caller that
    needs to clean up a partially-started chain has a concrete PID to act
    on even if a later setup step fails.
    """
    _say('OWNER-PID', os.getpid())
    owner = ExecutionOwner(db_path)
    owner.acquire()
    _say('OWNER-READY')

    helper_proc = subprocess.Popen(
        [sys.executable, os.path.abspath(__file__), 'helper', ping_fifo, pong_fifo],
        stdout=subprocess.PIPE, text=True)
    _say('HELPER-PID', helper_proc.pid)
    helper_ready_line = _read_line_bounded_or_none(helper_proc.stdout, 8.0)
    if helper_ready_line is None:
        _say('PROTOCOL-ERROR', 'helper produced no readiness line within 8.0s')
        helper_proc.kill()
        helper_proc.wait(timeout=5)
        return PROTOCOL_ERROR
    _say('HELPER-SAYS', helper_ready_line)

    def on_close():
        owner.close()
        _say('OWNER-CLOSED')
        return None

    def on_reap_helper():
        try:
            helper_proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            helper_proc.kill()
            helper_proc.wait(timeout=5)
            _say('PROTOCOL-ERROR', 'helper did not exit within 10s of being told EXIT')
            return PROTOCOL_ERROR
        _say('HELPER-REAPED')
        return None

    def on_exit():
        return 0

    return _command_loop({
        'CLOSE': on_close,
        'REAP-HELPER': on_reap_helper,
        'EXIT': on_exit,
    })


def role_subreaper_controller(db_path: str, ping_fifo: str, pong_fifo: str) -> int:
    """
    Become a subreaper for this process's own descendants, then relay.

    Becomes a subreaper for ITS OWN descendants only (never the shared
    pytest process) -- checked, not merely attempted: a failed prctl()
    call ends this role immediately with a distinct failure, since nothing
    downstream can be trusted otherwise. Spawns 'owner-with-helper' as its
    child and relays its stdout/stdin one hop each way, while also parsing
    the owner's relayed HELPER-PID line itself (not merely forwarding it
    as opaque text), so this role has its OWN tracked handle on the helper
    even if pytest never successfully reads that line. Every command from
    pytest EXCEPT the final 'CONTROLLER-EXIT' is forwarded to the owner and
    the controller's own command loop keeps running afterward -- otherwise,
    forwarding the owner's own 'EXIT' would also end the controller before
    pytest has had a chance to deal with the (possibly now-orphaned)
    helper, defeating the reap sweep below. Only after 'CONTROLLER-EXIT'
    (or EOF on stdin -- pytest closing the pipe counts as the same
    request, since a caller in trouble may not be able to write a clean
    command first) does this role wait for the owner and perform its final
    reap sweep.

    That sweep reports COMPLETE only when os.waitpid(-1, ...) itself
    reports no children remain at all (ECHILD) before its own deadline; if
    the deadline expires with a child still outstanding, the FAILURE is
    recorded as INCOMPLETE regardless of what happens next -- but this
    role does not then simply exit and leave that descendant for pytest to
    discover as an escaped orphan. It actively terminates every
    descendant it has a handle on (the owner, directly; the helper, via
    the PID parsed from the owner's own relay) and performs one more
    bounded reap pass, so the reported INCOMPLETE/nonzero outcome reflects
    a real diagnostic about the deadline, not a claim that a process is
    still running when it no longer is. The owner's own exit status is
    also checked explicitly, not discarded: an owner that exits any way
    other than cleanly (SIGKILLed out from under this controller, for
    instance, after its helper was already reaped through the ordinary
    REAP-HELPER path) is itself a reported failure, even when the reap
    sweep alone would otherwise read as COMPLETE.
    """
    libc = ctypes.CDLL(None, use_errno=True)
    rc = libc.prctl(PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0)
    if rc != 0:
        _say('SUBREAPER-SET-FAILED', rc)
        return PROTOCOL_ERROR
    _say('SUBREAPER-SET', rc)

    owner_proc = subprocess.Popen(
        [sys.executable, os.path.abspath(__file__), 'owner-with-helper',
         db_path, ping_fifo, pong_fifo],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    tracked = {'helper_pid': None}

    def relay_owner_stdout():
        for line in owner_proc.stdout:
            stripped = line.rstrip('\n')
            _say('OWNER-SAYS', stripped)
            parts = stripped.split()
            if len(parts) == 2 and parts[0] == 'HELPER-PID':
                try:
                    tracked['helper_pid'] = int(parts[1])
                except ValueError:
                    pass

    relay_thread = threading.Thread(target=relay_owner_stdout, daemon=True)
    relay_thread.start()

    while True:
        command = sys.stdin.readline()
        if command == '' or command.rstrip('\n') == 'CONTROLLER-EXIT':
            break
        try:
            owner_proc.stdin.write(command)
            owner_proc.stdin.flush()
        except BrokenPipeError:
            pass  # the owner already exited; nothing left to forward to

    # Close the owner's stdin as soon as this relay ends: an owner still
    # idling in its own command loop, waiting for a command that will
    # never come (e.g. this controller was told to wind down before any
    # EXIT was ever forwarded), sees EOF immediately and exits through its
    # own protocol-error path -- rather than this controller having to
    # wait out its full internal timeout for a response that was never
    # coming, which would otherwise eat most of a caller's own bounded
    # shutdown budget before this role's reap sweep even starts.
    try:
        owner_proc.stdin.close()
    except (BrokenPipeError, ValueError, OSError):
        pass

    try:
        owner_exit_code = owner_proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        owner_proc.kill()
        owner_exit_code = owner_proc.wait(timeout=5)
    relay_thread.join(timeout=5)

    # Bounded reap sweep. One line per reaped pid (easy for a caller to
    # parse unambiguously), then exactly one final status line.
    deadline = time.monotonic() + 5.0
    natural_complete = False
    while True:
        try:
            pid, status = os.waitpid(-1, os.WNOHANG)
        except ChildProcessError:
            natural_complete = True  # no children remain at all: genuine completion
            break
        if pid != 0:
            _say('CONTROLLER-REAPED-PID', pid, os.waitstatus_to_exitcode(status))
            continue  # immediately check for more, without waiting out the sleep below
        if time.monotonic() >= deadline:
            break  # deadline hit with a child still outstanding: NOT complete
        time.sleep(0.02)

    if not natural_complete:
        # Record failure -> terminate outstanding descendants -> reap them
        # -> exit nonzero. Never exit first and leave pytest to discover
        # an escaped orphan: use the actual handles this role has (its own
        # direct child, and the helper PID it parsed above), not a
        # command-line guess, since only real ancestry can be waited on.
        for pid in (owner_proc.pid, tracked['helper_pid']):
            if pid is None:
                continue
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        kill_deadline = time.monotonic() + 5.0
        while time.monotonic() < kill_deadline:
            try:
                pid, status = os.waitpid(-1, os.WNOHANG)
            except ChildProcessError:
                break
            if pid != 0:
                _say('CONTROLLER-REAPED-PID', pid, os.waitstatus_to_exitcode(status))
                continue
            time.sleep(0.02)

    _say('CONTROLLER-OWNER-EXIT-STATUS', owner_exit_code)
    _say('CONTROLLER-REAP-STATUS', 'COMPLETE' if natural_complete else 'INCOMPLETE')
    return 0 if (natural_complete and owner_exit_code == 0) else 1


def role_raw_fork_well_behaved(db_path: str, ready_fifo: str, done_fifo: str) -> int:
    """
    Fork a child that closes its inherited fd; the parent keeps its own.

    Uses the ACTUAL ExecutionOwner class, not a raw-flock stand-in: this is
    a regression test of this implementation's own descriptor, not of
    flock() in general. A dedicated, disposable subprocess performs a REAL
    os.fork() itself (never production code). fork() duplicates this
    process's entire memory, including the `owner` Python object -- the
    CHILD gets its OWN separate copy of that object, whose _fd happens to
    be the same integer as the parent's, referring to the same underlying
    open file description. The forked CHILD calls owner.close() on ITS OWN
    copy -- exercising the real close() path -- which closes only the
    child's fd-table entry for that number, never the parent's; this is
    the well-behaved direction, proving a correctly-behaving child cannot
    revoke the parent's ownership merely by existing. The parent's own
    wait for the child is itself bounded (WNOHANG polled to a deadline,
    then SIGKILL and a final blocking reap) rather than a bare, unbounded
    os.waitpid(pid, 0) -- this worker is meant to be bounded end to end,
    not merely bounded by whatever eventually kills the OUTER subprocess.

    The child reports its OWN pid alongside its readiness line
    (CHILD-READY <pid>): if the OUTER test process fails after this
    handshake but before writing to done_fifo, and then kills this whole
    role process (this PARENT) before its own reap loop below ever runs,
    the forked child is orphaned with no one left to waitpid() it. The
    reported pid lets the outer test fall back to a direct, ancestry-real
    PID signal in that case, never a command-line-substring guess.
    """
    owner = ExecutionOwner(db_path)
    owner.acquire()
    _say('PARENT-READY')
    pid = os.fork()
    if pid == 0:
        owner.close()  # well-behaved: the CHILD's own copy, its own fd number
        ready_fd = os.open(ready_fifo, os.O_WRONLY)
        os.write(ready_fd, f'CHILD-READY {os.getpid()}\n'.encode())
        os.close(ready_fd)
        done_fd = os.open(done_fifo, os.O_RDONLY)
        os.read(done_fd, 64)
        os.close(done_fd)
        os._exit(0)

    deadline = time.monotonic() + 10.0
    while True:
        reaped_pid, _status = os.waitpid(pid, os.WNOHANG)
        if reaped_pid == pid:
            break
        if time.monotonic() >= deadline:
            os.kill(pid, 9)
            os.waitpid(pid, 0)
            _say('PROTOCOL-ERROR', 'forked child did not exit in time; killed it')
            owner.close()
            return PROTOCOL_ERROR
    owner.close()  # the PARENT's own copy, unaffected by the child's close()
    _say('PARENT-DONE')
    return 0


def role_raw_fork_keeps_fd(db_path: str, ready_fifo: str) -> int:
    """
    Fork a child that keeps its inherited fd open; documents a limitation.

    Uses the ACTUAL ExecutionOwner class. A dedicated, disposable
    subprocess performs a REAL os.fork() itself, documenting the
    UNSUPPORTED direction: the forked CHILD deliberately does nothing to
    its own copy of the owner object (never calls close()), keeping its
    inherited duplicate fd open while it sleeps, while this PARENT closes
    ITS OWN copy and returns immediately -- simulating an owner that is
    gone while a forked child still holds a live duplicate of its lock.
    This is a test of a known limitation, not a new support promise;
    nothing here belongs in production code.
    """
    owner = ExecutionOwner(db_path)
    owner.acquire()
    pid = os.fork()
    if pid == 0:
        ready_fd = os.open(ready_fifo, os.O_WRONLY)
        os.write(ready_fd, f'{os.getpid()}\n'.encode())
        os.close(ready_fd)
        time.sleep(10.0)  # safety-net auto-exit if the test forgets to kill it
        os._exit(0)
    owner.close()  # the PARENT's own copy; the child's copy is untouched
    _say('PARENT-EXITING')
    return 0


def main() -> int:
    role = sys.argv[1]
    args = sys.argv[2:]
    roles = {
        'hold-wait': role_hold_wait,
        'hold-os-exit': role_hold_os_exit,
        'race-worker': role_race_worker,
        'helper': role_helper,
        'owner-with-helper': role_owner_with_helper,
        'subreaper-controller': role_subreaper_controller,
        'raw-fork-well-behaved': role_raw_fork_well_behaved,
        'raw-fork-keeps-fd': role_raw_fork_keeps_fd,
    }
    if role not in roles:
        raise SystemExit(f'unknown role: {role!r}')
    return roles[role](*args)


if __name__ == '__main__':
    raise SystemExit(main())
