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
Single-process tests for ExecutionOwner.

Covers construction/path validation, the acquire/close state machine, the
sidecar open contract, and fault injection. Real multi-process/fork
scenarios live in test_execution_owner_processes.py so both files stay
readable.
"""

import ast
import errno
import fcntl
import os
import pathlib
import sqlite3
import stat

import pytest

from task_resilience_ros import execution_owner
from task_resilience_ros.errors import ExecutionOwnerAlreadyHeld
from task_resilience_ros.execution_owner import ExecutionOwner

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
CORE_PACKAGE_DIR = REPO_ROOT / 'task_resilience' / 'task_resilience'
ROS_PACKAGE_DIR = REPO_ROOT / 'task_resilience_ros' / 'task_resilience_ros'


def _open_fd_count() -> int:
    return len(os.listdir('/proc/self/fd'))


def _imported_top_level_modules(py_file: pathlib.Path) -> set[str]:
    tree = ast.parse(py_file.read_text(), filename=str(py_file))
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                names.add(alias.name.split('.')[0])
        elif isinstance(node, ast.ImportFrom):
            if node.level and node.module is None:
                continue  # a bare `from . import x` names no external module
            if node.module:
                names.add(node.module.split('.')[0])
    return names


# --------------------------------------------------------------------------
# Construction: type and string-form validation
# --------------------------------------------------------------------------

def test_construct_rejects_bytes_with_type_error(tmp_path):
    with pytest.raises(TypeError):
        ExecutionOwner(str(tmp_path / 'mission.db').encode())


def test_construct_rejects_pathlike_bytes_with_type_error(tmp_path):
    class BytesPath:
        def __fspath__(self):
            return str(tmp_path / 'mission.db').encode()

    with pytest.raises(TypeError):
        ExecutionOwner(BytesPath())


def test_construct_rejects_unsupported_type_with_type_error():
    with pytest.raises(TypeError):
        ExecutionOwner(12345)


@pytest.mark.parametrize('bad', ['', ':memory:', 'file:foo.db', 'file:foo.db?mode=ro'])
def test_construct_rejects_prohibited_string_forms_with_value_error(bad):
    with pytest.raises(ValueError):
        ExecutionOwner(bad)


def test_construct_rejects_nul_byte_with_value_error(tmp_path):
    with pytest.raises(ValueError):
        ExecutionOwner(str(tmp_path / 'mission.db') + '\x00')


def test_construct_accepts_pathlike_str(tmp_path):
    p = tmp_path / 'mission.db'
    owner_str = ExecutionOwner(str(p))
    owner_pathlike = ExecutionOwner(p)
    assert owner_pathlike.canonical_db_path == owner_str.canonical_db_path
    assert owner_pathlike.lock_path == owner_str.lock_path


def test_construct_rejects_existing_directory_db_target(tmp_path):
    d = tmp_path / 'mission.db'
    d.mkdir()
    with pytest.raises(IsADirectoryError):
        ExecutionOwner(d)


def test_construct_rejects_existing_fifo_db_target(tmp_path):
    fifo = tmp_path / 'mission.db'
    os.mkfifo(fifo)
    with pytest.raises(ValueError):
        ExecutionOwner(fifo)


def test_construct_accepts_nonexistent_db_with_existing_parent(tmp_path):
    p = tmp_path / 'not_yet_created.db'
    owner = ExecutionOwner(p)
    assert owner.canonical_db_path == str(p.resolve())
    assert not p.exists()


def test_construct_does_not_create_open_or_connect_anything(tmp_path):
    p = tmp_path / 'mission.db'
    before = set(os.listdir(tmp_path))
    ExecutionOwner(p)
    after = set(os.listdir(tmp_path))
    assert before == after == set()


# --------------------------------------------------------------------------
# Path contract: realpath strictness, aliases, cwd stability
# --------------------------------------------------------------------------

def test_construct_rejects_missing_parent_with_file_not_found_error(tmp_path):
    with pytest.raises(FileNotFoundError):
        ExecutionOwner(tmp_path / 'no_such_dir' / 'mission.db')


def test_construct_rejects_nondirectory_parent_component_with_not_a_directory_error(tmp_path):
    regular_file = tmp_path / 'a_file'
    regular_file.write_bytes(b'')
    with pytest.raises(NotADirectoryError):
        ExecutionOwner(regular_file / 'mission.db')


def test_construct_rejects_symlink_loop_with_os_error(tmp_path):
    loop = tmp_path / 'loop'
    os.symlink(loop, loop)
    with pytest.raises(OSError) as exc_info:
        ExecutionOwner(loop)
    assert exc_info.value.errno == errno.ELOOP


def test_construct_rejects_permission_denied_parent(tmp_path):
    if os.geteuid() == 0:
        pytest.skip('running as root: permission denial cannot be demonstrated')
    noperm = tmp_path / 'noperm'
    noperm.mkdir(mode=0o000)
    try:
        with pytest.raises(PermissionError):
            ExecutionOwner(noperm / 'sub' / 'mission.db')
    finally:
        noperm.chmod(0o755)


def test_relative_and_absolute_same_db_resolve_identically(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    absolute = ExecutionOwner(tmp_path / 'mission.db')
    relative = ExecutionOwner('mission.db')
    assert absolute.canonical_db_path == relative.canonical_db_path
    assert absolute.lock_path == relative.lock_path


def test_cwd_change_after_construction_does_not_retroactively_change_paths(tmp_path, monkeypatch):
    other_dir = tmp_path / 'elsewhere'
    other_dir.mkdir()
    monkeypatch.chdir(tmp_path)
    owner = ExecutionOwner('mission.db')
    canonical_before = owner.canonical_db_path
    lock_before = owner.lock_path
    monkeypatch.chdir(other_dir)
    assert owner.canonical_db_path == canonical_before
    assert owner.lock_path == lock_before
    # repeated reads are stable, never re-resolved
    assert owner.canonical_db_path == canonical_before

    # Beyond property equality: acquire AFTER the cwd change, then create a
    # real throwaway SQLite database (stdlib sqlite3 only -- never
    # MissionStore; that integration is out of scope here) through the
    # object's own canonical_db_path, and confirm it lands at the identical
    # file the object was constructed against, from the ORIGINAL cwd, with
    # the sidecar beside it -- and that a second owner using that original
    # absolute path genuinely contends, not merely reports an equal string.
    try:
        owner.acquire()
        conn = sqlite3.connect(owner.canonical_db_path)
        try:
            conn.execute('CREATE TABLE t (x INTEGER)')
            conn.execute('INSERT INTO t VALUES (1)')
            conn.commit()
        finally:
            conn.close()

        db_stat = os.stat(canonical_before)
        assert os.path.samestat(db_stat, os.stat(tmp_path / 'mission.db'))
        lock_stat = os.stat(lock_before)
        assert os.path.samestat(lock_stat, os.stat(str(tmp_path / 'mission.db') + '.lock'))

        second = ExecutionOwner(tmp_path / 'mission.db')
        with pytest.raises(ExecutionOwnerAlreadyHeld):
            second.acquire()
        second.close()
    finally:
        owner.close()


def test_database_symlink_alias_resolves_to_same_canonical_path(tmp_path):
    real = tmp_path / 'real.db'
    real.write_bytes(b'')
    alias = tmp_path / 'alias.db'
    os.symlink(real, alias)
    owner_alias = ExecutionOwner(alias)
    owner_real = ExecutionOwner(real)
    assert owner_alias.canonical_db_path == owner_real.canonical_db_path
    assert owner_alias.lock_path == owner_real.lock_path
    owner_alias.acquire()
    try:
        with pytest.raises(ExecutionOwnerAlreadyHeld):
            owner_real.acquire()
    finally:
        owner_alias.close()


def test_parent_directory_symlink_alias_resolves_to_same_canonical_path(tmp_path):
    real_dir = tmp_path / 'realdir'
    real_dir.mkdir()
    alias_dir = tmp_path / 'aliasdir'
    os.symlink(real_dir, alias_dir)
    owner_alias = ExecutionOwner(alias_dir / 'mission.db')
    owner_real = ExecutionOwner(real_dir / 'mission.db')
    assert owner_alias.canonical_db_path == owner_real.canonical_db_path


def test_dangling_final_symlink_is_rejected(tmp_path):
    alias = tmp_path / 'alias.db'
    os.symlink(tmp_path / 'real.db', alias)  # target does not exist
    with pytest.raises(FileNotFoundError):
        ExecutionOwner(alias)
    assert not (tmp_path / 'alias.db.lock').exists()


def test_dangling_symlink_chain_is_rejected(tmp_path):
    os.symlink(tmp_path / 'c.db', tmp_path / 'b.db')
    os.symlink(tmp_path / 'b.db', tmp_path / 'a.db')
    with pytest.raises(FileNotFoundError):
        ExecutionOwner(tmp_path / 'a.db')


def test_dangling_parent_symlink_is_rejected(tmp_path):
    os.symlink(tmp_path / 'missing_target_dir', tmp_path / 'aliasdir')
    with pytest.raises(FileNotFoundError):
        ExecutionOwner(tmp_path / 'aliasdir' / 'mission.db')


def test_same_alias_before_and_after_first_database_creation(tmp_path):
    alias = tmp_path / 'alias.db'
    real = tmp_path / 'real.db'
    os.symlink(real, alias)
    with pytest.raises(FileNotFoundError):
        ExecutionOwner(alias)  # target does not exist yet: rejected
    real.write_bytes(b'')  # created by some means OTHER than through the alias
    owner_alias = ExecutionOwner(alias)
    owner_real = ExecutionOwner(real)
    assert owner_alias.canonical_db_path == owner_real.canonical_db_path


def test_ownership_can_be_acquired_before_ordinary_database_creation(tmp_path):
    db_path = tmp_path / 'mission.db'
    owner = ExecutionOwner(db_path)
    owner.acquire()
    try:
        assert not db_path.exists()
    finally:
        owner.close()


# --------------------------------------------------------------------------
# acquire()/close() state machine
# --------------------------------------------------------------------------

def test_first_acquire_succeeds(tmp_path):
    owner = ExecutionOwner(tmp_path / 'mission.db')
    owner.acquire()
    try:
        assert owner.acquired is True
    finally:
        owner.close()


def test_repeated_acquire_on_same_object_raises_and_keeps_original_held(tmp_path):
    db_path = tmp_path / 'mission.db'
    owner = ExecutionOwner(db_path)
    owner.acquire()
    try:
        with pytest.raises(RuntimeError):
            owner.acquire()
        # the original acquisition is unaffected: a third party still sees it held
        contender = ExecutionOwner(db_path)
        with pytest.raises(ExecutionOwnerAlreadyHeld):
            contender.acquire()
    finally:
        owner.close()


def test_acquire_after_close_raises(tmp_path):
    owner = ExecutionOwner(tmp_path / 'mission.db')
    owner.acquire()
    owner.close()
    with pytest.raises(RuntimeError):
        owner.acquire()


def test_close_before_acquire_is_a_noop_then_acquire_raises(tmp_path):
    owner = ExecutionOwner(tmp_path / 'mission.db')
    owner.close()  # never acquired; must not raise
    with pytest.raises(RuntimeError):
        owner.acquire()


def test_repeated_close_is_a_noop(tmp_path):
    owner = ExecutionOwner(tmp_path / 'mission.db')
    owner.acquire()
    owner.close()
    owner.close()
    owner.close()  # must never raise


def test_failed_acquisition_leaves_instance_available_for_explicit_retry(tmp_path):
    # NOTE: `contender` is deliberately never close()d after its failed
    # attempt below -- close() permanently retires an instance (including
    # one that never held anything), which would break the very thing this
    # test proves: that the SAME instance can be acquire()'d again later.
    # `holder` has no such constraint, so it gets the guarded cleanup: an
    # earlier version of this test left holder.acquire() unguarded, so an
    # unexpected failure of the very assertion this test exists to make
    # (`contender.acquired is False`) would abort the test before
    # holder.close() ever ran, leaking holder's fd.
    db_path = tmp_path / 'mission.db'
    holder = ExecutionOwner(db_path)
    holder.acquire()
    try:
        contender = ExecutionOwner(db_path)
        with pytest.raises(ExecutionOwnerAlreadyHeld):
            contender.acquire()
        assert contender.acquired is False
    finally:
        holder.close()

    contender.acquire()  # explicit retry on the SAME instance now succeeds
    try:
        assert contender.acquired is True
    finally:
        contender.close()


def test_context_manager_normal_exit_releases(tmp_path):
    db_path = tmp_path / 'mission.db'
    with ExecutionOwner(db_path) as owner:
        assert owner.acquired is True
    fresh = ExecutionOwner(db_path)
    fresh.acquire()
    fresh.close()


def test_context_manager_exceptional_exit_releases_and_does_not_suppress(tmp_path):
    db_path = tmp_path / 'mission.db'
    owner = ExecutionOwner(db_path)
    with pytest.raises(ValueError, match='boom'):
        with owner:
            raise ValueError('boom')
    fresh = ExecutionOwner(db_path)
    fresh.acquire()  # proves the lock was released despite the exception
    fresh.close()


def test_nested_context_entry_exception_caught_inside_outer_block_keeps_outer_held(tmp_path):
    db_path = tmp_path / 'mission.db'
    owner = ExecutionOwner(db_path)
    with owner:
        with pytest.raises(RuntimeError):
            with owner:
                pass  # unreachable
        # exception was caught INSIDE the outer block: outer is still held
        assert owner.acquired is True
        contender = ExecutionOwner(db_path)
        try:
            with pytest.raises(ExecutionOwnerAlreadyHeld):
                contender.acquire()
        finally:
            contender.close()  # idempotent; defensive if acquire() unexpectedly succeeded
    assert owner.acquired is False  # released by the outer block's own exit


def test_nested_context_entry_exception_escaping_outer_block_releases_outer(tmp_path):
    db_path = tmp_path / 'mission.db'
    owner = ExecutionOwner(db_path)
    with pytest.raises(RuntimeError):
        with owner:
            with owner:
                pass  # unreachable; RuntimeError propagates past BOTH `with` statements
    assert owner.acquired is False
    fresh = ExecutionOwner(db_path)
    fresh.acquire()  # proves the outer __exit__ ran despite the escaping exception
    fresh.close()


def test_same_process_duplicate_owners_contend(tmp_path):
    db_path = tmp_path / 'mission.db'
    first = ExecutionOwner(db_path)
    second = ExecutionOwner(db_path)
    first.acquire()
    try:
        with pytest.raises(ExecutionOwnerAlreadyHeld) as exc_info:
            second.acquire()
        assert exc_info.value.canonical_db_path == first.canonical_db_path
        assert exc_info.value.lock_path == first.lock_path
    finally:
        first.close()
        second.close()  # idempotent; defensive if acquire() unexpectedly succeeded


def test_independent_databases_do_not_contend(tmp_path):
    a = ExecutionOwner(tmp_path / 'a.db')
    b = ExecutionOwner(tmp_path / 'b.db')
    a.acquire()
    b.acquire()
    try:
        assert a.acquired and b.acquired
    finally:
        a.close()
        b.close()


def test_stale_sidecar_file_never_blocks_a_fresh_acquire(tmp_path):
    db_path = tmp_path / 'mission.db'
    lock_path = str(db_path.resolve()) + '.lock'
    open(lock_path, 'a').close()  # pre-existing, unlocked sidecar
    owner = ExecutionOwner(db_path)
    owner.acquire()
    try:
        assert owner.acquired is True
    finally:
        owner.close()


# --------------------------------------------------------------------------
# Sidecar open contract: flags, mode/umask, truncation, symlink/FIFO rejection
# --------------------------------------------------------------------------

def test_new_sidecar_is_empty(tmp_path):
    db_path = tmp_path / 'mission.db'
    owner = ExecutionOwner(db_path)
    owner.acquire()
    try:
        assert os.path.getsize(owner.lock_path) == 0
    finally:
        owner.close()
    assert os.path.getsize(owner.lock_path) == 0


def test_sidecar_persists_without_truncation_or_unlink_across_cycles(tmp_path):
    db_path = tmp_path / 'mission.db'
    owner = ExecutionOwner(db_path)
    for _ in range(3):
        owner = ExecutionOwner(db_path)
        owner.acquire()
        owner.close()
    assert os.path.exists(owner.lock_path)
    assert os.path.getsize(owner.lock_path) == 0


def test_preexisting_nonempty_sidecar_bytes_and_inode_preserved(tmp_path):
    db_path = tmp_path / 'mission.db'
    lock_path = str(db_path.resolve()) + '.lock'
    sentinel = b'PREEXISTING-DIAGNOSTIC-CONTENT-1234567890'
    with open(lock_path, 'wb') as f:
        f.write(sentinel)
    inode_before = os.stat(lock_path).st_ino

    owner = ExecutionOwner(db_path)
    owner.acquire()
    try:
        assert open(lock_path, 'rb').read() == sentinel
        assert os.stat(lock_path).st_ino == inode_before

        # a failed contention attempt must not touch content either
        contender = ExecutionOwner(db_path)
        try:
            with pytest.raises(ExecutionOwnerAlreadyHeld):
                contender.acquire()
            assert open(lock_path, 'rb').read() == sentinel
        finally:
            contender.close()  # idempotent; defensive if acquire() unexpectedly succeeded
    finally:
        owner.close()
    assert open(lock_path, 'rb').read() == sentinel
    assert os.stat(lock_path).st_ino == inode_before


def test_preexisting_sidecar_mode_preserved_not_chmoded(tmp_path):
    db_path = tmp_path / 'mission.db'
    lock_path = str(db_path.resolve()) + '.lock'
    open(lock_path, 'a').close()
    os.chmod(lock_path, 0o640)
    owner = ExecutionOwner(db_path)
    owner.acquire()
    owner.close()
    assert stat.S_IMODE(os.stat(lock_path).st_mode) == 0o640


def test_umask_0o077_actually_restricts_new_sidecar_to_0o600(tmp_path):
    db_path = tmp_path / 'mission.db'
    old_umask = os.umask(0o077)
    try:
        owner = ExecutionOwner(db_path)
        owner.acquire()
        owner.close()
    finally:
        os.umask(old_umask)
    mode = stat.S_IMODE(os.stat(owner.lock_path).st_mode)
    assert mode == 0o600, (
        'umask 0o077 must actually restrict a requested 0o644 to 0o600; '
        f'got {oct(mode)} -- a 0o022 umask would pass this assertion vacuously '
        'because it removes no bits already absent from 0o644')


def test_sidecar_final_symlink_is_rejected_before_flock(tmp_path):
    db_path = tmp_path / 'mission.db'
    real_target = tmp_path / 'real_target'
    real_target.write_bytes(b'')
    lock_path = str(db_path.resolve()) + '.lock'
    os.symlink(real_target, lock_path)
    owner = ExecutionOwner(db_path)
    try:
        with pytest.raises(OSError) as exc_info:
            owner.acquire()
        assert exc_info.value.errno == errno.ELOOP
        assert owner.acquired is False
        assert os.path.getsize(real_target) == 0  # never flocked/touched
    finally:
        owner.close()  # idempotent; defensive if acquire() unexpectedly succeeded


def test_sidecar_fifo_is_rejected_before_flock(tmp_path):
    db_path = tmp_path / 'mission.db'
    lock_path = str(db_path.resolve()) + '.lock'
    os.mkfifo(lock_path)
    owner = ExecutionOwner(db_path)
    try:
        with pytest.raises(ValueError):
            owner.acquire()
        assert owner.acquired is False
        assert stat.S_ISFIFO(os.stat(lock_path).st_mode)  # unchanged, not replaced
    finally:
        owner.close()  # idempotent; defensive if acquire() unexpectedly succeeded


def test_sidecar_directory_is_rejected(tmp_path):
    db_path = tmp_path / 'mission.db'
    lock_path = str(db_path.resolve()) + '.lock'
    os.makedirs(lock_path)
    owner = ExecutionOwner(db_path)
    try:
        with pytest.raises(IsADirectoryError):
            owner.acquire()
        assert owner.acquired is False
    finally:
        owner.close()  # idempotent; defensive if acquire() unexpectedly succeeded


def test_acquired_fd_is_close_on_exec(tmp_path):
    owner = ExecutionOwner(tmp_path / 'mission.db')
    owner.acquire()
    try:
        assert os.get_inheritable(owner._fd) is False
    finally:
        owner.close()


# --------------------------------------------------------------------------
# Failure-path fd cleanup: checked closure / fd-count comparison
# --------------------------------------------------------------------------

def test_parent_removed_after_construction_acquire_failure_allocates_no_fd(tmp_path):
    # Construction succeeds (the parent exists right now); the parent is
    # then removed, so acquire()'s own os.open() is what actually fails --
    # the genuine open()-time path, distinct from the construction-time
    # rejection already covered by
    # test_construct_rejects_missing_parent_with_file_not_found_error above.
    parent = tmp_path / 'will_be_removed'
    parent.mkdir()
    owner = ExecutionOwner(parent / 'mission.db')
    os.rmdir(parent)
    before = _open_fd_count()
    with pytest.raises(FileNotFoundError):
        owner.acquire()
    assert owner.acquired is False
    assert _open_fd_count() == before


def test_acquire_time_permission_denied_is_distinct_from_construction_and_contention(tmp_path):
    # Distinct from test_construct_rejects_permission_denied_parent (which
    # fails while CANONICALIZING the path, during __init__, via a parent
    # directory with no permissions at all) and from ordinary contention
    # (ExecutionOwnerAlreadyHeld, an EAGAIN from flock() itself): here,
    # construction succeeds against a normal, accessible parent, and the
    # sidecar file ALREADY EXISTS with permissions denying this process any
    # access to it at all, so acquire()'s own os.open() is what fails with a
    # genuine EACCES.
    if os.geteuid() == 0:
        pytest.skip('running as root: permission denial cannot be demonstrated')
    db_path = tmp_path / 'mission.db'
    lock_path = str(db_path) + '.lock'
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o000)
    os.close(fd)
    owner = ExecutionOwner(db_path)
    before = _open_fd_count()
    with pytest.raises(PermissionError) as exc_info:
        owner.acquire()
    assert not isinstance(exc_info.value, ExecutionOwnerAlreadyHeld)
    assert owner.acquired is False
    assert _open_fd_count() == before


def test_repeated_real_contention_does_not_leak_fds(tmp_path):
    db_path = tmp_path / 'mission.db'
    holder = ExecutionOwner(db_path)
    holder.acquire()
    try:
        before = _open_fd_count()
        for _ in range(200):
            contender = ExecutionOwner(db_path)
            with pytest.raises(ExecutionOwnerAlreadyHeld):
                contender.acquire()
        assert _open_fd_count() == before
    finally:
        holder.close()


def test_repeated_nonregular_sidecar_rejection_does_not_leak_fds(tmp_path):
    db_path = tmp_path / 'mission.db'
    lock_path = str(db_path.resolve()) + '.lock'
    os.mkfifo(lock_path)
    before = _open_fd_count()
    for _ in range(200):
        owner = ExecutionOwner(db_path)
        with pytest.raises(ValueError):
            owner.acquire()
    assert _open_fd_count() == before


# --------------------------------------------------------------------------
# E2: contention translation must surround only flock() itself
# --------------------------------------------------------------------------

def _patched_fstat_raising(monkeypatch, target_fd, exc):
    real_fstat = os.fstat

    def fake_fstat(fd):
        if fd == target_fd:
            raise exc
        return real_fstat(fd)

    monkeypatch.setattr(execution_owner.os, 'fstat', fake_fstat)


def test_eagain_before_flock_from_fstat_is_not_mistaken_for_already_held(tmp_path, monkeypatch):
    db_path = tmp_path / 'mission.db'
    owner = ExecutionOwner(db_path)
    # Patch os.open() to, once it knows the real fd os.open() just returned,
    # patch os.fstat() to fail ONLY for that fd -- every other fd (including
    # whatever pytest itself may be using concurrently) delegates to the
    # real fstat via _patched_fstat_raising.
    orig_open = execution_owner.os.open

    def fake_open_then_patch_fstat(path, flags, mode=0o777, *a, **kw):
        fd = orig_open(path, flags, mode, *a, **kw)
        _patched_fstat_raising(monkeypatch, fd, BlockingIOError(errno.EAGAIN, 'injected'))
        return fd

    real_fstat = os.fstat
    monkeypatch.setattr(execution_owner.os, 'open', fake_open_then_patch_fstat)

    before = _open_fd_count()
    try:
        with pytest.raises(BlockingIOError):
            owner.acquire()
        assert owner.acquired is False
        assert _open_fd_count() == before
    finally:
        owner.close()  # idempotent; defensive if acquire() unexpectedly succeeded
        # Undo both patches before checking recovery: fake_open_then_patch_fstat
        # re-arms the fstat fault for whatever NEW fd number it next sees (fd
        # numbers get reused), so leaving it active would also fail the fresh
        # acquire() below for the wrong reason.
        monkeypatch.setattr(execution_owner.os, 'open', orig_open)
        monkeypatch.setattr(execution_owner.os, 'fstat', real_fstat)

    # no one else holds this lock: a real acquire on a FRESH owner succeeds
    fresh = ExecutionOwner(db_path)
    try:
        fresh.acquire()
    finally:
        fresh.close()


def test_eagain_before_flock_from_fcntl_f_getfl_is_not_mistaken_for_already_held(
        tmp_path, monkeypatch):
    db_path = tmp_path / 'mission.db'
    owner = ExecutionOwner(db_path)
    real_fcntl = fcntl.fcntl
    calls = {'n': 0}

    def fake_fcntl(fd, cmd, *a):
        if cmd == fcntl.F_GETFL:
            calls['n'] += 1
            if calls['n'] == 1:
                raise BlockingIOError(errno.EAGAIN, 'injected F_GETFL failure')
        return real_fcntl(fd, cmd, *a)

    monkeypatch.setattr(execution_owner.fcntl, 'fcntl', fake_fcntl)
    before = _open_fd_count()
    try:
        with pytest.raises(BlockingIOError):
            owner.acquire()
        assert owner.acquired is False
        assert _open_fd_count() == before
    finally:
        owner.close()  # idempotent; defensive if acquire() unexpectedly succeeded


def test_eagain_before_flock_from_fcntl_f_setfl_is_not_mistaken_for_already_held(
        tmp_path, monkeypatch):
    # The companion to the F_GETFL case above: F_SETFL (clearing O_NONBLOCK)
    # is the OTHER fcntl call acquire() makes before ever reaching flock(),
    # and must be covered by the same narrow-scoping guarantee.
    db_path = tmp_path / 'mission.db'
    owner = ExecutionOwner(db_path)
    real_fcntl = fcntl.fcntl
    calls = {'n': 0}

    def fake_fcntl(fd, cmd, *a):
        if cmd == fcntl.F_SETFL:
            calls['n'] += 1
            if calls['n'] == 1:
                raise BlockingIOError(errno.EAGAIN, 'injected F_SETFL failure')
        return real_fcntl(fd, cmd, *a)

    monkeypatch.setattr(execution_owner.fcntl, 'fcntl', fake_fcntl)
    before = _open_fd_count()
    try:
        with pytest.raises(BlockingIOError) as exc_info:
            owner.acquire()
        assert not isinstance(exc_info.value, ExecutionOwnerAlreadyHeld)
        assert owner.acquired is False
        assert _open_fd_count() == before
    finally:
        owner.close()  # idempotent; defensive if acquire() unexpectedly succeeded


def test_post_open_eio_from_fstat_propagates_as_itself(tmp_path, monkeypatch):
    db_path = tmp_path / 'mission.db'
    owner = ExecutionOwner(db_path)
    orig_open = execution_owner.os.open

    def fake_open(path, flags, mode=0o777, *a, **kw):
        fd = orig_open(path, flags, mode, *a, **kw)
        _patched_fstat_raising(monkeypatch, fd, OSError(errno.EIO, 'injected EIO'))
        return fd

    monkeypatch.setattr(execution_owner.os, 'open', fake_open)
    before = _open_fd_count()
    try:
        with pytest.raises(OSError) as exc_info:
            owner.acquire()
        assert exc_info.value.errno == errno.EIO
        assert not isinstance(exc_info.value, ExecutionOwnerAlreadyHeld)
        assert _open_fd_count() == before
    finally:
        owner.close()  # idempotent; defensive if acquire() unexpectedly succeeded


def test_enolck_from_flock_propagates_as_itself_not_already_held(tmp_path, monkeypatch):
    # NOTE: owner is deliberately NOT close()d until the very end -- this
    # test's own point is that a non-contention failure leaves the SAME
    # instance retryable (close() would permanently retire it, defeating
    # that). The whole flow, including the retry, is wrapped in one
    # unconditional finally instead, so any assertion failure along the way
    # still closes whatever owner ends up holding.
    db_path = tmp_path / 'mission.db'
    owner = ExecutionOwner(db_path)
    real_flock = fcntl.flock

    def fake_flock(fd, op):
        raise OSError(errno.ENOLCK, 'injected: no locks available')

    monkeypatch.setattr(execution_owner.fcntl, 'flock', fake_flock)
    before = _open_fd_count()
    try:
        with pytest.raises(OSError) as exc_info:
            owner.acquire()
        assert exc_info.value.errno == errno.ENOLCK
        assert not isinstance(exc_info.value, ExecutionOwnerAlreadyHeld)
        assert _open_fd_count() == before

        monkeypatch.setattr(execution_owner.fcntl, 'flock', real_flock)
        # real flock still works afterward: this owner instance was left in
        # a clean, retryable state by the failed attempt.
        owner.acquire()
        assert owner.acquired is True
    finally:
        monkeypatch.setattr(execution_owner.fcntl, 'flock', real_flock)
        owner.close()


def test_non_exception_base_exception_during_acquire_still_cleans_up_fd(tmp_path, monkeypatch):
    db_path = tmp_path / 'mission.db'
    owner = ExecutionOwner(db_path)
    orig_open = execution_owner.os.open

    def fake_open(path, flags, mode=0o777, *a, **kw):
        fd = orig_open(path, flags, mode, *a, **kw)
        _patched_fstat_raising(monkeypatch, fd, KeyboardInterrupt())
        return fd

    monkeypatch.setattr(execution_owner.os, 'open', fake_open)
    before = _open_fd_count()
    try:
        with pytest.raises(KeyboardInterrupt):
            owner.acquire()
        assert owner.acquired is False
        assert _open_fd_count() == before
    finally:
        owner.close()  # idempotent; defensive if acquire() unexpectedly succeeded


def test_real_flock_contention_still_reported_correctly_alongside_fault_injection_tests(
        tmp_path):
    # Baseline, unaffected by any of the fault-injection tests above: genuine
    # contention is still translated correctly.
    db_path = tmp_path / 'mission.db'
    holder = ExecutionOwner(db_path)
    holder.acquire()
    try:
        contender = ExecutionOwner(db_path)
        with pytest.raises(ExecutionOwnerAlreadyHeld):
            contender.acquire()
    finally:
        holder.close()


# --------------------------------------------------------------------------
# close()-error / fd-reuse regression, and cleanup-error chaining
# --------------------------------------------------------------------------

def test_close_error_detaches_state_before_the_syscall_and_does_not_reuse_stale_fd(
        tmp_path, monkeypatch):
    db_path = tmp_path / 'mission.db'
    owner = ExecutionOwner(db_path)
    owner.acquire()
    owned_fd = owner._fd
    real_close = os.close
    fault = {'fired': False}

    def fake_close(fd):
        real_close(fd)  # the real kernel close always happens
        if fd == owned_fd and not fault['fired']:
            fault['fired'] = True
            raise OSError(errno.EIO, 'injected close EIO')

    monkeypatch.setattr(execution_owner.os, 'close', fake_close)

    with pytest.raises(OSError):
        owner.close()
    assert owner.acquired is False  # state detached even though close() raised

    # An unrelated file now legitimately reuses the freed fd number.
    unrelated_path = tmp_path / 'unrelated.txt'
    unrelated_fd = os.open(unrelated_path, os.O_CREAT | os.O_RDWR, 0o644)
    try:
        assert unrelated_fd == owned_fd, 'expected the kernel to reuse the just-closed fd'
        owner.close()  # must be a pure no-op: self._fd is already None
        os.fstat(unrelated_fd)  # still open: the second close() touched nothing
    finally:
        monkeypatch.setattr(execution_owner.os, 'close', real_close)
        os.close(unrelated_fd)


def test_acquisition_cleanup_close_error_chains_to_original_failure(tmp_path, monkeypatch):
    # Distinct from test_exceptional_context_exit_with_failing_close_chains_
    # ordinarily above: that test injects a close() failure on the WITH-
    # BLOCK exit path (the public close() method). This one targets
    # acquire()'s OWN internal cleanup -- the `except BaseException:
    # os.close(fd); raise` branch that runs when something fails AFTER
    # os.open() but BEFORE self._fd is ever assigned. A post-open failure
    # (injected on F_GETFL, the same call the two EAGAIN-before-flock tests
    # above use) triggers that internal cleanup; the cleanup's own
    # os.close() is then also made to fail, real close performed first, so
    # this covers close-error-during-cleanup, not merely close-error-during-
    # an-already-successful-acquisition.
    db_path = tmp_path / 'mission.db'
    owner = ExecutionOwner(db_path)
    real_fcntl = fcntl.fcntl
    real_close = os.close
    state = {'opened_fd': None, 'close_calls': 0}

    def fake_fcntl(fd, cmd, *a):
        if cmd == fcntl.F_GETFL:
            raise BlockingIOError(errno.EAGAIN, 'injected F_GETFL failure')
        return real_fcntl(fd, cmd, *a)

    def fake_close(fd):
        if fd == state['opened_fd']:
            state['close_calls'] += 1
        real_close(fd)  # the real kernel close always happens first
        if fd == state['opened_fd']:
            raise OSError(errno.EIO, 'injected acquisition-cleanup close failure')

    real_open = os.open

    def spying_open(path, flags, mode=0o777):
        fd = real_open(path, flags, mode)
        if path == owner.lock_path:
            state['opened_fd'] = fd
        return fd

    monkeypatch.setattr(execution_owner.fcntl, 'fcntl', fake_fcntl)
    monkeypatch.setattr(execution_owner.os, 'close', fake_close)
    monkeypatch.setattr(execution_owner.os, 'open', spying_open)
    before = _open_fd_count()
    try:
        with pytest.raises(
                OSError, match='injected acquisition-cleanup close failure') as exc_info:
            owner.acquire()
    finally:
        monkeypatch.setattr(execution_owner.os, 'open', real_open)
        monkeypatch.setattr(execution_owner.os, 'close', real_close)
        monkeypatch.setattr(execution_owner.fcntl, 'fcntl', real_fcntl)

    # the cleanup-close diagnostic supersedes the original F_GETFL failure
    # through ordinary exception chaining, exactly like the __exit__ case.
    assert isinstance(exc_info.value.__context__, BlockingIOError)
    assert exc_info.value.__context__.errno == errno.EAGAIN
    assert state['close_calls'] == 1, 'the cleanup path must attempt exactly one close()'
    assert owner.acquired is False
    assert _open_fd_count() == before, 'the real fd must not have leaked'

    fresh = ExecutionOwner(db_path)
    try:
        fresh.acquire()  # proves no leak: nothing else holds the lock
    finally:
        fresh.close()


def test_exceptional_context_exit_with_failing_close_chains_ordinarily(tmp_path, monkeypatch):
    # Exercises the REAL ExecutionOwner.close(), not a replacement that only
    # raises: a prior version of this test monkeypatched owner.close itself,
    # which never called the real close() at all and silently leaked the
    # lock descriptor while still appearing to pass. The fault is injected
    # one level down, at os.close(), narrowly scoped to this owner's own fd
    # and delegating any other fd to the real close unconditionally.
    db_path = tmp_path / 'mission.db'
    owner = ExecutionOwner(db_path)
    real_close = os.close
    state = {'owned_fd': None, 'fired': False}

    def fake_close(fd):
        real_close(fd)  # the real kernel close always happens
        if fd == state['owned_fd'] and not state['fired']:
            state['fired'] = True
            raise OSError(errno.EIO, 'injected close failure')

    before = _open_fd_count()
    monkeypatch.setattr(execution_owner.os, 'close', fake_close)
    try:
        with pytest.raises(OSError, match='injected close failure') as exc_info:
            with owner as entered:
                state['owned_fd'] = entered._fd
                raise ValueError('body error')
    finally:
        monkeypatch.setattr(execution_owner.os, 'close', real_close)

    # ordinary Python with-statement chaining: the __exit__ (close) error is
    # the one that propagates, with the body's error attached as __context__.
    assert isinstance(exc_info.value.__context__, ValueError)
    assert str(exc_info.value.__context__) == 'body error'
    assert owner.acquired is False, 'state must be detached despite the injected close failure'
    assert _open_fd_count() == before, 'the real fd must not have leaked'

    fresh = ExecutionOwner(db_path)
    try:
        fresh.acquire()  # proves no leak: nothing else holds the lock
    finally:
        fresh.close()


# --------------------------------------------------------------------------
# No database creation/modification by ExecutionOwner
# --------------------------------------------------------------------------

def test_absent_database_stays_absent_across_a_full_cycle(tmp_path):
    db_path = tmp_path / 'mission.db'
    owner = ExecutionOwner(db_path)
    owner.acquire()
    owner.close()
    assert not db_path.exists()


def test_existing_throwaway_database_bytes_and_inode_unchanged(tmp_path):
    db_path = tmp_path / 'mission.db'
    content = b'not a real sqlite file, just throwaway bytes'
    db_path.write_bytes(content)
    inode_before = os.stat(db_path).st_ino
    owner = ExecutionOwner(db_path)
    owner.acquire()
    owner.close()
    assert db_path.read_bytes() == content
    assert os.stat(db_path).st_ino == inode_before


def test_execution_owner_module_never_imports_sqlite3():
    # Source-review-style check, corroborating the behavioral tests above:
    # ExecutionOwner never opens a database connection of its own. The
    # docstring legitimately mentions "MissionStore" in prose (documenting
    # the future integration contract), so this checks the import set via
    # AST, not a substring match over the whole file including comments.
    imported = _imported_top_level_modules(ROS_PACKAGE_DIR / 'execution_owner.py')
    assert 'sqlite3' not in imported


# --------------------------------------------------------------------------
# Import boundaries (AST-based, not substring grep)
# --------------------------------------------------------------------------

def test_execution_owner_module_imports_nothing_from_core():
    imported = _imported_top_level_modules(ROS_PACKAGE_DIR / 'execution_owner.py')
    assert 'task_resilience' not in imported


def test_errors_module_imports_nothing_from_core():
    imported = _imported_top_level_modules(ROS_PACKAGE_DIR / 'errors.py')
    assert 'task_resilience' not in imported


def test_execution_owner_module_imports_no_ros_or_rclpy():
    imported = _imported_top_level_modules(ROS_PACKAGE_DIR / 'execution_owner.py')
    assert imported.isdisjoint({'rclpy', 'nav2_msgs', 'geometry_msgs'})


def test_core_never_imports_this_ros_package_or_process_lifecycle_modules():
    # The enduring, permanent-scope check: task_resilience must never import
    # task_resilience_ros, ROS/action packages, or process-lifecycle
    # dependencies -- unlike the narrower, current-files-only checks above,
    # this direction applies to every file in the core, forever.
    forbidden = {'task_resilience_ros', 'rclpy', 'nav2_msgs', 'geometry_msgs',
                 'fcntl', 'subprocess', 'multiprocessing'}
    assert CORE_PACKAGE_DIR.is_dir(), f'{CORE_PACKAGE_DIR} not found -- checked nothing'
    core_files = list(CORE_PACKAGE_DIR.glob('*.py'))
    assert core_files, f'{CORE_PACKAGE_DIR} contained no .py files -- checked nothing'
    for py_file in core_files:
        imported = _imported_top_level_modules(py_file)
        offending = imported & forbidden
        assert not offending, f'{py_file} imports forbidden module(s): {offending}'


def test_no_ros_runtime_required_to_import_execution_owner():
    # If rclpy/nav2 were ever accidentally imported, this would fail outside
    # a sourced ROS environment; importing this module never has been.
    import importlib
    importlib.reload(execution_owner)
