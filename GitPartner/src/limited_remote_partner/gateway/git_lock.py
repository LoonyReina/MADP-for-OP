from __future__ import annotations

import os
import signal
import shutil
import threading
import time
from pathlib import Path

from limited_remote_partner.core.process_utils import process_start_token


class GitLockError(RuntimeError):
    pass


GIT_LOCK_ERROR_TOKENS = (
    "another git process seems to be running",
    "index.lock",
    "packed-refs.lock",
    ".lock': file exists",
    '.lock": file exists',
    "unable to create",
    "cannot lock ref",
)

_THREAD_LOCKS_GUARD = threading.Lock()
_THREAD_LOCKS: dict[str, threading.RLock] = {}
_THREAD_DEPTHS = threading.local()


class GitOperationLock:
    """Small cross-process repo lock for GitPartner-managed git commands."""

    def __init__(
        self,
        repo_dir: Path,
        label: str,
        *,
        timeout_seconds: int | None = None,
        stale_seconds: int | None = None,
    ) -> None:
        self.repo_dir = repo_dir.resolve()
        self.label = label
        self.timeout_seconds = (
            timeout_seconds
            if timeout_seconds is not None
            else _int_from_env("GITPARTNER_GIT_OPERATION_LOCK_TIMEOUT_SECONDS", 180)
        )
        self.stale_seconds = (
            stale_seconds
            if stale_seconds is not None
            else _int_from_env("GITPARTNER_GIT_OPERATION_LOCK_STALE_SECONDS", 300)
        )
        self.lock_dir = self.repo_dir / ".partner_state" / "git-operation.lock.d"
        self._lock_key = str(self.lock_dir)
        self._thread_lock = _thread_lock(self._lock_key)
        self._thread_lock_acquired = False
        self._nested = False
        self._owns_lock = False

    def __enter__(self) -> "GitOperationLock":
        started = time.monotonic()
        if not self._thread_lock.acquire(timeout=max(1, self.timeout_seconds)):
            raise GitLockError(
                "timed out waiting for in-process GitPartner git operation lock "
                f"repo={self.repo_dir} lock={self.lock_dir}"
            )
        self._thread_lock_acquired = True
        depths = _thread_depths()
        depth = depths.get(self._lock_key, 0)
        depths[self._lock_key] = depth + 1
        if depth:
            self._nested = True
            return self

        deadline = started + max(1, self.timeout_seconds)
        try:
            self.lock_dir.parent.mkdir(parents=True, exist_ok=True)
            while True:
                now = int(time.time())
                try:
                    self.lock_dir.mkdir()
                    (self.lock_dir / "pid").write_text(
                        f"{os.getpid()}\n", encoding="utf-8"
                    )
                    (self.lock_dir / "start_token").write_text(
                        f"{process_start_token(os.getpid())}\n",
                        encoding="utf-8",
                    )
                    (self.lock_dir / "created_at_epoch").write_text(
                        f"{now}\n", encoding="utf-8"
                    )
                    (self.lock_dir / "label").write_text(
                        f"{self.label}\n", encoding="utf-8"
                    )
                    self._owns_lock = True
                    return self
                except FileExistsError:
                    self._clear_stale_operation_lock(now)
                    if time.monotonic() >= deadline:
                        pid = _read_text(self.lock_dir / "pid").strip() or "unknown"
                        created = _read_int(self.lock_dir / "created_at_epoch", 0)
                        active_git = active_git_pids(self.repo_dir)
                        raise GitLockError(
                            "timed out waiting for GitPartner git operation lock "
                            f"repo={self.repo_dir} lock={self.lock_dir} pid={pid} "
                            f"age_seconds={max(0, now - created)} "
                            f"active_git_pids={active_git}"
                        )
                    time.sleep(0.5)
        except Exception:
            self._release_thread_lock()
            raise

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        try:
            if self._owns_lock:
                pid = _read_text(self.lock_dir / "pid").strip()
                if pid == str(os.getpid()):
                    shutil.rmtree(self.lock_dir, ignore_errors=True)
                self._owns_lock = False
        finally:
            self._release_thread_lock()

    def _release_thread_lock(self) -> None:
        if not self._thread_lock_acquired:
            return
        depths = _thread_depths()
        depth = depths.get(self._lock_key, 0)
        if depth <= 1:
            depths.pop(self._lock_key, None)
        else:
            depths[self._lock_key] = depth - 1
        self._thread_lock.release()
        self._thread_lock_acquired = False
        self._nested = False

    def _clear_stale_operation_lock(self, now: int) -> None:
        pid = _read_text(self.lock_dir / "pid").strip()
        owner_token = _read_text(self.lock_dir / "start_token").strip()
        created = _read_int(self.lock_dir / "created_at_epoch", 0)
        if not created:
            try:
                created = int(self.lock_dir.stat().st_mtime)
            except FileNotFoundError:
                return
        age = max(0, now - created)
        if (not pid or not owner_token) and age < min(
            max(1, self.stale_seconds),
            10,
        ):
            return
        owner_alive = bool(pid and _process_identity_alive(pid, owner_token))
        if owner_alive and owner_token:
            return
        if owner_alive and not owner_token and age < max(1, self.stale_seconds):
            return
        active_git = active_git_pids(self.repo_dir)
        # A legacy lock has no process-start token.  After its stale threshold,
        # a reused live PID is not ownership proof; preserve it only while an
        # actual Git child still operates in this repository.
        if owner_alive and not owner_token and active_git:
            return
        if active_git:
            orphan_grace = _int_from_env(
                "GITPARTNER_GIT_ORPHAN_RECOVERY_SECONDS",
                30,
            )
            if age < max(1, orphan_grace):
                return
            _terminate_orphaned_git_processes(self.repo_dir, active_git)
            if active_git_pids(self.repo_dir):
                return
        shutil.rmtree(self.lock_dir, ignore_errors=True)


def is_git_lock_failure_text(text: str) -> bool:
    lowered = text.lower()
    return any(token in lowered for token in GIT_LOCK_ERROR_TOKENS)


def cleanup_stale_git_locks(
    repo_dir: Path, *, stale_seconds: int | None = None
) -> list[Path]:
    repo = repo_dir.resolve()
    git_dirs = _git_lock_roots(repo)
    if not git_dirs:
        return []
    threshold = (
        stale_seconds
        if stale_seconds is not None
        else _int_from_env("GITPARTNER_STALE_GIT_LOCK_SECONDS", 120)
    )
    if active_git_pids(repo):
        return []
    now = time.time()
    removed: list[Path] = []
    for git_dir in git_dirs:
        for path in git_dir.rglob("*.lock"):
            try:
                age = now - path.stat().st_mtime
            except FileNotFoundError:
                continue
            if threshold > 0 and age < threshold:
                continue
            try:
                path.unlink()
            except FileNotFoundError:
                continue
            removed.append(path)
    return removed


def cleanup_git_locks_if_idle(repo_dir: Path) -> list[Path]:
    """Remove repo Git lock files immediately when no Git process owns the repo."""

    repo = repo_dir.resolve()
    git_dirs = _git_lock_roots(repo)
    if not git_dirs or active_git_pids(repo):
        return []
    removed: list[Path] = []
    for git_dir in git_dirs:
        for path in git_dir.rglob("*.lock"):
            try:
                path.unlink()
            except FileNotFoundError:
                continue
            removed.append(path)
    return removed


def recover_git_locks_after_failure(repo_dir: Path) -> list[Path]:
    """Serialize orphan-lock cleanup with all GitPartner-managed repo work."""

    repo = repo_dir.resolve()
    with GitOperationLock(repo, "recover orphaned Git lock"):
        return cleanup_git_locks_if_idle(repo)


def force_cleanup_repo_git(repo_dir: Path) -> dict[str, object]:
    """Terminate Git helpers and clear lock/rebase state for exactly one repo."""

    repo = repo_dir.resolve()
    before = active_git_pids(repo)
    if before:
        _terminate_orphaned_git_processes(repo, before)
    remaining = active_git_pids(repo)
    if remaining:
        raise GitLockError(
            "force cleanup could not stop repository Git helpers "
            f"repo={repo} active_git_pids={remaining}"
        )
    operation_lock = repo / ".partner_state" / "git-operation.lock.d"
    shutil.rmtree(operation_lock, ignore_errors=True)
    removed = cleanup_git_locks_if_idle(repo)
    for git_dir in _git_lock_roots(repo):
        shutil.rmtree(git_dir / "rebase-merge", ignore_errors=True)
        shutil.rmtree(git_dir / "rebase-apply", ignore_errors=True)
    return {
        "repo": str(repo),
        "terminated_git_pids": before,
        "remaining_git_pids": remaining,
        "removed_locks": [str(path) for path in removed],
        "operation_lock_removed": not operation_lock.exists(),
    }


def terminate_repo_git_helpers(repo_dir: Path) -> list[int]:
    """Terminate Git helper processes whose current directory belongs to one repo."""

    repo = repo_dir.resolve()
    pids = active_git_pids(repo)
    if pids:
        _terminate_orphaned_git_processes(repo, pids)
    return active_git_pids(repo)


def _git_lock_roots(repo_dir: Path) -> list[Path]:
    """Return the repository-local Git metadata roots that may own lock files."""

    dot_git = repo_dir.resolve() / ".git"
    if dot_git.is_dir():
        return [dot_git.resolve()]
    if not dot_git.is_file():
        return []
    try:
        marker = dot_git.read_text(encoding="utf-8").strip()
    except (OSError, UnicodeError):
        return []
    prefix = "gitdir:"
    if not marker.lower().startswith(prefix):
        return []
    git_dir = Path(marker[len(prefix) :].strip())
    if not git_dir.is_absolute():
        git_dir = dot_git.parent / git_dir
    try:
        resolved = git_dir.resolve()
    except OSError:
        return []
    return [resolved] if resolved.is_dir() else []


def active_git_pids(
    repo_dir: Path,
    *,
    proc_root: Path = Path("/proc"),
) -> list[int]:
    if not proc_root.exists():
        return []
    repo_real = str(repo_dir.resolve())
    pids: list[int] = []
    for proc in proc_root.iterdir():
        if not proc.name.isdigit():
            continue
        pid = int(proc.name)
        if pid == os.getpid():
            continue
        args = _read_proc_args(proc / "cmdline")
        if not args or not _is_git_process_executable(args[0]):
            continue
        try:
            cwd = os.path.realpath(proc / "cwd")
        except OSError:
            cwd = ""
        if cwd == repo_real or cwd.startswith(repo_real + os.sep):
            pids.append(pid)
    return pids


def _read_proc_args(path: Path) -> list[str]:
    try:
        raw = path.read_bytes()
    except OSError:
        return []
    return [part.decode("utf-8", errors="replace") for part in raw.split(b"\0") if part]


def _is_git_process_executable(value: str) -> bool:
    executable = Path(value).name.lower().removesuffix(".exe")
    return executable == "git" or executable in {
        "git-http-fetch",
        "git-http-push",
        "git-receive-pack",
        "git-remote-http",
        "git-remote-https",
        "git-remote-ssh",
        "git-upload-pack",
    }


def _process_alive(pid_text: str) -> bool:
    try:
        pid = int(pid_text)
    except ValueError:
        return False
    if pid <= 0:
        return False
    if os.name == "nt":
        # Python implements os.kill through the Windows process API; signal 0
        # is not a portable read-only liveness probe there.  The creation token
        # opens the process without signalling it and also rejects PID reuse.
        return bool(process_start_token(pid))
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except SystemError:
        # CPython on Windows can surface an invalid signal-0 probe as a
        # SystemError even for a live process.  The creation token uses the
        # native process API and is the authoritative fallback there.
        return bool(process_start_token(pid))
    except OSError:
        return False
    return True


def _process_identity_alive(pid_text: str, start_token: str) -> bool:
    if not _process_alive(pid_text):
        return False
    if not start_token:
        return True
    try:
        pid = int(pid_text)
    except ValueError:
        return False
    try:
        return process_start_token(pid) == start_token
    except (OSError, SystemError):
        # The process may exit between the liveness probe and identity lookup.
        return False


def _terminate_orphaned_git_processes(repo_dir: Path, pids: list[int]) -> None:
    identities = [(pid, process_start_token(pid)) for pid in sorted(set(pids))]
    identities = [(pid, token) for pid, token in identities if token]
    if not identities:
        return
    print(
        "GITPARTNER_GIT_ORPHAN_RECOVERY " f"repo={repo_dir} identities={identities}",
        flush=True,
    )
    for pid, token in identities:
        if process_start_token(pid) == token:
            _signal_exact_process(pid, signal.SIGTERM)
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        if not any(process_start_token(pid) == token for pid, token in identities):
            return
        time.sleep(0.1)
    for pid, token in identities:
        if process_start_token(pid) == token:
            _signal_exact_process(pid, signal.SIGKILL)
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        if not any(process_start_token(pid) == token for pid, token in identities):
            return
        time.sleep(0.1)


def _signal_exact_process(pid: int, sig: signal.Signals) -> None:
    try:
        os.kill(pid, sig)
    except (ProcessLookupError, PermissionError, OSError, SystemError):
        return


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return ""


def _read_int(path: Path, default: int) -> int:
    try:
        return int(_read_text(path).strip())
    except ValueError:
        return default


def _int_from_env(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except ValueError:
        return default


def _thread_lock(key: str) -> threading.RLock:
    with _THREAD_LOCKS_GUARD:
        lock = _THREAD_LOCKS.get(key)
        if lock is None:
            lock = threading.RLock()
            _THREAD_LOCKS[key] = lock
        return lock


def _thread_depths() -> dict[str, int]:
    depths = getattr(_THREAD_DEPTHS, "depths", None)
    if depths is None:
        depths = {}
        _THREAD_DEPTHS.depths = depths
    return depths
