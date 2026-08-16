from __future__ import annotations

import base64
import os
import signal
import subprocess
import tempfile
import time
from pathlib import Path

from limited_remote_partner.core.config import RepoConfig
from limited_remote_partner.gateway.git_lock import (
    GitLockError,
    GitOperationLock,
    is_git_lock_failure_text,
    recover_git_locks_after_failure,
    terminate_repo_git_helpers,
)
from limited_remote_partner.core.process_utils import hidden_subprocess_kwargs, process_group_kwargs


class GitError(RuntimeError):
    pass


DIRECT_GIT_FAST_FAIL_ENV = {
    "GITPARTNER_GIT_RETRIES": "1",
    "GITPARTNER_GIT_TIMEOUT_SECONDS": "15",
    "GITPARTNER_GIT_RETRY_BASE_SECONDS": "0",
    "GITPARTNER_GIT_OPERATION_LOCK_TIMEOUT_SECONDS": "5",
}


TRANSIENT_GIT_ERROR_TOKENS = (
    "proxy connect aborted",
    "connection timed out",
    "operation timed out",
    "timed out",
    "timeout",
    "connection reset",
    "connection refused",
    "network is unreachable",
    "could not resolve host",
    "failed to connect",
    "gnutls recv error",
    "the remote end hung up unexpectedly",
    "rpc failed",
    "early eof",
    "http 502",
    "http 503",
    "http 504",
)


class GitClient:
    def __init__(self, repo: RepoConfig, repo_dir: Path) -> None:
        self.repo = repo
        self.repo_dir = repo_dir
        self.control_branch = repo.branch
        self.write_branch = (
            repo.result_branch.strip("/")
            if repo.result_branch and repo.result_branch != repo.branch
            else None
        )
        self._token = self._load_token()

    def ensure_worktree(self) -> None:
        if not (self.repo_dir / ".git").exists():
            raise GitError(f"repo_dir is not a Git worktree: {self.repo_dir}")

    def require_branch(self, expected_branch: str) -> None:
        result = self._run(
            ["symbolic-ref", "--quiet", "--short", "HEAD"],
            check=False,
        )
        current = result.stdout.strip() if result.returncode == 0 else ""
        if current != expected_branch:
            raise GitError(
                "GitPartner worktree branch mismatch "
                f"expected={expected_branch} actual={current or 'detached'}"
            )

    def configure_identity(self) -> None:
        self._run(["config", "user.name", self.repo.author_name])
        self._run(["config", "user.email", self.repo.author_email])

    def ensure_channel_branches(
        self,
        *,
        extra_channels: dict[str, str] | None = None,
    ) -> dict[str, object]:
        """Create missing endpoint channels from source without moving HEAD."""
        source_branch = self.repo.source_branch.strip("/")
        channels: dict[str, dict[str, object]] = {}
        channel_pairs = [
            ("control", self.control_branch),
            ("result", self.write_branch),
        ]
        channel_pairs.extend((extra_channels or {}).items())
        channel_pairs = [
            (channel_type, branch)
            for channel_type, branch in channel_pairs
            if branch
        ]
        remote_heads = self._remote_branch_heads(
            [source_branch, *(branch for _, branch in channel_pairs)]
        )
        source_commit = remote_heads.get(source_branch)
        if not source_commit:
            raise GitError(
                f"unable to fetch channel {source_branch}: remote branch is missing"
            )

        # A provisioned node only needs to verify its isolated channels. Avoid
        # downloading the continuously growing source branch on every resume.
        if all(branch in remote_heads for _, branch in channel_pairs):
            for channel_type, branch in channel_pairs:
                channels[channel_type] = {
                    "branch": branch,
                    "commit": remote_heads[branch],
                    "created": False,
                }
            return {
                "source_branch": source_branch,
                "source_commit": source_commit,
                "channels": channels,
            }

        self._fetch_branch(source_branch, required=True)
        source_commit = self.rev_parse(f"{self.repo.remote}/{source_branch}")
        for channel_type, branch in channel_pairs:
            exists = self._fetch_branch(branch, required=False)
            created = False
            if not exists:
                try:
                    self._run(
                        [
                            "push",
                            self.repo.remote,
                            f"{source_commit}:refs/heads/{branch}",
                        ],
                        with_auth=True,
                    )
                    created = True
                except GitError:
                    # Another bootstrap process may have won the create race.
                    if not self._fetch_branch(branch, required=False):
                        raise
                self._fetch_branch(branch, required=True)
            commit = self.rev_parse(f"{self.repo.remote}/{branch}")
            channels[channel_type] = {
                "branch": branch,
                "commit": commit,
                "created": created,
            }
        return {
            "source_branch": source_branch,
            "source_commit": source_commit,
            "channels": channels,
        }

    def _remote_branch_heads(self, branches: list[str]) -> dict[str, str]:
        refs = [f"refs/heads/{branch.strip('/')}" for branch in branches]
        result = self._run(
            ["ls-remote", "--heads", self.repo.remote, *refs],
            with_auth=True,
        )
        heads: dict[str, str] = {}
        expected = set(refs)
        for line in result.stdout.splitlines():
            fields = line.split()
            if len(fields) != 2 or fields[1] not in expected:
                continue
            heads[fields[1].removeprefix("refs/heads/")] = fields[0]
        return heads

    def fetch(self) -> str:
        self._fetch_remote_branch()
        return self.rev_parse(f"{self.repo.remote}/{self.repo.branch}")

    def pull_rebase(self) -> None:
        self._fetch_remote_branch()
        self.integrate_fetched_head()

    def integrate_fetched_head(self) -> None:
        fast_forward = self._run(["merge", "--ff-only", "FETCH_HEAD"], check=False)
        if fast_forward.returncode != 0:
            self._run(["rebase", "--autostash", "FETCH_HEAD"])

    def checkout_remote_head(self) -> None:
        self._run(["checkout", self.repo.branch])
        self._run(["reset", "--hard", f"{self.repo.remote}/{self.repo.branch}"])

    def rev_parse(self, ref: str = "HEAD") -> str:
        result = self._run(["rev-parse", ref])
        return result.stdout.strip()

    def show_text(self, ref_path: str, *, check: bool = True) -> str | None:
        result = self._run(["show", ref_path], check=check)
        if result.returncode != 0:
            return None
        return result.stdout

    def changed_paths(self, old_ref: str, new_ref: str) -> list[str]:
        if old_ref == new_ref:
            return []
        result = self._run(["diff", "--name-only", old_ref, new_ref])
        return [line.strip() for line in result.stdout.splitlines() if line.strip()]

    def has_common_ancestor(self, left_ref: str, right_ref: str) -> bool:
        result = self._run(
            ["merge-base", left_ref, right_ref],
            check=False,
        )
        return result.returncode == 0 and bool(result.stdout.strip())

    def has_path_changes(self, repo_paths: list[str]) -> bool:
        paths = sorted(set(path.rstrip("/") for path in repo_paths if path))
        if not paths:
            return False
        result = self._run(["status", "--porcelain", "--", *paths])
        return bool(result.stdout.strip())

    def commit_and_push(self, repo_paths: list[str], message: str, max_file_bytes: int) -> bool:
        with GitOperationLock(self.repo_dir, f"transaction: {message}"):
            return self._commit_and_push(repo_paths, message, max_file_bytes)

    def _commit_and_push(
        self,
        repo_paths: list[str],
        message: str,
        max_file_bytes: int,
    ) -> bool:
        paths = sorted(set(path.rstrip("/") for path in repo_paths if path))
        if not paths:
            return False
        for path in paths:
            self._check_upload_size(self.repo_dir / path, max_file_bytes)

        if self.write_branch and _result_owned_paths(paths):
            return self._commit_result_channel(paths, message)

        self._run(["add", "-A", "--", *paths])
        diff = self._run(["diff", "--cached", "--quiet"], check=False)
        if diff.returncode == 0:
            return self._push_pending_commits()

        self._run(["commit", "-m", message])
        return self._push_pending_commits()

    def _check_upload_size(self, path: Path, max_file_bytes: int) -> None:
        if path.is_file():
            if path.stat().st_size > max_file_bytes:
                raise GitError(f"refusing to upload file above size limit: {path}")
            return

        if path.is_dir():
            for item in path.rglob("*"):
                if item.is_file() and item.stat().st_size > max_file_bytes:
                    raise GitError(f"refusing to upload file above size limit: {item}")

    def _push_pending_commits(self) -> bool:
        remote_ref = f"{self.repo.remote}/{self.repo.branch}"
        ahead = self._run(["rev-list", "--count", f"{remote_ref}..HEAD"]).stdout.strip()
        if ahead in ("", "0"):
            return False

        self.pull_rebase()
        self._run(["push", self.repo.remote, f"HEAD:{self.repo.branch}"], with_auth=True)
        return True

    def _commit_result_channel(self, paths: list[str], message: str) -> bool:
        assert self.write_branch is not None
        # Result/report channels have concurrent writers. Even direct-mode
        # fast-fail settings must allow bounded lease-conflict recovery.
        attempts = max(3, _git_attempts())
        last_result: subprocess.CompletedProcess[str] | None = None
        for attempt in range(1, attempts + 1):
            cached_base = (
                self._cached_remote_branch_commit(self.write_branch)
                if attempt == 1
                else None
            )
            result_exists = cached_base is not None
            if cached_base is None:
                result_exists = self._fetch_branch(
                    self.write_branch,
                    required=False,
                )
            base = (
                cached_base
                or self.rev_parse(f"{self.repo.remote}/{self.write_branch}")
                if result_exists
                else self.rev_parse(f"{self.repo.remote}/{self.control_branch}")
            )
            commit = self._build_result_commit(base, paths, message)
            if not commit:
                return False
            expected_remote = base if result_exists else ""
            result = self._run(
                [
                    "push",
                    f"--force-with-lease=refs/heads/{self.write_branch}:{expected_remote}",
                    self.repo.remote,
                    f"{commit}:refs/heads/{self.write_branch}",
                ],
                with_auth=True,
                check=False,
            )
            if result.returncode == 0:
                self._run(
                    [
                        "update-ref",
                        f"refs/remotes/{self.repo.remote}/{self.write_branch}",
                        commit,
                    ]
                )
                return True
            last_result = result
            detail = f"{result.stdout}\n{result.stderr}".lower()
            retryable = (
                "non-fast-forward" in detail
                or "fetch first" in detail
                or "stale info" in detail
                or _is_transient_git_failure(result)
            )
            if attempt < attempts and retryable:
                time.sleep(_git_retry_delay_seconds(attempt))
                continue
            break
        assert last_result is not None
        raise GitError(
            f"unable to publish result channel {self.write_branch}\n"
            f"stdout: {self._redact(last_result.stdout.strip())}\n"
            f"stderr: {self._redact(last_result.stderr.strip())}"
        )

    def _cached_remote_branch_commit(self, branch: str) -> str | None:
        result = self._run(
            ["rev-parse", "--verify", f"refs/remotes/{self.repo.remote}/{branch}"],
            check=False,
        )
        if result.returncode != 0:
            return None
        commit = result.stdout.strip()
        return commit or None

    def _build_result_commit(
        self, base: str, paths: list[str], message: str
    ) -> str:
        with tempfile.TemporaryDirectory(prefix="gitpartner-result-index-") as temp:
            env = {"GIT_INDEX_FILE": str(Path(temp) / "index")}
            self._run(["read-tree", base], extra_env=env)
            normalized_paths: list[str] = []
            for repo_path in paths:
                normalized = repo_path.replace("\\", "/").strip("/")
                if not normalized:
                    raise GitError("result channel path cannot be empty")
                normalized_paths.append(normalized)
                source = self.repo_dir / normalized
                files = [source] if source.is_file() or source.is_symlink() else []
                if source.is_dir():
                    files = sorted(
                        item
                        for item in source.rglob("*")
                        if item.is_file() or item.is_symlink()
                    )
                for file_path in files:
                    if file_path.is_symlink():
                        raise GitError(
                            f"result channel cannot publish symlink: {file_path}"
                        )
            self._run(
                ["add", "-A", "--", *normalized_paths],
                extra_env=env,
            )
            tree = self._run(["write-tree"], extra_env=env).stdout.strip()
            base_tree = self._run(["rev-parse", f"{base}^{{tree}}"] ).stdout.strip()
            if tree == base_tree:
                return ""
            return self._run(
                ["commit-tree", tree, "-p", base, "-m", message],
                extra_env=env,
            ).stdout.strip()

    def _fetch_branch(self, branch: str, *, required: bool) -> bool:
        remote_ref = f"refs/remotes/{self.repo.remote}/{branch}"
        result = self._run(
            [
                "fetch",
                self.repo.remote,
                f"+refs/heads/{branch}:{remote_ref}",
            ],
            with_auth=True,
            check=False,
        )
        if result.returncode == 0:
            return True
        detail = f"{result.stdout}\n{result.stderr}".lower()
        missing = "couldn't find remote ref" in detail or "could not find remote ref" in detail
        if not required and missing:
            self._run(["update-ref", "-d", remote_ref], check=False)
            return False
        raise GitError(
            f"unable to fetch channel {branch}: {self._redact(result.stderr.strip())}"
        )

    def _fetch_remote_branch(self) -> None:
        self._run(
            [
                "fetch",
                self.repo.remote,
                f"refs/heads/{self.repo.branch}:refs/remotes/{self.repo.remote}/{self.repo.branch}",
            ],
            with_auth=True,
        )

    def _run(
        self,
        args: list[str],
        *,
        cwd: Path | None = None,
        with_auth: bool = False,
        check: bool = True,
        extra_env: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        command = ["git", "-c", f"safe.directory={_git_path(self.repo_dir)}"]
        network_command = _is_network_command(args, with_auth)
        if network_command:
            command.extend(["-c", "http.lowSpeedLimit=1", "-c", "http.lowSpeedTime=60"])
        auth_header = self._auth_header() if with_auth else None
        if auth_header:
            command.extend(["-c", f"http.extraHeader={auth_header}"])
        command.extend(args)

        network_attempts = (
            _git_attempts() if check and network_command else 1
        )
        lock_attempts = _git_lock_attempts() if check else 1
        transient_failures = 0
        lock_failures = 0
        result: subprocess.CompletedProcess[str] | None = None
        while True:
            try:
                with GitOperationLock(self.repo_dir, " ".join(args)):
                    if network_command:
                        result = _run_network_git(
                            command,
                            cwd=cwd or self.repo_dir,
                            timeout_seconds=_git_timeout_seconds(),
                        )
                    else:
                        env = _noninteractive_env()
                        if extra_env:
                            env.update(extra_env)
                        result = subprocess.run(
                            command,
                            cwd=str(cwd or self.repo_dir),
                            env=env,
                            stdin=subprocess.DEVNULL,
                            text=True,
                            encoding="utf-8",
                            errors="replace",
                            capture_output=True,
                            check=False,
                            timeout=_git_timeout_seconds(),
                            **hidden_subprocess_kwargs(),
                        )
            except GitLockError as exc:
                raise GitError(str(exc)) from exc
            except subprocess.TimeoutExpired as exc:
                result = subprocess.CompletedProcess(
                    command,
                    124,
                    _timeout_text(exc.stdout),
                    _timeout_text(exc.stderr)
                    or f"git command timed out after {_git_timeout_seconds()} seconds",
                )

            if not check or result.returncode == 0:
                return result
            if _is_transient_git_failure(result):
                transient_failures += 1
                if transient_failures < network_attempts:
                    time.sleep(_git_retry_delay_seconds(transient_failures))
                    continue
            if _is_git_lock_failure(result):
                lock_failures += 1
                if lock_failures < lock_attempts:
                    recover_git_locks_after_failure(self.repo_dir)
                    time.sleep(_git_lock_retry_delay_seconds(lock_failures))
                    continue
            break

        assert result is not None
        if check and result.returncode != 0:
            stderr = self._redact(result.stderr.strip())
            stdout = self._redact(result.stdout.strip())
            raise GitError(
                f"git {' '.join(args)} failed with exit {result.returncode}\n"
                f"stdout: {stdout}\nstderr: {stderr}"
            )
        return result

    def _auth_header(self) -> str | None:
        if not self._token or not self.repo.auth_username:
            return None
        pair = f"{self.repo.auth_username}:{self._token}".encode("utf-8")
        encoded = base64.b64encode(pair).decode("ascii")
        return f"Authorization: Basic {encoded}"

    def _load_token(self) -> str | None:
        path = self.repo_dir.resolve() / "api.txt"
        if not path.exists():
            return None
        token = path.read_text(encoding="utf-8").strip().strip('"').strip("'")
        return token or None

    def _redact(self, text: str) -> str:
        if self._token:
            return text.replace(self._token, "***")
        return text


def _run_network_git(
    command: list[str],
    *,
    cwd: Path,
    timeout_seconds: int,
) -> subprocess.CompletedProcess[str]:
    process = subprocess.Popen(
        command,
        cwd=str(cwd),
        env=_noninteractive_env(),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        **process_group_kwargs(),
    )
    try:
        stdout, stderr = process.communicate(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        _signal_process_group(process, signal.SIGTERM)
        terminate_repo_git_helpers(cwd)
        try:
            stdout, stderr = process.communicate(timeout=2)
        except subprocess.TimeoutExpired:
            _signal_process_group(
                process,
                getattr(signal, "SIGKILL", signal.SIGTERM),
            )
            terminate_repo_git_helpers(cwd)
            try:
                stdout, stderr = process.communicate(timeout=2)
            except subprocess.TimeoutExpired:
                stdout, stderr = "", (
                    "GitPartner could not reap the timed-out git process "
                    f"pid={process.pid}"
                )
        remaining = terminate_repo_git_helpers(cwd)
        if remaining:
            stderr = (
                f"{stderr}\nGitPartner could not terminate repository Git helpers: "
                f"{remaining}"
            ).strip()
        raise subprocess.TimeoutExpired(
            command,
            timeout_seconds,
            output=stdout,
            stderr=stderr,
        )
    return subprocess.CompletedProcess(
        command,
        int(process.returncode or 0),
        stdout,
        stderr,
    )


def _signal_process_group(process: subprocess.Popen[str], sig: signal.Signals) -> None:
    try:
        if os.name == "nt":
            if sig == getattr(signal, "SIGKILL", signal.SIGTERM):
                process.kill()
            else:
                process.terminate()
        else:
            os.killpg(process.pid, sig)
    except (ProcessLookupError, PermissionError, OSError):
        return


def _git_path(path: Path) -> str:
    return str(path.resolve()).replace("\\", "/")


def _is_network_command(args: list[str], with_auth: bool) -> bool:
    return with_auth or bool(args and args[0] in {"fetch", "pull", "push", "clone", "ls-remote"})


def _is_transient_git_failure(result: subprocess.CompletedProcess[str]) -> bool:
    text = f"{result.stdout}\n{result.stderr}".lower()
    return any(token in text for token in TRANSIENT_GIT_ERROR_TOKENS)


def _is_git_lock_failure(result: subprocess.CompletedProcess[str]) -> bool:
    return is_git_lock_failure_text(f"{result.stdout}\n{result.stderr}")


def _result_owned_paths(paths: list[str]) -> bool:
    return bool(paths) and all(
        path.replace("\\", "/").strip("/").split("/", 1)[0] == "output"
        for path in paths
    )


def _git_attempts() -> int:
    return max(1, _int_from_env("GITPARTNER_GIT_RETRIES", 4))


def _git_lock_attempts() -> int:
    return max(1, _int_from_env("GITPARTNER_GIT_LOCK_RETRIES", 4))


def _git_timeout_seconds() -> int:
    return max(1, _int_from_env("GITPARTNER_GIT_TIMEOUT_SECONDS", 120))


def _git_retry_delay_seconds(attempt: int) -> float:
    base = max(0, _int_from_env("GITPARTNER_GIT_RETRY_BASE_SECONDS", 2))
    return float(min(30, base * attempt))


def _git_lock_retry_delay_seconds(attempt: int) -> float:
    base = max(0, _int_from_env("GITPARTNER_GIT_LOCK_RETRY_BASE_SECONDS", 2))
    return float(min(15, base * attempt))


def _int_from_env(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except ValueError:
        return default


def _timeout_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def configure_direct_git_fast_fail(transport_mode: str) -> None:
    if transport_mode not in {"auto", "direct"}:
        return
    for key, value in DIRECT_GIT_FAST_FAIL_ENV.items():
        # A protocol-aware caller may grant a larger transport budget for a
        # specific route. Direct mode supplies fast-fail defaults, but must not
        # overwrite that immutable request policy after process startup.
        os.environ.setdefault(key, value)


def _noninteractive_env() -> dict[str, str]:
    env = os.environ.copy()
    env.setdefault("GIT_TERMINAL_PROMPT", "0")
    env.setdefault("GIT_ASKPASS", "")
    env.setdefault("SSH_ASKPASS", "")
    env.setdefault("GCM_INTERACTIVE", "Never")
    env.setdefault(
        "GIT_SSH_COMMAND",
        "ssh -o BatchMode=yes -o NumberOfPasswordPrompts=0 "
        "-o StrictHostKeyChecking=accept-new",
    )
    return env
