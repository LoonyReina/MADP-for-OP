from __future__ import annotations

import ctypes
import configparser
import os
import signal
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PartnerRoleProcess:
    pid: int
    args: tuple[str, ...]


def matching_partner_role_processes(
    role: str,
    *,
    repo_dir: Path | None = None,
    proc_root: Path = Path("/proc"),
) -> list[PartnerRoleProcess]:
    matches: list[PartnerRoleProcess] = []
    resolved_repo = repo_dir.resolve() if repo_dir is not None else None
    if not proc_root.is_dir():
        return matches
    for entry in proc_root.iterdir():
        if not entry.name.isdigit():
            continue
        try:
            command = (entry / "cmdline").read_bytes().split(b"\0")
        except (FileNotFoundError, PermissionError, ProcessLookupError, OSError):
            continue
        args = [part.decode("utf-8", errors="replace") for part in command if part]
        process_role = ""
        if "limited_remote_partner.cli.partner" in args:
            try:
                role_index = args.index("--role")
            except ValueError:
                continue
            if role_index + 1 < len(args):
                process_role = args[role_index + 1]
        elif "limited_remote_partner.gateway.server" in args:
            process_role = "server"
        elif "limited_remote_partner.gateway.client" in args:
            process_role = "client"
        elif args:
            executable = Path(args[0]).name.removesuffix(".exe")
            process_role = {
                "git-partner": _role_argument(args),
                "git-partner-server": "server",
                "git-partner-client": "client",
            }.get(executable, "")
        if process_role != role:
            continue
        if resolved_repo is not None and not _process_belongs_to_repo(
            entry,
            args,
            resolved_repo,
        ):
            continue
        matches.append(PartnerRoleProcess(pid=int(entry.name), args=tuple(args)))
    return sorted(matches, key=lambda process: process.pid)


def matching_partner_role_pids(
    role: str,
    *,
    repo_dir: Path | None = None,
    proc_root: Path = Path("/proc"),
) -> list[int]:
    return [
        process.pid
        for process in matching_partner_role_processes(
            role,
            repo_dir=repo_dir,
            proc_root=proc_root,
        )
    ]


def matching_legacy_partner_processes(
    *,
    repo_dir: Path | None = None,
    proc_root: Path = Path("/proc"),
) -> list[PartnerRoleProcess]:
    matches: list[PartnerRoleProcess] = []
    resolved_repo = repo_dir.resolve() if repo_dir is not None else None
    if not proc_root.is_dir():
        return matches
    for entry in proc_root.iterdir():
        if not entry.name.isdigit():
            continue
        try:
            command = (entry / "cmdline").read_bytes().split(b"\0")
        except (FileNotFoundError, PermissionError, ProcessLookupError, OSError):
            continue
        args = [part.decode("utf-8", errors="replace") for part in command if part]
        if not _is_legacy_partner_command(args):
            continue
        if resolved_repo is not None and not _process_belongs_to_repo(
            entry,
            args,
            resolved_repo,
        ):
            continue
        matches.append(PartnerRoleProcess(pid=int(entry.name), args=tuple(args)))
    return sorted(matches, key=lambda process: process.pid)


def matching_legacy_partner_processes_for_repo_identity(
    repo_dir: Path,
    *,
    proc_root: Path = Path("/proc"),
) -> list[PartnerRoleProcess]:
    target_repo = repo_dir.resolve()
    target_remotes = _git_remote_identities(target_repo)
    matches: list[PartnerRoleProcess] = []
    for process in matching_legacy_partner_processes(proc_root=proc_root):
        process_dir = proc_root / str(process.pid)
        roots = _process_repo_roots(process_dir, list(process.args))
        if target_repo in roots:
            matches.append(process)
            continue
        if target_remotes and any(
            target_remotes.intersection(_git_remote_identities(root)) for root in roots
        ):
            matches.append(process)
    return sorted(matches, key=lambda process: process.pid)


def matching_partner_role_processes_for_repo_identity(
    role: str,
    repo_dir: Path,
    *,
    proc_root: Path = Path("/proc"),
) -> list[PartnerRoleProcess]:
    target_repo = repo_dir.resolve()
    target_remotes = _git_remote_identities(target_repo)
    matches: list[PartnerRoleProcess] = []
    for process in matching_partner_role_processes(role, proc_root=proc_root):
        process_dir = proc_root / str(process.pid)
        roots = _process_repo_roots(process_dir, list(process.args))
        if target_repo in roots:
            matches.append(process)
            continue
        if target_remotes and any(
            target_remotes.intersection(_git_remote_identities(root)) for root in roots
        ):
            matches.append(process)
    return sorted(matches, key=lambda process: process.pid)


def _is_legacy_partner_command(args: list[str]) -> bool:
    if "limited_remote_partner.cli.main" in args:
        return True
    if not args:
        return False
    executable = Path(args[0]).name.removesuffix(".exe")
    return executable == "git-partner" and "--role" not in args


def _process_repo_roots(process_dir: Path, args: list[str]) -> set[Path]:
    candidates: list[Path] = []
    cwd = _process_cwd(process_dir)
    if cwd is not None:
        candidates.append(cwd)
    config = _argument_value(args, "--config")
    if config:
        candidates.append(_resolve_process_path(config, cwd).parent)
    roots: set[Path] = set()
    for candidate in candidates:
        current = candidate.resolve()
        for _depth in range(8):
            if (current / ".git").exists():
                roots.add(current)
                break
            if current.parent == current:
                break
            current = current.parent
    return roots


def _git_remote_identities(repo_dir: Path) -> set[str]:
    config_path = _git_config_path(repo_dir)
    if config_path is None:
        return set()
    parser = configparser.ConfigParser(interpolation=None)
    try:
        parser.read(config_path, encoding="utf-8")
    except (OSError, configparser.Error, UnicodeError):
        return set()
    identities: set[str] = set()
    for section in parser.sections():
        if not section.startswith('remote "'):
            continue
        value = parser.get(section, "url", fallback="").strip()
        if value:
            identities.add(_normalize_remote_identity(value, repo_dir))
    return {identity for identity in identities if identity}


def _git_config_path(repo_dir: Path) -> Path | None:
    dot_git = repo_dir / ".git"
    if dot_git.is_dir():
        path = dot_git / "config"
        return path if path.is_file() else None
    if not dot_git.is_file():
        return None
    try:
        marker = dot_git.read_text(encoding="utf-8").strip()
    except (OSError, UnicodeError):
        return None
    if not marker.lower().startswith("gitdir:"):
        return None
    git_dir = Path(marker.split(":", 1)[1].strip())
    if not git_dir.is_absolute():
        git_dir = (repo_dir / git_dir).resolve()
    common_dir = git_dir
    common_marker = git_dir / "commondir"
    if common_marker.is_file():
        try:
            common_value = common_marker.read_text(encoding="utf-8").strip()
        except (OSError, UnicodeError):
            return None
        if not common_value:
            return None
        common_dir = Path(common_value)
        if not common_dir.is_absolute():
            common_dir = (git_dir / common_dir).resolve()
    path = common_dir / "config"
    return path if path.is_file() else None


def _normalize_remote_identity(value: str, repo_dir: Path) -> str:
    remote = value.strip().rstrip("/")
    if remote.endswith(".git"):
        remote = remote[:-4]
    if "://" in remote:
        scheme, rest = remote.split("://", 1)
        authority, separator, path = rest.partition("/")
        authority = authority.rsplit("@", 1)[-1].lower()
        return f"{scheme.lower()}://{authority}/{path}".rstrip("/") if separator else f"{scheme.lower()}://{authority}"
    if ":" in remote and not Path(remote).is_absolute():
        authority, path = remote.split(":", 1)
        authority = authority.rsplit("@", 1)[-1].lower()
        return f"ssh://{authority}/{path}".rstrip("/")
    path = Path(remote)
    if not path.is_absolute():
        path = repo_dir / path
    try:
        return path.resolve().as_posix().rstrip("/")
    except OSError:
        return remote


def _process_belongs_to_repo(
    process_dir: Path,
    args: list[str],
    repo_dir: Path,
) -> bool:
    cwd = _process_cwd(process_dir)
    candidates: list[Path] = []
    if cwd is not None:
        candidates.append(cwd)
    config = _argument_value(args, "--config")
    if config:
        candidates.append(_resolve_process_path(config, cwd))
    if args:
        executable = Path(args[0])
        if executable.is_absolute():
            candidates.append(executable.resolve())
    for candidate in candidates:
        try:
            candidate.resolve().relative_to(repo_dir)
        except (OSError, ValueError):
            continue
        return True
    return False


def _process_cwd(process_dir: Path) -> Path | None:
    try:
        return (process_dir / "cwd").resolve(strict=True)
    except (FileNotFoundError, PermissionError, ProcessLookupError, OSError):
        return None


def _resolve_process_path(value: str, cwd: Path | None) -> Path:
    path = Path(value)
    if path.is_absolute() or cwd is None:
        return path.resolve()
    return (cwd / path).resolve()


def _argument_value(args: list[str], name: str) -> str:
    try:
        index = args.index(name)
    except ValueError:
        return ""
    if index + 1 >= len(args):
        return ""
    return args[index + 1]


def _role_argument(args: list[str]) -> str:
    try:
        role_index = args.index("--role")
    except ValueError:
        return ""
    if role_index + 1 >= len(args):
        return ""
    return args[role_index + 1]


def process_start_token(pid: int) -> str:
    if os.name == "nt":
        return _windows_process_start_token(pid)
    try:
        raw = (Path("/proc") / str(pid) / "stat").read_text(encoding="utf-8")
    except (FileNotFoundError, PermissionError, ProcessLookupError, OSError):
        if pid == os.getpid():
            return f"local-{pid}"
        try:
            os.kill(pid, 0)
        except (ProcessLookupError, PermissionError, OSError, SystemError):
            return ""
        return f"pid-{pid}"
    _prefix, separator, suffix = raw.rpartition(")")
    if not separator:
        return ""
    fields = suffix.strip().split()
    if len(fields) <= 19:
        return ""
    return fields[19]


def _windows_process_start_token(pid: int) -> str:
    if pid <= 0:
        return ""

    class FileTime(ctypes.Structure):
        _fields_ = [
            ("low", ctypes.c_uint32),
            ("high", ctypes.c_uint32),
        ]

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    open_process = kernel32.OpenProcess
    open_process.argtypes = [ctypes.c_uint32, ctypes.c_int, ctypes.c_uint32]
    open_process.restype = ctypes.c_void_p
    get_process_times = kernel32.GetProcessTimes
    get_process_times.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(FileTime),
        ctypes.POINTER(FileTime),
        ctypes.POINTER(FileTime),
        ctypes.POINTER(FileTime),
    ]
    get_process_times.restype = ctypes.c_int
    get_exit_code_process = kernel32.GetExitCodeProcess
    get_exit_code_process.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_uint32)]
    get_exit_code_process.restype = ctypes.c_int
    close_handle = kernel32.CloseHandle
    close_handle.argtypes = [ctypes.c_void_p]
    close_handle.restype = ctypes.c_int

    process_query_limited_information = 0x1000
    handle = open_process(process_query_limited_information, 0, pid)
    if not handle:
        return ""
    creation = FileTime()
    exit_time = FileTime()
    kernel = FileTime()
    user = FileTime()
    exit_code = ctypes.c_uint32()
    try:
        if not get_exit_code_process(handle, ctypes.byref(exit_code)):
            return ""
        if exit_code.value != 259:  # STILL_ACTIVE
            return ""
        if not get_process_times(
            handle,
            ctypes.byref(creation),
            ctypes.byref(exit_time),
            ctypes.byref(kernel),
            ctypes.byref(user),
        ):
            return ""
    finally:
        close_handle(handle)
    return f"win-{creation.high:08x}{creation.low:08x}"


def signal_exact_process_identity(
    pid: int,
    start_token: str,
    sig: int,
    *,
    pgid: int = 0,
) -> tuple[bool, str]:
    if pid <= 0 or not start_token or process_start_token(pid) != start_token:
        return False, "identity-mismatch"
    try:
        if pgid:
            if os.name == "nt" or pgid != pid or os.getpgid(pid) != pgid:
                return False, "pgid-mismatch"
            os.killpg(pgid, sig)
            return True, f"pgid={pgid}"
        os.kill(pid, sig)
        return True, f"pid={pid}"
    except (ProcessLookupError, PermissionError, OSError):
        return False, "signal-failed"


def stop_exact_process_identity(
    pid: int,
    start_token: str,
    *,
    timeout_seconds: float,
) -> dict[str, object]:
    if pid <= 0 or not start_token or process_start_token(pid) != start_token:
        return {"stopped": False, "forced": False, "detail": "identity-mismatch"}
    sent, detail = signal_exact_process_identity(pid, start_token, signal.SIGTERM)
    deadline = time.monotonic() + max(0.1, timeout_seconds)
    while sent and time.monotonic() < deadline:
        if process_start_token(pid) != start_token:
            break
        time.sleep(0.1)
    forced = False
    if process_start_token(pid) == start_token:
        force_signal = getattr(signal, "SIGKILL", signal.SIGTERM)
        forced, detail = signal_exact_process_identity(
            pid,
            start_token,
            force_signal,
        )
    return {"stopped": sent or forced, "forced": forced, "detail": detail}


def hidden_subprocess_kwargs() -> dict[str, object]:
    if os.name != "nt":
        return {}
    startupinfo = subprocess.STARTUPINFO()
    startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    startupinfo.wShowWindow = 0
    return {
        "creationflags": getattr(subprocess, "CREATE_NO_WINDOW", 0),
        "startupinfo": startupinfo,
    }


def process_group_kwargs() -> dict[str, object]:
    if os.name == "nt":
        return hidden_subprocess_kwargs()
    return {"start_new_session": True}
