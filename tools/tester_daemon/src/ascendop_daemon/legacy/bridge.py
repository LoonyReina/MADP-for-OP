from __future__ import annotations

import argparse
import json
import os
import queue
import re
import shutil
import socket
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from hashlib import sha1
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[5]
for package_root in (
    ROOT / "tools" / "tester_daemon" / "src",
    ROOT / "packages" / "ascendop_protocol" / "src",
    ROOT,
):
    if str(package_root) not in sys.path:
        sys.path.insert(0, str(package_root))

from ascendop_daemon.runtime.config_loader import load_config
from ascendop_daemon.runtime.control import read_stop_request
from ascendop_daemon.runtime.locking import process_alive, read_lock_pid
from ascendop_daemon.core.models import (
    casegen_session_ops,
    observed_operators,
    solver_thread_for,
    tester_thread_for,
    utc_now_iso,
)
from ascendop_daemon.legacy.native_relay_outbox import tester_casegen_active_covering_ack
from ascendop_daemon.runtime.process_inspection import (
    windows_process_table as native_windows_process_table,
)
from ascendop_daemon.automation.trigger_state import (
    ack_solver_trigger,
    ack_tester_trigger,
    delivery_reconciliation_pending,
    read_tester_trigger_ack_state,
    read_trigger_ack_state,
)


STATE_DIR = ROOT / "TestUtils" / "tester_daemon"
BRIDGE_LOCK = STATE_DIR / "solver_trigger_bridge.lock"
WORKERS_PATH = STATE_DIR / "solver_trigger_bridge_workers.json"
STATE_PATH = STATE_DIR / "solver_trigger_bridge_state.json"
HEARTBEAT_PATH = STATE_DIR / "solver_trigger_bridge_heartbeat.json"
STATUS_PATH = STATE_DIR / "SOLVER_TRIGGER_BRIDGE_STATUS.md"
EVENTS_PATH = STATE_DIR / "solver_trigger_bridge_events.jsonl"
THREAD_OBSERVATIONS_PATH = STATE_DIR / "solver_thread_observations.json"
THREAD_OBSERVATIONS_MD_PATH = STATE_DIR / "SOLVER_THREAD_OBSERVATIONS.md"
TESTER_THREAD_OBSERVATIONS_PATH = STATE_DIR / "tester_thread_observations.json"
TESTER_THREAD_OBSERVATIONS_MD_PATH = STATE_DIR / "TESTER_THREAD_OBSERVATIONS.md"
THREAD_POLL_STATUS_PATH = STATE_DIR / "solver_thread_poll_status.json"
APP_SERVER_PROXY_REPAIR_PATH = STATE_DIR / "app_server_proxy_repair.json"
RELAY_CAPABILITY_STATE_PATH = STATE_DIR / "relay_capability_state.json"
TURN_STATUS_POLL_SECONDS = 5.0
APP_SERVER_PROXY_REPAIR_COOLDOWN_SECONDS = 300
WATCH_OBSERVATION_FALLBACK_RETRY_SECONDS = 60
DELIVERY_FAILURE_RETRY_SECONDS = 60
REMOTE_CONTROL_FAILURE_RETRY_SECONDS = 300
WATCH_WORKER_MAX_WAIT_SECONDS = 5
REMOTE_CONTROL_ENABLE_WAIT_SECONDS = 10
REMOTE_CONTROL_READY_STATUSES = {"connected", "enabled", "ready"}
CODEX_CLI_RESUME_DELIVERY = "codex-cli-exec-resume"
CODEX_CLI_RESUME_VISIBILITY = "storage_visible_not_ide_visible"
ROLLOUT_PATH_CACHE_SECONDS = 60.0
_ROLLOUT_CACHE_LOCK = threading.Lock()
_ROLLOUT_PATH_CACHE: dict[
    tuple[str, tuple[str, ...]],
    tuple[float, dict[str, Path]],
] = {}
_ROLLOUT_TURN_CACHE: dict[str, tuple[int, int, dict[str, Any]]] = {}


class NativeDeliveryRequired(RuntimeError):
    pass


@dataclass(frozen=True)
class Trigger:
    kind: str
    op: str
    gate_stage: str
    key: str
    thread_id: str
    prompt_path: Path
    status: str
    plan_updated_at: str = ""
    model: str = ""
    thinking: str = ""

    @property
    def digest(self) -> str:
        return sha1(self.key.encode("utf-8")).hexdigest()[:12]


@dataclass(frozen=True)
class WatchTrigger:
    kind: str
    op: str
    gate_stage: str
    key: str
    thread_id: str
    turn_id: str

    @property
    def digest(self) -> str:
        return sha1(self.key.encode("utf-8")).hexdigest()[:12]


def daemon_side_app_server_allowed() -> bool:
    return os.name != "nt"


def daemon_delivery_workers_enabled() -> bool:
    """Return whether this process can create an IDE-visible agent turn."""
    return daemon_side_app_server_allowed() or codex_cli_resume_fallback_enabled()


class AppServerClient:
    def __init__(self) -> None:
        self.process: subprocess.Popen[str] | None = None
        self.reader_thread: threading.Thread | None = None
        self.stderr_thread: threading.Thread | None = None
        self.messages: queue.Queue[dict[str, Any]] = queue.Queue()
        self.next_id = 1
        self.server_mode = ""
        self.ws_url = ""
        self.ws: Any | None = None

    def __enter__(self) -> "AppServerClient":
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def start(self) -> None:
        if self.process is not None:
            return
        if not daemon_side_app_server_allowed():
            raise NativeDeliveryRequired(
                "daemon-side Codex app-server is disabled on Windows because IDE remote control is unavailable; "
                "use the App-side native relay outbox for IDE-visible delivery"
            )
        command, mode = app_server_command()
        self.server_mode = mode
        if mode == "ws":
            self.ws_url = command[-1]
        if mode == "proxy" and not app_server_proxy_socket_path().exists():
            repair = attempt_app_server_proxy_repair()
            if app_server_proxy_socket_path().exists():
                append_event("app_server_proxy_repair_recovered", repair)
            else:
                detail = proxy_repair_detail(repair)
                raise NativeDeliveryRequired(
                    "Codex app-server proxy control socket is not available: "
                    f"{app_server_proxy_socket_path()}; {detail}; "
                    "use codex_app.send_message_to_thread for IDE-visible delivery"
                )
        if mode == "proxy" and not app_server_proxy_socket_path().exists():
            raise NativeDeliveryRequired(
                "Codex app-server proxy control socket is not available: "
                f"{app_server_proxy_socket_path()}; use codex_app.send_message_to_thread for IDE-visible delivery"
            )
        self.process = subprocess.Popen(
            command,
            cwd=str(ROOT),
            env=codex_subprocess_env(),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            creationflags=process_creation_flags(),
            startupinfo=process_startupinfo(),
        )
        self.reader_thread = threading.Thread(
            target=self._stdout_reader if mode == "ws" else self._reader,
            daemon=True,
        )
        self.reader_thread.start()
        self.stderr_thread = threading.Thread(target=self._stderr_reader, daemon=True)
        self.stderr_thread.start()
        if mode == "ws":
            try:
                self._connect_ws()
            except Exception:
                self.close()
                raise
    def _reader(self) -> None:
        assert self.process is not None
        assert self.process.stdout is not None
        try:
            for line in self.process.stdout:
                line = line.strip()
                if not line:
                    continue
                try:
                    message = json.loads(line)
                except json.JSONDecodeError:
                    message = {"method": "bridge/non_json_stdout", "params": {"line": line}}
                self.messages.put(message)
        except Exception as exc:
            append_event("app_server_reader_failed", {"error": str(exc)})

    def _stdout_reader(self) -> None:
        assert self.process is not None
        assert self.process.stdout is not None
        try:
            for line in self.process.stdout:
                line = line.strip()
                if line:
                    append_event("app_server_stdout", {"line": line[:1000], "mode": self.server_mode})
        except Exception as exc:
            append_event("app_server_stdout_reader_failed", {"error": str(exc), "mode": self.server_mode})

    def _stderr_reader(self) -> None:
        assert self.process is not None
        assert self.process.stderr is not None
        try:
            for line in self.process.stderr:
                line = line.strip()
                if line:
                    append_event("app_server_stderr", {"line": line[:1000]})
        except Exception as exc:
            append_event("app_server_stderr_reader_failed", {"error": str(exc)})

    def _connect_ws(self) -> None:
        ws_module = websocket_client_module()
        deadline = time.monotonic() + 20
        last_error = ""
        while time.monotonic() < deadline:
            if self.process is not None and self.process.poll() is not None:
                raise RuntimeError(f"app-server exited before websocket connect: {self.process.returncode}")
            try:
                self.ws = ws_module.create_connection(self.ws_url, timeout=1, suppress_origin=True)
                self.ws.settimeout(1)
                return
            except Exception as exc:
                last_error = str(exc)
                time.sleep(0.25)
        raise RuntimeError(f"timed out connecting to app-server websocket {self.ws_url}: {last_error}")

    def call(self, method: str, params: dict[str, Any] | None = None, timeout_seconds: int = 60) -> dict[str, Any]:
        if self.ws is not None:
            return self._call_ws(method, params, timeout_seconds)
        if self.process is None or self.process.stdin is None:
            raise RuntimeError("app-server process is not started")
        request_id = self.next_id
        self.next_id += 1
        request = {"jsonrpc": "2.0", "id": request_id, "method": method}
        if params is not None:
            request["params"] = params
        self.process.stdin.write(json.dumps(request, ensure_ascii=False) + "\n")
        self.process.stdin.flush()

        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(f"app-server exited while waiting for method {method}")
            try:
                message = self.messages.get(timeout=max(0.1, min(1.0, deadline - time.monotonic())))
            except queue.Empty:
                continue
            if message.get("id") == request_id:
                if "error" in message:
                    raise RuntimeError(f"app-server {method} failed: {message['error']}")
                return message
            append_event("app_server_notification", {"method": message.get("method", ""), "params": slim_params(message)})
        raise TimeoutError(f"timed out waiting for app-server method {method}")

    def _call_ws(self, method: str, params: dict[str, Any] | None = None, timeout_seconds: int = 60) -> dict[str, Any]:
        if self.ws is None:
            raise RuntimeError("app-server websocket is not started")
        ws_module = websocket_client_module()
        request_id = self.next_id
        self.next_id += 1
        request = {"jsonrpc": "2.0", "id": request_id, "method": method}
        if params is not None:
            request["params"] = params
        self.ws.send(json.dumps(request, ensure_ascii=False))
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            if self.process is not None and self.process.poll() is not None:
                raise RuntimeError(f"app-server exited while waiting for method {method}")
            try:
                raw = self.ws.recv()
            except ws_module.WebSocketTimeoutException:
                continue
            except Exception as exc:
                raise RuntimeError(f"app-server websocket recv failed for method {method}: {exc}") from exc
            if not raw:
                continue
            try:
                message = json.loads(raw)
            except json.JSONDecodeError:
                append_event("app_server_non_json_ws", {"line": str(raw)[:1000]})
                continue
            if message.get("id") == request_id:
                if "error" in message:
                    raise RuntimeError(f"app-server {method} failed: {message['error']}")
                return message
            append_event("app_server_notification", {"method": message.get("method", ""), "params": slim_params(message)})
        raise TimeoutError(f"timed out waiting for app-server method {method}")


    def list_turns(self, thread_id: str, limit: int = 10) -> list[dict[str, Any]]:
        response = self.call(
            "thread/turns/list",
            {"threadId": thread_id, "limit": limit, "itemsView": "summary", "sortDirection": "desc"},
            timeout_seconds=60,
        )
        return turns_from_response(response)

    def read_thread_turns(self, thread_id: str) -> list[dict[str, Any]]:
        response = self.call("thread/read", {"threadId": thread_id, "includeTurns": True}, timeout_seconds=60)
        result = response.get("result", {}) if isinstance(response.get("result"), dict) else {}
        thread = result.get("thread", {}) if isinstance(result.get("thread"), dict) else {}
        turns = thread.get("turns")
        return [turn for turn in turns if isinstance(turn, dict)] if isinstance(turns, list) else []

    def get_turn(self, thread_id: str, turn_id: str) -> dict[str, Any]:
        for turn in self.list_turns(thread_id, limit=10):
            if str(turn.get("id", "") or "") == turn_id:
                return turn
        return {}

    def read_thread_turn(self, thread_id: str, turn_id: str) -> dict[str, Any]:
        for turn in reversed(self.read_thread_turns(thread_id)):
            if str(turn.get("id", "") or "") == turn_id:
                return turn
        return {}

    def get_turn_status(self, thread_id: str, turn_id: str) -> str:
        turn = self.get_turn(thread_id, turn_id)
        if not turn:
            return ""
        return str(turn.get("status", "") or "")

    def wait_for_native_turn(self, thread_id: str, turn_id: str, timeout_seconds: int = 30) -> dict[str, Any]:
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            turn = self.get_turn(thread_id, turn_id)
            if turn:
                return turn
            time.sleep(1.0)
        return {}
        return ""

    def wait_for_storage_turn(self, thread_id: str, turn_id: str, timeout_seconds: int = 30) -> dict[str, Any]:
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            turn = self.read_thread_turn(thread_id, turn_id)
            if turn:
                return turn
            time.sleep(1.0)
        return {}

    def wait_for_turn(self, thread_id: str, turn_id: str, timeout_seconds: int) -> str:
        deadline = time.monotonic() + timeout_seconds
        next_poll = time.monotonic()
        while time.monotonic() < deadline:
            try:
                message = self.messages.get(timeout=max(0.1, min(1.0, deadline - time.monotonic())))
            except queue.Empty:
                if self.process is not None and self.process.poll() is not None:
                    raise RuntimeError(f"app-server exited while waiting for turn {turn_id}")
                if time.monotonic() >= next_poll:
                    next_poll = time.monotonic() + TURN_STATUS_POLL_SECONDS
                    status = self.get_turn_status(thread_id, turn_id)
                    if not status or status == "inProgress":
                        storage_turn = self.read_thread_turn(thread_id, turn_id)
                        storage_status = str(storage_turn.get("status", "") or "") if storage_turn else ""
                        if storage_status and storage_status != "inProgress":
                            return storage_status
                        if not status:
                            status = storage_status
                    if status and status != "inProgress":
                        return status
                continue
            method = str(message.get("method", ""))
            params = message.get("params", {}) if isinstance(message.get("params"), dict) else {}
            append_event("app_server_notification", {"method": method, "params": slim_params(message)})
            if method != "turn/completed":
                continue
            turn = params.get("turn", {}) if isinstance(params.get("turn"), dict) else {}
            if params.get("threadId") == thread_id and turn.get("id") == turn_id:
                return str(turn.get("status", "completed") or "completed")
        return "timeout"

    def enable_remote_control(self, timeout_seconds: int = REMOTE_CONTROL_ENABLE_WAIT_SECONDS) -> dict[str, Any]:
        """Ask Codex Desktop to allow app-server initiated turns.

        Without this, a live websocket `turn/start` can create an IDE-visible
        turn and then immediately interrupt it when remote control is disabled.
        Older app-server builds may not support the method; that case is
        recorded but not fatal so read-only polling remains compatible.
        """
        started_at = utc_now_iso()
        try:
            response = self.call("remoteControl/enable", {}, timeout_seconds=10)
        except Exception as exc:
            return {
                "remote_control_supported": False,
                "remote_control_error": str(exc),
                "remote_control_status": "enable_failed",
                "remote_control_started_at": started_at,
            }
        result = response.get("result", {}) if isinstance(response.get("result"), dict) else {}
        status = str(result.get("status", "") or "unknown")
        detail: dict[str, Any] = {
            "remote_control_supported": True,
            "remote_control_status": status,
            "remote_control_started_at": started_at,
        }
        for key in ("serverName", "installationId", "environmentId"):
            if key in result:
                detail[f"remote_control_{key}"] = result.get(key)
        deadline = time.monotonic() + max(0, timeout_seconds)
        while status == "connecting" and time.monotonic() < deadline:
            try:
                message = self.messages.get(timeout=max(0.1, min(1.0, deadline - time.monotonic())))
            except queue.Empty:
                continue
            method = str(message.get("method", "") or "")
            params = message.get("params", {}) if isinstance(message.get("params"), dict) else {}
            append_event("app_server_notification", {"method": method, "params": slim_params(message)})
            if method != "remoteControl/status/changed":
                continue
            status = str(params.get("status", "") or status)
            detail["remote_control_status"] = status
            detail["remote_control_status_changed_at"] = utc_now_iso()
            if status not in {"connecting", ""}:
                break
        return detail

    def close(self) -> None:
        if self.process is None:
            return
        process = self.process
        try:
            ws = getattr(self, "ws", None)
            if ws is not None:
                try:
                    ws.close()
                except Exception as exc:
                    append_event("app_server_ws_close_error", {"pid": process.pid, "mode": self.server_mode, "error": str(exc)})
                self.ws = None
            if os.name == "nt":
                terminate_process_tree(process.pid)
            if process.poll() is None:
                try:
                    process.terminate()
                    process.communicate(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    try:
                        process.communicate(timeout=5)
                    except subprocess.TimeoutExpired:
                        append_event(
                            "app_server_close_timeout",
                            {"pid": process.pid, "mode": self.server_mode},
                        )
                except OSError as exc:
                    append_event(
                        "app_server_close_error",
                        {"pid": process.pid, "mode": self.server_mode, "error": str(exc)},
                    )
        finally:
            self.process = None


def app_server_command() -> tuple[list[str], str]:
    mode = os.environ.get("ASCENDOP_CODEX_APP_SERVER_MODE", "stdio").strip().lower() or "stdio"
    if mode not in {"stdio", "proxy", "ws"}:
        mode = "stdio"
    subcommand = "proxy" if mode == "proxy" else "--stdio"
    extra: list[str] = []
    if mode == "proxy":
        explicit_sock = os.environ.get("ASCENDOP_CODEX_APP_SERVER_PROXY_SOCK", "").strip()
        if explicit_sock:
            extra = ["--sock", explicit_sock]
    if mode == "ws":
        ws_url = os.environ.get("ASCENDOP_CODEX_APP_SERVER_WS_URL", "").strip() or allocate_loopback_ws_url()
        enable_remote_control = (
            os.environ.get("ASCENDOP_REQUIRE_IDE_VISIBLE_DELIVERY", "").strip().lower()
            in {"1", "true", "yes"}
        )
        feature_args = ["--enable", "remote_control"] if enable_remote_control else []
        return [codex_executable(), "app-server", *feature_args, "--listen", ws_url], mode
    return [codex_executable(), "app-server", subcommand, *extra], mode


def codex_executable() -> str:
    explicit = os.environ.get("ASCENDOP_CODEX_EXECUTABLE", "").strip()
    if explicit and Path(explicit).is_file():
        return explicit
    inherited = os.environ.get("CODEX_CLI_PATH", "").strip()
    if inherited and Path(inherited).is_file():
        return inherited
    if os.name == "nt":
        local_app_bin = Path(os.environ.get("LOCALAPPDATA", str(Path.home() / "AppData" / "Local"))) / "OpenAI" / "Codex" / "bin"
        bundled = sorted(
            local_app_bin.glob("*/codex.exe"),
            key=lambda path: path.stat().st_mtime_ns,
            reverse=True,
        )
        if bundled:
            return str(bundled[0])
        codex = shutil.which("codex.cmd") or shutil.which("codex.exe") or shutil.which("codex")
        if codex:
            return codex
    return shutil.which("codex") or "codex"


def codex_subprocess_env() -> dict[str, str]:
    env = os.environ.copy()
    ca_path = str(env.get("CODEX_CA_CERTIFICATE") or env.get("SSL_CERT_FILE") or "").strip()
    if not ca_path:
        try:
            import certifi  # type: ignore

            ca_path = str(certifi.where())
        except Exception:
            ca_path = ""
    if not ca_path and os.name == "nt":
        git_ca = Path(os.environ.get("ProgramFiles", r"C:\Program Files")) / "Git" / "usr" / "ssl" / "certs" / "ca-bundle.crt"
        if git_ca.is_file():
            ca_path = str(git_ca)
    if ca_path and Path(ca_path).is_file():
        env["CODEX_CA_CERTIFICATE"] = ca_path
        env["SSL_CERT_FILE"] = ca_path
    return env


def allocate_loopback_ws_url() -> str:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        _, port = sock.getsockname()
    return f"ws://127.0.0.1:{port}"


def websocket_client_module() -> Any:
    try:
        import websocket  # type: ignore

        return websocket
    except Exception as exc:
        raise RuntimeError("Python package websocket-client is required for app-server ws mode") from exc


def app_server_proxy_socket_path() -> Path:
    explicit = os.environ.get("ASCENDOP_CODEX_APP_SERVER_PROXY_SOCK", "").strip()
    if explicit:
        return Path(explicit)
    return Path.home() / ".codex" / "app-server-control" / "app-server-control.sock"


def attempt_app_server_proxy_repair() -> dict[str, Any]:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    previous = read_json(APP_SERVER_PROXY_REPAIR_PATH)
    previous_time = parse_timestamp(str(previous.get("time", "") or ""))
    current_socket = str(app_server_proxy_socket_path())
    now = datetime.now(timezone.utc)
    if (
        str(previous.get("socket", "") or "") == current_socket
        and previous_time is not None
        and (now - previous_time).total_seconds() < APP_SERVER_PROXY_REPAIR_COOLDOWN_SECONDS
    ):
        record = {
            **previous,
            "skipped": True,
            "skip_reason": "cooldown",
            "cooldown_seconds": APP_SERVER_PROXY_REPAIR_COOLDOWN_SECONDS,
            "socket_exists_after": app_server_proxy_socket_path().exists(),
        }
        write_app_server_proxy_repair(record)
        return record

    command = app_server_daemon_start_command()
    started_at = utc_now_iso()
    if os.name == "nt":
        record = {
            "time": started_at,
            "command": command,
            "returncode": None,
            "stdout_tail": "",
            "stderr_tail": "codex app-server daemon lifecycle is only supported on Unix platforms",
            "socket": str(app_server_proxy_socket_path()),
            "socket_exists_after": app_server_proxy_socket_path().exists(),
            "platform": sys.platform,
            "skipped": True,
            "skip_reason": "unsupported_platform",
        }
        write_app_server_proxy_repair(record)
        append_event("app_server_proxy_repair_skipped", record)
        return record
    try:
        completed = subprocess.run(
            command,
            cwd=str(ROOT),
            env=codex_subprocess_env(),
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            timeout=20,
            creationflags=diagnostic_creation_flags(),
            startupinfo=process_startupinfo(),
        )
        record = {
            "time": started_at,
            "command": command,
            "returncode": completed.returncode,
            "stdout_tail": completed.stdout[-1000:],
            "stderr_tail": completed.stderr[-1000:],
            "socket": str(app_server_proxy_socket_path()),
            "socket_exists_after": app_server_proxy_socket_path().exists(),
            "platform": sys.platform,
        }
    except Exception as exc:
        record = {
            "time": started_at,
            "command": command,
            "returncode": None,
            "error": str(exc),
            "socket": str(app_server_proxy_socket_path()),
            "socket_exists_after": app_server_proxy_socket_path().exists(),
            "platform": sys.platform,
        }
    write_app_server_proxy_repair(record)
    append_event("app_server_proxy_repair_attempted", record)
    return record


def app_server_daemon_start_command() -> list[str]:
    codex = shutil.which("codex") or shutil.which("codex.cmd") or shutil.which("codex.exe")
    if os.name == "nt":
        return [
            "cmd.exe",
            "/d",
            "/q",
            "/c",
            codex or "codex",
            "app-server",
            "daemon",
            "start",
        ]
    if codex:
        return [codex, "app-server", "daemon", "start"]
    return ["codex", "app-server", "daemon", "start"]


def write_app_server_proxy_repair(record: dict[str, Any]) -> None:
    APP_SERVER_PROXY_REPAIR_PATH.write_text(
        json.dumps(record, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def proxy_repair_detail(record: dict[str, Any]) -> str:
    if record.get("skipped"):
        return (
            "proxy repair skipped during cooldown "
            f"after returncode={record.get('returncode', '-')}"
        )
    if record.get("returncode") == 0 and not record.get("socket_exists_after"):
        return "proxy repair command succeeded but control socket is still absent"
    if record.get("returncode") not in {None, 0}:
        stderr = str(record.get("stderr_tail", "") or "").strip().replace("\n", " ")
        return f"proxy repair failed returncode={record.get('returncode')} stderr={stderr[:220]}"
    if record.get("error"):
        return f"proxy repair errored: {str(record.get('error'))[:220]}"
    return "proxy repair did not create the control socket"


def apply_app_server_policy_env(config: Any) -> None:
    policy = config.policy if hasattr(config, "policy") else {}
    mode = str(policy.get("solver_bridge_app_server_mode", "") or "").strip().lower()
    require_visible = bool(policy.get("solver_bridge_require_ide_visible_delivery", False))
    allow_storage_visible = bool(policy.get("solver_bridge_allow_storage_visible_delivery", False))
    cli_resume_fallback = bool(policy.get("solver_bridge_cli_resume_fallback", False))
    if require_visible and not allow_storage_visible and mode not in {"proxy", "ws"}:
        mode = "proxy"
    if mode in {"proxy", "stdio", "ws"}:
        os.environ["ASCENDOP_CODEX_APP_SERVER_MODE"] = mode
    if require_visible:
        os.environ["ASCENDOP_REQUIRE_IDE_VISIBLE_DELIVERY"] = "1"
    os.environ["ASCENDOP_ALLOW_STORAGE_VISIBLE_DELIVERY"] = "1" if allow_storage_visible else "0"
    os.environ["ASCENDOP_CODEX_CLI_RESUME_FALLBACK"] = "1" if cli_resume_fallback else "0"
    os.environ["ASCENDOP_LOCAL_TURN_TIMEOUT_SECONDS"] = str(
        int(policy.get("solver_bridge_local_turn_timeout_seconds", 7200) or 7200)
    )


def slim_params(message: dict[str, Any]) -> dict[str, Any]:
    params = message.get("params")
    if not isinstance(params, dict):
        return {}
    slim: dict[str, Any] = {}
    for key in ("threadId", "turnId", "status"):
        if key in params:
            slim[key] = params[key]
    turn = params.get("turn")
    if isinstance(turn, dict):
        slim["turn"] = {"id": turn.get("id"), "status": turn.get("status")}
    return slim


def turns_from_response(response: dict[str, Any]) -> list[dict[str, Any]]:
    result = response.get("result", {}) if isinstance(response.get("result"), dict) else {}
    data = result.get("data")
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    page = result.get("page")
    if isinstance(page, dict):
        page_data = page.get("data")
        if isinstance(page_data, list):
            return [item for item in page_data if isinstance(item, dict)]
    items = result.get("items")
    if isinstance(items, list):
        return [item for item in items if isinstance(item, dict)]
    return []


def thread_turns_from_read_response(response: dict[str, Any]) -> list[dict[str, Any]]:
    result = response.get("result", {}) if isinstance(response.get("result"), dict) else {}
    thread = result.get("thread", {}) if isinstance(result.get("thread"), dict) else {}
    turns = thread.get("turns")
    return [item for item in turns if isinstance(item, dict)] if isinstance(turns, list) else []


def turn_ack_details(turn: dict[str, Any], *, server_mode: str = "stdio") -> dict[str, Any]:
    storage_visible = bool(turn)
    live_app_server = server_mode in {"proxy", "ws"}
    ide_visible = storage_visible and live_app_server
    visibility = f"live_{server_mode}" if ide_visible else "storage_visible_not_ide_visible"
    details: dict[str, Any] = {
        "storage_visible": storage_visible,
        "native_visible": storage_visible,
        "ide_panel_visible": ide_visible,
        "ide_panel_visibility": visibility if storage_visible else "not_visible",
        "app_server_mode": server_mode,
    }
    if not turn:
        return details
    for key in ("id", "status", "startedAt", "completedAt", "durationMs"):
        if key in turn:
            details[f"native_{key}"] = turn.get(key)
    error = turn.get("error")
    if error:
        details["native_error"] = error
    return details


def is_empty_terminal_failure_turn(turn: dict[str, Any]) -> bool:
    status = str(turn.get("status", "") or "")
    if status not in {"interrupted", "failed", "cancelled"}:
        return False
    items = turn.get("items")
    return isinstance(items, list) and not items


def is_synthetic_solver_turn(turn: dict[str, Any]) -> bool:
    """Filter app bookkeeping turns out of solver liveness/visibility checks."""
    turn_id = str(turn.get("id", "") or "")
    if turn_id.startswith("rollout-"):
        return True
    has_time = turn.get("startedAt") not in (None, "") or turn.get("completedAt") not in (None, "")
    if has_time:
        return False
    items = turn.get("items")
    if not isinstance(items, list) or not items:
        return False
    return all(str(item.get("type", "") or "") == "contextCompaction" for item in items if isinstance(item, dict))


AGENT_OUTPUT_ITEM_TYPES = {
    "agentMessage",
    "fileChange",
    "toolCall",
    "toolResult",
    "command",
}


def turn_has_agent_output(turn: dict[str, Any]) -> bool:
    """Return False only when a turn is visibly user-only.

    Some thread APIs may omit item details; absence of an `items` list is treated
    as unknown rather than failed. An explicit list with only user/context items
    means native delivery reached the IDE, but the solver did not actually run.
    """
    items = turn.get("items")
    if not isinstance(items, list):
        return True
    if not items:
        return False
    for item in items:
        if not isinstance(item, dict):
            continue
        if str(item.get("type", "") or "") in AGENT_OUTPUT_ITEM_TYPES:
            return True
    return False


def turn_is_user_only(turn: dict[str, Any]) -> bool:
    items = turn.get("items")
    if not isinstance(items, list):
        return False
    return not turn_has_agent_output(turn)


def first_observable_solver_turn(turns: list[dict[str, Any]]) -> dict[str, Any]:
    for turn in turns:
        if not is_synthetic_solver_turn(turn):
            return turn
    return {}


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def load_ready_triggers(root: Path = ROOT) -> list[Trigger]:
    state_dir = root / "TestUtils" / "tester_daemon"
    ready: list[Trigger] = []
    for kind, plan_name, ack_reader in trigger_sources():
        plan = read_json(state_dir / plan_name)
        plan_updated_at = str(plan.get("updated_at", "") or "")
        ack_state = ack_reader(root)
        sent = ack_state.get("sent", {}) if isinstance(ack_state.get("sent"), dict) else {}
        triggers = plan.get("triggers", [])
        if not isinstance(triggers, list):
            continue
        for item in triggers:
            if not isinstance(item, dict):
                continue
            status = str(item.get("status", "") or "")
            key = str(item.get("key", "") or "")
            if status == "needs-native-delivery":
                record = sent.get(key)
                if isinstance(record, dict) and not delivery_retry_due(record):
                    continue
                status = "retry-ready"
            elif status not in {"ready", "retry-ready"}:
                continue
            if (
                kind == "tester"
                and status != "retry-ready"
                and tester_casegen_active_covering_ack(
                    sent,
                    str(item.get("op", "") or ""),
                    key,
                )
            ):
                continue
            if trigger_ack_suppresses(sent.get(key), status, plan_updated_at):
                continue
            thread_id = str(item.get("thread_id", "") or "")
            if not thread_id:
                continue
            prompt_rel = str(item.get("prompt_path", "") or "")
            prompt_path = root / prompt_rel
            ready.append(
                Trigger(
                    kind=kind,
                    op=str(item.get("op", "") or ""),
                    gate_stage=str(item.get("gate_stage", "") or ""),
                    key=key,
                    thread_id=thread_id,
                    prompt_path=prompt_path,
                    status=status,
                    plan_updated_at=plan_updated_at,
                    model=str(item.get("model", "") or ""),
                    thinking=str(item.get("thinking", "") or ""),
                )
            )
    return sort_ready_triggers(ready, state_dir)


def sort_ready_triggers(ready: list[Trigger], state_dir: Path) -> list[Trigger]:
    debt = traffic_debt_from_efficiency(state_dir)
    indexed = list(enumerate(ready))
    indexed.sort(key=lambda item: ready_trigger_sort_key(item[1], debt, item[0]))
    return [trigger for _, trigger in indexed]


def ready_trigger_sort_key(trigger: Trigger, debt: dict[str, int], index: int) -> tuple[int, int, int]:
    op_debt = debt.get(trigger.op, 0)
    casegen_priority = 1 if op_debt > 0 and (trigger.kind == "tester" or trigger_is_casegen(trigger)) else 0
    return (-op_debt, -casegen_priority, index)


def trigger_is_casegen(trigger: Trigger) -> bool:
    text = f"{trigger.gate_stage} {trigger.key}".lower()
    return "casegen" in text or "case-version" in text or "needs-case" in text


def traffic_debt_from_efficiency(state_dir: Path) -> dict[str, int]:
    efficiency = read_json(state_dir / "test_efficiency.json")
    balance = efficiency.get("traffic_balance", {})
    raw_debt = balance.get("debt", {}) if isinstance(balance, dict) else {}
    if not isinstance(raw_debt, dict):
        return {}
    debt: dict[str, int] = {}
    for op, value in raw_debt.items():
        try:
            debt[str(op)] = max(0, int(value))
        except (TypeError, ValueError):
            continue
    return debt


def read_relay_capability_state() -> dict[str, Any]:
    return read_json(RELAY_CAPABILITY_STATE_PATH)


def relay_delivery_backoff_due(record: dict[str, Any] | None = None) -> bool:
    record = record or read_relay_capability_state()
    if str(record.get("status", "") or "") != "blocked":
        return False
    retry_after = parse_timestamp(str(record.get("retry_after", "") or ""))
    return retry_after is not None and datetime.now(timezone.utc) < retry_after


def relay_probe_limited(record: dict[str, Any] | None = None) -> bool:
    record = record or read_relay_capability_state()
    status = str(record.get("status", "") or "")
    if status == "ready":
        return False
    return True


def write_relay_capability_state(record: dict[str, Any]) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    RELAY_CAPABILITY_STATE_PATH.write_text(
        json.dumps(record, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def record_remote_control_blocker(trigger: Trigger, metadata: dict[str, Any], error: str) -> None:
    retry_after = datetime.now(timezone.utc).timestamp() + REMOTE_CONTROL_FAILURE_RETRY_SECONDS
    record = {
        "updated_at": utc_now_iso(),
        "status": "blocked",
        "reason": "remote_control_not_ready",
        "retry_after": datetime.fromtimestamp(retry_after, timezone.utc).isoformat().replace("+00:00", "Z"),
        "kind": trigger.kind,
        "op": trigger.op,
        "gate_stage": trigger.gate_stage,
        "key": trigger.key,
        "thread_id": trigger.thread_id,
        "last_error": error,
    }
    for key, value in metadata.items():
        if key.startswith("remote_control_"):
            record[key] = value
    write_relay_capability_state(record)


def record_remote_control_ready(metadata: dict[str, Any]) -> None:
    write_relay_capability_state(
        {
            "updated_at": utc_now_iso(),
            "status": "ready",
            "reason": "remote_control_connected",
            **{
                key: value
                for key, value in metadata.items()
                if key.startswith("remote_control_")
            },
        }
    )


def load_watch_triggers(root: Path = ROOT) -> list[WatchTrigger]:
    state_dir = root / "TestUtils" / "tester_daemon"
    watch: list[WatchTrigger] = []
    for kind, plan_name, ack_reader in trigger_sources():
        plan = read_json(state_dir / plan_name)
        ack_state = ack_reader(root)
        sent = ack_state.get("sent", {}) if isinstance(ack_state.get("sent"), dict) else {}
        plan_triggers = plan.get("triggers", [])
        if not isinstance(plan_triggers, list):
            continue
        for item in plan_triggers:
            if not isinstance(item, dict):
                continue
            key = str(item.get("key", "") or "")
            record = sent.get(key) if isinstance(sent, dict) else None
            if not isinstance(record, dict):
                continue
            ack_status = str(record.get("status", "") or "")
            if ack_status not in {"sent", "active"}:
                continue
            plan_status = str(item.get("status", "") or "")
            if plan_status in {"completed", "cancelled", "removed"}:
                continue
            if not watch_retry_due(record):
                continue
            turn_id = str(record.get("turn_id", "") or record.get("native_id", "") or "")
            thread_id = str(record.get("thread_id", "") or item.get("thread_id", "") or "")
            if not turn_id or not thread_id:
                continue
            watch.append(
                WatchTrigger(
                    kind=kind,
                    op=str(item.get("op", "") or ""),
                    gate_stage=str(item.get("gate_stage", "") or ""),
                    key=key,
                    thread_id=thread_id,
                    turn_id=turn_id,
                )
            )
    return watch


def trigger_sources():
    return (
        ("solver", "solver_trigger_plan.json", read_trigger_ack_state),
        ("tester", "tester_trigger_plan.json", read_tester_trigger_ack_state),
    )


def trigger_ack_suppresses(record: object, trigger_status: str, plan_updated_at: str) -> bool:
    if not isinstance(record, dict):
        return False
    if delivery_reconciliation_pending(record):
        return True
    if trigger_status in {"ready", "retry-ready"}:
        status = str(record.get("status", "") or "")
        failure_kind = str(record.get("failure_kind", "") or "")
        no_agent_count = int(record.get("native_no_agent_output_count", 1) or 1)
        if status in {"interrupted", "failed", "cancelled"} and (
            str(record.get("thread_status_type", "") or "") == "systemError"
            or (failure_kind == "native_turn_no_agent_output" and no_agent_count >= 2)
        ):
            return True
        if status not in {"sent", "delivered", "acked", "active", "completed"}:
            if status == "needs-native-delivery":
                return not delivery_retry_due(record)
            if status == "failed" and not delivery_retry_due(record):
                return True
            return False
        if trigger_status == "retry-ready" and status == "completed":
            return False
        return True
    return False


def trigger_ack_is_current(record: object, plan_updated_at: str) -> bool:
    if not isinstance(record, dict):
        return False
    if record.get("status") not in {"sent", "delivered", "acked", "active", "completed"}:
        return False
    record_updated_at = parse_timestamp(str(record.get("updated_at", "") or ""))
    if record_updated_at is None:
        return False
    plan_time = parse_timestamp(plan_updated_at)
    return plan_time is None or record_updated_at >= plan_time


def watch_retry_due(record: dict[str, Any]) -> bool:
    retry_after = parse_timestamp(str(record.get("watch_retry_after", "") or ""))
    if retry_after is None:
        return True
    return datetime.now(timezone.utc) >= retry_after


def delivery_retry_due(record: dict[str, Any]) -> bool:
    retry_after = parse_timestamp(str(record.get("delivery_retry_after", "") or ""))
    if retry_after is not None:
        return datetime.now(timezone.utc) >= retry_after
    updated_at = parse_timestamp(str(record.get("updated_at", "") or ""))
    if updated_at is None:
        return True
    return datetime.now(timezone.utc) >= updated_at + timedelta(seconds=DELIVERY_FAILURE_RETRY_SECONDS)


def parse_timestamp(text: str) -> datetime | None:
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def append_event(event: str, payload: dict[str, Any]) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    record = {"time": utc_now_iso(), "event": event, **payload}
    with EVENTS_PATH.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=False) + "\n")


def write_thread_poll_status(status: str, payload: dict[str, Any] | None = None) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    record = {"updated_at": utc_now_iso(), "status": status}
    if payload:
        record.update(payload)
    THREAD_POLL_STATUS_PATH.write_text(
        json.dumps(record, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def read_workers() -> dict[str, Any]:
    data = read_json(WORKERS_PATH)
    workers = data.get("workers", {})
    return workers if isinstance(workers, dict) else {}


def write_workers(workers: dict[str, Any]) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    WORKERS_PATH.write_text(
        json.dumps({"updated_at": utc_now_iso(), "workers": workers}, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def prune_workers(workers: dict[str, Any], active_keys: set[str] | None = None) -> dict[str, Any]:
    kept: dict[str, Any] = {}
    for key, worker in workers.items():
        if not isinstance(worker, dict):
            continue
        pid = int(worker.get("pid", 0) or 0)
        if active_keys is not None and key not in active_keys:
            if pid > 0 and process_alive(pid):
                detached = dict(worker)
                if not detached.get("detached_from_plan"):
                    detached["detached_from_plan"] = True
                    detached["detached_at"] = utc_now_iso()
                    append_event(
                        "bridge_worker_detached_until_terminal",
                        {
                            "key": key,
                            "pid": pid,
                            "kind": worker.get("kind", ""),
                            "op": worker.get("op", ""),
                            "gate_stage": worker.get("gate_stage", ""),
                            "status": worker.get("status", ""),
                        },
                    )
                kept[key] = detached
                continue
            append_event(
                "bridge_worker_orphan_pruned",
                {
                    "key": key,
                    "pid": pid,
                    "kind": worker.get("kind", ""),
                    "op": worker.get("op", ""),
                    "gate_stage": worker.get("gate_stage", ""),
                    "status": worker.get("status", ""),
                },
            )
            if pid == os.getpid():
                append_event("bridge_worker_orphan_skip_self", {"key": key, "pid": pid})
            continue
        if pid > 0 and process_alive(pid):
            kept[key] = worker
        else:
            append_event("bridge_worker_pruned", {"key": key, "pid": pid, "status": worker.get("status", "")})
    return kept


def worker_owns_trigger_thread(workers: dict[str, Any], trigger: Trigger | WatchTrigger) -> bool:
    for worker in workers.values():
        if not isinstance(worker, dict):
            continue
        if str(worker.get("kind", "") or "") != trigger.kind:
            continue
        if str(worker.get("thread_id", "") or "") != trigger.thread_id:
            continue
        pid = int(worker.get("pid", 0) or 0)
        if pid > 0 and process_alive(pid):
            return True
    return False


def active_worker_keys(ready: list[Trigger], watch: list[WatchTrigger]) -> set[str]:
    return {trigger.key for trigger in ready} | {trigger.key for trigger in watch}


def current_plan_keys(root: Path = ROOT) -> set[str]:
    state_dir = root / "TestUtils" / "tester_daemon"
    keys: set[str] = set()
    for _, plan_name, _ in trigger_sources():
        plan = read_json(state_dir / plan_name)
        triggers = plan.get("triggers", [])
        if not isinstance(triggers, list):
            continue
        for item in triggers:
            if isinstance(item, dict):
                key = str(item.get("key", "") or "")
                status = str(item.get("status", "") or "")
                if key and status not in {"completed", "cancelled", "removed"}:
                    keys.add(key)
    return keys


def acquire_bridge_lock(replace_stale_after_seconds: int = 600) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
    try:
        fd = os.open(str(BRIDGE_LOCK), flags)
    except FileExistsError as exc:
        pid = read_lock_pid(BRIDGE_LOCK)
        if pid > 0 and process_alive(pid):
            raise RuntimeError(f"solver trigger bridge lock exists: {BRIDGE_LOCK}") from exc
        if pid > 0:
            BRIDGE_LOCK.unlink(missing_ok=True)
            fd = os.open(str(BRIDGE_LOCK), flags)
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                fh.write(f"pid={os.getpid()} acquired_at={utc_now_iso()}\n")
            return
        age_seconds = time.time() - BRIDGE_LOCK.stat().st_mtime if BRIDGE_LOCK.exists() else 0
        if replace_stale_after_seconds <= 0 or age_seconds < replace_stale_after_seconds:
            raise RuntimeError(f"solver trigger bridge stale lock is not old enough: {BRIDGE_LOCK}") from exc
        BRIDGE_LOCK.unlink(missing_ok=True)
        fd = os.open(str(BRIDGE_LOCK), flags)
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        fh.write(f"pid={os.getpid()} acquired_at={utc_now_iso()}\n")


def release_bridge_lock() -> None:
    try:
        BRIDGE_LOCK.unlink()
    except FileNotFoundError:
        pass


def write_heartbeat(mode: str, workers: dict[str, Any], ready_count: int) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    relay_state = read_relay_capability_state()
    remote_control_blocked = relay_delivery_backoff_due(relay_state)
    effective_delivery_blocked = remote_control_blocked and not codex_cli_resume_fallback_enabled()
    HEARTBEAT_PATH.write_text(
        json.dumps(
            {
                "time": utc_now_iso(),
                "pid": os.getpid(),
                "mode": mode,
                "ready_trigger_count": ready_count,
                "worker_count": len(workers),
                "delivery_blocked": effective_delivery_blocked,
                "effective_delivery_blocked": effective_delivery_blocked,
                "remote_control_delivery_blocked": remote_control_blocked,
                "delivery_blocked_raw": remote_control_blocked,
                "cli_resume_fallback_enabled": codex_cli_resume_fallback_enabled(),
                "delivery_blocker_reason": relay_state.get("reason", ""),
                "delivery_blocked_until": relay_state.get("retry_after", ""),
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def write_status(ready: list[Trigger], workers: dict[str, Any], mode: str, watch: list[WatchTrigger] | None = None) -> None:
    watch = watch or []
    relay_state = read_relay_capability_state()
    remote_control_blocked = relay_delivery_backoff_due(relay_state)
    cli_fallback = codex_cli_resume_fallback_enabled()
    delivery_blocked = remote_control_blocked and not cli_fallback
    lines = [
        "# Solver/Tester Trigger Bridge Status",
        "",
        f"- captured_at: {utc_now_iso()}",
        f"- mode: {mode}",
        f"- pid: {os.getpid()}",
        f"- ready_triggers: {len(ready)}",
        f"- watch_triggers: {len(watch)}",
        f"- active_workers: {len(workers)}",
        f"- delivery_blocked: {delivery_blocked}",
        f"- remote_control_delivery_blocked: {remote_control_blocked}",
        f"- cli_resume_fallback_enabled: {cli_fallback}",
        f"- delivery_blocker_reason: {relay_state.get('reason', '') or '-'}",
        f"- delivery_blocked_until: {relay_state.get('retry_after', '') or '-'}",
        "",
        "## Ready Triggers",
        "",
    ]
    if not ready:
        lines.append("- none")
    for trigger in ready:
        active = "yes" if trigger.key in workers else "no"
        lines.append(
            f"- {trigger.kind}/{trigger.op}: {trigger.gate_stage} status={trigger.status} "
            f"thread={trigger.thread_id} active_worker={active} prompt=`{relpath(trigger.prompt_path)}`"
        )
    lines.extend(["", "## Watch Triggers", ""])
    if not watch:
        lines.append("- none")
    for trigger in watch:
        active = "yes" if trigger.key in workers else "no"
        lines.append(
            f"- {trigger.kind}/{trigger.op}: {trigger.gate_stage} thread={trigger.thread_id} "
            f"turn={trigger.turn_id} active_worker={active}"
        )
    lines.extend(["", "## Workers", ""])
    if not workers:
        lines.append("- none")
    for key, worker in workers.items():
        lines.append(
            f"- pid={worker.get('pid')} op={worker.get('op')} status={worker.get('status')} "
            f"started_at={worker.get('started_at')} key={key}"
        )
    lines.append("")
    STATUS_PATH.write_text("\n".join(lines), encoding="utf-8")
    STATE_PATH.write_text(
        json.dumps(
            {
                "updated_at": utc_now_iso(),
                "mode": mode,
                "delivery_blocked": delivery_blocked,
                "effective_delivery_blocked": delivery_blocked,
                "remote_control_delivery_blocked": remote_control_blocked,
                "delivery_blocked_raw": remote_control_blocked,
                "cli_resume_fallback_enabled": cli_fallback,
                "delivery_blocker": relay_state,
                "ready_triggers": [
                    {
                        "op": t.op,
                        "kind": t.kind,
                        "gate_stage": t.gate_stage,
                        "key": t.key,
                        "thread_id": t.thread_id,
                        "status": t.status,
                        "prompt_path": relpath(t.prompt_path),
                    }
                    for t in ready
                ],
                "watch_triggers": [
                    {
                        "op": t.op,
                        "kind": t.kind,
                        "gate_stage": t.gate_stage,
                        "key": t.key,
                        "thread_id": t.thread_id,
                        "turn_id": t.turn_id,
                    }
                    for t in watch
                ],
                "workers": workers,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def poll_solver_threads(config: Any) -> dict[str, Any]:
    observed_at = utc_now_iso()
    now_epoch = datetime.now(timezone.utc).timestamp()
    live_poll_error = ""
    try:
        payload = poll_solver_threads_once(config, observed_at, now_epoch)
    except Exception as exc:
        live_poll_error = str(exc)
        append_event(
            "solver_threads_poll_degraded",
            {"error": live_poll_error, "reason": "live thread read unavailable; using rollout-storage fallback"},
        )
        payload = poll_solver_threads_from_rollout(config, observed_at, now_epoch)
        payload["poll_fallback"] = "rollout-storage"
        payload["fallback_reason"] = "live thread read unavailable"
        payload["live_poll_error"] = live_poll_error

    threads = payload.get("threads", []) if isinstance(payload.get("threads"), list) else []
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    THREAD_OBSERVATIONS_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    THREAD_OBSERVATIONS_MD_PATH.write_text(render_thread_observations(payload), encoding="utf-8")
    tester_payload = filter_thread_observations_by_role(payload, "tester")
    TESTER_THREAD_OBSERVATIONS_PATH.write_text(
        json.dumps(tester_payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    TESTER_THREAD_OBSERVATIONS_MD_PATH.write_text(
        render_thread_observations(tester_payload, title="Tester Thread Observations"),
        encoding="utf-8",
    )
    append_event(
        "solver_threads_polled",
        {"thread_count": len(threads), "ops": [item.get("op", "") for item in threads if isinstance(item, dict)]},
    )
    write_thread_poll_status(
        "degraded" if live_poll_error else "ok",
        {
            "thread_count": len(threads),
            "ops": [item.get("op", "") for item in threads if isinstance(item, dict)],
            "poll_fallback": payload.get("poll_fallback", ""),
            "fallback_reason": payload.get("fallback_reason", ""),
            "live_poll_error": live_poll_error,
        },
    )
    return payload


def poll_solver_threads_once(
    config: Any,
    observed_at: str,
    now_epoch: float,
    *,
    poll_fallback: str = "",
    fallback_reason: str = "",
) -> dict[str, Any]:
    storage_payload = poll_solver_threads_from_rollout(config, observed_at, now_epoch)
    storage_threads = storage_payload.get("threads", [])
    policy = getattr(config, "policy", {})
    storage_only = bool(
        policy.get("solver_thread_poll_storage_only", False)
        if isinstance(policy, dict)
        else False
    )
    storage_complete = isinstance(storage_threads, list) and (
        len(storage_threads) == len(configured_thread_targets(config))
        and all(isinstance(item, dict) and item.get("latest_turn_id") for item in storage_threads)
    )
    reconciliation_targets = live_reconciliation_targets(config)
    if isinstance(storage_threads, list) and storage_complete and (storage_only or not reconciliation_targets):
        return storage_payload

    targets = reconciliation_targets or configured_thread_targets(config)
    threads: list[dict[str, Any]] = [dict(item) for item in storage_threads if isinstance(item, dict)]
    thread_indexes = {
        str(item.get("thread_id", "") or ""): index
        for index, item in enumerate(threads)
        if item.get("thread_id")
    }
    with AppServerClient() as client:
        client.call(
            "initialize",
            {
                "clientInfo": {"name": "ascendop-solver-thread-poller", "version": "0.1"},
                "capabilities": {
                    "experimentalApi": True,
                    "optOutNotificationMethods": [
                        "thread/tokenUsage/updated",
                        "item/agentMessage/delta",
                        "reasoning/text/delta",
                        "reasoning/summary/text/delta",
                    ],
                },
            },
            timeout_seconds=30,
        )
        for role, op, thread_id in targets:
            existing_index = thread_indexes.get(thread_id)
            item: dict[str, Any] = {
                **(threads[existing_index] if existing_index is not None else {}),
                "op": op,
                "thread_id": thread_id,
                "role": role,
                "observation_source": "app-server-thread-read",
            }
            item["app_server_mode"] = client.server_mode
            if poll_fallback:
                item["poll_fallback"] = poll_fallback
            turns: list[dict[str, Any]] = []
            try:
                thread_read = client.call(
                    "thread/resume",
                    {"threadId": thread_id, "excludeTurns": False},
                    timeout_seconds=60,
                )
                item.update(thread_metadata_from_response(thread_read))
                turns = sorted(
                    thread_turns_from_read_response(thread_read),
                    key=lambda turn: timestamp_number(turn.get("completedAt"))
                    or timestamp_number(turn.get("startedAt"))
                    or 0,
                    reverse=True,
                )[:5]
                if not turns:
                    turns = client.list_turns(thread_id, limit=5)
            except Exception as exc:
                item["live_reconcile_error"] = str(exc)
                item["observation_source"] = "rollout-storage-live-reconcile-failed"
            skipped = turns[0] if turns and is_synthetic_solver_turn(turns[0]) else {}
            latest = first_observable_solver_turn(turns)
            if skipped:
                item["skipped_synthetic_turn_id"] = skipped.get("id", "")
                item["skipped_synthetic_turn_status"] = skipped.get("status", "")
            if latest:
                item_types = [
                    str(entry.get("type", "") or "")
                    for entry in latest.get("items", [])
                    if isinstance(entry, dict)
                ]
                item.update(
                    {
                        "latest_turn_id": latest.get("id", ""),
                        "latest_turn_status": latest.get("status", ""),
                        "latest_started_at": latest.get("startedAt"),
                        "latest_completed_at": latest.get("completedAt"),
                        "latest_duration_ms": latest.get("durationMs"),
                        "latest_has_agent_output": turn_has_agent_output(latest),
                        "latest_user_only_turn": turn_is_user_only(latest),
                        "latest_item_types": item_types[:12],
                        "recent_turns": compact_recent_turns(
                            [
                                turn
                                for turn in turns
                                if isinstance(turn, dict)
                                and not is_synthetic_solver_turn(turn)
                            ]
                        ),
                        "storage_visible": True,
                        "native_visible": True,
                        "ide_panel_visible": False,
                        "ide_panel_visibility": "storage_visible_not_ide_visible",
                    }
                )
                activity_epoch = timestamp_number(latest.get("completedAt")) or timestamp_number(latest.get("startedAt"))
                if activity_epoch is not None:
                    item["latest_activity_at"] = epoch_to_iso(activity_epoch)
                    item["idle_seconds"] = max(0, int(now_epoch - activity_epoch))
                    updated_epoch = timestamp_number(item.get("thread_updated_at"))
                    if updated_epoch is not None and updated_epoch + 1 < activity_epoch:
                        item["thread_header_stale"] = True
                        item["thread_header_lag_seconds"] = int(activity_epoch - updated_epoch)
            if existing_index is None:
                thread_indexes[thread_id] = len(threads)
                threads.append(item)
            else:
                threads[existing_index] = item

    payload = {
        "updated_at": observed_at,
        "threads": threads,
    }
    if poll_fallback:
        payload["poll_fallback"] = poll_fallback
        payload["fallback_reason"] = fallback_reason
    return payload


def live_reconciliation_targets(config: Any) -> list[tuple[str, str, str]]:
    configured = {
        (role, thread_id): (role, op, thread_id)
        for role, op, thread_id in configured_thread_targets(config)
    }
    targets: list[tuple[str, str, str]] = []
    for role, state, plan_name in (
        ("solver", read_trigger_ack_state(ROOT), "solver_trigger_plan.json"),
        ("tester", read_tester_trigger_ack_state(ROOT), "tester_trigger_plan.json"),
    ):
        plan = read_json(ROOT / "TestUtils" / "tester_daemon" / plan_name)
        active_keys = {
            str(item.get("key", "") or "")
            for item in plan.get("triggers", [])
            if isinstance(item, dict) and item.get("key")
        }
        sent = state.get("sent", {}) if isinstance(state.get("sent"), dict) else {}
        for key, record in sent.items():
            if str(key) not in active_keys:
                continue
            if not delivery_reconciliation_pending(record):
                continue
            thread_id = str(record.get("thread_id", "") or "") if isinstance(record, dict) else ""
            target = configured.get((role, thread_id))
            if target and target not in targets:
                targets.append(target)
    return targets


def configured_thread_targets(config: Any) -> list[tuple[str, str, str]]:
    targets: list[tuple[str, str, str]] = []
    observed = tuple(observed_operators(config))
    for op in sorted(observed):
        thread_id = solver_thread_for(config, op)
        if not thread_id:
            continue
        targets.append(("solver", str(op), str(thread_id)))
    if hasattr(config, "operator_sessions"):
        for op in casegen_session_ops(config):
            thread_id = tester_thread_for(config, op)
            if not thread_id:
                continue
            targets.append(("tester", str(op), str(thread_id)))
    return targets


def compact_recent_turns(
    turns: list[dict[str, Any]],
    *,
    limit: int = 8,
) -> list[dict[str, Any]]:
    compact: list[dict[str, Any]] = []
    for turn in turns[: max(1, int(limit))]:
        if not isinstance(turn, dict) or not turn.get("id"):
            continue
        item_types = [
            str(entry.get("type", "") or "")
            for entry in turn.get("items", [])
            if isinstance(entry, dict)
        ]
        relay_prompt = turn_relay_prompt_metadata(turn)
        compact.append(
            {
                "turn_id": str(turn.get("id", "") or ""),
                "turn_status": str(turn.get("status", "") or ""),
                "started_at": turn.get("startedAt"),
                "completed_at": turn.get("completedAt"),
                "duration_ms": turn.get("durationMs"),
                "last_activity_at": turn.get("lastActivityAt"),
                "has_agent_output": turn_has_agent_output(turn),
                "user_only_turn": turn_is_user_only(turn),
                "item_types": item_types[:12],
                **relay_prompt,
            }
        )
    return compact


_CODEX_DELEGATION_INPUT = re.compile(
    r"<codex_delegation>.*?<input>(.*?)</input>.*?</codex_delegation>",
    re.DOTALL,
)


def message_text_fragments(payload: Any) -> list[str]:
    if isinstance(payload, str):
        return [payload]
    if isinstance(payload, list):
        result: list[str] = []
        for item in payload:
            result.extend(message_text_fragments(item))
        return result
    if not isinstance(payload, dict):
        return []
    result = []
    for field in ("text", "message", "input"):
        value = payload.get(field)
        if isinstance(value, str):
            result.append(value)
    content = payload.get("content")
    if isinstance(content, (dict, list, str)):
        result.extend(message_text_fragments(content))
    return result


def relay_prompt_metadata_from_texts(
    texts: list[str],
    *,
    observed_at: float | None = None,
) -> dict[str, Any]:
    prompts: list[str] = []
    for text in texts:
        match = _CODEX_DELEGATION_INPUT.search(text)
        if match is None:
            continue
        prompts.append(match.group(1))
    if not prompts:
        return {}
    prompt_digests = list(
        dict.fromkeys(sha1(prompt.encode("utf-8")).hexdigest() for prompt in prompts)
    )
    return {
        "relay_prompt_sha1": prompt_digests[-1],
        "relay_prompt_sha1s": prompt_digests,
        "relay_prompt_chars": len(prompts[-1]),
        "relay_prompt_events": [
            {"sha1": digest, "observed_at": observed_at}
            for digest in prompt_digests
        ],
    }


def merge_relay_prompt_metadata(
    existing: dict[str, Any],
    update: dict[str, Any],
) -> None:
    if not update:
        return
    digests: list[str] = []
    current = existing.get("relay_prompt_sha1s", [])
    if isinstance(current, list):
        digests.extend(str(item) for item in current if item)
    elif existing.get("relay_prompt_sha1"):
        digests.append(str(existing.get("relay_prompt_sha1")))
    incoming = update.get("relay_prompt_sha1s", [])
    if isinstance(incoming, list):
        digests.extend(str(item) for item in incoming if item)
    elif update.get("relay_prompt_sha1"):
        digests.append(str(update.get("relay_prompt_sha1")))
    existing_events = existing.get("relay_prompt_events", [])
    incoming_events = update.get("relay_prompt_events", [])
    existing.update(update)
    existing["relay_prompt_sha1s"] = list(dict.fromkeys(digests))
    events: list[dict[str, Any]] = []
    for source in (existing_events, incoming_events):
        if not isinstance(source, list):
            continue
        for event in source:
            if not isinstance(event, dict) or not event.get("sha1"):
                continue
            identity = (str(event.get("sha1")), event.get("observed_at"))
            if any(
                (str(item.get("sha1")), item.get("observed_at")) == identity
                for item in events
            ):
                continue
            events.append(dict(event))
    existing["relay_prompt_events"] = events


def turn_relay_prompt_metadata(turn: dict[str, Any]) -> dict[str, Any]:
    digest = str(turn.get("relay_prompt_sha1", "") or "")
    if digest:
        digests = turn.get("relay_prompt_sha1s", [])
        if not isinstance(digests, list):
            digests = [digest]
        return {
            "relay_prompt_sha1": digest,
            "relay_prompt_sha1s": [str(item) for item in digests if item],
            "relay_prompt_chars": int(turn.get("relay_prompt_chars", 0) or 0),
            "relay_prompt_events": (
                [
                    dict(item)
                    for item in turn.get("relay_prompt_events", [])
                    if isinstance(item, dict)
                ]
                if isinstance(turn.get("relay_prompt_events"), list)
                else []
            ),
        }
    texts: list[str] = []
    for item in turn.get("items", []):
        if not isinstance(item, dict):
            continue
        item_type = str(item.get("type", "") or "")
        role = str(item.get("role", "") or "")
        if item_type != "userMessage" and role != "user":
            continue
        texts.extend(message_text_fragments(item))
    return relay_prompt_metadata_from_texts(texts)


def poll_solver_threads_from_rollout(
    config: Any,
    observed_at: str,
    now_epoch: float,
) -> dict[str, Any]:
    targets = configured_thread_targets(config)
    paths = rollout_paths_by_thread({thread_id for _, _, thread_id in targets})
    threads: list[dict[str, Any]] = []
    for role, op, thread_id in targets:
        path = paths.get(thread_id)
        item: dict[str, Any] = {
            "op": op,
            "thread_id": thread_id,
            "role": role,
            "app_server_mode": "rollout-storage",
            "poll_fallback": "rollout-storage",
        }
        if path is None:
            item["read_error"] = "Codex rollout storage not found for configured thread"
            threads.append(item)
            continue
        turn = read_latest_rollout_turn(path)
        item["rollout_path"] = str(path)
        if not turn:
            item["read_error"] = "no observable turn found in Codex rollout tail"
            threads.append(item)
            continue
        recent_turns = turn.get("recentTurns", [])
        recent_turns = (
            [entry for entry in recent_turns if isinstance(entry, dict)]
            if isinstance(recent_turns, list)
            else []
        )
        if not recent_turns:
            recent_turns = [turn]
        started_at = turn.get("startedAt")
        completed_at = turn.get("completedAt")
        last_activity = turn.get("lastActivityAt") or completed_at or started_at
        item_types = [
            str(entry.get("type", "") or "")
            for entry in turn.get("items", [])
            if isinstance(entry, dict)
        ]
        item.update(
            {
                "latest_turn_id": turn.get("id", ""),
                "latest_turn_status": turn.get("status", ""),
                "latest_started_at": started_at,
                "latest_completed_at": completed_at,
                "latest_duration_ms": turn.get("durationMs"),
                "latest_has_agent_output": turn_has_agent_output(turn),
                "latest_user_only_turn": turn_is_user_only(turn),
                "latest_item_types": item_types[:12],
                "recent_turns": compact_recent_turns(recent_turns),
                "storage_visible": True,
                "native_visible": True,
                "ide_panel_visible": False,
                "ide_panel_visibility": "shared_rollout_storage",
            }
        )
        activity_epoch = timestamp_number(last_activity)
        if activity_epoch is not None:
            item["latest_activity_at"] = epoch_to_iso(activity_epoch)
            item["idle_seconds"] = max(0, int(now_epoch - activity_epoch))
        threads.append(item)
    return {
        "updated_at": observed_at,
        "threads": threads,
        "poll_fallback": "rollout-storage",
        "fallback_reason": "local rollout avoids blocking delivery on Codex Desktop/app-server availability",
    }


def rollout_paths_by_thread(thread_ids: set[str]) -> dict[str, Path]:
    codex_home = Path(os.environ.get("CODEX_HOME", "") or (Path.home() / ".codex"))
    sessions_root = codex_home / "sessions"
    if not sessions_root.exists() or not thread_ids:
        return {}
    cache_key = (str(sessions_root.resolve()), tuple(sorted(thread_ids)))
    now = time.monotonic()
    with _ROLLOUT_CACHE_LOCK:
        cached = _ROLLOUT_PATH_CACHE.get(cache_key)
        if (
            cached is not None
            and now - cached[0] < ROLLOUT_PATH_CACHE_SECONDS
            and all(path.exists() for path in cached[1].values())
        ):
            return dict(cached[1])
    paths: dict[str, Path] = {}
    mtimes: dict[str, int] = {}
    for path in sessions_root.rglob("rollout-*.jsonl"):
        name = path.name
        matching = next((thread_id for thread_id in thread_ids if thread_id in name), "")
        if not matching:
            continue
        try:
            mtime = path.stat().st_mtime_ns
        except OSError:
            continue
        if mtime >= mtimes.get(matching, -1):
            paths[matching] = path
            mtimes[matching] = mtime
    with _ROLLOUT_CACHE_LOCK:
        _ROLLOUT_PATH_CACHE[cache_key] = (now, dict(paths))
    return paths


def read_latest_rollout_turn(path: Path, max_bytes: int = 16 * 1024 * 1024) -> dict[str, Any]:
    try:
        stat = path.stat()
    except OSError:
        return {}
    cache_key = str(path.resolve())
    with _ROLLOUT_CACHE_LOCK:
        cached = _ROLLOUT_TURN_CACHE.get(cache_key)
        if (
            cached is not None
            and cached[0] == stat.st_mtime_ns
            and cached[1] == stat.st_size
        ):
            return cached[2]
    try:
        with path.open("rb") as fh:
            fh.seek(0, os.SEEK_END)
            size = fh.tell()
            start = max(0, size - max_bytes)
            fh.seek(start)
            data = fh.read()
    except OSError:
        return {}
    if start > 0:
        newline = data.find(b"\n")
        data = data[newline + 1 :] if newline >= 0 else b""

    turns: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    current_turn_id = ""
    terminal_status = {
        "task_complete": "completed",
        "task_aborted": "interrupted",
        "turn_aborted": "interrupted",
        "task_failed": "failed",
        "task_cancelled": "cancelled",
        "task_canceled": "cancelled",
    }
    for raw_line in data.splitlines():
        try:
            record = json.loads(raw_line.decode("utf-8", errors="replace"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue
        if not isinstance(record, dict):
            continue
        timestamp = parse_timestamp(str(record.get("timestamp", "") or ""))
        epoch = timestamp.timestamp() if timestamp is not None else None
        top_type = str(record.get("type", "") or "")
        payload = record.get("payload", {}) if isinstance(record.get("payload"), dict) else {}
        payload_type = str(payload.get("type", "") or "")
        turn_id = str(payload.get("turn_id", "") or "")
        if top_type == "event_msg" and payload_type == "task_started" and turn_id:
            current_turn_id = turn_id
            if turn_id not in turns:
                order.append(turn_id)
            turns[turn_id] = {
                "id": turn_id,
                "status": "inProgress",
                "startedAt": epoch,
                "completedAt": None,
                "durationMs": None,
                "lastActivityAt": epoch,
                "items": [],
            }
            continue
        if top_type == "turn_context" and turn_id:
            current_turn_id = turn_id
            if turn_id not in turns:
                order.append(turn_id)
                turns[turn_id] = {
                    "id": turn_id,
                    "status": "inProgress",
                    "startedAt": epoch,
                    "completedAt": None,
                    "durationMs": None,
                    "lastActivityAt": epoch,
                    "items": [],
                }
            continue
        if top_type == "event_msg" and payload_type in terminal_status and turn_id:
            turn = turns.setdefault(
                turn_id,
                {"id": turn_id, "startedAt": epoch, "items": []},
            )
            if turn_id not in order:
                order.append(turn_id)
            turn["status"] = terminal_status[payload_type]
            turn["completedAt"] = epoch
            turn["lastActivityAt"] = epoch
            started = timestamp_number(turn.get("startedAt"))
            if started is not None and epoch is not None:
                turn["durationMs"] = max(0, int((epoch - started) * 1000))
            continue
        if not current_turn_id or current_turn_id not in turns:
            continue
        turn = turns[current_turn_id]
        if epoch is not None:
            turn["lastActivityAt"] = epoch
        item_type = ""
        if top_type == "event_msg" and payload_type in {"agent_message", "agent_reasoning"}:
            item_type = "agentMessage"
        elif top_type == "response_item":
            role = str(payload.get("role", "") or "")
            if role == "assistant" or payload_type in {
                "reasoning",
                "function_call",
                "function_call_output",
                "custom_tool_call",
                "custom_tool_call_output",
            }:
                item_type = "agentMessage" if role == "assistant" else payload_type
            elif role == "user":
                item_type = "userMessage"
        elif top_type == "event_msg" and payload_type == "user_message":
            item_type = "userMessage"
        if item_type:
            turn.setdefault("items", []).append({"type": item_type})
            if item_type == "userMessage":
                relay_prompt = relay_prompt_metadata_from_texts(
                    message_text_fragments(payload),
                    observed_at=epoch,
                )
                merge_relay_prompt_metadata(turn, relay_prompt)
    recent = [turns[turn_id] for turn_id in reversed(order[-8:]) if turn_id in turns]
    latest = dict(turns.get(order[-1], {})) if order else {}
    if latest:
        latest["recentTurns"] = recent
    with _ROLLOUT_CACHE_LOCK:
        _ROLLOUT_TURN_CACHE[cache_key] = (
            stat.st_mtime_ns,
            stat.st_size,
            latest,
        )
    return latest


def filter_thread_observations_by_role(payload: dict[str, Any], role: str) -> dict[str, Any]:
    threads = payload.get("threads", [])
    filtered = [
        item
        for item in threads
        if isinstance(item, dict) and str(item.get("role", "solver") or "solver") == role
    ] if isinstance(threads, list) else []
    result = {k: v for k, v in payload.items() if k != "threads"}
    result["threads"] = filtered
    return result


def thread_metadata_from_response(response: dict[str, Any]) -> dict[str, Any]:
    result = response.get("result", {}) if isinstance(response.get("result"), dict) else {}
    thread = result.get("thread", {}) if isinstance(result.get("thread"), dict) else {}
    metadata: dict[str, Any] = {}
    if not thread:
        return metadata
    status = thread.get("status")
    if isinstance(status, dict):
        metadata["thread_status_type"] = str(status.get("type", "") or "")
        if status.get("error"):
            metadata["thread_status_error"] = str(status.get("error", "") or "")
    elif status not in (None, ""):
        metadata["thread_status_type"] = str(status)
    for key in ("title", "updatedAt", "createdAt"):
        if key in thread:
            metadata[f"thread_{key[0].lower() + key[1:]}"] = thread.get(key)
    return metadata


def timestamp_number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def epoch_to_iso(value: float) -> str:
    return datetime.fromtimestamp(value, tz=timezone.utc).isoformat(timespec="seconds")


def render_thread_observations(payload: dict[str, Any], *, title: str = "Thread Observations") -> str:
    lines = [
        f"# {title}",
        "",
        f"- updated_at: {payload.get('updated_at', '')}",
        "",
        "| role | op | thread_id | latest_turn | status | ide_visible | idle_seconds | latest_activity | error |",
        "|---|---|---|---|---|---:|---:|---|---|",
    ]
    threads = payload.get("threads", [])
    if isinstance(threads, list) and threads:
        for item in threads:
            if not isinstance(item, dict):
                continue
            lines.append(
                f"| {item.get('role', 'solver') or 'solver'} | {item.get('op', '-')} | {item.get('thread_id', '-')} | "
                f"{item.get('latest_turn_id', '-') or '-'} | {item.get('latest_turn_status', '-') or '-'} | "
                f"{bool(item.get('ide_panel_visible'))} | "
                f"{item.get('idle_seconds', '-') if 'idle_seconds' in item else '-'} | "
                f"{item.get('latest_activity_at', '-') or '-'} | "
                f"{str(item.get('error') or item.get('read_error') or item.get('resume_error') or '').replace('|', '&#124;')} |"
            )
    else:
        lines.append("| - | - | - | - | - | - | - | - | - |")
    lines.append("")
    return "\n".join(lines)


def relpath(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def process_creation_flags() -> int:
    if os.name != "nt":
        return 0
    return getattr(subprocess, "CREATE_NO_WINDOW", 0)


def diagnostic_creation_flags() -> int:
    if os.name != "nt":
        return 0
    return getattr(subprocess, "CREATE_NO_WINDOW", 0)


def process_startupinfo() -> subprocess.STARTUPINFO | None:
    if os.name != "nt":
        return None
    startupinfo = subprocess.STARTUPINFO()
    startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    startupinfo.wShowWindow = 0
    return startupinfo


def terminate_process_tree(pid: int) -> None:
    if os.name != "nt":
        return
    pids = [pid]
    try:
        pids.extend(descendant_process_ids(windows_process_table(), {pid}))
    except Exception as exc:
        append_event("app_server_descendant_scan_error", {"pid": pid, "error": str(exc)})
    try:
        for target_pid in sorted(set(p for p in pids if p > 0), reverse=True):
            subprocess.run(
                ["taskkill", "/PID", str(target_pid), "/T", "/F"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
                creationflags=process_creation_flags(),
                startupinfo=process_startupinfo(),
            )
    except OSError as exc:
        append_event("app_server_taskkill_error", {"pid": pid, "error": str(exc)})


def windows_process_table() -> list[dict[str, Any]]:
    return native_windows_process_table()


def descendant_process_ids(processes: list[dict[str, Any]], root_pids: set[int]) -> set[int]:
    children: dict[int, list[int]] = {}
    for proc in processes:
        pid = int(proc.get("ProcessId", 0) or 0)
        parent = int(proc.get("ParentProcessId", 0) or 0)
        if pid <= 0:
            continue
        children.setdefault(parent, []).append(pid)
    descendants: set[int] = set()
    stack = list(root_pids)
    while stack:
        parent = stack.pop()
        for child in children.get(parent, []):
            if child in descendants:
                continue
            descendants.add(child)
            stack.append(child)
    return descendants


def background_python_executable() -> str:
    executable = Path(sys.executable)
    if os.name == "nt" and executable.name.lower() == "python.exe":
        pythonw = executable.with_name("pythonw.exe")
        if pythonw.exists():
            return str(pythonw)
    return sys.executable


def dispatch_workers(args: argparse.Namespace) -> int:
    config_path = (
        ROOT / args.config
        if args.config
        else ROOT / "tools/tester_daemon/config/s5_910b_gitpartner_glugrad_bitwise.json"
    )
    config = load_config(config_path, apply_completion_markers=True)
    apply_app_server_policy_env(config)
    interval = float(args.interval_seconds or config.policy.get("solver_bridge_interval_seconds", 5) or 5)
    fixed_max_workers = int(args.max_workers or 0)
    poll_setting = args.poll_threads_seconds if args.poll_threads_seconds > 0 else config.policy.get("solver_thread_poll_seconds", 0)
    poll_interval = float(poll_setting or 0)
    poll_enabled = poll_interval > 0
    acquire_bridge_lock(args.replace_stale_lock_after_seconds)
    try:
        write_heartbeat("starting", {}, 0)
        ticks = 0
        last_thread_poll = 0.0
        thread_poll_worker: threading.Thread | None = None
        delivery_deferred_reported = False
        if not poll_enabled:
            write_thread_poll_status("disabled", {"reason": "solver_thread_poll_seconds <= 0", "mode": "run"})
        while True:
            config = load_config(config_path, apply_completion_markers=True)
            if fixed_max_workers > 0:
                max_workers = fixed_max_workers
            else:
                configured = int(config.policy.get("solver_bridge_max_workers", 0) or 0)
                max_workers = configured if configured > 0 else max(4, len(observed_operators(config)) * 2)
            stop_request = read_stop_request(ROOT)
            if stop_request:
                append_event(
                    "bridge_stop_requested",
                    {
                        "requested_at": stop_request.get("requested_at", ""),
                        "reason": stop_request.get("reason", ""),
                    },
                )
                write_heartbeat("stopped", prune_workers(read_workers()), 0)
                return 0
            if (
                poll_enabled
                and time.monotonic() - last_thread_poll >= poll_interval
                and (thread_poll_worker is None or not thread_poll_worker.is_alive())
            ):
                thread_poll_worker = threading.Thread(
                    target=poll_solver_threads_guarded,
                    args=(config,),
                    name="ascendop-thread-observer",
                    daemon=True,
                )
                thread_poll_worker.start()
                last_thread_poll = time.monotonic()
            ready = load_ready_triggers(ROOT)
            watch = load_watch_triggers(ROOT)
            workers = prune_workers(read_workers(), active_worker_keys(ready, watch) | current_plan_keys(ROOT))
            relay_state = read_relay_capability_state()
            delivery_blocked = relay_delivery_backoff_due(relay_state)
            effective_delivery_blocked = delivery_blocked and not codex_cli_resume_fallback_enabled()
            probe_limited = relay_probe_limited(relay_state) and not codex_cli_resume_fallback_enabled()
            active_delivery_probe = probe_limited and any(
                str(worker.get("status", "") or "") == "started"
                for worker in workers.values()
                if isinstance(worker, dict)
            )
            delivery_workers_enabled = daemon_delivery_workers_enabled()
            if not delivery_workers_enabled and not delivery_deferred_reported:
                append_event(
                    "bridge_delivery_deferred_to_app_side_relay",
                    {
                        "platform": os.name,
                        "ready_count": len(ready),
                        "reason": "daemon-side IDE-visible delivery unavailable",
                    },
                )
                delivery_deferred_reported = True
            probe_started = False
            capacity = max(0, max_workers - len(workers))
            for trigger in ready:
                if capacity <= 0:
                    break
                if trigger.key in workers:
                    continue
                if worker_owns_trigger_thread(workers, trigger):
                    continue
                if effective_delivery_blocked:
                    continue
                if probe_limited and (probe_started or active_delivery_probe):
                    continue
                if not args.dry_run and not delivery_workers_enabled:
                    continue
                if args.dry_run:
                    append_event(
                        "bridge_dry_run_dispatch",
                        {"key": trigger.key, "op": trigger.op, "thread_id": trigger.thread_id},
                    )
                    probe_started = True
                    continue
                worker = start_worker(trigger, args.wait_seconds)
                workers[trigger.key] = worker
                capacity -= 1
                probe_started = True
            for trigger in watch:
                if capacity <= 0:
                    break
                if trigger.key in workers:
                    continue
                if worker_owns_trigger_thread(workers, trigger):
                    continue
                if args.dry_run:
                    append_event(
                        "bridge_dry_run_watch",
                        {"key": trigger.key, "op": trigger.op, "thread_id": trigger.thread_id, "turn_id": trigger.turn_id},
                    )
                    continue
                worker = start_watch_worker(trigger, args.wait_seconds)
                workers[trigger.key] = worker
                capacity -= 1
            write_workers(workers)
            write_heartbeat("run", workers, len(ready))
            write_status(ready, workers, "run", watch)
            ticks += 1
            if args.once or (args.max_ticks and ticks >= args.max_ticks):
                return 0
            time.sleep(max(1.0, interval))
    finally:
        release_bridge_lock()


def poll_solver_threads_guarded(config: Any) -> None:
    try:
        poll_solver_threads(config)
    except Exception as exc:
        append_event("solver_threads_poll_failed", {"error": str(exc)})
        write_thread_poll_status("failed", {"error": str(exc), "mode": "run"})


def start_worker(trigger: Trigger, wait_seconds: int) -> dict[str, Any]:
    logs = STATE_DIR / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    stamp = utc_now_iso().replace(":", "").replace("+", "Z")
    stdout_path = logs / f"{trigger.kind}_bridge_{trigger.op}_{trigger.digest}_{stamp}.out.log"
    stderr_path = logs / f"{trigger.kind}_bridge_{trigger.op}_{trigger.digest}_{stamp}.err.log"
    cmd = [
        background_python_executable(),
        str(Path(__file__).resolve()),
        "deliver",
        "--key",
        trigger.key,
        "--wait-seconds",
        str(wait_seconds),
    ]
    creationflags = process_creation_flags()
    with stdout_path.open("w", encoding="utf-8") as out, stderr_path.open("w", encoding="utf-8") as err:
        proc = subprocess.Popen(
            cmd,
            cwd=str(ROOT),
            stdout=out,
            stderr=err,
            creationflags=creationflags,
            startupinfo=process_startupinfo(),
        )
    append_event(
        "bridge_worker_started",
        {"key": trigger.key, "kind": trigger.kind, "op": trigger.op, "thread_id": trigger.thread_id, "pid": proc.pid},
    )
    return {
        "pid": proc.pid,
        "kind": trigger.kind,
        "op": trigger.op,
        "gate_stage": trigger.gate_stage,
        "thread_id": trigger.thread_id,
        "prompt_path": relpath(trigger.prompt_path),
        "status": "started",
        "started_at": utc_now_iso(),
        "stdout": relpath(stdout_path),
        "stderr": relpath(stderr_path),
    }


def start_watch_worker(trigger: WatchTrigger, wait_seconds: int) -> dict[str, Any]:
    logs = STATE_DIR / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    stamp = utc_now_iso().replace(":", "").replace("+", "Z")
    stdout_path = logs / f"{trigger.kind}_bridge_watch_{trigger.op}_{trigger.digest}_{stamp}.out.log"
    stderr_path = logs / f"{trigger.kind}_bridge_watch_{trigger.op}_{trigger.digest}_{stamp}.err.log"
    cmd = [
        background_python_executable(),
        str(Path(__file__).resolve()),
        "watch",
        "--key",
        trigger.key,
        "--wait-seconds",
        str(wait_seconds),
    ]
    creationflags = process_creation_flags()
    with stdout_path.open("w", encoding="utf-8") as out, stderr_path.open("w", encoding="utf-8") as err:
        proc = subprocess.Popen(
            cmd,
            cwd=str(ROOT),
            stdout=out,
            stderr=err,
            creationflags=creationflags,
            startupinfo=process_startupinfo(),
        )
    append_event(
        "bridge_watch_worker_started",
        {
            "key": trigger.key,
            "kind": trigger.kind,
            "op": trigger.op,
            "thread_id": trigger.thread_id,
            "turn_id": trigger.turn_id,
            "pid": proc.pid,
        },
    )
    return {
        "pid": proc.pid,
        "kind": trigger.kind,
        "op": trigger.op,
        "gate_stage": trigger.gate_stage,
        "thread_id": trigger.thread_id,
        "turn_id": trigger.turn_id,
        "prompt_path": "",
        "status": "watching",
        "started_at": utc_now_iso(),
        "stdout": relpath(stdout_path),
        "stderr": relpath(stderr_path),
    }


def deliver(args: argparse.Namespace) -> int:
    trigger = find_trigger(args.key)
    if trigger is None:
        append_event("bridge_deliver_missing_trigger", {"key": args.key})
        return 2
    if not trigger.prompt_path.exists():
        append_event("bridge_deliver_missing_prompt", {"key": trigger.key, "prompt_path": relpath(trigger.prompt_path)})
        return 2
    prompt = trigger.prompt_path.read_text(encoding="utf-8")
    try:
        result = deliver_with_app_server(trigger, prompt, args.wait_seconds)
    except NativeDeliveryRequired as exc:
        record = current_ack_record_for_trigger(trigger)
        if app_side_visible_delivery_ack(record, trigger):
            append_event(
                "bridge_deliver_preserved_app_side_ack",
                {
                    "key": trigger.key,
                    "kind": trigger.kind,
                    "op": trigger.op,
                    "turn_id": str(record.get("turn_id") or record.get("native_id") or ""),
                    "error": str(exc),
                },
            )
            return 0
        if codex_cli_resume_fallback_enabled():
            try:
                result = deliver_with_codex_cli(trigger, prompt, args.wait_seconds, native_error=str(exc))
            except Exception as cli_exc:
                ack_trigger_delivery(
                    trigger,
                    "needs-native-delivery",
                    {
                        "error": str(exc),
                        "cli_resume_error": str(cli_exc),
                        "delivery_required": "codex-app-send-message-to-thread",
                        "ide_panel_visible": False,
                        "ide_panel_visibility": "native_delivery_required",
                        **delivery_retry_metadata(exc),
                    },
                )
                append_event(
                    "bridge_deliver_cli_resume_failed",
                    {
                        "key": trigger.key,
                        "kind": trigger.kind,
                        "op": trigger.op,
                        "native_error": str(exc),
                        "error": str(cli_exc),
                    },
                )
                return 2
            append_event("bridge_deliver_cli_resume", {"key": trigger.key, "kind": trigger.kind, "op": trigger.op, **result})
            return 0
        ack_trigger_delivery(
            trigger,
            "needs-native-delivery",
            {
                "error": str(exc),
                "delivery_required": "codex-app-send-message-to-thread",
                "ide_panel_visible": False,
                "ide_panel_visibility": "native_delivery_required",
                **delivery_retry_metadata(exc),
            },
        )
        append_event(
            "bridge_deliver_needs_native_delivery",
            {"key": trigger.key, "kind": trigger.kind, "op": trigger.op, "error": str(exc)},
        )
        return 2
    except Exception as exc:
        if codex_cli_resume_fallback_enabled() and native_delivery_fallback_error(exc):
            try:
                result = deliver_with_codex_cli(trigger, prompt, args.wait_seconds, native_error=str(exc))
            except Exception as cli_exc:
                ack_trigger_delivery(trigger, "failed", {"error": str(exc), "cli_resume_error": str(cli_exc), **delivery_retry_metadata(exc)})
                append_event(
                    "bridge_deliver_cli_resume_failed",
                    {
                        "key": trigger.key,
                        "kind": trigger.kind,
                        "op": trigger.op,
                        "native_error": str(exc),
                        "error": str(cli_exc),
                    },
                )
                return 2
            append_event("bridge_deliver_cli_resume", {"key": trigger.key, "kind": trigger.kind, "op": trigger.op, **result})
            return 0
        ack_trigger_delivery(trigger, "failed", {"error": str(exc), **delivery_retry_metadata(exc)})
        append_event("bridge_deliver_failed", {"key": trigger.key, "kind": trigger.kind, "op": trigger.op, "error": str(exc)})
        return 2
    append_event("bridge_deliver_sent", {"key": trigger.key, "kind": trigger.kind, "op": trigger.op, **result})
    return 0


def watch(args: argparse.Namespace) -> int:
    trigger = find_watch_trigger(args.key)
    if trigger is None:
        append_event("bridge_watch_missing_trigger", {"key": args.key})
        return 2
    ack_state = read_tester_trigger_ack_state(ROOT) if trigger.kind == "tester" else read_trigger_ack_state(ROOT)
    sent = ack_state.get("sent", {}) if isinstance(ack_state.get("sent"), dict) else {}
    record = sent.get(trigger.key) if isinstance(sent, dict) else None
    if not isinstance(record, dict):
        append_event("bridge_watch_missing_ack", {"key": trigger.key})
        return 2

    try:
        with AppServerClient() as client:
            client.call(
                "initialize",
                {
                    "clientInfo": {"name": "ascendop-trigger-watch-bridge", "version": "0.1"},
                    "capabilities": {
                        "experimentalApi": True,
                        "optOutNotificationMethods": [
                            "thread/tokenUsage/updated",
                            "item/agentMessage/delta",
                            "reasoning/text/delta",
                            "reasoning/summary/text/delta",
                        ],
                    },
                },
                timeout_seconds=30,
            )
            wait_timeout = min(max(0, args.wait_seconds), WATCH_WORKER_MAX_WAIT_SECONDS)
            wait_status = client.wait_for_turn(trigger.thread_id, trigger.turn_id, wait_timeout)
            final_status = "active" if wait_status == "timeout" else wait_status
            final_turn = client.read_thread_turn(trigger.thread_id, trigger.turn_id) or client.get_turn(
                trigger.thread_id,
                trigger.turn_id,
            )
            if final_status == "active" and codex_cli_resume_ack_lost_process(record) and not matching_thread_observation(trigger):
                ack_watch_trigger_delivery(
                    trigger,
                    "failed",
                    {
                        "turn_id": trigger.turn_id,
                        "method": "turn/watch-codex-cli-process-check",
                        "wait_status": wait_status,
                        "error": (
                            "codex-cli resume process exited before a matching IDE thread turn "
                            "could be observed"
                        ),
                        "delivery": record.get("delivery") or record.get("relay") or CODEX_CLI_RESUME_DELIVERY,
                        "ide_panel_visible": False,
                        "ide_panel_visibility": record.get("ide_panel_visibility")
                        or CODEX_CLI_RESUME_VISIBILITY,
                        "app_server_mode": "codex-cli",
                        "native_id": trigger.turn_id,
                        "native_status": "failed",
                        "failure_kind": "codex_cli_process_dead_no_thread_progress",
                    },
                )
                append_event(
                    "bridge_watch_codex_cli_process_dead",
                    {
                        "key": trigger.key,
                        "kind": trigger.kind,
                        "op": trigger.op,
                        "turn_id": trigger.turn_id,
                    },
                )
                return 0
            if final_status in {"interrupted", "failed", "cancelled"} and app_side_visible_ack(record, trigger):
                fallback = watch_failure_fallback(
                    trigger,
                    record,
                    RuntimeError(f"app-server watch returned {final_status} for IDE-visible app-side turn"),
                )
                if fallback is not None:
                    fallback_status, fallback_metadata = fallback
                    ack_watch_trigger_delivery(trigger, fallback_status, fallback_metadata)
                    append_event(
                        "bridge_watch_terminal_conflict_fallback",
                        {
                            "key": trigger.key,
                            "kind": trigger.kind,
                            "op": trigger.op,
                            "status": fallback_status,
                            "wait_status": wait_status,
                        },
                    )
                    return 0
            ack_watch_trigger_delivery(
                trigger,
                final_status,
                {
                    "turn_id": trigger.turn_id,
                    "method": "turn/watch",
                    "wait_status": wait_status,
                    **turn_ack_details(final_turn, server_mode=client.server_mode),
                },
            )
    except Exception as exc:
        fallback = watch_failure_fallback(trigger, record, exc)
        if fallback is not None:
            fallback_status, fallback_metadata = fallback
            ack_watch_trigger_delivery(trigger, fallback_status, fallback_metadata)
            append_event(
                "bridge_watch_observation_fallback",
                {
                    "key": trigger.key,
                    "kind": trigger.kind,
                    "op": trigger.op,
                    "status": fallback_status,
                    "error": str(exc),
                },
            )
            return 0
        ack_watch_trigger_delivery(trigger, "failed", {"error": str(exc), "method": "turn/watch"})
        append_event("bridge_watch_failed", {"key": trigger.key, "kind": trigger.kind, "op": trigger.op, "error": str(exc)})
        return 2

    append_event(
        "bridge_watch_updated",
        {
            "key": trigger.key,
            "kind": trigger.kind,
            "op": trigger.op,
            "turn_id": trigger.turn_id,
            "wait_status": wait_status,
        },
    )
    return 0


def watch_failure_fallback(
    trigger: WatchTrigger, record: dict[str, Any], exc: Exception
) -> tuple[str, dict[str, Any]] | None:
    """Do not downgrade app-side IDE-visible turns when the Windows proxy is unavailable."""
    observed = matching_thread_observation(trigger)
    if observed:
        observed_status = str(observed.get("latest_turn_status", "") or "")
        if observed_status in {"inProgress", "completed"}:
            if observed_status == "completed" and observed.get("latest_has_agent_output") is False:
                return (
                    "failed",
                    {
                        "turn_id": trigger.turn_id,
                        "method": "turn/watch-observation-fallback",
                        "wait_status": observed_status,
                        "failure_kind": "native_turn_no_agent_output",
                        "error": "IDE-visible turn completed without agent output",
                        **observation_ack_details(observed),
                    },
                )
            return (
                "active" if observed_status == "inProgress" else "completed",
                {
                    "turn_id": trigger.turn_id,
                    "method": "turn/watch-observation-fallback",
                    "wait_status": observed_status,
                    "error": str(exc),
                    **watch_retry_metadata(exc),
                    **observation_ack_details(observed),
                },
            )

    if codex_cli_resume_ack_lost_process(record):
        return (
            "failed",
            {
                "turn_id": trigger.turn_id,
                "method": "turn/watch-codex-cli-process-check",
                "wait_status": "process-dead",
                "error": (
                    "codex-cli resume process exited before a matching IDE thread turn "
                    "could be observed"
                ),
                "delivery": record.get("delivery") or record.get("relay") or CODEX_CLI_RESUME_DELIVERY,
                "ide_panel_visible": False,
                "ide_panel_visibility": record.get("ide_panel_visibility")
                or CODEX_CLI_RESUME_VISIBILITY,
                "app_server_mode": "codex-cli",
                "native_id": trigger.turn_id,
                "native_status": "failed",
                "failure_kind": "codex_cli_process_dead_no_thread_progress",
                **watch_retry_metadata(exc),
            },
        )

    if app_side_visible_ack(record, trigger):
        local_owner = (
            str(record.get("delivery", "") or "").startswith("app-server-")
            or str(record.get("remote_control_status", "") or "") == "storage-visible-local-owner"
        ) and str(record.get("app_server_mode", "") or "") not in {
            "proxy",
            "ws",
            "thread-observation",
            "codex-cli",
        }
        if local_owner:
            return None
        return (
            "active",
            {
                "turn_id": trigger.turn_id,
                "method": "turn/watch-app-side-ack-fallback",
                "wait_status": "proxy-unavailable",
                "error": str(exc),
                "delivery": record.get("delivery") or record.get("relay") or "codex-app-send-message-to-thread",
                "ide_panel_visible": True,
                "ide_panel_visibility": record.get("ide_panel_visibility")
                or "confirmed_by_app_side_ack",
                "app_server_mode": "thread-observation",
                "native_id": trigger.turn_id,
                "native_status": str(record.get("native_status") or record.get("turn_status") or "inProgress"),
                **watch_retry_metadata(exc),
            },
        )
    return None


def codex_cli_resume_ack_lost_process(record: dict[str, Any]) -> bool:
    delivery = str(record.get("delivery") or record.get("relay") or "")
    if delivery != CODEX_CLI_RESUME_DELIVERY:
        return False
    if "cli_returncode" in record:
        return False
    try:
        pid = int(record.get("cli_pid", 0) or 0)
    except (TypeError, ValueError):
        pid = 0
    return pid > 0 and not process_alive(pid)


def codex_cli_resume_fallback_enabled() -> bool:
    value = os.environ.get("ASCENDOP_CODEX_CLI_RESUME_FALLBACK", "0").strip().lower()
    return value not in {"0", "false", "no", "off"}


def native_delivery_fallback_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return (
        "remote control not ready" in text
        or "native delivery required" in text
        or "proxy control socket" in text
        or "app-server proxy" in text
        or "app-server exited" in text
        or "turn failed before the prompt was recorded" in text
    )


def deliver_with_codex_cli(
    trigger: Trigger,
    prompt: str,
    wait_seconds: int,
    *,
    native_error: str = "",
) -> dict[str, Any]:
    logs = STATE_DIR / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    stamp = utc_now_iso().replace(":", "").replace("+", "Z")
    stdout_path = logs / f"{trigger.kind}_cli_resume_{trigger.op}_{trigger.digest}_{stamp}.out.jsonl"
    stderr_path = logs / f"{trigger.kind}_cli_resume_{trigger.op}_{trigger.digest}_{stamp}.err.log"
    synthetic_turn_id = ""
    command = codex_exec_resume_command(trigger)
    with stdout_path.open("w", encoding="utf-8") as out, stderr_path.open("w", encoding="utf-8") as err:
        proc = subprocess.Popen(
            command,
            cwd=str(ROOT),
            env=codex_subprocess_env(),
            stdin=subprocess.PIPE,
            stdout=out,
            stderr=err,
            text=True,
            encoding="utf-8",
            errors="replace",
            creationflags=process_creation_flags(),
            startupinfo=process_startupinfo(),
        )
        synthetic_turn_id = f"codex-cli-{proc.pid}"
        active_metadata = {
            "turn_id": synthetic_turn_id,
            "method": "codex exec resume",
            "delivery": CODEX_CLI_RESUME_DELIVERY,
            "ide_panel_visible": False,
            "ide_panel_visibility": CODEX_CLI_RESUME_VISIBILITY,
            "app_server_mode": "codex-cli",
            "native_visible": True,
            "storage_visible": True,
            "native_status": "inProgress",
            "turn_status": "inProgress",
            "wait_status": "running",
            "cli_pid": proc.pid,
            "cli_command": redact_command(command),
            "cli_stdout": relpath(stdout_path),
            "cli_stderr": relpath(stderr_path),
            "native_delivery_error": native_error,
            "delivery_started_at": utc_now_iso(),
            "completion_unconfirmed": False,
            "control_plane_unavailable": False,
            "orphaned_delivery_owner": False,
        }
        ack_trigger_delivery(trigger, "active", active_metadata)
        append_event(
            "bridge_cli_resume_started",
            {
                "key": trigger.key,
                "kind": trigger.kind,
                "op": trigger.op,
                "thread_id": trigger.thread_id,
                "pid": proc.pid,
                "stdout": relpath(stdout_path),
                "stderr": relpath(stderr_path),
                "native_error": native_error,
            },
        )
        assert proc.stdin is not None
        try:
            proc.stdin.write(prompt)
            if not prompt.endswith("\n"):
                proc.stdin.write("\n")
            proc.stdin.close()
        except OSError as exc:
            ack_trigger_delivery(
                trigger,
                "failed",
                {
                    **active_metadata,
                    "error": f"failed to write prompt to codex cli stdin: {exc}",
                    "native_status": "failed",
                },
            )
            raise
        timeout = max(1, int(wait_seconds or 1))
        try:
            returncode = proc.wait(timeout=timeout)
            timed_out = False
        except subprocess.TimeoutExpired:
            returncode = None
            timed_out = True

    if timed_out:
        metadata = {
            **active_metadata,
            "wait_status": "timeout",
            "native_status": "inProgress",
            "cli_timeout_seconds": timeout,
        }
        ack_trigger_delivery(trigger, "active", metadata)
        append_event(
            "bridge_cli_resume_timeout",
            {
                "key": trigger.key,
                "kind": trigger.kind,
                "op": trigger.op,
                "pid": active_metadata.get("cli_pid"),
                "timeout_seconds": timeout,
            },
        )
        return {
            "sender": "codex-cli",
            "method": "codex exec resume",
            "turn_id": synthetic_turn_id,
            "wait_status": "timeout",
            "returncode": None,
            "stdout": relpath(stdout_path),
            "stderr": relpath(stderr_path),
        }

    parsed_turn_id = extract_cli_turn_id_from_log(stdout_path, fallback=synthetic_turn_id)
    stdout_tail = read_tail(stdout_path, 2000)
    stderr_tail = read_tail(stderr_path, 2000)
    final_status = "completed" if returncode == 0 else "failed"
    final_metadata = {
        **active_metadata,
        "turn_id": parsed_turn_id,
        "native_id": parsed_turn_id,
        "native_status": "completed" if returncode == 0 else "failed",
        "turn_status": "completed" if returncode == 0 else "failed",
        "cli_returncode": returncode,
        "cli_stdout_tail": stdout_tail,
        "cli_stderr_tail": stderr_tail,
    }
    if returncode != 0:
        final_metadata["error"] = f"codex cli resume failed returncode={returncode}"
    ack_trigger_delivery(trigger, final_status, final_metadata)
    return {
        "sender": "codex-cli",
        "method": "codex exec resume",
        "turn_id": parsed_turn_id,
        "returncode": returncode,
        "wait_status": "completed" if returncode == 0 else "failed",
        "stdout": relpath(stdout_path),
        "stderr": relpath(stderr_path),
    }


def codex_exec_resume_command(trigger: Trigger) -> list[str]:
    command = [
        codex_executable(),
        "exec",
        "-C",
        str(ROOT),
        "--dangerously-bypass-approvals-and-sandbox",
    ]
    if trigger.model:
        command.extend(["--model", trigger.model])
    if trigger.thinking:
        command.extend(["-c", f'model_reasoning_effort="{trigger.thinking}"'])
    command.extend([
        "--json",
        "resume",
        trigger.thread_id,
        "-",
    ])
    return command


def redact_command(command: list[str]) -> list[str]:
    return list(command)


def extract_cli_turn_id_from_log(path: Path, *, fallback: str = "") -> str:
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return fallback
    for line in reversed(lines):
        line = line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        found = first_json_key(payload, {"turn_id", "turnId"})
        if isinstance(found, str) and found:
            return found
    return fallback


def first_json_key(value: Any, keys: set[str]) -> Any:
    if isinstance(value, dict):
        for key in keys:
            if key in value:
                return value[key]
        for child in value.values():
            found = first_json_key(child, keys)
            if found:
                return found
    elif isinstance(value, list):
        for child in value:
            found = first_json_key(child, keys)
            if found:
                return found
    return None


def read_tail(path: Path, max_chars: int) -> str:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""
    return text[-max(1, max_chars):]


def watch_retry_metadata(exc: Exception) -> dict[str, Any]:
    retry_after = datetime.now(timezone.utc).timestamp() + WATCH_OBSERVATION_FALLBACK_RETRY_SECONDS
    return {
        "watch_retry_after": datetime.fromtimestamp(retry_after, timezone.utc).isoformat().replace("+00:00", "Z"),
        "watch_retry_reason": "proxy_unavailable_observation_fallback",
        "last_watch_error": str(exc),
    }


def delivery_retry_metadata(exc: Exception) -> dict[str, Any]:
    error_text = str(exc).lower()
    retry_seconds = (
        REMOTE_CONTROL_FAILURE_RETRY_SECONDS
        if "remote control not ready" in error_text
        else DELIVERY_FAILURE_RETRY_SECONDS
    )
    retry_after = datetime.now(timezone.utc).timestamp() + retry_seconds
    metadata = {
        "delivery_retry_after": datetime.fromtimestamp(retry_after, timezone.utc).isoformat().replace("+00:00", "Z"),
        "delivery_retry_reason": (
            "remote_control_not_ready"
            if "remote control not ready" in error_text
            else "transient_app_server_failure"
            if "app-server exited" in error_text
            else "native_delivery_required"
        ),
        "last_delivery_error": str(exc),
    }
    if "app-server exited" in error_text:
        metadata["failure_kind"] = "local_app_server_process_exit"
    return metadata


def remote_control_ready(metadata: dict[str, Any]) -> bool:
    if not metadata.get("remote_control_supported", False):
        return False
    status = str(metadata.get("remote_control_status", "") or "").lower()
    return status in REMOTE_CONTROL_READY_STATUSES


def matching_thread_observation(trigger: WatchTrigger) -> dict[str, Any]:
    observations = read_json(THREAD_OBSERVATIONS_PATH)
    threads = observations.get("threads", [])
    if not isinstance(threads, list):
        return {}
    for item in threads:
        if not isinstance(item, dict):
            continue
        if str(item.get("thread_id", "") or "") != trigger.thread_id:
            continue
        if str(item.get("latest_turn_id", "") or "") != trigger.turn_id:
            continue
        return item
    return {}


def observation_ack_details(observed: dict[str, Any]) -> dict[str, Any]:
    return {
        "turn_status": observed.get("latest_turn_status"),
        "storage_visible": bool(observed.get("storage_visible", True)),
        "native_visible": bool(observed.get("native_visible", True)),
        "ide_panel_visible": False,
        "ide_panel_visibility": "storage_visible_not_ide_visible",
        "app_server_mode": "thread-observation",
        "native_id": observed.get("latest_turn_id"),
        "native_status": observed.get("latest_turn_status"),
        "native_startedAt": observed.get("latest_started_at"),
        "native_completedAt": observed.get("latest_completed_at"),
        "native_durationMs": observed.get("latest_duration_ms"),
        "latest_has_agent_output": observed.get("latest_has_agent_output"),
        "latest_user_only_turn": observed.get("latest_user_only_turn"),
    }


def app_side_visible_ack(record: dict[str, Any], trigger: WatchTrigger) -> bool:
    if not bridge_record_ide_visible(record):
        return False
    if str(record.get("key", "") or "") != trigger.key:
        return False
    if str(record.get("thread_id", "") or "") != trigger.thread_id:
        return False
    if str(record.get("turn_id") or record.get("native_id") or "") != trigger.turn_id:
        return False
    delivery = str(record.get("delivery") or record.get("relay") or "")
    visibility = str(record.get("ide_panel_visibility", "") or "")
    return delivery in {
        "codex-app-send-message-to-thread",
        "codex_app.send_message_to_thread",
        "ide-native-relay",
    } or visibility in {
        "confirmed_by_native_relay",
        "live_proxy",
        "live_ws",
    }


def current_ack_record_for_trigger(trigger: Trigger) -> dict[str, Any]:
    state = read_tester_trigger_ack_state(ROOT) if trigger.kind == "tester" else read_trigger_ack_state(ROOT)
    sent = state.get("sent", {}) if isinstance(state.get("sent"), dict) else {}
    record = sent.get(trigger.key) if isinstance(sent, dict) else None
    return record if isinstance(record, dict) else {}


def app_side_visible_delivery_ack(record: dict[str, Any], trigger: Trigger) -> bool:
    if not bridge_record_ide_visible(record):
        return False
    if str(record.get("key", "") or "") != trigger.key:
        return False
    if str(record.get("thread_id", "") or "") != trigger.thread_id:
        return False
    if not str(record.get("turn_id") or record.get("native_id") or ""):
        return False
    delivery = str(record.get("delivery") or record.get("relay") or "")
    visibility = str(record.get("ide_panel_visibility", "") or "")
    return delivery in {
        "codex-app-send-message-to-thread",
        "codex_app.send_message_to_thread",
        "ide-native-relay",
    } or visibility in {
        "confirmed_by_native_relay",
        "live_proxy",
        "live_ws",
    }


def bridge_record_ide_visible(record: dict[str, Any]) -> bool:
    if record.get("ide_panel_visible") is not True:
        return False
    delivery = str(record.get("delivery") or record.get("relay") or "")
    visibility = str(record.get("ide_panel_visibility", "") or "")
    return delivery in {
        "codex-app-send-message-to-thread",
        "codex_app.send_message_to_thread",
        "ide-native-relay",
    } or visibility in {"confirmed_by_native_relay", "live_proxy", "live_ws"}


def ack_trigger_delivery(trigger: Trigger, status: str, metadata: dict[str, Any]) -> None:
    if trigger.kind == "tester":
        ack_tester_trigger(ROOT, trigger.key, trigger.thread_id, status, metadata)
        return
    ack_solver_trigger(ROOT, trigger.key, trigger.thread_id, status, metadata)


def ack_watch_trigger_delivery(trigger: WatchTrigger, status: str, metadata: dict[str, Any]) -> None:
    if trigger.kind == "tester":
        ack_tester_trigger(ROOT, trigger.key, trigger.thread_id, status, metadata)
        return
    ack_solver_trigger(ROOT, trigger.key, trigger.thread_id, status, metadata)


def find_trigger(key: str) -> Trigger | None:
    for trigger in load_ready_triggers(ROOT):
        if trigger.key == key:
            return trigger
    return None


def find_watch_trigger(key: str) -> WatchTrigger | None:
    for trigger in load_watch_triggers(ROOT):
        if trigger.key == key:
            return trigger
    return None


def deliver_with_app_server(trigger: Trigger, prompt: str, wait_seconds: int) -> dict[str, Any]:
    client_message_id = "ascendop-trigger-" + sha1(
        f"{trigger.key}|{trigger.plan_updated_at}|{utc_now_iso()}".encode("utf-8")
    ).hexdigest()[:20]
    with AppServerClient() as client:
        require_ide_visible = os.environ.get("ASCENDOP_REQUIRE_IDE_VISIBLE_DELIVERY", "").strip() in {
            "1",
            "true",
            "yes",
        }
        allow_storage_visible = os.environ.get("ASCENDOP_ALLOW_STORAGE_VISIBLE_DELIVERY", "").strip() in {
            "1",
            "true",
            "yes",
        }
        if require_ide_visible and not allow_storage_visible:
            if client.server_mode not in {"proxy", "ws"}:
                raise NativeDeliveryRequired(
                    f"configured to require IDE-visible {trigger.kind} delivery, "
                    f"but app-server mode is {client.server_mode}"
                )
        client.call(
            "initialize",
            {
                "clientInfo": {"name": "ascendop-solver-trigger-bridge", "version": "0.1"},
                "capabilities": {
                    "experimentalApi": True,
                    "optOutNotificationMethods": [
                        "thread/tokenUsage/updated",
                        "item/agentMessage/delta",
                        "reasoning/text/delta",
                        "reasoning/summary/text/delta",
                    ],
                },
            },
            timeout_seconds=30,
        )
        remote_control: dict[str, Any] = {
            "remote_control_supported": False,
            "remote_control_status": "storage-visible-local-owner",
        }
        if client.server_mode in {"proxy", "ws"}:
            remote_control = client.enable_remote_control()
            append_event(
                "bridge_remote_control_enable",
                {
                    "key": trigger.key,
                    "kind": trigger.kind,
                    "op": trigger.op,
                    **remote_control,
                },
            )
            if not remote_control_ready(remote_control):
                error = (
                    f"remote control not ready for IDE-visible {trigger.kind} delivery: "
                    f"status={remote_control.get('remote_control_status', '')} "
                    f"supported={remote_control.get('remote_control_supported', '')} "
                    f"error={remote_control.get('remote_control_error', '')}"
                )
                if not allow_storage_visible:
                    record_remote_control_blocker(trigger, remote_control, error)
                    raise RuntimeError(error)
                remote_control["remote_control_fallback"] = "shared-rollout-storage"
                remote_control["remote_control_error"] = error
            else:
                record_remote_control_ready(remote_control)
        resume = client.call(
            "thread/resume",
            {"threadId": trigger.thread_id, "excludeTurns": True},
            timeout_seconds=60,
        )
        settings_params: dict[str, Any] = {
            "threadId": trigger.thread_id,
            "approvalPolicy": "never",
            "sandboxPolicy": {"type": "dangerFullAccess"},
            "cwd": str(ROOT),
        }
        if trigger.model:
            settings_params["model"] = trigger.model
        if trigger.thinking:
            settings_params["effort"] = trigger.thinking
        client.call("thread/settings/update", settings_params, timeout_seconds=60)
        thread = resume.get("result", {}).get("thread", {}) if isinstance(resume.get("result"), dict) else {}
        status = thread.get("status", {}) if isinstance(thread, dict) else {}
        method = "turn/start"
        params: dict[str, Any] = {
            "threadId": trigger.thread_id,
            "input": [{"type": "text", "text": prompt}],
            "clientUserMessageId": client_message_id,
            "cwd": str(ROOT),
            "approvalPolicy": "never",
            "sandboxPolicy": {"type": "dangerFullAccess"},
            "responsesapiClientMetadata": {
                "ascendop_bridge": f"{trigger.kind}_trigger",
                "ascendop_op": trigger.op,
                "ascendop_trigger_digest": trigger.digest,
            },
        }
        if trigger.model:
            params["model"] = trigger.model
        if trigger.thinking:
            params["effort"] = trigger.thinking
        active_turn_id = ""
        if isinstance(status, dict) and status.get("type") == "active":
            active_turn_id = find_active_turn_id(client, trigger.thread_id)
            if active_turn_id:
                method = "turn/steer"
                params = {
                    "threadId": trigger.thread_id,
                    "expectedTurnId": active_turn_id,
                    "input": [{"type": "text", "text": prompt}],
                    "clientUserMessageId": client_message_id,
                    "responsesapiClientMetadata": params["responsesapiClientMetadata"],
                }
        start = client.call(method, params, timeout_seconds=90)
        turn = start.get("result", {}).get("turn", {}) if isinstance(start.get("result"), dict) else {}
        turn_id = str(turn.get("id", "") or active_turn_id or "")
        turn_status = str(turn.get("status", "") or "")
        native_turn = client.wait_for_native_turn(trigger.thread_id, turn_id, timeout_seconds=30) if turn_id else {}
        storage_turn = client.wait_for_storage_turn(trigger.thread_id, turn_id, timeout_seconds=30) if turn_id else {}
        visible_turn = storage_turn or native_turn
        if turn_id and not visible_turn:
            raise RuntimeError(
                f"{trigger.kind} trigger turn is not readable from thread storage {trigger.thread_id}: turn_id={turn_id}"
            )
        native_status = str(visible_turn.get("status", "") or "")
        if is_empty_terminal_failure_turn(visible_turn):
            raise RuntimeError(
                f"{trigger.kind} trigger turn {turn_id} failed before the prompt was recorded; "
                "treating relay delivery as failed"
            )
        ack_details = {
            "turn_id": turn_id,
            "turn_status": turn_status,
            "method": method,
            "delivery": f"app-server-{client.server_mode}",
            "owner_app_server_mode": client.server_mode,
            "delivery_worker_pid": os.getpid(),
            "app_server_pid": int(getattr(getattr(client, "process", None), "pid", 0) or 0),
            "delivery_started_at": utc_now_iso(),
            "completion_unconfirmed": False,
            "control_plane_unavailable": False,
            "orphaned_delivery_owner": False,
            **remote_control,
            **turn_ack_details(visible_turn, server_mode=client.server_mode),
        }
        if allow_storage_visible and visible_turn:
            ack_details.update(
                {
                    "ide_panel_visible": False,
                    "ide_panel_visibility": "storage_visible_not_ide_visible",
                    "approval_policy": "never",
                    "sandbox_policy": "danger-full-access",
                    "permission_profile": "disabled",
                    "model": trigger.model,
                    "thinking": trigger.thinking,
                }
            )
        ack_trigger_delivery(trigger, "active", ack_details)
        append_event(
            "bridge_deliver_acked",
            {
                "key": trigger.key,
                "kind": trigger.kind,
                "op": trigger.op,
                "method": method,
                "turn_id": turn_id,
                "turn_status": turn_status,
                "storage_visible": ack_details.get("storage_visible", False),
                "native_visible": ack_details.get("native_visible", False),
                "ide_panel_visible": ack_details.get("ide_panel_visible", False),
                "ide_panel_visibility": ack_details.get("ide_panel_visibility", ""),
                "app_server_mode": ack_details.get("app_server_mode", client.server_mode),
                "native_status": native_status,
            },
        )
        configured_timeout = int(os.environ.get("ASCENDOP_LOCAL_TURN_TIMEOUT_SECONDS", "7200") or 7200)
        terminal_wait_seconds = max(int(wait_seconds or 0), configured_timeout)
        wait_status = client.wait_for_turn(trigger.thread_id, turn_id, terminal_wait_seconds) if turn_id else "failed"
        final_turn = client.read_thread_turn(trigger.thread_id, turn_id) or client.get_turn(
            trigger.thread_id,
            turn_id,
        )
        final_status = "failed" if wait_status == "timeout" else wait_status
        if final_status == "completed" and not turn_has_agent_output(final_turn):
            final_status = "failed"
            ack_details["failure_kind"] = "native_turn_no_agent_output"
            ack_details["error"] = "full-access app-server turn completed without agent output"
        if wait_status == "timeout":
            ack_details["failure_kind"] = "local_app_server_turn_timeout"
            ack_details["error"] = f"full-access app-server turn exceeded {terminal_wait_seconds}s"
        final_details = {
            **ack_details,
            **turn_ack_details(final_turn, server_mode=client.server_mode),
            "wait_status": wait_status,
            "native_status": str(final_turn.get("status", "") or final_status),
        }
        if allow_storage_visible and final_turn:
            final_details["ide_panel_visible"] = False
            final_details["ide_panel_visibility"] = "storage_visible_not_ide_visible"
        ack_trigger_delivery(trigger, final_status, final_details)
        return {
            "sender": f"app-server-{client.server_mode}",
            "method": method,
            "turn_id": turn_id,
            "turn_status": turn_status,
            "storage_visible": ack_details.get("storage_visible", False),
            "native_visible": ack_details.get("native_visible", False),
            "ide_panel_visible": ack_details.get("ide_panel_visible", False),
            "ide_panel_visibility": ack_details.get("ide_panel_visibility", ""),
            "app_server_mode": ack_details.get("app_server_mode", client.server_mode),
            "native_status": final_details.get("native_status", native_status),
            "wait_status": wait_status,
            "final_status": final_status,
        }


def find_active_turn_id(client: AppServerClient, thread_id: str) -> str:
    data = client.list_turns(thread_id, limit=1)
    if not data:
        return ""
    turn = data[0]
    if turn.get("status") == "inProgress":
        return str(turn.get("id", "") or "")
    return ""


def bridge_status(args: argparse.Namespace) -> int:
    config = load_config(
        ROOT / getattr(args, "config", "") if getattr(args, "config", "") else ROOT / "tools/tester_daemon/config/s5_910b_gitpartner_glugrad_bitwise.json",
        apply_completion_markers=True,
    )
    apply_app_server_policy_env(config)
    poll_interval = float(config.policy.get("solver_thread_poll_seconds", 0) or 0)
    if poll_interval > 0:
        try:
            poll_solver_threads(config)
        except Exception as exc:
            append_event("solver_threads_poll_failed", {"error": str(exc), "mode": "status"})
            write_thread_poll_status("failed", {"error": str(exc), "mode": "status"})
    else:
        write_thread_poll_status("disabled", {"reason": "solver_thread_poll_seconds <= 0", "mode": "status"})
    ready = load_ready_triggers(ROOT)
    watch = load_watch_triggers(ROOT)
    workers = prune_workers(read_workers())
    write_workers(workers)
    write_heartbeat("status", workers, len(ready))
    write_status(ready, workers, "status", watch)
    print(STATUS_PATH.read_text(encoding="utf-8"), end="")
    pending = [t for t in ready if t.key not in workers]
    pending_watch = [t for t in watch if t.key not in workers]
    return 2 if pending or pending_watch else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="AscendOP solver trigger bridge")
    sub = parser.add_subparsers(dest="command", required=True)

    p_run = sub.add_parser("run", help="run persistent solver trigger bridge")
    p_run.add_argument("--config", default="tools/tester_daemon/config/s5_910b_gitpartner_glugrad_bitwise.json")
    p_run.add_argument("--interval-seconds", type=float, default=0.0)
    p_run.add_argument("--max-workers", type=int, default=0)
    p_run.add_argument("--wait-seconds", type=int, default=1800)
    p_run.add_argument("--poll-threads-seconds", type=float, default=0.0)
    p_run.add_argument("--max-ticks", type=int, default=0)
    p_run.add_argument("--once", action="store_true")
    p_run.add_argument("--dry-run", action="store_true")
    p_run.add_argument("--replace-stale-lock-after-seconds", type=int, default=600)
    p_run.set_defaults(func=dispatch_workers)

    p_deliver = sub.add_parser("deliver", help="internal worker: deliver one trigger by key")
    p_deliver.add_argument("--key", required=True)
    p_deliver.add_argument("--wait-seconds", type=int, default=1800)
    p_deliver.set_defaults(func=deliver)

    p_watch = sub.add_parser("watch", help="internal worker: watch an existing trigger turn by key")
    p_watch.add_argument("--key", required=True)
    p_watch.add_argument("--wait-seconds", type=int, default=1800)
    p_watch.set_defaults(func=watch)

    p_status = sub.add_parser("status", help="write and print bridge status")
    p_status.add_argument("--config", default="tools/tester_daemon/config/s5_910b_gitpartner_glugrad_bitwise.json")
    p_status.set_defaults(func=bridge_status)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
