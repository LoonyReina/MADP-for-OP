from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Protocol, Sequence

from ..provider import (
    AgentProviderProfile,
    PreparedDriverRuntime,
    provider_private_root,
)


@dataclass(frozen=True)
class DriverProbe:
    driver: str
    available: bool
    executable: str
    executable_digest: str
    version: str
    capabilities: dict[str, bool]
    error: str = ""


@dataclass(frozen=True)
class DriverResult:
    status: str
    session_id: str
    exit_code: int | None
    completion: dict[str, Any]
    raw_output_path: Path
    stderr_path: Path
    error: str = ""


class AgentDriver(Protocol):
    driver_id: str

    def probe(self) -> DriverProbe: ...

    def start(
        self,
        *,
        prompt: str,
        workspace: Path,
        run_root: Path,
        timeout_seconds: int,
        heartbeat: Callable[[], None],
        resume_session_id: str = "",
    ) -> DriverResult: ...

    def observe(self, result: DriverResult) -> dict[str, Any]: ...

    def heartbeat(self, session_id: str) -> dict[str, Any]: ...

    def cancel(self, session_id: str) -> dict[str, Any]: ...

    def resume(
        self,
        *,
        session_id: str,
        prompt: str,
        workspace: Path,
        run_root: Path,
        timeout_seconds: int,
        heartbeat: Callable[[], None],
    ) -> DriverResult: ...

    def collect(self, result: DriverResult) -> dict[str, Any]: ...


class SubprocessAgentDriver:
    driver_id = ""
    executable_names: tuple[str, ...] = ()
    capabilities = {
        "stream_json": True,
        "resume": True,
        "structured_output": True,
    }
    requires_private_runtime = False

    def __init__(self, provider_profile: AgentProviderProfile | None = None) -> None:
        self.provider_profile = provider_profile

    def probe(self) -> DriverProbe:
        executable = self._find_executable()
        if executable is None:
            return DriverProbe(
                driver=self.driver_id,
                available=False,
                executable="",
                executable_digest="",
                version="",
                capabilities=dict(self.capabilities),
                error="executable-not-found",
            )
        try:
            completed = subprocess.run(
                [executable, "--version"],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=15,
                check=False,
            )
            version = (completed.stdout or completed.stderr).strip().splitlines()[0]
            digest = _file_digest(Path(executable))
            return DriverProbe(
                driver=self.driver_id,
                available=completed.returncode == 0,
                executable=executable,
                executable_digest=digest,
                version=version,
                capabilities=dict(self.capabilities),
                error="" if completed.returncode == 0 else "version-probe-failed",
            )
        except (OSError, subprocess.TimeoutExpired, IndexError) as exc:
            return DriverProbe(
                driver=self.driver_id,
                available=False,
                executable=executable,
                executable_digest=_file_digest(Path(executable)),
                version="",
                capabilities=dict(self.capabilities),
                error=str(exc),
            )

    def start(
        self,
        *,
        prompt: str,
        workspace: Path,
        run_root: Path,
        timeout_seconds: int,
        heartbeat: Callable[[], None],
        resume_session_id: str = "",
    ) -> DriverResult:
        probe = self.probe()
        if not probe.available:
            raise RuntimeError(f"{self.driver_id} is unavailable: {probe.error}")
        run_root.mkdir(parents=True, exist_ok=True)
        raw_output_path = run_root / "events.jsonl"
        stderr_path = run_root / "stderr.log"
        completion_path = run_root / "completion.json"
        env = os.environ.copy()
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        runtime = self._prepare_provider_runtime(run_root=run_root, environment=env)
        command = self.command(
            executable=probe.executable,
            workspace=workspace,
            run_root=run_root,
            completion_path=completion_path,
            resume_session_id=resume_session_id,
            prompt=prompt,
            provider_runtime=runtime,
        )
        started = time.monotonic()
        last_heartbeat = 0.0
        last_failure_check = 0.0
        fatal_failure: dict[str, Any] | None = None
        try:
            with raw_output_path.open("wb") as stdout, stderr_path.open("wb") as stderr:
                process = subprocess.Popen(
                    command,
                    cwd=workspace,
                    env=runtime.environment,
                    stdin=subprocess.PIPE,
                    stdout=stdout,
                    stderr=stderr,
                    creationflags=(
                        subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
                    ),
                    start_new_session=os.name != "nt",
                )
                assert process.stdin is not None
                process.stdin.write(self.stdin_payload(prompt))
                process.stdin.close()
                timed_out = False
                while process.poll() is None:
                    elapsed = time.monotonic() - started
                    if elapsed >= timeout_seconds:
                        timed_out = True
                        _terminate(process)
                        break
                    if elapsed - last_heartbeat >= 5.0:
                        heartbeat()
                        last_heartbeat = elapsed
                    if elapsed - last_failure_check >= 0.5:
                        fatal_failure = self.stream_failure(raw_output_path)
                        last_failure_check = elapsed
                        if fatal_failure is not None:
                            _terminate(process)
                            break
                    time.sleep(0.2)
                exit_code = process.wait(timeout=10)
        except Exception:
            runtime.cleanup()
            raise
        completion = self._read_completion(completion_path, raw_output_path)
        session_id = str(
            completion.get("session_id")
            or _extract_session_id(raw_output_path)
            or resume_session_id
            or ""
        )
        status = str(completion.get("status") or "")
        if fatal_failure is not None:
            status = "failed"
            completion.update(fatal_failure)
        elif timed_out:
            status = "uncertain"
            completion.setdefault("failure_class", "agent-timeout")
            completion.setdefault("error", "agent CLI exceeded its granted turn budget")
        elif exit_code != 0 and status not in {"failed", "uncertain"}:
            status = "failed"
            completion.setdefault("failure_class", "agent-adapter")
            completion.setdefault(
                "error", f"{self.driver_id} process exited before a valid completion"
            )
        elif status not in {"completed", "failed", "uncertain", "cancelled"}:
            status = "completed" if exit_code == 0 else "failed"
        if exit_code != 0:
            completion.setdefault("exit_code", exit_code)
            if status == "failed" and "failure_class" not in completion:
                completion["failure_class"] = "agent-adapter"
                completion["error"] = (
                    f"{self.driver_id} returned a failed process without a valid "
                    "structured completion"
                )
        completion["status"] = status
        if status != "uncertain":
            runtime.cleanup()
        else:
            runtime.close_transport()
        return DriverResult(
            status=status,
            session_id=session_id,
            exit_code=exit_code,
            completion=completion,
            raw_output_path=raw_output_path,
            stderr_path=stderr_path,
            error="timeout" if timed_out else "",
        )

    def _prepare_provider_runtime(
        self, *, run_root: Path, environment: dict[str, str]
    ) -> PreparedDriverRuntime:
        if self.provider_profile is None:
            if self.requires_private_runtime:
                private_root = provider_private_root(run_root)
                private_root.mkdir(parents=True, exist_ok=True)
                return PreparedDriverRuntime(
                    environment=environment,
                    private_root=private_root,
                )
            return PreparedDriverRuntime(environment=environment)
        return self.provider_profile.prepare(
            driver=self.driver_id,
            run_root=run_root,
            inherited_environment=environment,
        )

    def stream_failure(self, raw_output_path: Path) -> dict[str, Any] | None:
        del raw_output_path
        return None

    def stdin_payload(self, prompt: str) -> bytes:
        return prompt.encode("utf-8")

    def observe(self, result: DriverResult) -> dict[str, Any]:
        return {
            "driver": self.driver_id,
            "status": result.status,
            "session_id": result.session_id,
            "exit_code": result.exit_code,
        }

    def heartbeat(self, session_id: str) -> dict[str, Any]:
        return {"driver": self.driver_id, "session_id": session_id, "state": "observed"}

    def cancel(self, session_id: str) -> dict[str, Any]:
        return {"driver": self.driver_id, "session_id": session_id, "state": "cancel-requested"}

    def resume(
        self,
        *,
        session_id: str,
        prompt: str,
        workspace: Path,
        run_root: Path,
        timeout_seconds: int,
        heartbeat: Callable[[], None],
    ) -> DriverResult:
        return self.start(
            prompt=prompt,
            workspace=workspace,
            run_root=run_root,
            timeout_seconds=timeout_seconds,
            heartbeat=heartbeat,
            resume_session_id=session_id,
        )

    def collect(self, result: DriverResult) -> dict[str, Any]:
        return dict(result.completion)

    def command(
        self,
        *,
        executable: str,
        workspace: Path,
        run_root: Path,
        completion_path: Path,
        resume_session_id: str,
        prompt: str,
        provider_runtime: PreparedDriverRuntime,
    ) -> Sequence[str]:
        raise NotImplementedError

    def _read_completion(
        self, completion_path: Path, raw_output_path: Path
    ) -> dict[str, Any]:
        if completion_path.is_file():
            try:
                value = json.loads(completion_path.read_text(encoding="utf-8-sig"))
                if isinstance(value, dict):
                    return value
            except (OSError, json.JSONDecodeError):
                pass
        return _extract_completion(raw_output_path)

    def _find_executable(self) -> str | None:
        for name in self.executable_names:
            found = shutil.which(name)
            if found:
                return str(Path(found).resolve())
        return None


def completion_schema() -> dict[str, Any]:
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "required": ["status", "summary", "artifacts"],
        "properties": {
            "status": {
                "type": "string",
                "enum": ["completed", "failed", "uncertain", "cancelled"],
            },
            "summary": {"type": "string"},
            "artifacts": {"type": "array", "items": {"type": "string"}},
        },
        "additionalProperties": False,
    }


def write_completion_schema(path: Path) -> None:
    path.write_text(json.dumps(completion_schema(), indent=2) + "\n", encoding="utf-8")


def _extract_completion(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    last: dict[str, Any] = {}
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            candidate = _find_completion(value)
            if candidate:
                last = candidate
    return last


def _extract_session_id(path: Path) -> str:
    if not path.is_file():
        return ""
    session_id = ""
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        candidate = _find_session_id(value)
        if candidate:
            session_id = candidate
    return session_id


def _find_session_id(value: Any) -> str:
    if isinstance(value, dict):
        for key in ("session_id", "thread_id", "conversation_id"):
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()
        for candidate in value.values():
            found = _find_session_id(candidate)
            if found:
                return found
    elif isinstance(value, list):
        for candidate in value:
            found = _find_session_id(candidate)
            if found:
                return found
    return ""


def _find_completion(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        if value.get("status") in {"completed", "failed", "uncertain", "cancelled"}:
            return dict(value)
        for key in ("structured_output", "result", "message", "content"):
            candidate = value.get(key)
            if isinstance(candidate, str):
                try:
                    candidate = json.loads(candidate)
                except json.JSONDecodeError:
                    continue
            found = _find_completion(candidate)
            if found:
                return found
    if isinstance(value, list):
        for item in reversed(value):
            found = _find_completion(item)
            if found:
                return found
    return {}


def _file_digest(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return hashlib.sha256(str(path).encode("utf-8")).hexdigest()


def _terminate(process: subprocess.Popen[Any]) -> None:
    if process.poll() is not None:
        return
    if os.name == "nt":
        subprocess.run(
            ["taskkill", "/PID", str(process.pid), "/T", "/F"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
    else:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
