from __future__ import annotations

import json
import hashlib
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from ascendop_daemon.control_plane.control_database import ControlDatabase
from ascendop_daemon.core.clock import boot_id
from ascendop_daemon.runtime.process_identity import process_start_token
from ascendop_daemon.runtime.process_adapter import workspace_process_environment
from ascendop_daemon.runtime.workflow_adapter import resolve_workflow_adapter


class TypedActionExecutor:
    def __init__(
        self,
        root: Path,
        database: ControlDatabase,
        *,
        worker_id: str,
        producer_generation: str,
        claim_seconds: int,
        local_timeout_seconds: int,
        device_timeout_seconds: int,
    ) -> None:
        self.root = root.resolve()
        self.database = database
        self.worker_id = worker_id
        self.producer_generation = producer_generation
        self.claim_seconds = max(5, int(claim_seconds))
        self.local_timeout_seconds = max(1, int(local_timeout_seconds))
        self.device_timeout_seconds = max(1, int(device_timeout_seconds))

    def launch_once(self) -> dict[str, Any]:
        action = self.database.claim_workflow_action(
            self.worker_id,
            producer_generation=self.producer_generation,
            lease_seconds=self.claim_seconds,
        )
        if action is None:
            return {"state": "idle", "launched": False}
        claim_token = str(action.pop("claim_token"))
        attempt_id = str(action.pop("attempt_id"))
        attempt_ordinal = int(action.pop("attempt_ordinal"))
        action.pop("claimed_by", None)
        action.pop("claim_expires_at", None)
        started_at = self.database.start_workflow_action(
            str(action["action_id"]), claim_token
        )
        payload_root = self.root / ".ascendop-work" / "workflow-actions"
        payload_root.mkdir(parents=True, exist_ok=True)
        payload_path = payload_root / f"{action['action_id']}.json"
        payload = {
            "root": str(self.root),
            "database": str(self.database.path),
            "worker_id": self.worker_id,
            "claim_token": claim_token,
            "started_at": started_at,
            "attempt_id": attempt_id,
            "attempt_ordinal": attempt_ordinal,
            "claim_seconds": self.claim_seconds,
            "local_timeout_seconds": self.local_timeout_seconds,
            "device_timeout_seconds": self.device_timeout_seconds,
            "action": action,
        }
        logs = self.root / ".ascendop-work" / "workflow-actions" / "logs"
        logs.mkdir(parents=True, exist_ok=True)
        stdout_path = logs / f"{action['action_id']}.out.log"
        stderr_path = logs / f"{action['action_id']}.err.log"
        payload["stdout_path"] = str(stdout_path)
        payload["stderr_path"] = str(stderr_path)
        payload_path.write_text(
            json.dumps(payload, ensure_ascii=True, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )
        command = [
            sys.executable,
            str(self.root / "tools" / "tester_daemon" / "daemon.py"),
            "workflow-action-worker",
            "--payload",
            str(payload_path),
        ]
        try:
            with stdout_path.open("a", encoding="utf-8") as stdout, stderr_path.open(
                "a", encoding="utf-8"
            ) as stderr:
                process = subprocess.Popen(
                    command,
                    cwd=self.root,
                    stdout=stdout,
                    stderr=stderr,
                    env=workspace_process_environment(self.root),
                    creationflags=process_creation_flags(),
                    startupinfo=process_startupinfo(),
                )
        except OSError as exc:
            self.database.complete_workflow_action(
                str(action["action_id"]),
                claim_token,
                status="failed",
                worker_id=self.worker_id,
                producer_generation=self.producer_generation,
                started_at=started_at,
                return_code=None,
                details={"failure_class": "host-build", "error": str(exc)},
            )
            return {
                "state": "failed",
                "launched": False,
                "action_id": action["action_id"],
                "error": str(exc),
            }
        return {
            "state": "running",
            "launched": True,
            "action_id": action["action_id"],
            "attempt_id": attempt_id,
            "attempt_ordinal": attempt_ordinal,
            "pid": process.pid,
            "payload": str(payload_path),
            "stdout": str(stdout_path),
            "stderr": str(stderr_path),
        }


def run_action_worker(payload_path: Path) -> int:
    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    root = Path(str(payload["root"])).resolve()
    database = ControlDatabase(Path(str(payload["database"])))
    action = payload["action"]
    arguments = action["arguments"]
    command = action_command(root, arguments)
    lease_seconds = max(5, int(payload["claim_seconds"]))
    timeout = (
        int(payload["device_timeout_seconds"])
        if action.get("resource_class") == "device"
        else int(payload["local_timeout_seconds"])
    )
    return_code: int | None = None
    status = "failed"
    details: dict[str, Any] = {
        "operation": arguments["operation"],
        "attempt_id": str(payload["attempt_id"]),
        "attempt_ordinal": int(payload["attempt_ordinal"]),
        "timeout_seconds": timeout,
        "stdout_path": relative_log_path(root, Path(str(payload["stdout_path"]))),
        "stderr_path": relative_log_path(root, Path(str(payload["stderr_path"]))),
    }
    try:
        details["artifact_validation"] = validate_action_artifacts(root, action)
    except (OSError, ValueError) as exc:
        details.update({"failure_class": "protocol", "error": str(exc)})
        database.complete_workflow_action(
            str(action["action_id"]),
            str(payload["claim_token"]),
            status=status,
            worker_id=str(payload["worker_id"]),
            producer_generation=str(action["producer_generation"]),
            started_at=str(payload["started_at"]),
            return_code=None,
            details=details,
        )
        return 1
    worker_token = _wait_for_start_token(os.getpid())
    if not worker_token:
        details.update(
            {
                "failure_class": "protocol",
                "error": "workflow worker process identity unavailable",
            }
        )
        database.complete_workflow_action(
            str(action["action_id"]),
            str(payload["claim_token"]),
            status=status,
            worker_id=str(payload["worker_id"]),
            producer_generation=str(action["producer_generation"]),
            started_at=str(payload["started_at"]),
            return_code=None,
            details=details,
        )
        return 1
    database.attach_workflow_action_process(
        str(action["action_id"]),
        str(payload["claim_token"]),
        role="worker",
        pid=os.getpid(),
        start_token=worker_token,
        boot_id=boot_id(),
        lease_seconds=lease_seconds,
    )
    child: subprocess.Popen[Any] | None = None
    child_stdout = None
    child_stderr = None
    try:
        child_stdout = Path(str(payload["stdout_path"])).open("a", encoding="utf-8")
        child_stderr = Path(str(payload["stderr_path"])).open("a", encoding="utf-8")
        child = subprocess.Popen(
            command,
            cwd=root,
            stdout=child_stdout,
            stderr=child_stderr,
            env=workspace_process_environment(root),
            creationflags=process_creation_flags(),
            startupinfo=process_startupinfo(),
        )
        child_token = _wait_for_start_token(child.pid)
        if not child_token:
            raise OSError("workflow child process identity unavailable")
        database.attach_workflow_action_process(
            str(action["action_id"]),
            str(payload["claim_token"]),
            role="child",
            pid=child.pid,
            start_token=child_token,
            boot_id=boot_id(),
            lease_seconds=lease_seconds,
        )
        deadline = time.monotonic() + timeout
        heartbeat_interval = max(1.0, min(5.0, lease_seconds / 3.0))
        while child.poll() is None:
            if time.monotonic() >= deadline:
                raise subprocess.TimeoutExpired(command, timeout)
            if not database.heartbeat_workflow_action(
                str(action["action_id"]),
                str(payload["claim_token"]),
                lease_seconds=lease_seconds,
            ):
                raise RuntimeError("workflow action claim was lost while running")
            time.sleep(heartbeat_interval)
        return_code = int(child.returncode)
        status = "succeeded" if return_code == 0 else "failed"
        details["failure_class"] = (
            ""
            if return_code == 0
            else failure_class_for_operation(str(arguments["operation"]))
        )
    except subprocess.TimeoutExpired:
        _terminate_child(child)
        details.update(
            {
                "failure_class": (
                    "transport"
                    if action.get("resource_class") == "device"
                    else "host-build"
                ),
                "termination_reason": "workflow-action-timeout",
            }
        )
    except (OSError, RuntimeError) as exc:
        _terminate_child(child)
        details.update({"failure_class": "protocol", "error": str(exc)})
    finally:
        if child_stdout is not None:
            child_stdout.close()
        if child_stderr is not None:
            child_stderr.close()
        details["stdout_tail"] = read_log_tail(Path(str(payload["stdout_path"])))
        details["stderr_tail"] = read_log_tail(Path(str(payload["stderr_path"])))
    if status == "succeeded":
        postcondition = validate_action_postcondition(
            root,
            action,
            stdout_path=Path(str(payload["stdout_path"])),
        )
        details["postcondition"] = postcondition
        if not postcondition["satisfied"]:
            status = "failed"
            details["failure_class"] = failure_class_for_operation(
                str(arguments["operation"])
            )
            details["error"] = str(postcondition["error"])
    database.complete_workflow_action(
        str(action["action_id"]),
        str(payload["claim_token"]),
        status=status,
        worker_id=str(payload["worker_id"]),
        producer_generation=str(action["producer_generation"]),
        started_at=str(payload["started_at"]),
        return_code=return_code,
        details=details,
    )
    return 0 if status == "succeeded" else 1


def validate_action_postcondition(
    root: Path,
    action: dict[str, Any],
    *,
    stdout_path: Path,
) -> dict[str, Any]:
    operation = str(action.get("arguments", {}).get("operation") or "")
    if operation == "publish-test-request":
        output = read_json_output(stdout_path)
        errors = output.get("errors")
        generated = int(output.get("generated_count", 0) or 0)
        requests = output.get("requests")
        satisfied = (
            str(output.get("schema") or "")
            == "ascendop.test-request-generation.v1"
            and isinstance(errors, list)
            and not errors
            and isinstance(requests, list)
            and generated == len(requests)
            and generated > 0
        )
        return {
            "schema": "ascendop.workflow-action-postcondition.v1",
            "operation": operation,
            "satisfied": satisfied,
            "generated_count": generated,
            "error_count": len(errors) if isinstance(errors, list) else -1,
            "error": (
                ""
                if satisfied
                else "request scan returned no effect or reported errors"
            ),
        }
    if operation == "agent-source-promote":
        output = read_json_output(stdout_path)
        arguments = action.get("arguments", {})
        positional = list(arguments.get("positional", []))
        identity = action.get("candidate_identity", {})
        expected_action_id = (
            str(identity.get("agent_action_id") or "")
            if isinstance(identity, dict)
            else ""
        )
        expected_digest = (
            str(identity.get("source_after_digest") or "")
            if isinstance(identity, dict)
            else ""
        )
        receipt_path = None
        if len(positional) == 1:
            seal = (root / str(positional[0])).resolve()
            if root.resolve() in seal.parents:
                receipt_path = seal.parent / "promotion-receipt.json"
        receipt = (
            read_json_output(receipt_path)
            if receipt_path is not None and receipt_path.is_file()
            else {}
        )
        satisfied = (
            output.get("schema") == "ascendop.agent-source-promotion-receipt.v1"
            and output == receipt
            and output.get("action_id") == expected_action_id
            and output.get("source_after_digest") == expected_digest
        )
        return {
            "schema": "ascendop.workflow-action-postcondition.v1",
            "operation": operation,
            "satisfied": satisfied,
            "agent_action_id": str(output.get("action_id") or ""),
            "source_after_digest": str(output.get("source_after_digest") or ""),
            "error": "" if satisfied else "source promotion receipt is missing or inconsistent",
        }
    if operation == "agent-case-promote":
        output = read_json_output(stdout_path)
        arguments = action.get("arguments", {})
        positional = list(arguments.get("positional", []))
        identity = action.get("candidate_identity", {})
        expected_action_id = (
            str(identity.get("agent_action_id") or "")
            if isinstance(identity, dict)
            else ""
        )
        expected_version = (
            str(identity.get("case_version") or "")
            if isinstance(identity, dict)
            else ""
        )
        expected_digest = (
            str(identity.get("source_after_digest") or "")
            if isinstance(identity, dict)
            else ""
        )
        receipt_path = None
        if len(positional) == 1:
            seal = (root / str(positional[0])).resolve()
            if root.resolve() in seal.parents:
                receipt_path = seal.parent / "case-promotion-receipt.json"
        receipt = (
            read_json_output(receipt_path)
            if receipt_path is not None and receipt_path.is_file()
            else {}
        )
        satisfied = (
            output.get("schema") == "ascendop.agent-case-promotion-receipt.v1"
            and output == receipt
            and output.get("action_id") == expected_action_id
            and output.get("candidate_version") == expected_version
            and output.get("source_after_digest") == expected_digest
            and int(output.get("case_count", 0) or 0) > 0
        )
        return {
            "schema": "ascendop.workflow-action-postcondition.v1",
            "operation": operation,
            "satisfied": satisfied,
            "agent_action_id": str(output.get("action_id") or ""),
            "case_version": str(output.get("candidate_version") or ""),
            "case_content_digest": str(output.get("case_content_digest") or ""),
            "error": "" if satisfied else "case promotion receipt is missing or inconsistent",
        }
    if operation == "agent-output-promote":
        output = read_json_output(stdout_path)
        arguments = action.get("arguments", {})
        positional = list(arguments.get("positional", []))
        identity = action.get("candidate_identity", {})
        expected_action_id = (
            str(identity.get("agent_action_id") or "")
            if isinstance(identity, dict)
            else ""
        )
        expected_contracts_digest = (
            str(identity.get("contracts_digest") or "")
            if isinstance(identity, dict)
            else ""
        )
        expected_source_after = (
            str(identity.get("source_after_digest") or "")
            if isinstance(identity, dict)
            else ""
        )
        receipt_path = None
        if len(positional) == 1:
            seal = (root / str(positional[0])).resolve()
            if root.resolve() in seal.parents:
                receipt_path = seal.parent / "output-promotion-receipt.json"
        receipt = (
            read_json_output(receipt_path)
            if receipt_path is not None and receipt_path.is_file()
            else {}
        )
        source_receipt_path = (
            receipt_path.parent / "promotion-receipt.json"
            if receipt_path is not None
            else None
        )
        source_receipt = (
            read_json_output(source_receipt_path)
            if source_receipt_path is not None
            and source_receipt_path.is_file()
            else {}
        )
        source_satisfied = not expected_source_after or (
            source_receipt.get("schema")
            == "ascendop.agent-source-promotion-receipt.v1"
            and source_receipt.get("action_id") == expected_action_id
            and source_receipt.get("source_after_digest") == expected_source_after
        )
        satisfied = (
            output.get("schema")
            == "ascendop.agent-output-promotion-receipt.v1"
            and output == receipt
            and output.get("action_id") == expected_action_id
            and output.get("contracts_digest") == expected_contracts_digest
            and isinstance(output.get("outputs"), list)
            and bool(output.get("outputs"))
            and source_satisfied
        )
        return {
            "schema": "ascendop.workflow-action-postcondition.v1",
            "operation": operation,
            "satisfied": satisfied,
            "agent_action_id": str(output.get("action_id") or ""),
            "contracts_digest": str(output.get("contracts_digest") or ""),
            "output_count": len(output.get("outputs", []))
            if isinstance(output.get("outputs"), list)
            else -1,
            "source_after_digest": str(
                source_receipt.get("source_after_digest") or ""
            ),
            "error": ""
            if satisfied
            else "workflow output promotion receipt is missing or inconsistent",
        }
    if operation in {
        "collect-profiler-evidence",
        "register-solver-diagnostic",
        "retry-solver-diagnostic",
    }:
        from ascendop_daemon.workflow.solver_diagnostics import observe_request

        arguments = action.get("arguments", {})
        positional = list(arguments.get("positional", []))
        options = dict(arguments.get("options", {}))
        if len(positional) < 3:
            return {
                "schema": "ascendop.workflow-action-postcondition.v1",
                "operation": operation,
                "satisfied": False,
                "error": "diagnostic action identity is incomplete",
            }
        identity = {
            "operator": str(positional[0]),
            "case_version": str(positional[1]),
            "result_version": str(positional[2]),
            "blocker_generation": str(options.get("blocker_generation") or ""),
        }
        typed_request = (
            root
            / "TestUtils"
            / "casegen"
            / identity["operator"]
            / "case"
            / identity["case_version"]
            / "SOLVER_DIAGNOSTIC_REQUEST.json"
        )
        if operation == "collect-profiler-evidence" and not typed_request.is_file():
            from ascendop_daemon.workflow.profiler_request_state import (
                observe_profiler_request,
            )

            observation = observe_profiler_request(root, **identity)
        else:
            observation = observe_request(root, **identity)
        status = str(observation.get("status") or "")
        satisfied = status in {
            "ready",
            "registered",
            "collecting",
            "complete",
            "completed",
        }
        if operation == "retry-solver-diagnostic":
            expected_attempt = int(options.get("expected_request_attempt") or 0)
            satisfied = (
                satisfied
                and int(observation.get("request_attempt") or 0)
                == expected_attempt
                and bool(observation.get("retry_engine_generation"))
            )
        return {
            "schema": "ascendop.workflow-action-postcondition.v1",
            "operation": operation,
            "satisfied": satisfied,
            "observed_status": status,
            "request_attempt": int(observation.get("request_attempt") or 0),
            "retry_engine_generation": str(
                observation.get("retry_engine_generation") or ""
            ),
            "error": "" if satisfied else str(observation.get("error") or status),
        }
    if operation == "reconcile-terminal-result":
        arguments = action.get("arguments", {})
        positional = list(arguments.get("positional", []))
        options = dict(arguments.get("options", {}))
        source = (root / str(options.get("source_result") or "")).resolve()
        destination = (
            root
            / "operators_testresult"
            / str(positional[0] if positional else "")
            / str(positional[1] if len(positional) > 1 else "")
            / "RESULT.md"
        ).resolve()
        source_digest = (
            hashlib.sha256(source.read_bytes()).hexdigest()
            if source.is_file()
            else ""
        )
        destination_digest = (
            hashlib.sha256(destination.read_bytes()).hexdigest()
            if destination.is_file()
            else ""
        )
        satisfied = bool(source_digest) and source_digest == destination_digest
        return {
            "schema": "ascendop.workflow-action-postcondition.v1",
            "operation": operation,
            "satisfied": satisfied,
            "source_digest": source_digest,
            "destination_digest": destination_digest,
            "error": "" if satisfied else "canonical terminal result was not reconciled",
        }
    return {
        "schema": "ascendop.workflow-action-postcondition.v1",
        "operation": operation,
        "satisfied": True,
        "verification": "process-return-code",
        "error": "",
    }


def read_json_output(path: Path, max_bytes: int = 1024 * 1024) -> dict[str, Any]:
    try:
        size = path.stat().st_size
        if size <= 0 or size > max_bytes:
            raise ValueError(f"JSON action output size is invalid: {size}")
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        return {
            "schema": "",
            "generated_count": 0,
            "requests": [],
            "errors": [{"error": f"invalid action JSON output: {exc}"}],
        }
    return value if isinstance(value, dict) else {
        "schema": "",
        "generated_count": 0,
        "requests": [],
        "errors": [{"error": "action JSON output must be an object"}],
    }


def _wait_for_start_token(pid: int, timeout_seconds: float = 2.0) -> str:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        token = process_start_token(pid)
        if token:
            return token
        time.sleep(0.02)
    return ""


def _terminate_child(process: subprocess.Popen[Any] | None) -> None:
    if process is None or process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def action_command(root: Path, arguments: dict[str, Any]) -> list[str]:
    operation = str(arguments["operation"])
    positional = [str(value) for value in arguments.get("positional", [])]
    options = arguments.get("options", {})
    if operation == "publish-test-request":
        command = [
            sys.executable,
            str(root / "tools" / "tester_daemon" / "daemon.py"),
            "request",
            "scan",
        ]
    elif operation == "agent-source-promote":
        command = [
            sys.executable,
            str(root / "tools" / "tester_daemon" / "daemon.py"),
            "agent",
            "source-promote",
        ]
    elif operation == "agent-case-promote":
        command = [
            sys.executable,
            str(root / "tools" / "tester_daemon" / "daemon.py"),
            "agent",
            "case-promote",
        ]
    elif operation == "agent-output-promote":
        command = [
            sys.executable,
            str(root / "tools" / "tester_daemon" / "daemon.py"),
            "agent",
            "output-promote",
        ]
    else:
        command = [sys.executable, str(resolve_workflow_adapter(root).path), operation]
    command.extend(positional)
    for key in sorted(options):
        value = options[key]
        flag = "--" + key.replace("_", "-")
        if value is True:
            command.append(flag)
        elif value is False or value is None:
            continue
        else:
            command.extend((flag, str(value)))
    return command


def failure_class_for_operation(operation: str) -> str:
    if operation == "collect-profiler-evidence":
        return "profiler"
    if operation in {"register-solver-diagnostic", "retry-solver-diagnostic"}:
        return "protocol"
    if operation in {
        "agent-source-promote",
        "agent-case-promote",
        "agent-output-promote",
    }:
        return "protocol"
    if operation == "publish-test-request":
        return "protocol"
    if operation in {
        "gitpartner-run-submit",
        "gitpartner-run-msopgen",
        "gitpartner-recover-blocked",
        "gitpartner-recover-worktree",
        "gitpartner-heartbeat",
        "gitpartner-cancel-stalled",
    }:
        return "transport"
    if operation in {
        "prepare-submit",
        "restore-submit",
        "requeue-submit",
        "promote-release",
        "create-v1-regression-sentinel",
        "record-v1-regression-sentinel",
        "create-case-regression-sentinel",
        "record-case-regression-sentinel",
    }:
        return "business"
    if operation == "reconcile-terminal-result":
        return "protocol"
    return "protocol"


def relative_log_path(root: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return str(path.resolve())


def read_log_tail(path: Path, max_chars: int = 4000) -> str:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""
    return text[-max(0, int(max_chars)) :]


def validate_action_artifacts(root: Path, action: dict[str, Any]) -> dict[str, Any]:
    root = root.resolve()
    artifacts = action.get("artifacts", [])
    if not isinstance(artifacts, list):
        raise ValueError("workflow action artifacts must be a list")
    validated: list[dict[str, Any]] = []
    seen: set[Path] = set()
    for index, artifact in enumerate(artifacts):
        if not isinstance(artifact, dict):
            raise ValueError(f"workflow artifact {index} must be an object")
        locator = str(artifact.get("path") or artifact.get("uri") or "")
        if not locator or locator.startswith(("/", "\\")):
            raise ValueError(f"workflow artifact {index} path is invalid")
        raw_path = root / locator
        if raw_path.is_symlink():
            raise ValueError(f"workflow artifact cannot be a symlink: {locator}")
        candidate = raw_path.resolve()
        if root not in candidate.parents or candidate in seen:
            raise ValueError(f"workflow artifact {index} path is unbounded or duplicate")
        seen.add(candidate)
        if candidate.is_symlink() or not candidate.is_file():
            raise ValueError(f"workflow artifact is not a regular file: {locator}")
        payload = candidate.read_bytes()
        digest = hashlib.sha256(payload).hexdigest()
        if digest != str(artifact.get("sha256") or "").lower():
            raise ValueError(f"workflow artifact digest mismatch: {locator}")
        declared_size = artifact.get("size")
        if declared_size is not None and int(declared_size) != len(payload):
            raise ValueError(f"workflow artifact size mismatch: {locator}")
        validated.append(
            {"path": candidate.relative_to(root).as_posix(), "sha256": digest}
        )
    return {"count": len(validated), "artifacts": validated}


def process_creation_flags() -> int:
    return getattr(subprocess, "CREATE_NO_WINDOW", 0) if os.name == "nt" else 0


def process_startupinfo() -> subprocess.STARTUPINFO | None:
    if os.name != "nt":
        return None
    startupinfo = subprocess.STARTUPINFO()
    startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    startupinfo.wShowWindow = 0
    return startupinfo
