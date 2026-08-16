from __future__ import annotations

import hashlib
import json
import locale
import os
import subprocess
import sys
import tempfile
import time
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from typing import Any

from ascendop_daemon.core.atomic_io import read_json_mapping, write_json_atomic
from ascendop_daemon.core.windows_privilege import is_elevated_windows_process


RESIDENT_TASK_NAME = "AscendOP-V4-Resident-Bootstrap"
LEGACY_RESIDENT_TASK_NAMES = (
    "AscendOP-V4-Resident-Watchdog",
    "AscendOP-S6-Resident-Watchdog",
)
TASK_SCHEMA = "ascendop.v4-resident-bootstrap-task.v2"
TASK_NAMESPACE = "http://schemas.microsoft.com/windows/2004/02/mit/task"
TASK_SCHEDULE_SECONDS = 120
TASK_EXPECTED_SECONDS = 30
TASK_EXECUTION_LIMIT_SECONDS = 90
TASK_EXECUTION_LIMIT = "PT1M30S"
TASK_PROBE_TIMEOUT_SECONDS = 90
TASK_STALE_INSTANCE_SECONDS = 120
TASK_BOOT_DELAY = "PT30S"
TASK_PRINCIPAL = "S-1-5-18"
TASK_OPERATIONAL_CHANNEL = "Microsoft-Windows-TaskScheduler/Operational"
TASK_DIGEST_PLACEHOLDER = "0" * 64
TASK_STAGE_SCHEMA = "ascendop.v4-resident-bootstrap-stage.v1"
TASK_STAGE_NAME = "v4-resident-bootstrap-stage.json"


class ResidentWatchdogError(RuntimeError):
    pass


def end_active_resident_task() -> dict[str, Any]:
    """End only the current Windows task invocation, leaving its schedule intact."""

    if os.name != "nt":
        return {
            "task_name": RESIDENT_TASK_NAME,
            "state": "not-applicable",
            "returncode": 0,
        }
    attempts = [
        _end_task(name)
        for name in (RESIDENT_TASK_NAME, *LEGACY_RESIDENT_TASK_NAMES)
    ]
    return {
        "task_name": RESIDENT_TASK_NAME,
        "state": (
            "ended"
            if any(item["returncode"] == 0 for item in attempts)
            else "not-running-or-unavailable"
        ),
        "returncode": 0 if any(item["returncode"] == 0 for item in attempts) else 1,
        "attempts": attempts,
    }


def resident_task_xml(
    root: Path,
    *,
    config: Path,
    interval_seconds: float,
    probe_id: str = "bootstrap-probe",
    registration_generation: str = "test-registration",
    python_executable: Path | None = None,
    launcher: Path | None = None,
) -> str:
    xml, _digest = _resident_task_document(
        root,
        config=config,
        interval_seconds=interval_seconds,
        probe_id=probe_id,
        registration_generation=registration_generation,
        python_executable=python_executable,
        launcher=launcher,
    )
    return xml


def _resident_task_document(
    root: Path,
    *,
    config: Path,
    interval_seconds: float,
    probe_id: str,
    registration_generation: str,
    python_executable: Path | None,
    launcher: Path | None,
) -> tuple[str, str]:
    root = root.resolve()
    python = _console_python(python_executable or Path(sys.executable))
    launcher = launcher or _active_launcher(root)
    if not launcher.is_file():
        raise ResidentWatchdogError(f"resident launcher is missing: {launcher}")
    provisional = _render_resident_task_xml(
        root,
        config=config,
        interval_seconds=interval_seconds,
        probe_id=probe_id,
        registration_generation=registration_generation,
        task_definition_digest=TASK_DIGEST_PLACEHOLDER,
        python=python,
        launcher=launcher,
    )
    definition_digest = hashlib.sha256(provisional.encode("utf-8")).hexdigest()
    return (
        _render_resident_task_xml(
            root,
            config=config,
            interval_seconds=interval_seconds,
            probe_id=probe_id,
            registration_generation=registration_generation,
            task_definition_digest=definition_digest,
            python=python,
            launcher=launcher,
        ),
        definition_digest,
    )


def _render_resident_task_xml(
    root: Path,
    *,
    config: Path,
    interval_seconds: float,
    probe_id: str,
    registration_generation: str,
    task_definition_digest: str,
    python: Path,
    launcher: Path,
) -> str:
    ET.register_namespace("", TASK_NAMESPACE)
    q = lambda name: f"{{{TASK_NAMESPACE}}}{name}"
    task = ET.Element(q("Task"), {"version": "1.4"})
    registration = ET.SubElement(task, q("RegistrationInfo"))
    ET.SubElement(registration, q("Description")).text = (
        "AscendOP Flow V4 bounded no-window resident bootstrap."
    )
    triggers = ET.SubElement(task, q("Triggers"))
    boot_trigger = ET.SubElement(triggers, q("BootTrigger"))
    ET.SubElement(boot_trigger, q("Enabled")).text = "true"
    ET.SubElement(boot_trigger, q("Delay")).text = TASK_BOOT_DELAY
    trigger = ET.SubElement(triggers, q("TimeTrigger"))
    repetition = ET.SubElement(trigger, q("Repetition"))
    ET.SubElement(repetition, q("Interval")).text = (
        f"PT{TASK_SCHEDULE_SECONDS // 60}M"
    )
    ET.SubElement(repetition, q("StopAtDurationEnd")).text = "false"
    ET.SubElement(trigger, q("StartBoundary")).text = datetime.now().astimezone().isoformat(
        timespec="seconds"
    )
    ET.SubElement(trigger, q("Enabled")).text = "true"
    principals = ET.SubElement(task, q("Principals"))
    principal = ET.SubElement(principals, q("Principal"), {"id": "System"})
    ET.SubElement(principal, q("UserId")).text = TASK_PRINCIPAL
    ET.SubElement(principal, q("LogonType")).text = "ServiceAccount"
    ET.SubElement(principal, q("RunLevel")).text = "HighestAvailable"
    settings = ET.SubElement(task, q("Settings"))
    for name, value in (
        ("MultipleInstancesPolicy", "StopExisting"),
        ("DisallowStartIfOnBatteries", "false"),
        ("StopIfGoingOnBatteries", "false"),
        ("AllowHardTerminate", "true"),
        ("StartWhenAvailable", "true"),
        ("RunOnlyIfNetworkAvailable", "false"),
        ("AllowStartOnDemand", "true"),
        ("Enabled", "true"),
        ("Hidden", "true"),
        ("RunOnlyIfIdle", "false"),
        ("WakeToRun", "false"),
        ("ExecutionTimeLimit", TASK_EXECUTION_LIMIT),
        ("Priority", "7"),
    ):
        ET.SubElement(settings, q(name)).text = value
    restart = ET.SubElement(settings, q("RestartOnFailure"))
    ET.SubElement(restart, q("Interval")).text = "PT1M"
    ET.SubElement(restart, q("Count")).text = "1"
    actions = ET.SubElement(task, q("Actions"), {"Context": "System"})
    execute = ET.SubElement(actions, q("Exec"))
    stdout, stderr = _bootstrap_log_paths(
        root / ".ascendop-work" / "runtime" / "logs", probe_id
    )
    arguments = subprocess.list2cmdline(
        [
            str(launcher),
            "ensure-resident",
            "--root",
            str(root),
            "--config",
            str(config),
            "--interval-seconds",
            str(max(0.05, interval_seconds)),
            "--quiet",
            "--bootstrap-probe-id",
            probe_id,
            "--bootstrap-task-name",
            RESIDENT_TASK_NAME,
            "--bootstrap-registration-generation",
            registration_generation,
            "--bootstrap-task-definition-digest",
            task_definition_digest,
            "--bootstrap-stdout",
            str(stdout),
            "--bootstrap-stderr",
            str(stderr),
        ]
    )
    ET.SubElement(execute, q("Command")).text = str(python)
    ET.SubElement(execute, q("Arguments")).text = arguments
    ET.SubElement(execute, q("WorkingDirectory")).text = str(root)
    return ET.tostring(task, encoding="unicode")


def stage_resident_task_installation(
    root: Path,
    *,
    config: Path,
    interval_seconds: float,
    python_executable: Path | None = None,
) -> dict[str, Any]:
    """Prepare and inspect the privileged task registration without mutating it."""

    if os.name != "nt":
        raise ResidentWatchdogError("resident task staging requires Windows")
    root = root.resolve()
    runtime = root / ".ascendop-work" / "runtime"
    runtime.mkdir(parents=True, exist_ok=True)
    logs = runtime / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    launcher = _active_launcher(root)
    python = _console_python(python_executable or Path(sys.executable))
    release_generation = _active_release_generation(root)
    identity = {
        "release_generation": release_generation,
        "launcher": str(launcher),
        "python_executable": str(python),
        "config": str(config.resolve()),
        "interval_seconds": max(0.05, interval_seconds),
    }
    identity_digest = hashlib.sha256(
        json.dumps(identity, ensure_ascii=True, sort_keys=True).encode("ascii")
    ).hexdigest()
    stage_path = runtime / TASK_STAGE_NAME
    staged = read_json_mapping(stage_path)
    xml_path = runtime / "v4-resident-bootstrap-task.xml"
    reusable = (
        staged.get("schema") == TASK_STAGE_SCHEMA
        and staged.get("identity_digest") == identity_digest
        and xml_path.is_file()
    )
    if reusable:
        try:
            xml = xml_path.read_text(encoding="utf-16")
        except OSError:
            reusable = False
        else:
            reusable = (
                hashlib.sha256(xml.encode("utf-8")).hexdigest()
                == staged.get("xml_sha256")
            )
    if not reusable:
        probe_id = uuid.uuid4().hex
        registration_generation = uuid.uuid4().hex
        xml, task_definition_digest = _resident_task_document(
            root,
            config=config,
            interval_seconds=interval_seconds,
            probe_id=probe_id,
            registration_generation=registration_generation,
            python_executable=python,
            launcher=launcher,
        )
        _write_text_atomic(xml_path, xml, encoding="utf-16")
        staged = {
            "schema": TASK_STAGE_SCHEMA,
            "identity": identity,
            "identity_digest": identity_digest,
            "probe_id": probe_id,
            "registration_generation": registration_generation,
            "task_definition_digest": task_definition_digest,
            "xml_path": str(xml_path),
            "xml_sha256": hashlib.sha256(xml.encode("utf-8")).hexdigest(),
        }
    operational = _query_task_scheduler_operational_log()
    registered = _verify_registered_task_definition(xml)
    current = bool(operational["enabled"] and registered["matches"])
    elevated = is_elevated_windows_process()
    staged.update(
        {
            "state": (
                "current"
                if current
                else "ready-to-apply" if elevated else "admin-required"
            ),
            "elevated": elevated,
            "operational_log": operational,
            "registered_definition": registered,
            "required_action": (
                "none"
                if current
                else "run scripts/install_v4_windows_bootstrap.ps1 elevated"
            ),
        }
    )
    write_json_atomic(stage_path, staged, ensure_ascii=True, sort_keys=True)
    return staged


def install_resident_task(
    root: Path,
    *,
    config: Path,
    interval_seconds: float,
    python_executable: Path | None = None,
) -> dict[str, Any]:
    if os.name != "nt":
        raise ResidentWatchdogError("resident task installation requires Windows")
    if sys.platform == "win32" and not is_elevated_windows_process():
        raise ResidentWatchdogError(
            "bootstrap-admin-required: SYSTEM task registration and Task Scheduler "
            "Operational logging require an elevated process"
        )
    root = root.resolve()
    runtime = root / ".ascendop-work" / "runtime"
    staged = stage_resident_task_installation(
        root,
        config=config,
        interval_seconds=interval_seconds,
        python_executable=python_executable,
    )
    probe_id = str(staged["probe_id"])
    registration_generation = str(staged["registration_generation"])
    task_definition_digest = str(staged["task_definition_digest"])
    xml_path = Path(str(staged["xml_path"]))
    xml = xml_path.read_text(encoding="utf-16")
    launcher = _active_launcher(root)
    logs = runtime / "logs"
    ended = end_active_resident_task()
    operational = _ensure_task_scheduler_operational_log()
    if not operational["enabled"]:
        raise ResidentWatchdogError(
            "Task Scheduler Operational logging is unavailable"
        )
    completed = _run_schtasks(
        [
            "schtasks.exe",
            "/Create",
            "/TN",
            RESIDENT_TASK_NAME,
            "/XML",
            str(xml_path),
            "/F",
        ],
        timeout=30,
    )
    if completed.returncode != 0:
        raise ResidentWatchdogError(
            f"resident task installation failed: {completed.stderr.strip()}"
        )
    registered_definition = _verify_registered_task_definition(xml)
    if not registered_definition["matches"]:
        retired = _retire_task(RESIDENT_TASK_NAME)
        raise ResidentWatchdogError(
            "registered resident bootstrap task definition does not match "
            f"the immutable release contract: {retired}"
        )
    enabled = _enable_task(RESIDENT_TASK_NAME)
    if enabled["returncode"] != 0:
        raise ResidentWatchdogError(
            "resident task was created but could not be enabled: "
            f"{enabled['stderr']}"
        )
    run_now = _run_schtasks(
        ["schtasks.exe", "/Run", "/TN", RESIDENT_TASK_NAME],
        timeout=10,
    )
    probe = _wait_for_bootstrap_terminal(
        runtime / "latest-windows-bootstrap-attempt.json",
        probe_id,
        registration_generation=registration_generation,
        expected_release_generation=_active_release_generation(root),
        timeout_seconds=TASK_PROBE_TIMEOUT_SECONDS,
    )
    scheduler_events = _wait_for_task_scheduler_evidence(
        RESIDENT_TASK_NAME,
        timeout_seconds=TASK_EXPECTED_SECONDS,
    )
    accepted_terminal = bool(probe) and probe.get("state") in {
        "succeeded",
        "paused",
    }
    if (
        run_now.returncode != 0
        or not accepted_terminal
        or scheduler_events["matched_event_count"] <= 0
    ):
        ended_failed_probe = _end_task(RESIDENT_TASK_NAME)
        disabled_failed_probe = _disable_task(RESIDENT_TASK_NAME)
        probe_stdout, probe_stderr = _bootstrap_log_paths(logs, probe_id)
        failed = {
            "schema": TASK_SCHEMA,
            "task_name": RESIDENT_TASK_NAME,
            "state": "probe-failed",
            "probe_id": probe_id,
            "registration_generation": registration_generation,
            "task_definition_digest": task_definition_digest,
            "probe_timeout_seconds": TASK_PROBE_TIMEOUT_SECONDS,
            "run_returncode": run_now.returncode,
            "run_stdout": run_now.stdout.strip(),
            "run_stderr": run_now.stderr.strip(),
            "probe_stdout": str(probe_stdout),
            "probe_stderr": str(probe_stderr),
            "enable_before_probe": enabled,
            "end_after_probe": ended_failed_probe,
            "disable_after_probe": disabled_failed_probe,
            "operational_log": operational,
            "registered_definition": registered_definition,
            "scheduler_events": scheduler_events,
            "bootstrap_attempt": probe,
        }
        write_json_atomic(
            runtime / "v4-resident-bootstrap-task.json",
            failed,
            ensure_ascii=True,
            sort_keys=True,
        )
        raise ResidentWatchdogError(
            "resident task action did not publish an accepted bootstrap terminal"
        )
    retired_legacy = [_retire_task(name) for name in LEGACY_RESIDENT_TASK_NAMES]
    probe_stdout, probe_stderr = _bootstrap_log_paths(logs, probe_id)
    receipt = {
        "schema": TASK_SCHEMA,
        "task_name": RESIDENT_TASK_NAME,
        "state": "installed" if probe["state"] == "succeeded" else "installed-paused",
        "xml_path": str(xml_path),
        "xml_sha256": hashlib.sha256(xml.encode("utf-8")).hexdigest(),
        "task_definition_digest": task_definition_digest,
        "registration_generation": registration_generation,
        "action_mode": "direct-python",
        "python_executable": str(_console_python(python_executable or Path(sys.executable))),
        "launcher": str(launcher),
        "probe_stdout": str(probe_stdout),
        "probe_stderr": str(probe_stderr),
        "execution_time_limit": TASK_EXECUTION_LIMIT,
        "expected_bootstrap_seconds": TASK_EXPECTED_SECONDS,
        "stale_instance_seconds": TASK_STALE_INSTANCE_SECONDS,
        "probe": probe,
        "operational_log": operational,
        "registered_definition": registered_definition,
        "staged_registration": str(runtime / TASK_STAGE_NAME),
        "scheduler_events": scheduler_events,
        "enable_before_probe": enabled,
        "previous_invocation": ended,
        "retired_legacy_tasks": retired_legacy,
    }
    write_json_atomic(
        runtime / "v4-resident-bootstrap-task.json",
        receipt,
        ensure_ascii=True,
        sort_keys=True,
    )
    return receipt


def _bootstrap_log_paths(logs: Path, probe_id: str) -> tuple[Path, Path]:
    return (
        logs / f"v4-resident-bootstrap-{probe_id}.out.log",
        logs / f"v4-resident-bootstrap-{probe_id}.err.log",
    )


def _console_python(executable: Path) -> Path:
    executable = executable.resolve()
    if executable.name.lower() == "pythonw.exe":
        executable = executable.with_name("python.exe")
    if executable.name.lower() != "python.exe" or not executable.is_file():
        raise ResidentWatchdogError(
            f"a concrete Windows console Python is required: {executable}"
        )
    return executable


def _active_launcher(root: Path) -> Path:
    active_path = root / ".ascendop-work" / "runtime" / "active-release.json"
    try:
        active = json.loads(active_path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        active = {}
    generation = str(active.get("release_generation") or "") if isinstance(active, dict) else ""
    if generation:
        launcher = Path(str(active.get("daemon_launcher_path") or ""))
        if not str(active.get("daemon_launcher_path") or "") or not launcher.is_file():
            raise ResidentWatchdogError(
                "active release does not expose its immutable daemon launcher"
            )
        return launcher.resolve()
    return (root / "tools" / "tester_daemon" / "launch_s5_910b.py").resolve()


def _run_schtasks(command: list[str], *, timeout: int) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        capture_output=True,
        text=True,
        encoding=locale.getencoding(),
        errors="replace",
        timeout=timeout,
        check=False,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )


def _end_task(task_name: str) -> dict[str, Any]:
    completed = _run_schtasks(
        ["schtasks.exe", "/End", "/TN", task_name],
        timeout=10,
    )
    return {
        "task_name": task_name,
        "returncode": completed.returncode,
        "stdout": completed.stdout.strip(),
        "stderr": completed.stderr.strip(),
    }


def _retire_task(task_name: str) -> dict[str, Any]:
    ended = _end_task(task_name)
    deleted = _run_schtasks(
        ["schtasks.exe", "/Delete", "/TN", task_name, "/F"],
        timeout=10,
    )
    return {
        "task_name": task_name,
        "end_returncode": ended["returncode"],
        "delete_returncode": deleted.returncode,
        "delete_stdout": deleted.stdout.strip(),
        "delete_stderr": deleted.stderr.strip(),
    }


def _disable_task(task_name: str) -> dict[str, Any]:
    completed = _run_schtasks(
        ["schtasks.exe", "/Change", "/TN", task_name, "/Disable"],
        timeout=10,
    )
    return {
        "task_name": task_name,
        "returncode": completed.returncode,
        "stdout": completed.stdout.strip(),
        "stderr": completed.stderr.strip(),
    }


def _enable_task(task_name: str) -> dict[str, Any]:
    completed = _run_schtasks(
        ["schtasks.exe", "/Change", "/TN", task_name, "/Enable"],
        timeout=10,
    )
    return {
        "task_name": task_name,
        "returncode": completed.returncode,
        "stdout": completed.stdout.strip(),
        "stderr": completed.stderr.strip(),
    }


def _write_text_atomic(path: Path, value: str, *, encoding: str) -> None:
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding=encoding, newline="") as handle:
            handle.write(value)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, path)
    except BaseException:
        Path(temp_name).unlink(missing_ok=True)
        raise


def _wait_for_bootstrap_terminal(
    path: Path,
    probe_id: str,
    *,
    registration_generation: str,
    expected_release_generation: str,
    timeout_seconds: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + max(0.1, timeout_seconds)
    while time.monotonic() < deadline:
        try:
            value = json.loads(path.read_text(encoding="utf-8-sig"))
        except (OSError, json.JSONDecodeError):
            value = {}
        if (
            isinstance(value, dict)
            and value.get("schema") == "ascendop.windows-bootstrap-attempt.v1"
            and value.get("probe_id") == probe_id
            and value.get("registration_generation") == registration_generation
        ):
            state = str(value.get("state") or "")
            if state in {"failed", "quarantined", "paused"}:
                return value
            if state == "succeeded" and _bootstrap_ready(
                value,
                expected_release_generation=expected_release_generation,
            ):
                return value
        time.sleep(0.1)
    return {}


def _bootstrap_ready(
    value: dict[str, Any],
    *,
    expected_release_generation: str,
) -> bool:
    boundaries = value.get("boundaries")
    if not isinstance(boundaries, list):
        return False
    observed = {
        str(row.get("boundary") or "")
        for row in boundaries
        if isinstance(row, dict) and row.get("state") == "observed"
    }
    required = {
        "demand_accepted",
        "task_instance_visible",
        "action_started",
        "python_entered",
        "generation_verified",
        "child_adopted_or_started",
        "resident_probe_ready",
        "bootstrap_terminal",
    }
    return required <= observed and (
        not expected_release_generation
        or value.get("release_generation") == expected_release_generation
    )


def _active_release_generation(root: Path) -> str:
    try:
        value = json.loads(
            (root / ".ascendop-work" / "runtime" / "active-release.json").read_text(
                encoding="utf-8-sig"
            )
        )
    except (OSError, json.JSONDecodeError):
        return "development"
    if not isinstance(value, dict):
        return "development"
    return str(value.get("release_generation") or "development")


def _query_task_scheduler_operational_log() -> dict[str, Any]:
    query = _run_schtasks(
        ["wevtutil.exe", "gli", TASK_OPERATIONAL_CHANNEL],
        timeout=10,
    )
    return {
        "channel": TASK_OPERATIONAL_CHANNEL,
        "enabled": query.returncode == 0 and _operational_enabled(query.stdout),
        "query_returncode": query.returncode,
        "query_stdout": query.stdout.strip(),
        "query_stderr": query.stderr.strip(),
    }


def _ensure_task_scheduler_operational_log() -> dict[str, Any]:
    before = _query_task_scheduler_operational_log()
    enabled_before = bool(before["enabled"])
    query = None
    enable = None
    if before["query_returncode"] == 0 and not enabled_before:
        enable = _run_schtasks(
            ["wevtutil.exe", "sl", TASK_OPERATIONAL_CHANNEL, "/e:true"],
            timeout=10,
        )
        query = _run_schtasks(
            ["wevtutil.exe", "gli", TASK_OPERATIONAL_CHANNEL],
            timeout=10,
        )
    after = before if query is None else {
        "channel": TASK_OPERATIONAL_CHANNEL,
        "enabled": query.returncode == 0 and _operational_enabled(query.stdout),
        "query_returncode": query.returncode,
        "query_stdout": query.stdout.strip(),
        "query_stderr": query.stderr.strip(),
    }
    return {
        "channel": TASK_OPERATIONAL_CHANNEL,
        "enabled": after["enabled"],
        "enabled_before": enabled_before,
        "query_returncode": after["query_returncode"],
        "query_stdout": after["query_stdout"],
        "query_stderr": after["query_stderr"],
        "enable_returncode": None if enable is None else enable.returncode,
        "enable_stdout": "" if enable is None else enable.stdout.strip(),
        "enable_stderr": "" if enable is None else enable.stderr.strip(),
    }


def _operational_enabled(output: str) -> bool:
    normalized = " ".join(str(output or "").lower().split())
    return "enabled: true" in normalized or "enabled:true" in normalized


def _verify_registered_task_definition(expected_xml: str) -> dict[str, Any]:
    query = _run_schtasks(
        ["schtasks.exe", "/Query", "/TN", RESIDENT_TASK_NAME, "/XML"],
        timeout=15,
    )
    expected_digest = _canonical_xml_digest(expected_xml)
    observed_digest = (
        _canonical_xml_digest(query.stdout) if query.returncode == 0 else ""
    )
    return {
        "task_name": RESIDENT_TASK_NAME,
        "matches": bool(observed_digest) and observed_digest == expected_digest,
        "query_returncode": query.returncode,
        "query_stderr": query.stderr.strip(),
        "expected_canonical_sha256": expected_digest,
        "observed_canonical_sha256": observed_digest,
    }


def _canonical_xml_digest(value: str) -> str:
    ET.register_namespace("", TASK_NAMESPACE)
    try:
        root = ET.fromstring(str(value or "").lstrip("\ufeff\r\n \t"))
    except ET.ParseError:
        return ""
    payload = ET.tostring(root, encoding="utf-8")
    return hashlib.sha256(payload).hexdigest()


def _wait_for_task_scheduler_evidence(
    task_name: str,
    *,
    timeout_seconds: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + max(0.1, timeout_seconds)
    latest = {
        "channel": TASK_OPERATIONAL_CHANNEL,
        "task_name": task_name,
        "query_returncode": 1,
        "matched_event_count": 0,
        "payload_sha256": "",
        "query_stderr": "not queried",
    }
    while time.monotonic() < deadline:
        query = _run_schtasks(
            [
                "wevtutil.exe",
                "qe",
                TASK_OPERATIONAL_CHANNEL,
                "/f:xml",
                "/rd:true",
                "/c:100",
            ],
            timeout=15,
        )
        payload = query.stdout
        matched = payload.count(task_name)
        latest = {
            "channel": TASK_OPERATIONAL_CHANNEL,
            "task_name": task_name,
            "query_returncode": query.returncode,
            "matched_event_count": matched,
            "payload_sha256": (
                hashlib.sha256(payload.encode("utf-8")).hexdigest()
                if payload
                else ""
            ),
            "query_stderr": query.stderr.strip(),
        }
        if query.returncode == 0 and matched > 0:
            return latest
        time.sleep(0.2)
    return latest
