from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import posixpath
import re
import shlex
import subprocess
import tarfile
import tempfile
from pathlib import Path
from typing import Any

from limited_remote_partner.core.config import AppConfig, load_config
from limited_remote_partner.core.process_utils import (
    hidden_subprocess_kwargs,
    matching_partner_role_pids,
    matching_partner_role_processes,
    process_start_token,
)
from limited_remote_partner.gateway.relay import RelayError, ScpTransport


DEFAULT_SYNC_PATHS = (
    "src",
    "scripts",
    "configs",
    "services",
    "pyproject.toml",
    "README.md",
    "docs",
)

CANN90_MEDIA_PROFILE = "cann90-910b-media"
CANN90_MEDIA_DIR = "cann-9.0.0-aarch64"
CANN90_MEDIA_FILES = {
    "Ascend-cann-toolkit_9.0.0_linux-aarch64.run": 1223256884,
    "Ascend-cann-910b-ops_9.0.0_linux-aarch64.run": 2294817130,
}


def main() -> None:
    args = _parse_args()
    config_path = Path(args.config).resolve()
    config = load_config(config_path)
    target_role = args.target_role
    target_host = args.target_host or _default_target_host(config, target_role)
    target_dir = args.target_dir or _default_target_dir(config, target_role)
    if not target_host:
        raise SystemExit(
            "target host is empty; pass --target-host or use a config whose "
            "relay peer host is set"
        )
    if not target_dir:
        raise SystemExit(
            "target dir cannot be inferred; pass --target-dir explicitly"
        )

    transport = ScpTransport(config)
    if args.action == "sync-code":
        sync_code(config, transport, target_host, target_dir, args)
        return
    if args.action == "restart-service":
        restart_service(transport, target_host, target_dir, target_role, args)
        return
    if args.action == "cancel-request":
        cancel_request(transport, target_host, target_dir, target_role, args)
        return
    if args.action == "endpoint-runtime":
        report = endpoint_runtime_command(
            transport,
            target_host,
            target_dir,
            target_role,
            args,
        )
        print(json.dumps(report, ensure_ascii=True, indent=2, sort_keys=True))
        return
    if args.action == "bootstrap":
        sync_code(config, transport, target_host, target_dir, args)
        restart_service(transport, target_host, target_dir, target_role, args)
        return
    raise SystemExit(f"unsupported action: {args.action}")


def sync_code(
    config: AppConfig,
    transport: ScpTransport,
    target_host: str,
    target_dir: str,
    args: argparse.Namespace,
) -> dict[str, Any]:
    staging_dir = args.remote_staging_dir or f"{target_dir.rstrip('/')}/work/lan_ops/incoming"
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_root = Path(temp_dir)
        archive_path = temp_root / "gitpartner-code.tgz"
        build_code_archive(config.repo_dir, archive_path, tuple(args.sync_path))
        transport.push_file(
            archive_path,
            target_host,
            f"{staging_dir.rstrip('/')}/gitpartner-code.tgz",
        )
    script = "\n".join(
        [
            "set -euo pipefail",
            f"mkdir -p {shlex.quote(target_dir)}",
            f"tar -xzf {shlex.quote(staging_dir.rstrip('/') + '/gitpartner-code.tgz')} "
            f"-C {shlex.quote(target_dir)}",
            f"cd {shlex.quote(target_dir)}",
            "test -f pyproject.toml",
            "test -d src/limited_remote_partner",
            "echo GITPARTNER_LAN_SYNC_OK",
        ]
    )
    result = transport.run_ssh_shell(target_host, script)
    return _process_report("sync-code", result)


def sync_artifact_profile(
    transport: ScpTransport,
    target_host: str,
    target_dir: str,
    target_role: str,
    args: argparse.Namespace,
    request_id: str,
) -> dict[str, Any]:
    profile = str(getattr(args, "artifact_profile", "") or "")
    if profile != CANN90_MEDIA_PROFILE:
        raise RelayError(f"unsupported artifact profile: {profile}")
    if target_role != "client" or not target_host:
        raise RelayError(
            "lan-sync-artifact requires a registered remote client target"
        )

    source = Path.home() / "AscendOP" / "packages" / CANN90_MEDIA_DIR
    manifest_path = source / "manifest.json"
    if not manifest_path.is_file():
        raise RelayError(f"artifact manifest is missing: {manifest_path}")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RelayError(f"invalid artifact manifest: {exc}") from exc
    raw_artifacts = manifest.get("artifacts")
    if not isinstance(raw_artifacts, list):
        raise RelayError("artifact manifest has no artifacts list")
    by_name = {
        str(item.get("name") or ""): item
        for item in raw_artifacts
        if isinstance(item, dict)
    }
    if set(by_name) != set(CANN90_MEDIA_FILES):
        raise RelayError("artifact manifest file set does not match the profile")
    for name, expected_size in CANN90_MEDIA_FILES.items():
        path = source / name
        item = by_name[name]
        expected_digest = str(item.get("sha256") or "")
        if (
            not path.is_file()
            or path.stat().st_size != expected_size
            or int(item.get("size_bytes", -1)) != expected_size
            or len(expected_digest) != 64
        ):
            raise RelayError(f"artifact metadata mismatch: {name}")
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
                digest.update(chunk)
        if digest.hexdigest() != expected_digest:
            raise RelayError(f"artifact checksum mismatch: {name}")

    package_parent = posixpath.join(
        posixpath.dirname(target_dir.rstrip("/")),
        "packages",
    )
    final_dir = posixpath.join(package_parent, CANN90_MEDIA_DIR)
    incoming_dir = f"{final_dir}.incoming-{request_id}"
    prepare = "\n".join(
        [
            "set -euo pipefail",
            f"FINAL={shlex.quote(final_dir)}",
            f"INCOMING={shlex.quote(incoming_dir)}",
            'mkdir -p "$(dirname "$FINAL")"',
            'rm -rf "$INCOMING"',
            'mkdir -p "$INCOMING"',
            'AVAILABLE_KIB="$(df -Pk "$(dirname "$FINAL")" | '
            "awk 'NR==2 {print $4}')\"",
            'test -n "$AVAILABLE_KIB"',
            'test "$AVAILABLE_KIB" -ge 5242880',
            'echo "CANN90_REMOTE_SPACE_OK available_kib=$AVAILABLE_KIB"',
        ]
    )
    transport.run_ssh_shell(target_host, prepare)
    transport.push_dir(
        source,
        target_host,
        incoming_dir,
        timeout_seconds=3600,
    )
    expected_lines = [
        f"    {name!r}: {size},"
        for name, size in CANN90_MEDIA_FILES.items()
    ]
    verify = "\n".join(
        [
            "set -euo pipefail",
            f"FINAL={shlex.quote(final_dir)}",
            f"INCOMING={shlex.quote(incoming_dir)}",
            'python3 - "$INCOMING" <<\'PY\'',
            "import hashlib, json, pathlib, sys",
            "root = pathlib.Path(sys.argv[1])",
            "manifest = json.loads((root / 'manifest.json').read_text(encoding='utf-8'))",
            "expected = {",
            *expected_lines,
            "}",
            "items = {str(x['name']): x for x in manifest['artifacts']}",
            "assert set(items) == set(expected)",
            "for name, size in expected.items():",
            "    path = root / name",
            "    assert path.stat().st_size == size",
            "    digest = hashlib.sha256()",
            "    with path.open('rb') as stream:",
            "        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b''):",
            "            digest.update(chunk)",
            "    assert digest.hexdigest() == str(items[name]['sha256'])",
            "print('CANN90_REMOTE_HASH_OK')",
            "PY",
            'OLD="${FINAL}.previous-${RANDOM}"',
            'rm -rf "$OLD"',
            'if [ -e "$FINAL" ]; then mv "$FINAL" "$OLD"; fi',
            'mv "$INCOMING" "$FINAL"',
            'rm -rf "$OLD"',
            'echo "CANN90_ARTIFACT_SYNC_COMPLETE destination=$FINAL"',
        ]
    )
    result = transport.run_ssh_shell(target_host, verify)
    report = _process_report("sync-artifact", result)
    report["artifact_profile"] = profile
    report["destination"] = final_dir
    return report


def inspect_artifact_profile(
    target_dir: str,
    args: argparse.Namespace,
) -> dict[str, Any]:
    profile = str(getattr(args, "artifact_profile", "") or "")
    if profile != CANN90_MEDIA_PROFILE:
        raise RelayError(f"unsupported artifact profile: {profile}")

    source = Path.home() / "AscendOP" / "packages" / CANN90_MEDIA_DIR
    files: list[dict[str, Any]] = []
    for name in CANN90_MEDIA_FILES:
        for suffix in ("", ".part"):
            path = source / f"{name}{suffix}"
            if path.is_file():
                files.append(
                    {
                        "name": path.name,
                        "size_bytes": path.stat().st_size,
                        "complete_name": not suffix,
                    }
                )

    manifest_path = source / "manifest.json"
    manifest: dict[str, Any] | None = None
    manifest_error = ""
    if manifest_path.is_file():
        try:
            raw_manifest = json.loads(
                manifest_path.read_text(encoding="utf-8")
            )
            manifest = (
                raw_manifest if isinstance(raw_manifest, dict) else None
            )
            if manifest is None:
                manifest_error = "manifest root is not an object"
        except (OSError, json.JSONDecodeError) as exc:
            manifest_error = f"{type(exc).__name__}: {exc}"

    process_scan = subprocess.run(
        ["ps", "-eo", "pid=,ppid=,etime=,stat=,args="],
        text=True,
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        **hidden_subprocess_kwargs(),
    )
    processes = [
        line.strip()
        for line in process_scan.stdout.splitlines()
        if (
            "stage_cann90_media_on_gateway.sh" in line
            or (
                "curl " in line
                and "Ascend-cann-" in line
                and "9.0.0" in line
            )
        )
    ]

    stage_request = str(
        getattr(args, "diagnose_request_id", "") or ""
    ).strip()
    tmux_status: dict[str, Any] | None = None
    tmux_log_tail = ""
    if stage_request:
        tmux_root = (
            Path(target_dir)
            / "work"
            / "gitpartner_tmux"
            / stage_request
        )
        status_path = tmux_root / "tmux_status.json"
        if status_path.is_file():
            try:
                raw_status = json.loads(
                    status_path.read_text(encoding="utf-8")
                )
                tmux_status = (
                    raw_status if isinstance(raw_status, dict) else None
                )
            except (OSError, json.JSONDecodeError):
                tmux_status = None
        log_path = tmux_root / "tmux_command.log"
        if log_path.is_file():
            tmux_log_tail = "\n".join(
                log_path.read_text(
                    encoding="utf-8",
                    errors="replace",
                ).splitlines()[-80:]
            )[-12000:]

    return {
        "step": "inspect-artifact",
        "artifact_profile": profile,
        "source": str(source),
        "manifest_present": manifest_path.is_file(),
        "manifest_error": manifest_error,
        "manifest": manifest,
        "files": files,
        "processes": processes,
        "stage_request_id": stage_request,
        "tmux_status": tmux_status,
        "tmux_log_tail": tmux_log_tail,
    }


def restart_service(
    transport: ScpTransport,
    target_host: str,
    target_dir: str,
    target_role: str,
    args: argparse.Namespace,
) -> dict[str, Any]:
    service_name = args.service_name or f"git-partner-{target_role}"
    script = build_restart_script(
        target_dir=target_dir,
        target_role=target_role,
        service_name=service_name,
        config_path=args.remote_config,
        allow_process_fallback=not args.no_process_fallback,
        cleanup_request_id=getattr(args, "cleanup_request_id", ""),
    )
    result = transport.run_ssh_shell(target_host, script)
    return _process_report("restart-service", result)


def write_node_ack(
    transport: ScpTransport,
    target_host: str,
    target_dir: str,
    target_role: str,
    args: argparse.Namespace,
) -> dict[str, Any]:
    if target_role != "client":
        raise RelayError("lan-node-ack only supports the client role")
    try:
        ack = json.loads(str(args.node_ack_json or ""))
    except json.JSONDecodeError as exc:
        raise RelayError(f"invalid node acknowledgement JSON: {exc}") from exc
    if not isinstance(ack, dict):
        raise RelayError("node acknowledgement must be an object")
    if (
        ack.get("schema") != "ascendop.node-ack.v1"
        or ack.get("state") != "accepted"
    ):
        raise RelayError("node acknowledgement must be an accepted v1 object")
    for key in ("node_id", "endpoint_id", "generation", "session_id"):
        if not str(ack.get(key) or ""):
            raise RelayError(f"node acknowledgement is missing {key}")
    encoded = base64.b64encode(
        json.dumps(
            ack,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).decode("ascii")
    script = "\n".join(
        [
            "set -euo pipefail",
            f"cd {shlex.quote(target_dir)}",
            f"ACK_B64={shlex.quote(encoded)}",
            f"REMOTE_CONFIG={shlex.quote(str(args.remote_config))}",
            "export ACK_B64",
            "export REMOTE_CONFIG",
            "python3 - <<'PY'",
            "import base64, json, os, tempfile",
            "from pathlib import Path",
            "ack = json.loads(base64.b64decode(os.environ['ACK_B64']).decode('utf-8'))",
            "config_path = Path(os.environ['REMOTE_CONFIG'])",
            "config = json.loads(config_path.read_text(encoding='utf-8-sig'))",
            "if config.get('node', {}).get('node_id') != ack['node_id']:",
            "    raise SystemExit('NODE_ACK_NODE_MISMATCH')",
            "if config.get('endpoint', {}).get('endpoint_id') != ack['endpoint_id']:",
            "    raise SystemExit('NODE_ACK_ENDPOINT_MISMATCH')",
            "if config.get('endpoint', {}).get('generation') != ack['generation']:",
            "    raise SystemExit('NODE_ACK_GENERATION_MISMATCH')",
            "state_dir = Path(config.get('io', {}).get('state_dir', ''))",
            "if not str(state_dir) or state_dir.is_absolute() or '..' in state_dir.parts:",
            "    raise SystemExit('NODE_ACK_STATE_DIR_INVALID')",
            "target = state_dir / 'central_ack.json'",
            "target.parent.mkdir(parents=True, exist_ok=True)",
            "current = {}",
            "try:",
            "    current = json.loads(target.read_text(encoding='utf-8-sig'))",
            "except (OSError, json.JSONDecodeError):",
            "    pass",
            "if current and current != ack:",
            "    for key in ('node_id', 'endpoint_id', 'generation'):",
            "        if current.get(key) != ack.get(key):",
            "            raise SystemExit('NODE_ACK_IDENTITY_CONFLICT:' + key)",
            "    if current.get('session_id') != ack.get('session_id') and str(current.get('accepted_at', '')) >= str(ack.get('accepted_at', '')):",
            "        raise SystemExit('NODE_ACK_SESSION_REPLAY')",
            "fd, name = tempfile.mkstemp(prefix='.central_ack.', suffix='.tmp', dir=str(target.parent))",
            "try:",
            "    with os.fdopen(fd, 'w', encoding='utf-8') as handle:",
            "        json.dump(ack, handle, ensure_ascii=True, indent=2, sort_keys=True)",
            "        handle.write('\\n')",
            "        handle.flush()",
            "        os.fsync(handle.fileno())",
            "    os.replace(name, target)",
            "finally:",
            "    try:",
            "        os.unlink(name)",
            "    except FileNotFoundError:",
            "        pass",
            "print('GITPARTNER_LAN_NODE_ACK_OK node_id=' + ack['node_id'] + ' session_id=' + ack['session_id'])",
            "PY",
        ]
    )
    result = transport.run_ssh_shell(target_host, script)
    return _process_report("node-ack", result)


def apply_local_node_ack(
    config_path: Path,
    node_ack: str | dict[str, Any],
) -> dict[str, Any]:
    try:
        ack = json.loads(node_ack) if isinstance(node_ack, str) else dict(node_ack)
    except (json.JSONDecodeError, TypeError, ValueError) as exc:
        raise RelayError(f"invalid node acknowledgement JSON: {exc}") from exc
    if (
        ack.get("schema") != "ascendop.node-ack.v1"
        or ack.get("state") != "accepted"
    ):
        raise RelayError("node acknowledgement must be an accepted v1 object")
    for key in ("node_id", "endpoint_id", "generation", "session_id"):
        if not str(ack.get(key) or ""):
            raise RelayError(f"node acknowledgement is missing {key}")

    config = load_config(config_path.resolve())
    expected = {
        "node_id": config.node.node_id,
        "endpoint_id": config.endpoint.endpoint_id,
        "generation": config.endpoint.generation,
    }
    for key, value in expected.items():
        if ack.get(key) != value:
            raise RelayError(f"node acknowledgement {key} mismatch")

    target = config.repo_dir / config.io.state_dir / "central_ack.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    current: dict[str, Any] = {}
    try:
        loaded = json.loads(target.read_text(encoding="utf-8-sig"))
        if isinstance(loaded, dict):
            current = loaded
    except (OSError, json.JSONDecodeError):
        pass
    if current and current != ack:
        for key in ("node_id", "endpoint_id", "generation"):
            if current.get(key) != ack.get(key):
                raise RelayError(f"node acknowledgement identity conflict: {key}")
        if (
            current.get("session_id") != ack.get("session_id")
            and str(current.get("accepted_at") or "")
            >= str(ack.get("accepted_at") or "")
        ):
            raise RelayError("node acknowledgement session replay")

    descriptor, temporary_name = tempfile.mkstemp(
        prefix=".central_ack.",
        suffix=".tmp",
        dir=str(target.parent),
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(ack, handle, ensure_ascii=True, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, target)
    finally:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
    return {
        "action": "node-ack",
        "node_id": ack["node_id"],
        "endpoint_id": ack["endpoint_id"],
        "generation": ack["generation"],
        "session_id": ack["session_id"],
        "ack_path": str(target),
        "idempotent": current == ack,
    }


def schedule_local_restart_service(
    transport: ScpTransport,
    target_dir: str,
    target_role: str,
    args: argparse.Namespace,
    request_id: str,
    *,
    delay_seconds: int = 60,
) -> dict[str, Any]:
    service_name = args.service_name or f"git-partner-{target_role}"
    safe_request_id = "".join(
        char if char.isalnum() or char in "-_." else "_" for char in request_id
    )
    resident_pids = matching_partner_role_pids(
        target_role,
        repo_dir=Path(target_dir),
    )
    if not resident_pids:
        raise RelayError(
            "refusing scheduled restart without a resident "
            f"GitPartner {target_role} process; found {resident_pids}"
        )
    identity_parts = [(pid, process_start_token(pid)) for pid in resident_pids]
    if any(not token for _pid, token in identity_parts):
        raise RelayError(
            "refusing scheduled restart without stable process start tokens: "
            f"{identity_parts}"
        )
    expected_identities = " ".join(
        f"{pid}:{token}" for pid, token in identity_parts
    )
    work_dir = Path(target_dir) / "work" / "lan_ops"
    work_dir.mkdir(parents=True, exist_ok=True)
    script_path = work_dir / f"restart-after-sync-{safe_request_id}.sh"
    log_path = work_dir / f"restart-after-sync-{safe_request_id}.log"
    intent_path = work_dir / f"restart-{target_role}.intent"
    intent_tmp = work_dir / f".{intent_path.name}.{safe_request_id}.tmp"
    intent_tmp.write_text(request_id + "\n", encoding="utf-8")
    intent_tmp.replace(intent_path)
    restart_script = build_restart_script(
        target_dir=target_dir,
        target_role=target_role,
        service_name=service_name,
        config_path=args.remote_config,
        allow_process_fallback=not args.no_process_fallback,
        cleanup_request_id=getattr(args, "cleanup_request_id", ""),
    )
    script_path.write_text(
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        f"sleep {max(1, int(delay_seconds))}\n"
        f"INTENT={shlex.quote(str(intent_path))}\n"
        f"TOKEN={shlex.quote(request_id)}\n"
        f"EXPECTED_IDENTITIES={shlex.quote(expected_identities)}\n"
        f"ROLE={shlex.quote(target_role)}\n"
        f"TARGET_DIR={shlex.quote(target_dir)}\n"
        "if [ ! -f \"$INTENT\" ] || [ \"$(cat \"$INTENT\")\" != \"$TOKEN\" ]; then\n"
        "  echo GITPARTNER_LAN_SELF_RESTART_ABORTED_SUPERSEDED\n"
        "  exit 0\n"
        "fi\n"
        "current_role_identities() {\n"
        "  PYTHONPATH=\"$TARGET_DIR/src${PYTHONPATH:+:$PYTHONPATH}\" python3 - \"$ROLE\" \"$TARGET_DIR\" <<'PY'\n"
        "from pathlib import Path\n"
        "import sys\n"
        "from limited_remote_partner.core.process_utils import matching_partner_role_pids, process_start_token\n"
        "print(' '.join(f'{pid}:{process_start_token(pid)}' for pid in matching_partner_role_pids(sys.argv[1], repo_dir=Path(sys.argv[2]))))\n"
        "PY\n"
        "}\n"
        "CURRENT_IDENTITIES=$(current_role_identities || true)\n"
        "if [ \"$CURRENT_IDENTITIES\" != \"$EXPECTED_IDENTITIES\" ]; then\n"
        "  echo GITPARTNER_LAN_SELF_RESTART_ABORTED_IDENTITY_SET expected=$EXPECTED_IDENTITIES current=$CURRENT_IDENTITIES\n"
        "  exit 0\n"
        "fi\n"
        f"{restart_script}\n",
        encoding="utf-8",
    )
    launcher = "\n".join(
        [
            "set -euo pipefail",
            f"SCRIPT={shlex.quote(str(script_path))}",
            f"LOG={shlex.quote(str(log_path))}",
            'chmod 700 "$SCRIPT"',
            "if command -v setsid >/dev/null 2>&1; then",
            '  nohup setsid -f bash "$SCRIPT" > "$LOG" 2>&1 < /dev/null &',
            "  mode=setsid",
            "else",
            '  nohup bash "$SCRIPT" > "$LOG" 2>&1 < /dev/null &',
            "  mode=nohup",
            "fi",
            (
                'echo "GITPARTNER_LAN_SELF_RESTART_SCHEDULED '
                f'request_id={safe_request_id} delay_seconds={max(1, int(delay_seconds))} '
                f'expected_identities={shlex.quote(expected_identities)} mode=$mode"'
            ),
        ]
    )
    result = transport.run_ssh_shell(None, launcher)
    return _process_report("restart-service-scheduled", result)


def schedule_duplicate_service_reconciliation(
    transport: ScpTransport,
    target_dir: str,
    target_role: str,
    request_id: str,
    *,
    delay_seconds: int = 30,
) -> dict[str, Any]:
    processes = matching_partner_role_processes(
        target_role,
        repo_dir=Path(target_dir),
    )
    fallback = [
        process
        for process in processes
        if "--allow-scp-relay" in process.args and "--transport" not in process.args
    ]
    alternatives = [process for process in processes if process not in fallback]
    if len(fallback) != 1 or not alternatives:
        summary = [
            {"pid": process.pid, "args": list(process.args)} for process in processes
        ]
        raise RelayError(
            "refusing duplicate reconciliation without exactly one known fallback "
            f"and at least one alternative resident: {summary}"
        )
    expected_pid = fallback[0].pid
    safe_request_id = "".join(
        char if char.isalnum() or char in "-_." else "_" for char in request_id
    )
    work_dir = Path(target_dir) / "work" / "lan_ops"
    work_dir.mkdir(parents=True, exist_ok=True)
    script_path = work_dir / f"reconcile-after-sync-{safe_request_id}.sh"
    log_path = work_dir / f"reconcile-after-sync-{safe_request_id}.log"
    intent_path = work_dir / f"reconcile-{target_role}.intent"
    intent_tmp = work_dir / f".{intent_path.name}.{safe_request_id}.tmp"
    intent_tmp.write_text(request_id + "\n", encoding="utf-8")
    intent_tmp.replace(intent_path)
    script_path.write_text(
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        f"sleep {max(1, int(delay_seconds))}\n"
        f"INTENT={shlex.quote(str(intent_path))}\n"
        f"TOKEN={shlex.quote(request_id)}\n"
        f"EXPECTED_PID={expected_pid}\n"
        f"ROLE={shlex.quote(target_role)}\n"
        "if [ ! -f \"$INTENT\" ] || [ \"$(cat \"$INTENT\")\" != \"$TOKEN\" ]; then\n"
        "  echo GITPARTNER_LAN_RECONCILE_ABORTED_SUPERSEDED\n"
        "  exit 0\n"
        "fi\n"
        "if ! kill -0 \"$EXPECTED_PID\" 2>/dev/null; then\n"
        "  echo GITPARTNER_LAN_RECONCILE_ALREADY_GONE\n"
        "  exit 0\n"
        "fi\n"
        "EXPECTED_ARGS=$(ps -o args= -p \"$EXPECTED_PID\" 2>/dev/null || true)\n"
        "case \"$EXPECTED_ARGS\" in *\"limited_remote_partner.cli.partner\"*) ;; *) echo GITPARTNER_LAN_RECONCILE_ABORTED_PID_IDENTITY; exit 0 ;; esac\n"
        "case \"$EXPECTED_ARGS\" in *\"--role $ROLE\"*) ;; *) echo GITPARTNER_LAN_RECONCILE_ABORTED_PID_IDENTITY; exit 0 ;; esac\n"
        "case \"$EXPECTED_ARGS\" in *\"--allow-scp-relay\"*) ;; *) echo GITPARTNER_LAN_RECONCILE_ABORTED_NOT_FALLBACK; exit 0 ;; esac\n"
        "OTHER_PIDS=$(ps -eo pid=,args= | awk -v expected=\"$EXPECTED_PID\" -v role=\"$ROLE\" '\n"
        "  $1 == expected { next }\n"
        "  $0 ~ /limited_remote_partner[.]partner/ && $0 ~ \"--role \" role { print $1 }\n"
        "')\n"
        "if [ -z \"$OTHER_PIDS\" ]; then\n"
        "  echo GITPARTNER_LAN_RECONCILE_ABORTED_NO_ALTERNATIVE\n"
        "  exit 0\n"
        "fi\n"
        "kill \"$EXPECTED_PID\" 2>/dev/null || true\n"
        "sleep 3\n"
        "if kill -0 \"$EXPECTED_PID\" 2>/dev/null; then kill -9 \"$EXPECTED_PID\" 2>/dev/null || true; fi\n"
        "echo GITPARTNER_LAN_RECONCILE_RETIRED pid=$EXPECTED_PID alternatives=$OTHER_PIDS\n",
        encoding="utf-8",
    )
    launcher = "\n".join(
        [
            "set -euo pipefail",
            f"SCRIPT={shlex.quote(str(script_path))}",
            f"LOG={shlex.quote(str(log_path))}",
            'chmod 700 "$SCRIPT"',
            "if command -v setsid >/dev/null 2>&1; then",
            '  nohup setsid -f bash "$SCRIPT" > "$LOG" 2>&1 < /dev/null &',
            "  mode=setsid",
            "else",
            '  nohup bash "$SCRIPT" > "$LOG" 2>&1 < /dev/null &',
            "  mode=nohup",
            "fi",
            (
                'echo "GITPARTNER_LAN_RECONCILE_SCHEDULED '
                f'request_id={safe_request_id} delay_seconds={max(1, int(delay_seconds))} '
                f'expected_pid={expected_pid} mode=$mode"'
            ),
        ]
    )
    result = transport.run_ssh_shell(None, launcher)
    return _process_report("reconcile-service-scheduled", result)


def cancel_request(
    transport: ScpTransport,
    target_host: str,
    target_dir: str,
    target_role: str,
    args: argparse.Namespace,
) -> dict[str, Any]:
    request_id = (
        getattr(args, "cancel_request_id", "")
        or getattr(args, "cleanup_request_id", "")
    )
    if not request_id:
        raise RelayError("cancel_request needs --cancel-request-id or --cleanup-request-id")
    script = build_cancel_script(
        target_dir=target_dir,
        target_role=target_role,
        request_id=request_id,
        reason=getattr(args, "cancel_reason", "") or "LAN recovery requested cancellation",
    )
    result = transport.run_ssh_shell(target_host, script)
    return _process_report("cancel-request", result)


def start_tmux_command(
    transport: ScpTransport,
    target_host: str,
    target_dir: str,
    target_role: str,
    args: argparse.Namespace,
    request_id: str,
) -> dict[str, Any]:
    if target_role not in {"server", "client"}:
        raise RelayError(
            "server-tmux-command target_role must be server or client"
        )
    script_b64 = getattr(args, "script_b64", "")
    if not script_b64:
        raise RelayError("server-tmux-command needs script_b64")
    session_name = getattr(args, "tmux_session", "") or f"gitpartner-{request_id}"
    script = build_tmux_command_launcher(
        target_dir=target_dir,
        request_id=request_id,
        session_name=session_name,
        script_b64=script_b64,
    )
    result = transport.run_ssh_shell(target_host, script)
    return _process_report("server-tmux-command", result)


def endpoint_runtime_command(
    transport: ScpTransport,
    target_host: str | None,
    target_dir: str,
    target_role: str,
    args: argparse.Namespace,
) -> dict[str, Any]:
    script = build_endpoint_runtime_script(
        target_dir=target_dir,
        target_role=target_role,
        endpoint_action=str(getattr(args, "endpoint_action", "") or ""),
        source_repo=str(getattr(args, "source_repo", "") or target_dir),
        worktree=str(getattr(args, "worktree", "") or ""),
        control_branch=str(getattr(args, "control_branch", "") or ""),
        endpoint_config=str(getattr(args, "endpoint_config", "") or ""),
        endpoint_role=str(getattr(args, "endpoint_role", "") or ""),
        endpoint_remote=str(getattr(args, "endpoint_remote", "") or "origin"),
        import_login_network_env=bool(
            getattr(args, "import_login_network_env", False)
        ),
    )
    result = transport.run_ssh_shell(target_host, script)
    report = _process_report("endpoint-runtime", result)
    try:
        runtime = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise RelayError(
            "endpoint-runtime returned invalid JSON: "
            f"{_tail(result.stdout, 2000)}"
        ) from exc
    if not isinstance(runtime, dict):
        raise RelayError("endpoint-runtime response must be a JSON object")
    service = runtime.get("service")
    if not isinstance(service, dict):
        raise RelayError("endpoint-runtime response is missing service status")
    endpoint_action = str(runtime.get("action") or "")
    running = bool(service.get("running"))
    healthy = bool(
        service.get(
            "healthy",
            running and service.get("service_state", "running") == "running",
        )
    )
    if endpoint_action in {"start", "status"} and not healthy:
        raise RelayError(
            "endpoint-runtime did not leave a healthy service running: "
            f"{runtime.get('endpoint_id', '')}"
        )
    if endpoint_action == "stop" and running:
        raise RelayError(
            "endpoint-runtime did not stop the service: "
            f"{runtime.get('endpoint_id', '')}"
        )
    report["runtime"] = runtime
    return report


def build_endpoint_runtime_script(
    *,
    target_dir: str,
    target_role: str,
    endpoint_action: str,
    source_repo: str,
    worktree: str,
    control_branch: str,
    endpoint_config: str,
    endpoint_role: str,
    endpoint_remote: str,
    import_login_network_env: bool = False,
) -> str:
    if target_role not in {"server", "client"}:
        raise RelayError("endpoint-runtime target_role must be server or client")
    if endpoint_action not in {"provision", "start", "status", "stop"}:
        raise RelayError(
            "endpoint-runtime endpoint_action must be provision, start, status, or stop"
        )
    if endpoint_role not in {"server", "client", "local"}:
        raise RelayError(
            "endpoint-runtime endpoint_role must be server, client, or local"
        )
    if not worktree:
        raise RelayError("endpoint-runtime needs worktree")
    if not endpoint_config:
        raise RelayError("endpoint-runtime needs endpoint_config")
    if not _safe_git_token(control_branch):
        raise RelayError("endpoint-runtime control_branch is unsafe")
    if not _safe_git_token(endpoint_remote):
        raise RelayError("endpoint-runtime endpoint_remote is unsafe")
    command = [
        "python3",
        "-m",
        "limited_remote_partner.endpoint.endpoint_runtime",
        endpoint_action,
        "--source-repo",
        source_repo,
        "--worktree",
        worktree,
        "--control-branch",
        control_branch,
        "--config",
        endpoint_config,
        "--role",
        endpoint_role,
        "--remote",
        endpoint_remote,
    ]
    if import_login_network_env:
        command.append("--import-login-network-env")
    quoted = " ".join(shlex.quote(item) for item in command)
    return "\n".join(
        [
            "set -euo pipefail",
            f"TARGET_DIR={shlex.quote(target_dir)}",
            f"SOURCE_REPO={shlex.quote(source_repo)}",
            'test -d "$TARGET_DIR/src/limited_remote_partner"',
            'test -d "$SOURCE_REPO/.git"',
            'export PYTHONPATH="$TARGET_DIR/src${PYTHONPATH:+:$PYTHONPATH}"',
            quoted,
        ]
    )


def _safe_git_token(value: str) -> bool:
    return bool(
        value
        and not value.startswith("-")
        and ".." not in value
        and re.fullmatch(r"[A-Za-z0-9._/-]+", value)
    )


def diagnose_peer(
    transport: ScpTransport,
    target_host: str,
    target_dir: str,
    target_role: str,
    *,
    request_id: str = "",
) -> dict[str, Any]:
    script = build_diagnose_script(
        target_dir=target_dir,
        target_role=target_role,
        request_id=request_id,
    )
    try:
        result = transport.run_ssh_shell(target_host, script)
    except Exception as exc:
        return {
            "step": "diagnose-peer",
            "reachable": False,
            "error": str(exc),
        }
    report = _process_report("diagnose-peer", result)
    report["reachable"] = True
    return report


def build_code_archive(
    repo_dir: Path,
    archive_path: Path,
    sync_paths: tuple[str, ...] = DEFAULT_SYNC_PATHS,
) -> None:
    repo_dir = repo_dir.resolve()
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(archive_path, "w:gz") as archive:
        for rel_path in sync_paths:
            normalized = rel_path.replace("\\", "/").strip("/")
            if not normalized:
                continue
            source = (repo_dir / normalized).resolve()
            if not source.exists():
                continue
            if repo_dir != source and repo_dir not in source.parents:
                raise RelayError(f"sync path escapes repository: {rel_path}")
            archive.add(source, arcname=normalized, recursive=True, filter=_tar_filter)


def build_tmux_command_launcher(
    *,
    target_dir: str,
    request_id: str,
    session_name: str,
    script_b64: str,
) -> str:
    target_dir_q = shlex.quote(target_dir)
    request_q = shlex.quote(request_id)
    session_q = shlex.quote(session_name)
    script_b64_q = shlex.quote(script_b64)
    work_dir = f"work/gitpartner_tmux/{request_id}"
    out_dir = f"output/{request_id}"
    work_dir_q = shlex.quote(work_dir)
    out_dir_q = shlex.quote(out_dir)
    return "\n".join(
        [
            "set -euo pipefail",
            f"cd {target_dir_q}",
            f"SESSION={session_q}",
            f"REQUEST_ID={request_q}",
            f"WORK_DIR={work_dir_q}",
            f"OUT_DIR={out_dir_q}",
            f"SCRIPT_B64={script_b64_q}",
            "mkdir -p \"$WORK_DIR\" \"$OUT_DIR\"",
            "COMMAND_SH=\"$WORK_DIR/command.sh\"",
            "WRAPPER_SH=\"$WORK_DIR/wrapper.sh\"",
            "python3 - \"$COMMAND_SH\" \"$SCRIPT_B64\" <<'PY'",
            "import base64, pathlib, sys",
            "path = pathlib.Path(sys.argv[1])",
            "path.write_bytes(base64.b64decode(sys.argv[2]))",
            "path.chmod(0o700)",
            "PY",
            "cat > \"$WRAPPER_SH\" <<'SH'",
            "#!/usr/bin/env bash",
            "set +e",
            f"cd {target_dir_q} || exit 1",
            f"REQUEST_ID={request_q}",
            f"WORK_DIR={work_dir_q}",
            f"OUT_DIR={out_dir_q}",
            "COMMAND_SH=\"$WORK_DIR/command.sh\"",
            "LOG=\"$WORK_DIR/tmux_command.log\"",
            "STATUS=\"$WORK_DIR/tmux_status.json\"",
            "mkdir -p \"$WORK_DIR\" \"$OUT_DIR\"",
            "STARTED=\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"",
            "{",
            "  echo \"GITPARTNER_TMUX_COMMAND_START request_id=$REQUEST_ID\"",
            "  echo \"GITPARTNER_TMUX_LAUNCH_DELAY_SECONDS=8\"",
            "  sleep 8",
            "  bash \"$COMMAND_SH\"",
            "  RC=$?",
            "  echo \"GITPARTNER_TMUX_COMMAND_EXIT=$RC\"",
            "} > \"$LOG\" 2>&1",
            "FINISHED=\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"",
            "python3 - \"$STATUS\" \"$REQUEST_ID\" \"$RC\" \"$STARTED\" \"$FINISHED\" <<'PY'",
            "import json, sys",
            "path, request_id, rc, started, finished = sys.argv[1:6]",
            "exit_code = int(rc)",
            "payload = {",
            "    'state': 'success' if exit_code == 0 else 'failed',",
            "    'transport': 'server-local-tmux',",
            "    'request_id': request_id,",
            "    'exit_code': exit_code,",
            "    'started_at': started,",
            "    'finished_at': finished,",
            "    'log': 'tmux_command.log',",
            "}",
            "open(path, 'w', encoding='utf-8').write(json.dumps(payload, ensure_ascii=False, indent=2) + '\\n')",
            "PY",
            "cp \"$LOG\" \"$OUT_DIR/tmux_command.log\"",
            "cp \"$STATUS\" \"$OUT_DIR/tmux_status.json\"",
            "if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then",
            "  git add \"$OUT_DIR/tmux_command.log\" \"$OUT_DIR/tmux_status.json\" || true",
            "  if ! git diff --cached --quiet; then",
            "    if [ -f scripts/git_with_token.sh ]; then",
            "      bash scripts/git_with_token.sh --token-file api.txt -- commit -m \"git_partner_tmux_result: $REQUEST_ID\" || true",
            "      bash scripts/git_with_token.sh --token-file api.txt -- push origin main || true",
            "    else",
            "      git -c core.fsmonitor=false commit -m \"git_partner_tmux_result: $REQUEST_ID\" || true",
            "      git -c core.fsmonitor=false push origin main || true",
            "    fi",
            "  fi",
            "fi",
            "SH",
            "chmod +x \"$WRAPPER_SH\"",
            "if command -v tmux >/dev/null 2>&1; then",
            "  tmux has-session -t \"$SESSION\" 2>/dev/null && tmux kill-session -t \"$SESSION\" || true",
            "  tmux new-session -d -s \"$SESSION\" \"bash \\\"$WRAPPER_SH\\\"\"",
            "  echo \"GITPARTNER_SERVER_TMUX_STARTED session=$SESSION request_id=$REQUEST_ID\"",
            "  tmux ls | grep -F \"$SESSION\" || true",
            "else",
            "  nohup bash \"$WRAPPER_SH\" </dev/null >/dev/null 2>&1 &",
            "  BACKGROUND_PID=$!",
            "  echo \"GITPARTNER_SERVER_BACKGROUND_STARTED pid=$BACKGROUND_PID request_id=$REQUEST_ID\"",
            "fi",
        ]
    )


def build_restart_script(
    *,
    target_dir: str,
    target_role: str,
    service_name: str,
    config_path: str,
    allow_process_fallback: bool,
    cleanup_request_id: str = "",
) -> str:
    role_q = shlex.quote(target_role)
    service_q = shlex.quote(service_name)
    target_dir_q = shlex.quote(target_dir)
    config_q = shlex.quote(config_path)
    cleanup_request_q = shlex.quote(cleanup_request_id)
    service_file = f"services/git-partner-{target_role}.service"
    service_file_q = shlex.quote(service_file)
    fallback_lines = [
        "mkdir -p work/logs",
        f"LOG=work/logs/git-partner-{target_role}.log",
        f"ROLE={role_q}",
        f"TARGET_DIR={target_dir_q}",
        f"CLEANUP_REQUEST_ID={cleanup_request_q}",
        "export PYTHONPATH=\"$TARGET_DIR/src${PYTHONPATH:+:$PYTHONPATH}\"",
        f"CONFIG={config_q}",
        "NETWORK_EXPORTS=$(python3 -m limited_remote_partner.core.login_environment shell-exports --config \"$CONFIG\")",
        "if [ -n \"$NETWORK_EXPORTS\" ]; then eval \"$NETWORK_EXPORTS\"; fi",
        "python3 -m limited_remote_partner.core.login_environment report --config \"$CONFIG\"",
        "gitpartner_role_pids() {",
        "  PYTHONPATH=\"$TARGET_DIR/src${PYTHONPATH:+:$PYTHONPATH}\" python3 - \"$ROLE\" \"$TARGET_DIR\" <<'PY'",
        "from pathlib import Path",
        "import sys",
        "from limited_remote_partner.core.process_utils import matching_partner_role_pids",
        "for pid in matching_partner_role_pids(sys.argv[1], repo_dir=Path(sys.argv[2])):",
        "    print(pid)",
        "PY",
        "}",
        "gitpartner_role_identities() {",
        "  PYTHONPATH=\"$TARGET_DIR/src${PYTHONPATH:+:$PYTHONPATH}\" python3 - \"$ROLE\" \"$TARGET_DIR\" <<'PY'",
        "from pathlib import Path",
        "import sys",
        "from limited_remote_partner.core.process_utils import matching_partner_role_processes_for_repo_identity, process_start_token",
        "for process in matching_partner_role_processes_for_repo_identity(sys.argv[1], Path(sys.argv[2])):",
        "    print(f'{process.pid}:{process_start_token(process.pid)}')",
        "PY",
        "}",
        "gitpartner_legacy_identities() {",
        "  PYTHONPATH=\"$TARGET_DIR/src${PYTHONPATH:+:$PYTHONPATH}\" python3 - \"$TARGET_DIR\" <<'PY'",
        "from pathlib import Path",
        "import sys",
        "from limited_remote_partner.core.process_utils import matching_legacy_partner_processes_for_repo_identity, process_start_token",
        "for process in matching_legacy_partner_processes_for_repo_identity(Path(sys.argv[1])):",
        "    print(f'{process.pid}:{process_start_token(process.pid)}')",
        "PY",
        "}",
        "gitpartner_pid_start_token() {",
        "  PYTHONPATH=\"$TARGET_DIR/src${PYTHONPATH:+:$PYTHONPATH}\" python3 - \"$1\" <<'PY'",
        "import sys",
        "from limited_remote_partner.core.process_utils import process_start_token",
        "print(process_start_token(int(sys.argv[1])))",
        "PY",
        "}",
        *_exact_status_signal_helper_lines(),
        "echo GITPARTNER_LAN_PROCESS_RESTARTING",
        "if [ -n \"$CLEANUP_REQUEST_ID\" ]; then",
        "  gitpartner_signal_exact_status SIGTERM \"work/relay/inbox/$CLEANUP_REQUEST_ID/result/status.json\" \"work/relay/inbox/$CLEANUP_REQUEST_ID/result/heartbeat.json\" || true",
        "  sleep 2",
        "  gitpartner_signal_exact_status SIGKILL \"work/relay/inbox/$CLEANUP_REQUEST_ID/result/status.json\" \"work/relay/inbox/$CLEANUP_REQUEST_ID/result/heartbeat.json\" || true",
        "fi",
        "ROLE_IDENTITIES=$(gitpartner_role_identities || true)",
        "LEGACY_IDENTITIES=$(gitpartner_legacy_identities || true)",
        "if [ -n \"$LEGACY_IDENTITIES\" ]; then",
        "  echo GITPARTNER_LAN_LEGACY_PROCESS_FOUND identities=$LEGACY_IDENTITIES",
        "fi",
        "OLD_IDENTITIES=$(printf '%s\\n%s\\n' \"$ROLE_IDENTITIES\" \"$LEGACY_IDENTITIES\" | sed '/^$/d' | sort -u)",
        "if [ -n \"$OLD_IDENTITIES\" ]; then",
        "  for identity in $OLD_IDENTITIES; do",
        "    pid=${identity%%:*}; token=${identity#*:}",
        "    current=$(gitpartner_pid_start_token \"$pid\" || true)",
        "    if [ -n \"$token\" ] && [ \"$current\" = \"$token\" ]; then",
        "      if kill \"$pid\" 2>/dev/null; then",
        "        echo GITPARTNER_LAN_PROCESS_SIGNAL_SENT pid=$pid start_token=$token signal=SIGTERM",
        "      fi",
        "    else",
        "      echo GITPARTNER_LAN_PROCESS_SIGNAL_SKIPPED pid=$pid expected_start_token=$token current_start_token=$current signal=SIGTERM",
        "    fi",
        "  done",
        "  sleep 2",
        "  forced=false",
        "  for identity in $OLD_IDENTITIES; do",
        "    pid=${identity%%:*}; token=${identity#*:}",
        "    current=$(gitpartner_pid_start_token \"$pid\" || true)",
        "    if [ -n \"$token\" ] && [ \"$current\" = \"$token\" ]; then",
        "      if kill -9 \"$pid\" 2>/dev/null; then",
        "        echo GITPARTNER_LAN_PROCESS_SIGNAL_SENT pid=$pid start_token=$token signal=SIGKILL",
        "        forced=true",
        "      fi",
        "    else",
        "      echo GITPARTNER_LAN_PROCESS_SIGNAL_SKIPPED pid=$pid expected_start_token=$token current_start_token=$current signal=SIGKILL",
        "    fi",
        "  done",
        "  [ \"$forced\" = true ] && echo GITPARTNER_LAN_PROCESS_FORCE_KILLED || true",
        "fi",
        "sleep 1",
        "REMAINING_LEGACY=$(gitpartner_legacy_identities || true)",
        "if [ -n \"$REMAINING_LEGACY\" ]; then",
        "  echo GITPARTNER_LAN_LEGACY_PROCESS_REMAINS identities=$REMAINING_LEGACY",
        "  exit 1",
        "fi",
        "for adopt_wait in 1 2 3 4 5 6 7 8 9 10; do",
        "  ADOPTED_PIDS=$(gitpartner_role_pids || true)",
        "  ADOPTED_COUNT=$(printf '%s\\n' $ADOPTED_PIDS | sed '/^$/d' | wc -l | tr -d ' ')",
        "  if [ \"$ADOPTED_COUNT\" = 1 ]; then",
        "    echo GITPARTNER_LAN_PROCESS_ADOPTED pids=$ADOPTED_PIDS",
        "    sleep 6",
        "    ADOPTED_STABLE_PIDS=$(gitpartner_role_pids || true)",
        "    ADOPTED_STABLE_COUNT=$(printf '%s\\n' $ADOPTED_STABLE_PIDS | sed '/^$/d' | wc -l | tr -d ' ')",
        "    if [ \"$ADOPTED_STABLE_COUNT\" = 1 ]; then",
        "      echo GITPARTNER_LAN_PROCESS_ADOPTED_STABLE pids=$ADOPTED_STABLE_PIDS",
        "      exit 0",
        "    fi",
        "  elif [ \"$ADOPTED_COUNT\" -gt 1 ]; then",
        "    echo GITPARTNER_LAN_PROCESS_ADOPTION_WAIT_MULTIPLE pids=$ADOPTED_PIDS",
        "  fi",
        "  sleep 1",
        "done",
        "REMAINING_PIDS=$(gitpartner_role_pids || true)",
        "if [ -n \"$REMAINING_PIDS\" ]; then",
        "  echo GITPARTNER_LAN_PROCESS_ADOPTION_AMBIGUOUS pids=$REMAINING_PIDS",
        "  exit 1",
        "fi",
        "if command -v setsid >/dev/null 2>&1; then",
        (
            f"  nohup setsid -f env PYTHONPATH={shlex.quote(target_dir.rstrip('/') + '/src')} "
            "python3 -m limited_remote_partner.cli.partner "
            f"--config {config_q} --role {role_q} --allow-scp-relay "
            "> \"$LOG\" 2>&1 < /dev/null &"
        ),
        "  echo GITPARTNER_LAN_PROCESS_START_MODE=setsid",
        "else",
        (
            f"  nohup env PYTHONPATH={shlex.quote(target_dir.rstrip('/') + '/src')} "
            "python3 -m limited_remote_partner.cli.partner "
            f"--config {config_q} --role {role_q} --allow-scp-relay "
            "> \"$LOG\" 2>&1 < /dev/null &"
        ),
        "  echo GITPARTNER_LAN_PROCESS_START_MODE=nohup",
        "fi",
        (
            "sleep 2; NEW_PIDS=$(gitpartner_role_pids || true); "
            "if [ -n \"$NEW_PIDS\" ]; then "
            "echo \"$NEW_PIDS\" | xargs -r ps -fp || true; "
            "echo GITPARTNER_LAN_PROCESS_STARTED; "
            "else echo GITPARTNER_LAN_PROCESS_START_FAILED; "
            "tail -120 \"$LOG\" || true; exit 1; fi"
        ),
        (
            "sleep 6; STABLE_PIDS=$(gitpartner_role_pids || true); "
            "if [ -n \"$STABLE_PIDS\" ]; then "
            "echo GITPARTNER_LAN_PROCESS_STABLE; "
            "else echo GITPARTNER_LAN_PROCESS_EARLY_EXIT; "
            "tail -120 \"$LOG\" || true; exit 1; fi"
        ),
    ]
    if not allow_process_fallback:
        fallback_lines = [
            "echo GITPARTNER_LAN_SYSTEMD_SERVICE_NOT_AVAILABLE",
            "exit 1",
        ]
    return "\n".join(
        [
            "set -euo pipefail",
            f"cd {target_dir_q}",
            f"SERVICE={service_q}",
            f"SERVICE_FILE={service_file_q}",
            f"CONFIG={config_q}",
            f"export PYTHONPATH={shlex.quote(target_dir.rstrip('/') + '/src')}"
            '${PYTHONPATH:+:$PYTHONPATH}',
            "python3 - \"$CONFIG\" <<'PY'",
            "from pathlib import Path",
            "import sys",
            "from limited_remote_partner.core.config import load_config",
            "from limited_remote_partner.endpoint.endpoint_runtime import activate_runtime_identity",
            "config_path = Path(sys.argv[1]).resolve()",
            "config = load_config(config_path, base_dir=Path.cwd())",
            "if config.node_lifecycle.enabled:",
            "    identity = activate_runtime_identity(config_path, config)",
            "    print('GITPARTNER_LAN_RUNTIME_IDENTITY_ACTIVE ' + "
            "' '.join(f'{key}={value}' for key, value in sorted(identity.items())))",
            "else:",
            "    print('GITPARTNER_LAN_RUNTIME_IDENTITY_SKIPPED lifecycle=disabled')",
            "PY",
            "if command -v systemctl >/dev/null 2>&1; then",
            "  if [ -f \"$SERVICE_FILE\" ] && sudo -n true >/dev/null 2>&1; then",
            "    sudo -n cp \"$SERVICE_FILE\" \"/etc/systemd/system/$SERVICE.service\"",
            "    sudo -n systemctl daemon-reload || true",
            "    echo GITPARTNER_LAN_SYSTEMD_SERVICE_REFRESHED",
            "  fi",
            "  if sudo -n systemctl status \"$SERVICE\" >/dev/null 2>&1 || "
            "sudo -n systemctl list-unit-files \"$SERVICE.service\" 2>/dev/null "
            "| grep -q \"^$SERVICE.service\"; then",
            "    sudo -n systemctl daemon-reload || true",
            "    sudo -n systemctl restart \"$SERVICE\"",
            "    sudo -n systemctl --no-pager --full status \"$SERVICE\" || true",
            "    echo GITPARTNER_LAN_SYSTEMD_RESTARTED",
            "    exit 0",
            "  fi",
            "  if systemctl --user status \"$SERVICE\" >/dev/null 2>&1 || "
            "systemctl --user list-unit-files \"$SERVICE.service\" 2>/dev/null "
            "| grep -q \"^$SERVICE.service\"; then",
            "    systemctl --user daemon-reload || true",
            "    systemctl --user restart \"$SERVICE\"",
            "    systemctl --user --no-pager --full status \"$SERVICE\" || true",
            "    echo GITPARTNER_LAN_USER_SYSTEMD_RESTARTED",
            "    exit 0",
            "  fi",
            "fi",
            *fallback_lines,
        ]
    )


def _node_enrollment_diagnostic_script() -> str:
    return r"""echo NODE_ENROLLMENT_DIAG_START
if command -v python3 >/dev/null 2>&1; then
  python3 - "$TARGET_DIR" <<'PY'
import glob
import json
import os
import stat
import sys
from pathlib import Path

root = Path(sys.argv[1]).expanduser().resolve(strict=False)


def load_json(path):
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception as exc:
        print(
            "NODE_ENROLLMENT_JSON_ERROR "
            f"path={path} error={type(exc).__name__}:{exc}"
        )
        return None


def resolved_path(raw_path):
    path = Path(os.path.expandvars(os.path.expanduser(str(raw_path))))
    if not path.is_absolute():
        path = root / path
    return path.resolve(strict=False)


def token_metadata(raw_path, source):
    if not raw_path:
        print(f"NODE_TOKEN source={source} configured=no")
        return
    path = resolved_path(raw_path)
    try:
        st = path.stat()
    except FileNotFoundError:
        print(f"NODE_TOKEN source={source} path={path} exists=no")
        return
    except OSError as exc:
        print(
            "NODE_TOKEN "
            f"source={source} path={path} "
            f"stat_error={type(exc).__name__}:{exc}"
        )
        return
    mode = stat.S_IMODE(st.st_mode)
    print(
        "NODE_TOKEN "
        f"source={source} path={path} exists=yes "
        f"regular={path.is_file()} size={st.st_size} mode={mode:04o} "
        f"uid={st.st_uid} gid={st.st_gid} readable={os.access(path, os.R_OK)} "
        f"nonempty={bool(st.st_size)}"
    )


launcher = root / "scripts" / "start_gitpartner_service.sh"
node_launcher = root / "scripts" / "start_gitpartner_node.sh"
print(
    "NODE_LAUNCHER "
    f"path={launcher} exists={launcher.is_file()} "
    f"executable={os.access(launcher, os.X_OK)}"
)
if launcher.is_file():
    launcher_texts = []
    for feature_path in (launcher, node_launcher):
        if not feature_path.is_file():
            continue
        try:
            launcher_texts.append(feature_path.read_text(encoding="utf-8-sig"))
        except OSError as exc:
            print(
                "NODE_LAUNCHER_READ_ERROR "
                f"path={feature_path} error={type(exc).__name__}:{exc}"
            )
    if launcher_texts:
        launcher_text = "\n".join(launcher_texts)
        print(
            "NODE_LAUNCHER_FEATURES "
            f"enroll={'--enroll' in launcher_text} "
            "resume="
            f"{'GITPARTNER_ENROLL_RESUME' in launcher_text or 'resume-node' in launcher_text}"
        )
else:
    candidates = []
    for dirpath, dirnames, filenames in os.walk(root):
        current = Path(dirpath)
        try:
            depth = len(current.relative_to(root).parts)
        except ValueError:
            continue
        dirnames[:] = [
            name
            for name in dirnames
            if name
            not in {
                ".git",
                ".partner_state",
                "input",
                "output",
                "work",
                "__pycache__",
            }
        ]
        if depth >= 5:
            dirnames[:] = []
        if current.name == "scripts" and "start_gitpartner_service.sh" in filenames:
            candidates.append(current / "start_gitpartner_service.sh")
    for candidate in sorted(candidates)[:20]:
        print(
            "NODE_LAUNCHER_CANDIDATE "
            f"path={candidate} repo_root={candidate.parent.parent}"
        )
    print(f"NODE_LAUNCHER_CANDIDATE_COUNT value={len(candidates)}")

token_metadata(root / "api.txt", "repo-local-default")
config_paths = sorted(
    {
        *(Path(item) for item in glob.glob(str(root / "configs" / "nodes" / "*.json"))),
        *(Path(item) for item in glob.glob(str(root / "configs" / "*.json"))),
    }
)
node_config_count = 0
for path in config_paths:
    payload = load_json(path)
    if not isinstance(payload, dict):
        continue
    repo = payload.get("repo") if isinstance(payload.get("repo"), dict) else {}
    relay = payload.get("relay") if isinstance(payload.get("relay"), dict) else {}
    node = payload.get("node") if isinstance(payload.get("node"), dict) else {}
    endpoint = (
        payload.get("endpoint")
        if isinstance(payload.get("endpoint"), dict)
        else {}
    )
    lifecycle = (
        payload.get("node_lifecycle")
        if isinstance(payload.get("node_lifecycle"), dict)
        else {}
    )
    if node or lifecycle:
        node_config_count += 1
    relative = path.relative_to(root)
    print(
        "NODE_CONFIG "
        f"path={relative} node_id={node.get('node_id', '')!s} "
        f"endpoint_id={endpoint.get('endpoint_id', '')!s} "
        f"generation={endpoint.get('generation', '')!s} "
        f"role={relay.get('role', '')!s} "
        f"transport_mode={relay.get('transport_mode', '')!s} "
        f"client_work_dir={relay.get('client_work_dir', '')!s} "
        f"client_inbox_dir={relay.get('client_inbox_dir', '')!s} "
        f"registration_state={lifecycle.get('registration_state', '')!s} "
        f"repo_dir={payload.get('repo_dir', '')!s} "
        f"branch={repo.get('branch', '')!s} "
        f"result_branch={repo.get('result_branch', '')!s} "
        f"token_file={repo.get('token_file', '')!s} "
        f"auth_username={repo.get('auth_username', '')!s}"
    )
    token_metadata(repo.get("token_file", ""), f"config:{relative}")

    repo_dir = resolved_path(payload.get("repo_dir") or ".")
    io = payload.get("io") if isinstance(payload.get("io"), dict) else {}
    state_dir = str(io.get("state_dir") or ".partner_state")
    identity_path = (repo_dir / state_dir / "node_identity.json").resolve(
        strict=False
    )
    identity = load_json(identity_path) if identity_path.is_file() else None
    if isinstance(identity, dict):
        bootstrap = (
            identity.get("channel_bootstrap")
            if isinstance(identity.get("channel_bootstrap"), dict)
            else {}
        )
        print(
            "NODE_IDENTITY "
            f"path={identity_path} node_id={identity.get('node_id', '')!s} "
            f"endpoint_id={identity.get('endpoint_id', '')!s} "
            f"generation={identity.get('generation', '')!s} "
            f"registration_state={identity.get('registration_state', '')!s} "
            f"channel_state={bootstrap.get('state', '')!s} "
            f"error_code={bootstrap.get('error_code', '')!s} "
            f"error={bootstrap.get('error', '')!s}"
        )
    else:
        print(f"NODE_IDENTITY path={identity_path} exists=no")

    node_id = str(node.get("node_id") or "")
    service_path = (repo_dir / state_dir / "node_service.json").resolve(
        strict=False
    )
    service = load_json(service_path) if service_path.is_file() else None
    if isinstance(service, dict):
        print(
            "NODE_SERVICE "
            f"path={service_path} state={service.get('state', '')!s} "
            f"supervisor_pid={service.get('supervisor_pid', '')!s} "
            f"child_pid={service.get('child_pid', '')!s} "
            f"last_exit_code={service.get('last_exit_code', '')!s} "
            f"last_runtime_seconds={service.get('last_runtime_seconds', '')!s} "
            f"restart_count={service.get('restart_count', '')!s} "
            f"reason={service.get('reason', '')!s}"
        )
    else:
        print(f"NODE_SERVICE path={service_path} exists=no")

    if node_id:
        service_log = repo_dir / "work" / "logs" / f"git-partner-node-{node_id}.log"
        if service_log.is_file():
            print(f"NODE_SERVICE_LOG_START path={service_log}")
            try:
                lines = service_log.read_text(
                    encoding="utf-8-sig", errors="replace"
                ).splitlines()
                for line in lines[-120:]:
                    print(line)
            except OSError as exc:
                print(
                    "NODE_SERVICE_LOG_READ_ERROR "
                    f"path={service_log} error={type(exc).__name__}:{exc}"
                )
            print(f"NODE_SERVICE_LOG_DONE path={service_log}")
        else:
            print(f"NODE_SERVICE_LOG path={service_log} exists=no")

print(f"NODE_CONFIG_COUNT value={node_config_count}")
print("NODE_TOKEN_POLICY value=repo-root-api.txt-only")
PY
else
  echo NODE_ENROLLMENT_PYTHON3_MISSING
fi
echo NODE_ENROLLMENT_DIAG_DONE"""


def build_diagnose_script(
    *,
    target_dir: str,
    target_role: str,
    request_id: str = "",
) -> str:
    target_dir_q = shlex.quote(target_dir)
    role_q = shlex.quote(target_role)
    request_id_q = shlex.quote(request_id)
    return "\n".join(
        [
            "set +e",
            f"TARGET_DIR={target_dir_q}",
            f"ROLE={role_q}",
            f"REQUEST_ID={request_id_q}",
            "READY_DIR=\"$TARGET_DIR/work/relay/inbox/.ready\"",
            "SERVICE_LOG=\"$TARGET_DIR/work/logs/git-partner-$ROLE.log\"",
            "RESTART_DIR=\"$TARGET_DIR/work/lan_ops\"",
            "RESTART_INTENT=\"$RESTART_DIR/restart-$ROLE.intent\"",
            "GIT_LOCK_DIR=\"$TARGET_DIR/.partner_state/git-operation.lock.d\"",
            "echo GITPARTNER_LAN_DIAG_START",
            "echo HOSTNAME=$(hostname 2>/dev/null)",
            "echo USER=$(whoami 2>/dev/null)",
            "echo UPTIME=$(uptime -p 2>/dev/null)",
            "test -d \"$TARGET_DIR\" && echo TARGET_DIR_OK || echo TARGET_DIR_MISSING",
            "test -d \"$TARGET_DIR/work/relay/inbox\" && echo INBOX_DIR_OK || echo INBOX_DIR_MISSING",
            _node_enrollment_diagnostic_script(),
            "echo GITEE_NETWORK_PROBE_START",
            "printf 'GITEE_DNS='",
            "getent ahosts gitee.com 2>/dev/null | awk '{print $1}' | sort -u | paste -sd, - || true",
            "if command -v curl >/dev/null 2>&1; then",
            "  if curl -4 -sS -I --max-time 10 -o /dev/null -w 'GITEE_CURL_IPV4 http=%{http_code} connect=%{time_connect} total=%{time_total} remote=%{remote_ip}\\n' https://gitee.com; then echo GITEE_CURL_IPV4_RC=0; else echo GITEE_CURL_IPV4_RC=$?; fi",
            "  if curl -6 -sS -I --max-time 10 -o /dev/null -w 'GITEE_CURL_IPV6 http=%{http_code} connect=%{time_connect} total=%{time_total} remote=%{remote_ip}\\n' https://gitee.com; then echo GITEE_CURL_IPV6_RC=0; else echo GITEE_CURL_IPV6_RC=$?; fi",
            "  GITEE_IPV4S=$(getent ahostsv4 gitee.com 2>/dev/null | awk '{print $1}' | sort -u)",
            "  for ip in $GITEE_IPV4S; do",
            "    if curl -4 -sS -I --connect-timeout 5 --max-time 8 --resolve \"gitee.com:443:$ip\" -o /dev/null -w \"GITEE_CURL_IPV4_ADDR ip=$ip http=%{http_code} connect=%{time_connect} total=%{time_total} remote=%{remote_ip}\\\\n\" https://gitee.com; then echo \"GITEE_CURL_IPV4_ADDR_RC ip=$ip rc=0\"; else rc=$?; echo \"GITEE_CURL_IPV4_ADDR_RC ip=$ip rc=$rc\"; fi",
            "  done",
            "else",
            "  echo GITEE_CURL_MISSING",
            "fi",
            "echo PROXY_ENV_START",
            "env | grep -Ei '^(http|https|all|no)_proxy=' | sed -E 's#(://)[^/@]+@#\\1***@#' || true",
            "echo PROXY_ENV_DONE",
            "echo GIT_REMOTE_START",
            "git -C \"$TARGET_DIR\" remote get-url origin 2>/dev/null | sed -E 's#(https?://)[^/@]+@#\\1***@#' || true",
            "echo GIT_REMOTE_DONE",
            "echo LOGIN_SHELL_NETWORK_START",
            "if command -v bash >/dev/null 2>&1 && command -v timeout >/dev/null 2>&1; then",
            "  timeout 15 bash -lic 'echo LOGIN_PROXY_ENV_START; env | grep -Ei \"^(http|https|all|no)_proxy=\" || true; echo LOGIN_PROXY_ENV_DONE; if command -v curl >/dev/null 2>&1; then if curl -4 -sS -I --connect-timeout 5 --max-time 8 -o /dev/null -w \"LOGIN_GITEE_CURL_IPV4 http=%{http_code} connect=%{time_connect} total=%{time_total} remote=%{remote_ip}\\\\n\" https://gitee.com; then echo LOGIN_GITEE_CURL_IPV4_RC=0; else echo LOGIN_GITEE_CURL_IPV4_RC=$?; fi; fi' 2>&1 | sed -E 's#(://)[^/@]+@#\\1***@#' || true",
            "else",
            "  echo LOGIN_SHELL_NETWORK_SKIPPED",
            "fi",
            "echo LOGIN_SHELL_NETWORK_DONE",
            "echo GIT_NETWORK_CONFIG_START",
            "git -C \"$TARGET_DIR\" config --show-origin --get-regexp '^(http\\..*\\.proxy|http\\.proxy|https\\.proxy|url\\..*\\.insteadof)$' 2>/dev/null | sed -E 's#(://)[^/@]+@#\\1***@#' || true",
            "echo GIT_NETWORK_CONFIG_DONE",
            "echo GITEE_NETWORK_PROBE_DONE",
            "echo GIT_AUTH_PROBE_START",
            "if [ -s \"$TARGET_DIR/api.txt\" ] && [ -d \"$TARGET_DIR/.git\" ]; then",
            "  python3 - \"$TARGET_DIR\" <<'PY'",
            "import base64, json, os, subprocess, sys",
            "from pathlib import Path",
            "root = Path(sys.argv[1]).resolve()",
            "token = (root / 'api.txt').read_text(encoding='utf-8-sig').strip().strip(chr(34)).strip(chr(39))",
            "username = 'git-user'",
            "configs = sorted((root / 'configs' / 'nodes').glob('*.json'))",
            "if configs:",
            "    try:",
            "        raw = json.loads(configs[0].read_text(encoding='utf-8-sig'))",
            "        username = str((raw.get('repo') or {}).get('auth_username') or username)",
            "    except (OSError, ValueError):",
            "        pass",
            "encoded = base64.b64encode(f'{username}:{token}'.encode()).decode()",
            "command = [",
            "    'git', '-c', f'http.extraHeader=Authorization: Basic {encoded}',",
            "    'ls-remote', '--heads', 'origin',",
            "    'refs/heads/main', 'refs/heads/gp/nodes',",
            "    'refs/heads/gp/control/example-node',",
            "]",
            "env = os.environ.copy()",
            "env['GIT_TERMINAL_PROMPT'] = '0'",
            "env['GIT_TRACE_CURL'] = '1'",
            "env.pop('GIT_ASKPASS', None)",
            "env.pop('SSH_ASKPASS', None)",
            "try:",
            "    result = subprocess.run(command, cwd=root, env=env, stdin=subprocess.DEVNULL, text=True, encoding='utf-8', errors='replace', capture_output=True, timeout=15, check=False)",
            "except subprocess.TimeoutExpired:",
            "    print('GIT_AUTH_PROBE_RC=124')",
            "    print('GIT_AUTH_PROBE_ERROR=timeout_after_15_seconds')",
            "else:",
            "    print(f'GIT_AUTH_PROBE_RC={result.returncode}')",
            "    text = (result.stdout + '\\n' + result.stderr).replace(token, '***').replace(encoded, '***')",
            "    for line in text.strip().splitlines()[-60:]:",
            "        print(line)",
            "PY",
            "else",
            "  echo GIT_AUTH_PROBE_SKIPPED",
            "fi",
            "echo GIT_AUTH_PROBE_DONE",
            "if [ -n \"$REQUEST_ID\" ]; then",
            "  test -f \"$READY_DIR/$REQUEST_ID.ready\" && echo REQUEST_READY_MARKER_PRESENT || echo REQUEST_READY_MARKER_ABSENT",
            "  test -d \"$TARGET_DIR/work/relay/inbox/$REQUEST_ID\" && echo REQUEST_DIR_OK || echo REQUEST_DIR_MISSING",
            "  test -f \"$TARGET_DIR/work/relay/inbox/$REQUEST_ID/request.json\" && echo REQUEST_JSON_OK || echo REQUEST_JSON_MISSING",
            "  test -f \"$TARGET_DIR/work/relay/inbox/$REQUEST_ID/$REQUEST_ID/request.json\" && echo REQUEST_JSON_NESTED_OK || echo REQUEST_JSON_NESTED_MISSING",
            "  test -d \"$TARGET_DIR/work/relay/inbox/$REQUEST_ID/payload\" && echo PAYLOAD_DIR_OK || echo PAYLOAD_DIR_MISSING",
            "  test -d \"$TARGET_DIR/work/relay/inbox/$REQUEST_ID/$REQUEST_ID/payload\" && echo PAYLOAD_DIR_NESTED_OK || echo PAYLOAD_DIR_NESTED_MISSING",
            "  test -d \"$TARGET_DIR/work/relay/inbox/$REQUEST_ID/result\" && echo RESULT_DIR_OK || echo RESULT_DIR_MISSING",
            "  test -f \"$TARGET_DIR/work/relay/inbox/$REQUEST_ID/DONE\" && echo REQUEST_DONE_PRESENT || echo REQUEST_DONE_ABSENT",
            "  test -f \"$TARGET_DIR/work/relay/inbox/$REQUEST_ID/cancel.json\" && echo REQUEST_CANCEL_PRESENT || echo REQUEST_CANCEL_ABSENT",
            "  if [ -f \"$TARGET_DIR/work/relay/inbox/$REQUEST_ID/result/status.json\" ]; then echo REQUEST_RESULT_STATUS_START; tail -80 \"$TARGET_DIR/work/relay/inbox/$REQUEST_ID/result/status.json\"; echo REQUEST_RESULT_STATUS_DONE; fi",
            "  if [ -f \"$TARGET_DIR/work/relay/inbox/$REQUEST_ID/result/heartbeat.json\" ]; then echo REQUEST_HEARTBEAT_STATUS_START; tail -80 \"$TARGET_DIR/work/relay/inbox/$REQUEST_ID/result/heartbeat.json\"; echo REQUEST_HEARTBEAT_STATUS_DONE; fi",
            "  if [ -d \"$TARGET_DIR/work/relay/inbox/$REQUEST_ID\" ]; then echo REQUEST_TREE_START; find \"$TARGET_DIR/work/relay/inbox/$REQUEST_ID\" -maxdepth 3 -mindepth 1 -printf '%P\\n' 2>/dev/null | sort | head -80; echo REQUEST_TREE_DONE; fi",
            "  TMUX_REQUEST_DIR=\"$TARGET_DIR/work/gitpartner_tmux/$REQUEST_ID\"",
            "  if [ -f \"$TMUX_REQUEST_DIR/tmux_status.json\" ]; then echo REQUEST_TMUX_STATUS_START; tail -80 \"$TMUX_REQUEST_DIR/tmux_status.json\"; echo REQUEST_TMUX_STATUS_DONE; else echo REQUEST_TMUX_STATUS_MISSING; fi",
            "  if [ -f \"$TMUX_REQUEST_DIR/tmux_command.log\" ]; then echo REQUEST_TMUX_LOG_START; tail -120 \"$TMUX_REQUEST_DIR/tmux_command.log\"; echo REQUEST_TMUX_LOG_DONE; else echo REQUEST_TMUX_LOG_MISSING; fi",
            "fi",
            "echo READY_MARKERS_START",
            "if [ -d \"$READY_DIR\" ]; then",
            "  READY_MARKER_COUNT=$(find \"$READY_DIR\" -maxdepth 1 -type f -name '*.ready' 2>/dev/null | wc -l | tr -d ' ')",
            "  echo READY_MARKER_COUNT=$READY_MARKER_COUNT",
            "  find \"$READY_DIR\" -maxdepth 1 -type f -name '*.ready' -printf '%f\n' 2>/dev/null | sort | head -40",
            "else",
            "  echo READY_MARKER_DIR_MISSING",
            "  echo READY_MARKER_COUNT=0",
            "fi",
            "echo READY_MARKERS_DONE",
            "echo GIT_OPERATION_LOCK_START",
            "if [ -d \"$GIT_LOCK_DIR\" ]; then",
            "  echo GIT_OPERATION_LOCK_PRESENT",
            "  for field in pid created_at_epoch label; do [ -f \"$GIT_LOCK_DIR/$field\" ] && printf '%s=' \"$field\" && head -1 \"$GIT_LOCK_DIR/$field\"; done",
            "else",
            "  echo GIT_OPERATION_LOCK_ABSENT",
            "fi",
            "echo GIT_OPERATION_LOCK_DONE",
            "echo PENDING_REQUESTS_START",
            "if [ -d \"$TARGET_DIR/work/relay/inbox\" ]; then",
            "  for d in \"$TARGET_DIR\"/work/relay/inbox/*; do",
            "    [ -d \"$d\" ] || continue",
            "    name=$(basename \"$d\")",
            "    done_state=NO_DONE; [ -f \"$d/DONE\" ] && done_state=DONE",
            "    result_state=NO_RESULT",
            "    if [ -f \"$d/result/status.json\" ]; then",
            "      state=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1])).get(\"state\", \"UNKNOWN\"))' \"$d/result/status.json\" 2>/dev/null || echo BAD_STATUS)",
            "      result_state=RESULT_$state",
            "    fi",
            "    request_state=NO_REQUEST; [ -f \"$d/request.json\" ] && request_state=REQUEST_OK",
            "    [ \"$request_state\" = NO_REQUEST ] && [ -f \"$d/$name/request.json\" ] && request_state=REQUEST_NESTED_OK",
            "    payload_state=NO_PAYLOAD; [ -d \"$d/payload\" ] && payload_state=PAYLOAD_OK",
            "    [ \"$payload_state\" = NO_PAYLOAD ] && [ -d \"$d/$name/payload\" ] && payload_state=PAYLOAD_NESTED_OK",
            "    printf 'PENDING_REQUEST:%s:%s:%s:%s:%s\\n' \"$name\" \"$done_state\" \"$request_state\" \"$payload_state\" \"$result_state\"",
            "  done | sort | head -40",
            "fi",
            "echo PENDING_REQUESTS_DONE",
            "echo PROCESS_SCAN_START",
            "ps -eo pid=,ppid=,stat=,args= | awk -v role=\"$ROLE\" '",
            "  $0 ~ /limited_remote_partner[.]partner/ && $0 ~ \"--role \" role { print; next }",
            "  $0 ~ /(^|[[:space:]])git-partner([[:space:]]|$)/ && $0 ~ \"--role \" role { print; next }",
            "  role == \"server\" && $0 ~ /-m limited_remote_partner[.]server([[:space:]]|$)/ { print; next }",
            "  role == \"client\" && $0 ~ /-m limited_remote_partner[.]client([[:space:]]|$)/ { print; next }",
            "  role == \"server\" && $0 ~ /(^|[/[:space:]])git-partner-server([[:space:]]|$)/ { print; next }",
            "  role == \"client\" && $0 ~ /(^|[/[:space:]])git-partner-client([[:space:]]|$)/ { print; next }",
            "'",
            "echo PROCESS_SCAN_DONE",
            "echo SELF_RESTART_STATE_START",
            "if [ -f \"$RESTART_INTENT\" ]; then printf 'SELF_RESTART_INTENT='; head -1 \"$RESTART_INTENT\"; else echo SELF_RESTART_INTENT_MISSING; fi",
            "if [ -d \"$RESTART_DIR\" ]; then",
            "  find \"$RESTART_DIR\" -maxdepth 1 -type f -name 'restart-after-sync-*.log' -printf '%T@ %p\\n' 2>/dev/null | sort -nr | head -3 | cut -d' ' -f2- | while IFS= read -r log; do",
            "    [ -n \"$log\" ] || continue",
            "    echo SELF_RESTART_LOG_START=$(basename \"$log\")",
            "    tail -80 \"$log\" 2>/dev/null || true",
            "    echo SELF_RESTART_LOG_DONE=$(basename \"$log\")",
            "  done",
            "fi",
            "echo SELF_RESTART_STATE_DONE",
            "echo SERVICE_LOG_START",
            "if [ -f \"$SERVICE_LOG\" ]; then tail -120 \"$SERVICE_LOG\"; else echo SERVICE_LOG_MISSING; fi",
            "echo SERVICE_LOG_DONE",
            "echo GITPARTNER_LAN_DIAG_DONE",
            "exit 0",
        ]
    )


def build_cancel_script(
    *,
    target_dir: str,
    target_role: str,
    request_id: str,
    reason: str,
) -> str:
    target_dir_q = shlex.quote(target_dir)
    role_q = shlex.quote(target_role)
    request_q = shlex.quote(request_id)
    reason_q = shlex.quote(reason)
    return "\n".join(
        [
            "set -euo pipefail",
            f"cd {target_dir_q}",
            f"ROLE={role_q}",
            f"TARGET_DIR={target_dir_q}",
            f"REQUEST_ID={request_q}",
            f"REASON={reason_q}",
            "TASK_DIR=\"work/relay/inbox/$REQUEST_ID\"",
            "mkdir -p \"$TASK_DIR\"",
            "python3 - \"$TASK_DIR/cancel.json\" \"$REQUEST_ID\" \"$REASON\" <<'PY'",
            "import json, sys",
            "path, request_id, reason = sys.argv[1:4]",
            "payload = {",
            "    'cancel': True,",
            "    'request_id': request_id,",
            "    'reason': reason,",
            "}",
            "open(path, 'w', encoding='utf-8').write(json.dumps(payload, ensure_ascii=False, indent=2) + '\\n')",
            "PY",
            "echo GITPARTNER_LAN_CANCEL_MARKER_WRITTEN",
            *_exact_status_signal_helper_lines(),
            "gitpartner_signal_exact_status SIGTERM \"$TASK_DIR/result/status.json\" \"$TASK_DIR/result/heartbeat.json\" || true",
            "sleep 2",
            "gitpartner_signal_exact_status SIGKILL \"$TASK_DIR/result/status.json\" \"$TASK_DIR/result/heartbeat.json\" || true",
            "echo GITPARTNER_LAN_CANCEL_DONE",
        ]
    )


def _exact_status_signal_helper_lines() -> list[str]:
    return [
        "gitpartner_signal_exact_status() {",
            "  SIGNAL_NAME=$1",
            "  shift",
            "  PYTHONPATH=\"$TARGET_DIR/src${PYTHONPATH:+:$PYTHONPATH}\" python3 - \"$SIGNAL_NAME\" \"$@\" <<'PY'",
            "import json, signal, sys",
            "from pathlib import Path",
            "from limited_remote_partner.core.process_utils import signal_exact_process_identity",
            "signal_name, *status_paths = sys.argv[1:]",
            "sig = getattr(signal, signal_name)",
            "seen = set()",
            "for raw_path in status_paths:",
            "    path = Path(raw_path)",
            "    if not path.is_file():",
            "        continue",
            "    try:",
            "        data = json.loads(path.read_text(encoding='utf-8'))",
            "    except (OSError, ValueError):",
            "        print(f'GITPARTNER_EXACT_SIGNAL_SKIP_INVALID_STATUS path={path}')",
            "        continue",
            "    process = data.get('process') if isinstance(data, dict) else None",
            "    if not isinstance(process, dict):",
            "        continue",
            "    try:",
            "        pid = int(process.get('pid') or 0)",
            "        pgid = int(process.get('pgid') or 0)",
            "    except (TypeError, ValueError):",
            "        continue",
            "    token = str(process.get('start_token') or '')",
            "    identity = (pid, token, pgid)",
            "    if identity in seen:",
            "        continue",
            "    seen.add(identity)",
            "    sent, target = signal_exact_process_identity(pid, token, sig, pgid=pgid)",
            "    if not sent:",
            "        print(f'GITPARTNER_EXACT_SIGNAL_SKIPPED pid={pid} reason={target}')",
            "        continue",
            "    print(f'GITPARTNER_EXACT_SIGNAL_SENT {target} signal={signal_name} start_token={token}')",
            "PY",
        "}",
    ]


def _default_target_host(config: AppConfig, target_role: str) -> str:
    if target_role == "client":
        return config.relay.client_ssh
    if target_role == "server":
        return config.relay.server_ssh
    raise SystemExit("target role must be client or server")


def _default_target_dir(config: AppConfig, target_role: str) -> str:
    if target_role == "client":
        return _strip_suffix(
            config.relay.client_inbox_dir,
            "/work/relay/inbox",
        )
    if target_role == "server":
        return _strip_suffix(
            config.relay.server_return_dir,
            "/work/relay/return",
        )
    raise SystemExit("target role must be client or server")


def _strip_suffix(path: str, suffix: str) -> str:
    normalized = path.rstrip("/")
    if normalized.endswith(suffix):
        return normalized[: -len(suffix)]
    return ""


def _tar_filter(info: tarfile.TarInfo) -> tarfile.TarInfo | None:
    parts = set(Path(info.name).parts)
    if "__pycache__" in parts:
        return None
    if info.name.endswith((".pyc", ".pyo")):
        return None
    return info


def _process_report(
    step: str,
    result: subprocess.CompletedProcess[str],
) -> dict[str, Any]:
    return {
        "step": step,
        "returncode": result.returncode,
        "stdout": _tail(result.stdout),
        "stderr": _tail(result.stderr),
    }


def _tail(text: str, limit: int = 12000) -> str:
    text = text.strip()
    if len(text) <= limit:
        return text
    return text[-limit:]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "LAN GitPartner maintenance helper. Run it on the online peer to "
            "copy local GitPartner code to the offline peer and restart/start "
            "its GitPartner service."
        )
    )
    parser.add_argument("--config", default="configs/partner.json")
    parser.add_argument(
        "--target-role",
        choices=("client", "server"),
        required=True,
        help="peer role to update/start",
    )
    parser.add_argument(
        "--target-host",
        help="SSH host for the peer; defaults to relay.client_ssh/server_ssh",
    )
    parser.add_argument(
        "--target-dir",
        help="GitPartner directory on the peer; defaults from relay paths",
    )
    parser.add_argument(
        "--remote-config",
        default="configs/partner.json",
        help="config path used by the remote git-partner process",
    )
    parser.add_argument(
        "--remote-staging-dir",
        help="temporary directory on the peer for the code archive",
    )
    parser.add_argument(
        "--service-name",
        help="systemd service name; defaults to git-partner-<target-role>",
    )
    parser.add_argument(
        "--cleanup-request-id",
        default="",
        help="also terminate stale peer processes whose command line contains this request id",
    )
    parser.add_argument(
        "--cancel-request-id",
        default="",
        help="request id whose B-side relay task should receive cancel.json",
    )
    parser.add_argument(
        "--cancel-reason",
        default="LAN operator requested cancellation",
        help="reason written to B-side cancel.json",
    )
    parser.add_argument(
        "--sync-path",
        action="append",
        default=list(DEFAULT_SYNC_PATHS),
        help="repository path to include in the code archive; repeatable",
    )
    parser.add_argument(
        "--no-process-fallback",
        action="store_true",
        help="fail instead of starting a nohup Python process when systemd is unavailable",
    )
    parser.add_argument(
        "--endpoint-action",
        choices=("provision", "start", "status", "stop"),
        default="status",
    )
    parser.add_argument(
        "--source-repo",
        default="",
        help="source GitPartner repo; defaults to target-dir",
    )
    parser.add_argument("--worktree", default="")
    parser.add_argument("--control-branch", default="")
    parser.add_argument("--endpoint-config", default="")
    parser.add_argument(
        "--endpoint-role",
        choices=("server", "client", "local"),
        default="local",
    )
    parser.add_argument("--endpoint-remote", default="origin")
    parser.add_argument("--import-login-network-env", action="store_true")
    parser.add_argument(
        "action",
        choices=(
            "sync-code",
            "restart-service",
            "cancel-request",
            "bootstrap",
            "endpoint-runtime",
        ),
        help="operation to perform",
    )
    return parser.parse_args()


if __name__ == "__main__":
    main()
