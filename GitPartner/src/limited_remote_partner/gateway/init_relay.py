from __future__ import annotations

import argparse
import json
import os
import shutil
from pathlib import Path
from typing import Any

from limited_remote_partner.core.config import AppConfig, load_config
from limited_remote_partner.gateway.relay import utc_now


def init_relay(config_path: Path, *, write_report: bool = True) -> dict[str, Any]:
    config = load_config(config_path)
    created_dirs: list[str] = []
    skipped_dirs: list[str] = []
    secret_files: list[dict[str, str]] = []
    tool_checks: list[dict[str, str]] = []

    def ensure_dir(path: Path | None, label: str) -> None:
        if path is None:
            skipped_dirs.append(label)
            return
        path.mkdir(parents=True, exist_ok=True)
        created_dirs.append(str(path))

    ensure_dir(config.repo_dir, str(config.repo_dir))
    for repo_path in (
        config.io.input_dir,
        config.io.output_dir,
        config.io.exchange_dir,
        config.io.state_dir,
    ):
        ensure_dir(_local_path(config.repo_dir, repo_path), repo_path)

    ensure_dir(config.repo_dir / "work" / "secrets", "work/secrets")
    ensure_dir(config.repo_dir / "work" / "relay", "work/relay")

    if config.relay.role == "server":
        ensure_dir(
            _local_path(config.repo_dir, config.relay.server_return_dir),
            config.relay.server_return_dir,
        )
        _record_secret(secret_files, "gitee token", config.repo.token_file)
        _record_secret(secret_files, "A to B password", config.relay.client_password_file)
    elif config.relay.role == "client":
        ensure_dir(
            _local_path(config.repo_dir, config.relay.client_inbox_dir),
            config.relay.client_inbox_dir,
        )
        ensure_dir(
            _local_path(config.repo_dir, config.relay.client_work_dir),
            config.relay.client_work_dir,
        )
        _record_secret(secret_files, "B to A password", config.relay.server_password_file)
    else:
        ensure_dir(
            _local_path(config.repo_dir, config.relay.client_inbox_dir),
            config.relay.client_inbox_dir,
        )
        ensure_dir(
            _local_path(config.repo_dir, config.relay.server_return_dir),
            config.relay.server_return_dir,
        )

    if config.relay.client_ssh or config.relay.server_ssh:
        _record_tool(tool_checks, "ssh")
        _record_tool(tool_checks, "scp")
    if config.relay.client_password_file or config.relay.server_password_file:
        _record_tool(tool_checks, "sshpass")

    report = {
        "generated_at": utc_now(),
        "config": str(config_path),
        "repo_dir": str(config.repo_dir),
        "relay_role": config.relay.role,
        "client_ssh": config.relay.client_ssh or "",
        "server_ssh": config.relay.server_ssh or "",
        "client_inbox_dir": config.relay.client_inbox_dir,
        "client_work_dir": config.relay.client_work_dir,
        "server_return_dir": config.relay.server_return_dir,
        "created_dirs": sorted(set(created_dirs)),
        "skipped_dirs": sorted(set(skipped_dirs)),
        "secret_files": secret_files,
        "tool_checks": tool_checks,
    }
    if write_report:
        _write_report(config, report)
    return report


def _record_secret(records: list[dict[str, str]], label: str, path: str | None) -> None:
    if not path:
        return
    if _is_non_native_posix_path(path):
        records.append(
            {
                "label": label,
                "path": path,
                "status": "unchecked-posix-path-on-windows",
            }
        )
        return
    secret_path = Path(os.path.expanduser(path))
    secret_path.parent.mkdir(parents=True, exist_ok=True)
    records.append(
        {
            "label": label,
            "path": str(secret_path),
            "status": "present" if secret_path.exists() else "missing",
        }
    )


def _record_tool(records: list[dict[str, str]], name: str) -> None:
    records.append(
        {
            "name": name,
            "status": "present" if shutil.which(name) else "missing",
        }
    )


def _write_report(config: AppConfig, report: dict[str, Any]) -> None:
    report_path = config.repo_dir / "work" / "relay" / "INIT_STATUS.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# GitPartner Relay Init Status",
        "",
        f"Generated: {report['generated_at']}",
        f"Config: `{report['config']}`",
        f"Repo dir: `{report['repo_dir']}`",
        f"Role: `{report['relay_role']}`",
        "",
        "## Relay",
        "",
        f"- client_ssh: `{report['client_ssh'] or 'local'}`",
        f"- client_inbox_dir: `{report['client_inbox_dir']}`",
        f"- client_work_dir: `{report['client_work_dir']}`",
        f"- server_ssh: `{report['server_ssh'] or 'local'}`",
        f"- server_return_dir: `{report['server_return_dir']}`",
        "",
        "## Created Directories",
        "",
        *[f"- `{path}`" for path in report["created_dirs"]],
        "",
    ]
    skipped = report.get("skipped_dirs", [])
    if skipped:
        lines.extend(["## Skipped Non-Native Paths", ""])
        lines.extend(f"- `{path}`" for path in skipped)
        lines.append("")

    lines.extend(["## Local Secrets", ""])
    secrets = report["secret_files"]
    if secrets:
        lines.extend(
            f"- {item['label']}: `{item['path']}` [{item['status']}]"
            for item in secrets
        )
    else:
        lines.append("- none required by this config")

    lines.extend(["", "## Tools", ""])
    tools = report["tool_checks"]
    if tools:
        lines.extend(f"- {item['name']}: {item['status']}" for item in tools)
    else:
        lines.append("- no ssh/scp tools required by this config")

    missing = [
        item
        for item in (*report["secret_files"], *report["tool_checks"])
        if item.get("status") == "missing"
    ]
    lines.extend(["", "## Next Steps", ""])
    if missing:
        lines.append("Create the missing local secret files or install missing tools, then rerun:")
        lines.append("")
        lines.append("```bash")
        lines.append("git-partner-init-relay --config <config>")
        lines.append("```")
    else:
        lines.append("Local relay initialization checks are complete.")
    lines.append("")
    lines.append("Secret contents are never written by this command and this report is under ignored `work/`.")
    lines.append("")

    report_path.write_text("\n".join(lines), encoding="utf-8")

    json_path = config.repo_dir / "work" / "relay" / "init_status.json"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")


def _local_path(base: Path, path: str) -> Path | None:
    if _is_non_native_posix_path(path):
        return None
    candidate = Path(os.path.expanduser(path))
    if candidate.is_absolute():
        return candidate
    return (base / candidate).resolve()


def _is_non_native_posix_path(path: str) -> bool:
    return os.name == "nt" and path.startswith("/")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="path to server/client config JSON")
    parser.add_argument("--no-report", action="store_true", help="create dirs but skip work/relay report")
    parser.add_argument("--strict", action="store_true", help="exit nonzero if any required secret/tool is missing")
    parser.add_argument("--json", action="store_true", help="print machine-readable status JSON")
    args = parser.parse_args()

    report = init_relay(Path(args.config).resolve(), write_report=not args.no_report)
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print(f"relay init complete for role={report['relay_role']}")
        print("report: work/relay/INIT_STATUS.md")

    missing = [
        item
        for item in (*report["secret_files"], *report["tool_checks"])
        if item.get("status") == "missing"
    ]
    if args.strict and missing:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
