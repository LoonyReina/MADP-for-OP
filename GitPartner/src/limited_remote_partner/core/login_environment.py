from __future__ import annotations

import argparse
import json
import os
import shlex
import shutil
import subprocess
from collections.abc import Mapping
from typing import Any
from urllib.parse import urlsplit

from limited_remote_partner.core.process_utils import hidden_subprocess_kwargs


NETWORK_ENV_KEYS = (
    "http_proxy",
    "https_proxy",
    "all_proxy",
    "no_proxy",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "NO_PROXY",
    "GIT_SSL_CAINFO",
    "SSL_CERT_FILE",
    "CURL_CA_BUNDLE",
    "REQUESTS_CA_BUNDLE",
)
IMPORT_MARKER = "GITPARTNER_LOGIN_NETWORK_ENV_IMPORTED"
IMPORT_SOURCE = "GITPARTNER_LOGIN_NETWORK_ENV_SOURCE"


def import_login_network_environment(
    environment: Mapping[str, str] | None = None,
    *,
    enabled: bool = False,
    timeout_seconds: float = 8,
) -> tuple[dict[str, str], dict[str, Any]]:
    base = dict(os.environ if environment is None else environment)
    if _disabled(base) or not enabled or base.get(IMPORT_MARKER) == "1":
        report = network_environment_report(base)
        report["requested"] = enabled
        return base, report

    shell = base.get("SHELL") or shutil.which("bash") or ""
    imported: dict[str, str] = {}
    error = ""
    if shell:
        try:
            result = subprocess.run(
                [shell, "-lic", "env -0"],
                env=base,
                stdin=subprocess.DEVNULL,
                capture_output=True,
                timeout=timeout_seconds,
                check=False,
                **hidden_subprocess_kwargs(),
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            error = f"{type(exc).__name__}: {exc}"
        else:
            if result.returncode == 0:
                imported = _parse_allowed_environment(result.stdout)
            else:
                error = f"login shell exited {result.returncode}"
    else:
        error = "login shell unavailable"

    applied: list[str] = []
    for key in NETWORK_ENV_KEYS:
        value = imported.get(key, "")
        if value and not base.get(key):
            base[key] = value
            applied.append(key)
    base[IMPORT_MARKER] = "1"
    base[IMPORT_SOURCE] = "login-shell" if applied else "existing-or-empty"
    report = network_environment_report(base)
    report["requested"] = enabled
    report["applied"] = applied
    report["error"] = error
    return base, report


def network_environment_report(environment: Mapping[str, str]) -> dict[str, Any]:
    values = {
        key: _redact_value(key, value)
        for key in NETWORK_ENV_KEYS
        if (value := environment.get(key, ""))
    }
    return {
        "schema": "gitpartner.network-environment.v1",
        "source": environment.get(IMPORT_SOURCE, "process"),
        "enabled": bool(values),
        "variables": values,
    }


def shell_exports(
    original: Mapping[str, str],
    imported: Mapping[str, str],
) -> str:
    rows: list[str] = []
    for key in (*NETWORK_ENV_KEYS, IMPORT_MARKER, IMPORT_SOURCE):
        value = imported.get(key, "")
        if value and value != original.get(key, ""):
            rows.append(f"export {key}={shlex.quote(value)}")
    return "\n".join(rows)


def _parse_allowed_environment(payload: bytes) -> dict[str, str]:
    allowed = set(NETWORK_ENV_KEYS)
    result: dict[str, str] = {}
    for raw in payload.split(b"\0"):
        if b"=" not in raw:
            continue
        raw_key, raw_value = raw.split(b"=", 1)
        key = raw_key.decode("utf-8", errors="replace")
        if key not in allowed:
            continue
        result[key] = raw_value.decode("utf-8", errors="replace")
    return result


def _disabled(environment: Mapping[str, str]) -> bool:
    return environment.get("GITPARTNER_IMPORT_LOGIN_NETWORK_ENV", "1").lower() in {
        "0",
        "false",
        "no",
        "off",
    }


def _redact_value(key: str, value: str) -> str:
    if "proxy" not in key.lower():
        return value
    try:
        parsed = urlsplit(value)
    except ValueError:
        return "***"
    if not parsed.scheme or not parsed.hostname:
        return "***"
    port = f":{parsed.port}" if parsed.port is not None else ""
    return f"{parsed.scheme}://{parsed.hostname}{port}"


def _enabled_from_config(path: str) -> bool:
    if not path:
        return False
    try:
        raw = json.loads(
            open(path, "r", encoding="utf-8-sig").read()
        )
    except (OSError, ValueError, TypeError):
        return False
    section = raw.get("network_environment")
    return bool(
        isinstance(section, dict)
        and section.get("login_shell_import", False)
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Import a bounded login-shell network environment"
    )
    parser.add_argument("action", choices=("shell-exports", "report"))
    parser.add_argument("--config", default="")
    parser.add_argument(
        "--enable-login-shell-import",
        action="store_true",
        help="explicit bootstrap override before a node config exists",
    )
    args = parser.parse_args(argv)
    original = dict(os.environ)
    enabled = (
        args.enable_login_shell_import
        or _enabled_from_config(args.config)
    )
    imported, report = import_login_network_environment(
        original,
        enabled=enabled,
    )
    if args.action == "shell-exports":
        print(shell_exports(original, imported))
    else:
        print(json.dumps(report, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
