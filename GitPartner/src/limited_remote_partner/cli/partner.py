from __future__ import annotations

import argparse
import json
import sys
from copy import deepcopy
from pathlib import Path
from typing import Any

from limited_remote_partner.core.config import load_config
from limited_remote_partner.gateway.git_client import configure_direct_git_fast_fail
from limited_remote_partner.endpoint.node_lifecycle import NodeLifecycleReporter


VALID_ROLES = {"auto", "server", "client", "local"}
VALID_TRANSPORTS = {"direct", "auto", "relay"}


def main() -> None:
    args = _parse_args()
    config_path = Path(args.config).resolve()
    effective_path = _write_effective_config(config_path, args)
    role = _effective_role(effective_path, args.role)

    if args.print_effective_config:
        sys.stdout.write(effective_path.read_text(encoding="utf-8"))
        return

    config = load_config(effective_path)
    configure_direct_git_fast_fail(config.relay.transport_mode)
    with NodeLifecycleReporter(config, effective_path, role):
        if role == "server":
            from limited_remote_partner.gateway.server import main as server_main

            _call_main(
                server_main,
                _server_argv(effective_path, once=args.once, no_wait=args.no_wait),
            )
            return
        if role == "client":
            from limited_remote_partner.gateway.client import main as client_main

            _call_main(client_main, _client_argv(effective_path, once=args.once))
            return
        if role == "local":
            from limited_remote_partner.cli.main import main as local_main

            _call_main(local_main, ["git-partner", "--config", str(effective_path)])
            return

    raise SystemExit(f"unsupported effective role: {role}")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Unified GitPartner launcher. Defaults to direct mode; add "
            "--allow-scp-relay to use auto direct-with-SCP-fallback mode."
        )
    )
    parser.add_argument("--config", required=True, help="path to unified config JSON")
    parser.add_argument(
        "--role",
        choices=sorted(VALID_ROLES),
        default="auto",
        help="runtime role; auto uses relay.role from the effective config",
    )
    parser.add_argument(
        "--transport",
        choices=sorted(VALID_TRANSPORTS),
        help="explicit transport override",
    )
    parser.add_argument(
        "--allow-scp-relay",
        action="store_true",
        help="use auto transport so SCP relay may run if direct claim is missing",
    )
    parser.add_argument(
        "--once",
        nargs="?",
        const="__server_once__",
        help=(
            "server: dispatch current input once; client: run one relay request id "
            "from the inbox"
        ),
    )
    parser.add_argument(
        "--no-wait",
        action="store_true",
        help="server --once only: return after dispatching relay payload",
    )
    parser.add_argument(
        "--print-effective-config",
        action="store_true",
        help="print the merged role-specific config and exit",
    )
    return parser.parse_args()


def _write_effective_config(config_path: Path, args: argparse.Namespace) -> Path:
    raw = json.loads(config_path.read_text(encoding="utf-8-sig"))
    if not isinstance(raw, dict):
        raise SystemExit(f"config must be a JSON object: {config_path}")

    role = args.role
    merged = _apply_role_overlay(raw, role)
    relay = merged.setdefault("relay", {})
    if not isinstance(relay, dict):
        raise SystemExit("relay config must be an object")

    if role != "auto":
        relay["role"] = role
    configured_transport = str(relay.get("transport_mode", "direct")).lower()
    relay["transport_mode"] = _transport_mode(args, configured_transport)

    state_dir = _state_dir(merged)
    state_dir.mkdir(parents=True, exist_ok=True)
    effective_path = state_dir / f"effective-{relay.get('role', 'auto')}.json"
    effective_path.write_text(
        json.dumps(merged, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return effective_path


def _apply_role_overlay(raw: dict[str, Any], role: str) -> dict[str, Any]:
    merged = deepcopy(raw)
    roles = merged.pop("roles", {})
    if role == "auto":
        role = str(merged.get("relay", {}).get("role", "local")).lower()
    if isinstance(roles, dict) and role in roles:
        overlay = roles[role]
        if not isinstance(overlay, dict):
            raise SystemExit(f"roles.{role} must be an object")
        merged = _deep_merge(merged, overlay)
    return merged


def _transport_mode(args: argparse.Namespace, configured_transport: str) -> str:
    if args.transport:
        return args.transport
    if args.allow_scp_relay:
        if configured_transport == "relay":
            return "relay"
        return "auto"
    return "direct"


def _effective_role(effective_path: Path, requested_role: str) -> str:
    if requested_role != "auto":
        return requested_role
    raw = json.loads(effective_path.read_text(encoding="utf-8"))
    role = str(raw.get("relay", {}).get("role", "local")).lower()
    if role not in VALID_ROLES - {"auto"}:
        raise SystemExit("effective relay.role must be server, client, or local")
    return role


def _state_dir(raw: dict[str, Any]) -> Path:
    repo_dir = Path(str(raw.get("repo_dir", ".")))
    io = raw.get("io", {})
    state = ".partner_state"
    if isinstance(io, dict):
        state = str(io.get("state_dir", state))
    if Path(state).is_absolute():
        return Path(state)
    return (repo_dir / state).resolve()


def _deep_merge(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    result = deepcopy(base)
    for key, value in overlay.items():
        if (
            key in result
            and isinstance(result[key], dict)
            and isinstance(value, dict)
        ):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = deepcopy(value)
    return result


def _server_argv(
    effective_path: Path,
    *,
    once: str | None,
    no_wait: bool,
) -> list[str]:
    argv = ["git-partner", "--config", str(effective_path)]
    if once:
        argv.append("--once")
    if no_wait:
        argv.append("--no-wait")
    return argv


def _client_argv(effective_path: Path, *, once: str | None) -> list[str]:
    argv = ["git-partner", "--config", str(effective_path)]
    if once and once != "__server_once__":
        argv.extend(["--once", once])
    return argv


def _call_main(func, argv: list[str]) -> None:
    previous = sys.argv
    try:
        sys.argv = argv
        func()
    finally:
        sys.argv = previous


if __name__ == "__main__":
    main()
