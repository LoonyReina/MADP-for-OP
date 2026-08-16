from __future__ import annotations

from pathlib import Path
from typing import Any

from ascendop_daemon.registry.models import SystemRegistryError


def parse_management_binding(
    management: dict[str, Any],
    *,
    endpoint_id: str,
    transport_mode: str,
    transport_gateway_id: str,
    environment_remote_root: str,
) -> dict[str, object]:
    mode = str(management.get("mode") or "")
    direct = {
        "management_ssh_alias": str(management.get("ssh_alias") or ""),
        "management_remote_gitpartner_root": str(
            management.get("remote_gitpartner_root") or ""
        ),
        "management_remote_runtime_root": str(
            management.get("remote_runtime_root") or ""
        ),
        "management_service_environment_file": str(
            management.get("service_environment_file") or ""
        ),
        "management_cann_environment_script": str(
            management.get("cann_environment_script") or ""
        ),
    }
    relay = {
        "management_gateway_id": str(management.get("gateway_id") or ""),
        "management_target_repo_relative": str(
            management.get("target_repo_relative") or ""
        ),
        "management_runtime_root_relative": str(
            management.get("runtime_root_relative") or ""
        ),
        "management_drain_timeout_seconds": _integer_field(
            management,
            "drain_timeout_seconds",
            endpoint_id=endpoint_id,
        ),
    }
    if not mode:
        if any(direct.values()) or any(relay.values()):
            raise SystemRegistryError(
                f"route endpoint {endpoint_id} management binding requires mode"
            )
        return {"management_mode": "", **direct, **relay}
    if mode == "direct-ssh":
        _validate_direct(
            direct,
            relay,
            endpoint_id=endpoint_id,
            environment_remote_root=environment_remote_root,
        )
    elif mode == "relay-maintenance":
        _validate_relay(
            direct,
            relay,
            endpoint_id=endpoint_id,
            transport_mode=transport_mode,
            transport_gateway_id=transport_gateway_id,
        )
    else:
        raise SystemRegistryError(
            f"route endpoint {endpoint_id} has invalid management binding "
            f"mode {mode!r}"
        )
    return {"management_mode": mode, **direct, **relay}


def _validate_direct(
    direct: dict[str, object],
    relay: dict[str, object],
    *,
    endpoint_id: str,
    environment_remote_root: str,
) -> None:
    alias = str(direct["management_ssh_alias"])
    if not alias or any(character not in _SAFE_TOKEN for character in alias):
        raise SystemRegistryError(
            f"route endpoint {endpoint_id} management ssh_alias must be a "
            "non-empty safe token"
        )
    expected_prefix = environment_remote_root.rstrip("/") + "/"
    for key, label in (
        ("management_remote_gitpartner_root", "remote_gitpartner_root"),
        ("management_remote_runtime_root", "remote_runtime_root"),
    ):
        value = str(direct[key])
        _absolute_posix(value, endpoint_id=endpoint_id, field=label)
        if not value.startswith(expected_prefix):
            raise SystemRegistryError(
                f"route endpoint {endpoint_id} management {label} must be "
                f"below environment remote_root {environment_remote_root}"
            )
    for key, label in (
        ("management_service_environment_file", "service_environment_file"),
        ("management_cann_environment_script", "cann_environment_script"),
    ):
        _absolute_posix(
            str(direct[key]), endpoint_id=endpoint_id, field=label
        )
    if any(relay.values()):
        raise SystemRegistryError(
            f"route endpoint {endpoint_id} direct management cannot declare "
            "relay maintenance fields"
        )


def _validate_relay(
    direct: dict[str, object],
    relay: dict[str, object],
    *,
    endpoint_id: str,
    transport_mode: str,
    transport_gateway_id: str,
) -> None:
    if transport_mode != "lan-relay" or not transport_gateway_id:
        raise SystemRegistryError(
            f"route endpoint {endpoint_id} relay maintenance requires "
            "a lan-relay transport"
        )
    if relay["management_gateway_id"] != transport_gateway_id:
        raise SystemRegistryError(
            f"route endpoint {endpoint_id} management gateway must match "
            f"transport gateway {transport_gateway_id}"
        )
    for key, label in (
        ("management_target_repo_relative", "target_repo_relative"),
        ("management_runtime_root_relative", "runtime_root_relative"),
    ):
        _safe_relative(
            str(relay[key]), endpoint_id=endpoint_id, field=label
        )
    timeout = int(relay["management_drain_timeout_seconds"])
    if not 1 <= timeout <= 300:
        raise SystemRegistryError(
            f"route endpoint {endpoint_id} relay maintenance drain timeout "
            "must be between 1 and 300 seconds"
        )
    if any(direct.values()):
        raise SystemRegistryError(
            f"route endpoint {endpoint_id} relay maintenance cannot declare "
            "direct SSH fields"
        )


def _integer_field(
    value: dict[str, Any], key: str, *, endpoint_id: str
) -> int:
    try:
        return int(value.get(key, 0) or 0)
    except (TypeError, ValueError) as exc:
        raise SystemRegistryError(
            f"route endpoint {endpoint_id} management {key} must be an integer"
        ) from exc


def _absolute_posix(value: str, *, endpoint_id: str, field: str) -> None:
    if not value.startswith("/") or any(
        token in value for token in ("\x00", "\n", "\r")
    ):
        raise SystemRegistryError(
            f"route endpoint {endpoint_id} management {field} must be an "
            "absolute POSIX path"
        )


def _safe_relative(value: str, *, endpoint_id: str, field: str) -> None:
    candidate = Path(value)
    if (
        not value
        or candidate.is_absolute()
        or ".." in candidate.parts
        or any(token in value for token in ("\x00", "\n", "\r"))
    ):
        raise SystemRegistryError(
            f"route endpoint {endpoint_id} management {field} must be a "
            "safe relative path"
        )


_SAFE_TOKEN = frozenset(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._-"
)
