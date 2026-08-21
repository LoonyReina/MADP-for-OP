from __future__ import annotations

import json
from pathlib import Path


class ReleaseBundleError(RuntimeError):
    pass


def validate_campaign_alignment(
    *,
    daemon_config: Path,
    official_eval_config: Path,
) -> None:
    """Reject a release whose workflow and official state machine disagree."""

    try:
        daemon_value = json.loads(daemon_config.read_text(encoding="utf-8-sig"))
        official_value = json.loads(
            official_eval_config.read_text(encoding="utf-8-sig")
        )
    except (OSError, json.JSONDecodeError) as exc:
        raise ReleaseBundleError(
            "release campaign alignment requires readable JSON configs"
        ) from exc
    if not isinstance(daemon_value, dict) or not isinstance(official_value, dict):
        raise ReleaseBundleError("release campaign configs must be JSON objects")

    season = str(daemon_value.get("season") or "").strip()
    if not season:
        return
    campaigns = official_value.get("campaigns", [])
    enabled_campaigns = {
        str(item.get("campaign_id") or "").strip()
        for item in campaigns
        if isinstance(item, dict) and bool(item.get("enabled", True))
    }
    enabled_campaigns.discard("")
    if enabled_campaigns and season not in enabled_campaigns:
        raise ReleaseBundleError(
            "daemon/official-eval campaign mismatch: "
            f"daemon season={season!r} enabled official campaigns="
            f"{sorted(enabled_campaigns)!r}"
        )


def validate_flow_v5_role_alignment(
    *,
    daemon_config: Path,
    official_eval_config: Path,
    system_registry: Path,
) -> None:
    """Bind Manager, Assistant, and Developer release authority exactly once."""

    daemon_value = _read_object(daemon_config, "daemon config")
    official_value = _read_object(official_eval_config, "official-eval config")
    registry_value = _read_object(system_registry, "system registry")
    policy = _object(daemon_value.get("policy"), "daemon policy")
    submission = _object(
        official_value.get("submission"),
        "official-eval submission config",
    )
    bindings = registry_value.get("role_bindings")
    if not isinstance(bindings, list) or not bindings:
        raise ReleaseBundleError("Flow V5 release has no role bindings")
    active = [
        item
        for item in bindings
        if isinstance(item, dict) and item.get("state") == "active"
    ]

    assistant_target = str(
        submission.get("assistant_target_id") or "cannjudge-assistant"
    ).strip()
    assistant_principal = str(
        submission.get("assistant_principal_id")
        or "ascendop-system-assistant"
    ).strip()
    assistant_binding_id = str(
        submission.get("assistant_role_binding_id")
        or "binding-system-assistant-v1"
    ).strip()
    assistant = _one_binding(
        active,
        role="assistant",
        role_binding_id=assistant_binding_id,
        label="official Assistant",
    )
    if assistant.get("native_session_id") != assistant_target:
        raise ReleaseBundleError(
            "official Assistant target does not match its role binding"
        )
    if assistant.get("principal_id") != assistant_principal:
        raise ReleaseBundleError(
            "official Assistant principal does not match its role binding"
        )
    _require_capabilities(assistant, {"official-platform"}, "official Assistant")

    manager_matches = [
        item
        for item in active
        if item.get("role") == "manager"
        and item.get("native_session_id") == assistant_target
        and item.get("principal_id") == assistant_principal
    ]
    if len(manager_matches) != 1:
        raise ReleaseBundleError(
            "official test session must have exactly one active Manager binding"
        )
    _require_capabilities(
        manager_matches[0],
        {"flow-control", "user-interaction"},
        "official test-session Manager",
    )
    forbidden = sorted(
        {
            str(item.get("role") or "")
            for item in active
            if item.get("native_session_id") == assistant_target
            and item.get("role") in {"solver", "tester", "developer"}
        }
    )
    if forbidden:
        raise ReleaseBundleError(
            "official test session has forbidden roles: " + ", ".join(forbidden)
        )

    developer_target = str(
        policy.get("flow_v5_developer_target_id") or ""
    ).strip()
    if not developer_target:
        raise ReleaseBundleError("Flow V5 Developer target is not configured")
    developer_matches = [
        item
        for item in active
        if item.get("role") == "developer"
        and item.get("native_session_id") == developer_target
    ]
    if len(developer_matches) != 1:
        raise ReleaseBundleError(
            "Flow V5 Developer target must have exactly one active binding"
        )
    if developer_target == assistant_target:
        raise ReleaseBundleError(
            "Developer and Manager+Assistant must use isolated native sessions"
        )
    _require_capabilities(
        developer_matches[0],
        {"framework-write", "protocol-publish"},
        "Flow V5 Developer",
    )


def _read_object(path: Path, label: str) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ReleaseBundleError(f"{label} is not valid JSON") from exc
    if not isinstance(value, dict):
        raise ReleaseBundleError(f"{label} must be a JSON object")
    return value


def _object(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ReleaseBundleError(f"{label} must be an object")
    return value


def _one_binding(
    bindings: list[dict[str, object]],
    *,
    role: str,
    role_binding_id: str,
    label: str,
) -> dict[str, object]:
    matches = [
        item
        for item in bindings
        if item.get("role") == role
        and item.get("role_binding_id") == role_binding_id
    ]
    if len(matches) != 1:
        raise ReleaseBundleError(
            f"{label} must have exactly one active role binding"
        )
    return matches[0]


def _require_capabilities(
    binding: dict[str, object],
    required: set[str],
    label: str,
) -> None:
    scope = _object(binding.get("scope"), f"{label} scope")
    capabilities = scope.get("capabilities")
    observed = {
        str(item)
        for item in capabilities
    } if isinstance(capabilities, list) else set()
    missing = sorted(required - observed)
    if missing:
        raise ReleaseBundleError(
            f"{label} binding is missing capabilities: " + ", ".join(missing)
        )
