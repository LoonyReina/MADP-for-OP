from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from ascendop_agent_runner.provider import (
    AgentProviderConfigurationError,
    AgentProviderProfile,
)


DRIVERS = ["codex-cli", "claude-code-cli", "kimi-code-cli"]


def test_profile_loads_shell_export_without_exposing_secret(tmp_path: Path) -> None:
    secret = "sk-test-provider-secret"
    config = _profile_config(tmp_path, secret=secret)

    profile = AgentProviderProfile.load(root=tmp_path, config=config)

    assert profile is not None
    assert profile.model == "deepseek-v4-flash"
    assert profile.responses_bridge_socket_idle_timeout_seconds == 120
    assert profile.read_secret() == secret
    public = json.dumps(profile.public_binding(), sort_keys=True)
    assert secret not in public
    assert str(tmp_path) not in public


def test_profile_rejects_malformed_or_multi_export_secret(tmp_path: Path) -> None:
    config = _profile_config(tmp_path, secret="sk-test-first-secret")
    (tmp_path / "api-ds.txt").write_text(
        "export ANTHROPIC_AUTH_TOKEN=sk-test-first-secret\n"
        "export DEEPSEEK_API_KEY=sk-test-second-secret\n",
        encoding="ascii",
    )

    with pytest.raises(
        AgentProviderConfigurationError, match="exactly one shell export"
    ):
        AgentProviderProfile.load(root=tmp_path, config=config)


def test_driver_environments_are_flash_only_and_scrub_conflicts(
    tmp_path: Path,
) -> None:
    secret = "sk-test-provider-secret"
    profile = AgentProviderProfile.load(
        root=tmp_path, config=_profile_config(tmp_path, secret=secret)
    )
    assert profile is not None
    inherited = {
        "PATH": "test-path",
        "OPENAI_API_KEY": "old-openai-secret",
        "ANTHROPIC_MODEL": "old-model",
        "KIMI_MODEL_NAME": "old-model",
    }

    claude = profile.prepare(
        driver="claude-code-cli",
        run_root=tmp_path / "claude-run",
        inherited_environment=inherited,
    )
    kimi = profile.prepare(
        driver="kimi-code-cli",
        run_root=tmp_path / "kimi-run",
        inherited_environment=inherited,
    )

    assert claude.environment["ANTHROPIC_BASE_URL"].endswith("/anthropic")
    assert claude.environment["ANTHROPIC_AUTH_TOKEN"] == secret
    assert "ANTHROPIC_API_KEY" not in claude.environment
    assert {
        claude.environment[name]
        for name in (
            "ANTHROPIC_MODEL",
            "ANTHROPIC_DEFAULT_OPUS_MODEL",
            "ANTHROPIC_DEFAULT_SONNET_MODEL",
            "ANTHROPIC_DEFAULT_HAIKU_MODEL",
            "CLAUDE_CODE_SUBAGENT_MODEL",
        )
    } == {"deepseek-v4-flash"}
    assert "OPENAI_API_KEY" not in claude.environment
    assert kimi.environment["KIMI_MODEL_NAME"] == "deepseek-v4-flash"
    assert kimi.environment["KIMI_MODEL_PROVIDER_TYPE"] == "openai"
    assert kimi.environment["KIMI_MODEL_API_KEY"] == secret
    assert kimi.environment["PATH"] == "test-path"
    assert kimi.environment["KIMI_CODE_HOME"] == str(kimi.private_root)
    assert kimi.environment["KIMI_DISABLE_TELEMETRY"] == "1"
    assert kimi.environment["KIMI_CODE_NO_AUTO_UPDATE"] == "1"
    assert kimi.environment["KIMI_DISABLE_CRON"] == "1"
    assert kimi.environment["KIMI_LOOP_MAX_ATTEMPTS_PER_STEP"] == "1"

    kimi_root = kimi.private_root
    kimi.cleanup()
    assert kimi_root is not None and not kimi_root.exists()


def test_codex_uses_private_responses_profile_and_cleans_terminal_secret(
    tmp_path: Path,
) -> None:
    secret = "sk-test-provider-secret"
    profile = AgentProviderProfile.load(
        root=tmp_path, config=_profile_config(tmp_path, secret=secret)
    )
    assert profile is not None
    run_root = tmp_path / "run"
    run_root.mkdir()

    prepared = profile.prepare(
        driver="codex-cli",
        run_root=run_root,
        inherited_environment={"PATH": "test-path", "OPENAI_API_KEY": "old"},
    )

    codex_home = Path(prepared.environment["CODEX_HOME"])
    config_text = (codex_home / "config.toml").read_text(encoding="utf-8")
    catalog = json.loads((codex_home / "models.json").read_text(encoding="utf-8"))
    assert 'model = "deepseek-v4-flash"' in config_text
    assert re.search(r'base_url = "http://127\.0\.0\.1:\d+/"', config_text)
    assert 'wire_api = "responses"' in config_text
    assert secret in config_text
    assert "OPENAI_API_KEY" not in prepared.environment
    assert [item["slug"] for item in catalog["models"]] == [
        "deepseek-v4-flash"
    ]

    prepared.cleanup()

    assert not codex_home.exists()


def test_registration_generation_tracks_declared_credential_rotation(
    tmp_path: Path,
) -> None:
    first = AgentProviderProfile.load(
        root=tmp_path,
        config=_profile_config(
            tmp_path,
            secret="sk-test-provider-secret",
            credential_generation="credential-v1",
        ),
    )
    second_config = tmp_path / "second.json"
    _write_config(second_config, credential_generation="credential-v2")
    second = AgentProviderProfile.load(root=tmp_path, config=second_config)
    assert first is not None and second is not None

    first_generation = first.registration_generation(
        driver="codex-cli", executable_digest="a" * 64
    )
    second_generation = second.registration_generation(
        driver="codex-cli", executable_digest="a" * 64
    )

    assert first_generation != second_generation
    assert "sk-test-provider-secret" not in first.contract_digest


def test_profile_fails_closed_when_release_registry_drifts(tmp_path: Path) -> None:
    config = _profile_config(tmp_path, secret="sk-test-provider-secret")
    registry = tmp_path / "docs" / "flow_v4" / "variables.json"
    registry.parent.mkdir(parents=True)
    defaults = {
        "agent.provider_profile_id": "deepseek-v4-flash",
        "agent.provider_model": "deepseek-v4-flash",
        "agent.provider_openai_base_url": "https://api.deepseek.com/",
        "agent.provider_anthropic_base_url": "https://api.deepseek.com/anthropic",
        "agent.provider_credential_generation": "credential-v1",
        "agent.responses_bridge_socket_idle_timeout_seconds": 121,
    }
    registry.write_text(
        json.dumps(
            {
                "variables": [
                    {"id": key, "default": value}
                    for key, value in defaults.items()
                ]
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(
        AgentProviderConfigurationError,
        match="agent.responses_bridge_socket_idle_timeout_seconds",
    ):
        AgentProviderProfile.load(root=tmp_path, config=config)


def _profile_config(
    root: Path,
    *,
    secret: str,
    credential_generation: str = "credential-v1",
) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    (root / "api-ds.txt").write_text(
        f"export ANTHROPIC_AUTH_TOKEN={secret}\n", encoding="ascii"
    )
    config = root / "config.json"
    _write_config(config, credential_generation=credential_generation)
    return config


def _write_config(path: Path, *, credential_generation: str) -> None:
    path.write_text(
        json.dumps(
            {
                "agent_execution": {
                    "provider_profile": {
                        "profile_id": "deepseek-v4-flash",
                        "credential_generation": credential_generation,
                        "secret_file": "api-ds.txt",
                        "secret_format": "shell-export",
                        "model": "deepseek-v4-flash",
                        "openai_base_url": "https://api.deepseek.com/",
                        "anthropic_base_url": "https://api.deepseek.com/anthropic",
                        "responses_bridge_socket_idle_timeout_seconds": 120,
                        "drivers": DRIVERS,
                    }
                }
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
