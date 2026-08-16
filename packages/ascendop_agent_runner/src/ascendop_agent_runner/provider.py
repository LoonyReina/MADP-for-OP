from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

from .responses_bridge import DeepSeekResponsesBridge


PROFILE_SCHEMA = "ascendop.agent-provider-profile.v1"
DEEPSEEK_FLASH_MODEL = "deepseek-v4-flash"
DEEPSEEK_OPENAI_BASE_URL = "https://api.deepseek.com/"
DEEPSEEK_ANTHROPIC_BASE_URL = "https://api.deepseek.com/anthropic"
SUPPORTED_DRIVERS = frozenset(
    {"codex-cli", "claude-code-cli", "kimi-code-cli"}
)
_SECRET_LINE = re.compile(
    r"^export\s+(?:ANTHROPIC_AUTH_TOKEN|DEEPSEEK_API_KEY)\s*=\s*"
    r"(?P<quote>['\"]?)(?P<secret>[A-Za-z0-9._-]+)(?P=quote)$"
)
_CONFLICTING_ENVIRONMENT = frozenset(
    {
        "ANTHROPIC_API_KEY",
        "ANTHROPIC_AUTH_TOKEN",
        "ANTHROPIC_BASE_URL",
        "ANTHROPIC_MODEL",
        "ANTHROPIC_DEFAULT_OPUS_MODEL",
        "ANTHROPIC_DEFAULT_SONNET_MODEL",
        "ANTHROPIC_DEFAULT_HAIKU_MODEL",
        "CLAUDE_CODE_SUBAGENT_MODEL",
        "CLAUDE_CODE_EFFORT_LEVEL",
        "CLAUDE_CODE_AUTO_COMPACT_WINDOW",
        "CODEX_HOME",
        "DEEPSEEK_API_KEY",
        "OPENAI_API_KEY",
        "OPENAI_BASE_URL",
        "KIMI_MODEL_NAME",
        "KIMI_MODEL_API_KEY",
        "KIMI_MODEL_PROVIDER_TYPE",
        "KIMI_MODEL_BASE_URL",
        "KIMI_MODEL_MAX_CONTEXT_SIZE",
        "KIMI_MODEL_CAPABILITIES",
        "KIMI_MODEL_REASONING_KEY",
        "KIMI_CODE_HOME",
        "KIMI_DISABLE_TELEMETRY",
        "KIMI_CODE_NO_AUTO_UPDATE",
        "KIMI_DISABLE_CRON",
        "KIMI_LOOP_MAX_ATTEMPTS_PER_STEP",
        "KIMI_CODE_BACKGROUND_KEEP_ALIVE_ON_EXIT",
    }
)


class AgentProviderConfigurationError(RuntimeError):
    pass


@dataclass(frozen=True)
class PreparedDriverRuntime:
    environment: dict[str, str] = field(repr=False)
    private_root: Path | None = None
    responses_bridge: DeepSeekResponsesBridge | None = field(
        default=None, repr=False, compare=False
    )

    def close_transport(self) -> None:
        if self.responses_bridge is not None:
            self.responses_bridge.close()

    def cleanup(self) -> None:
        self.close_transport()
        if self.private_root is not None and self.private_root.is_dir():
            _remove_private_tree(self.private_root)

    def write_private_text(self, name: str, value: str) -> Path:
        if self.private_root is None:
            raise AgentProviderConfigurationError(
                "Agent driver requires an attempt-private runtime"
            )
        if Path(name).name != name:
            raise AgentProviderConfigurationError(
                "Private Agent runtime file name must be a basename"
            )
        path = self.private_root / name
        _write_private_text(path, value)
        return path


@dataclass(frozen=True)
class AgentProviderProfile:
    root: Path
    profile_id: str
    credential_generation: str
    secret_file: str
    secret_format: str
    model: str
    openai_base_url: str
    anthropic_base_url: str
    responses_bridge_socket_idle_timeout_seconds: int
    drivers: tuple[str, ...]

    @classmethod
    def load(cls, *, root: Path, config: Path) -> AgentProviderProfile | None:
        try:
            raw = json.loads(config.read_text(encoding="utf-8-sig"))
        except (OSError, json.JSONDecodeError) as exc:
            raise AgentProviderConfigurationError(
                "Agent provider config is unreadable"
            ) from exc
        execution = raw.get("agent_execution") if isinstance(raw, dict) else None
        value = execution.get("provider_profile") if isinstance(execution, dict) else None
        if value is None:
            return None
        if not isinstance(value, dict):
            raise AgentProviderConfigurationError(
                "agent_execution.provider_profile must be an object"
            )
        profile = cls(
            root=root.resolve(),
            profile_id=_required(value, "profile_id"),
            credential_generation=_required(value, "credential_generation"),
            secret_file=_required(value, "secret_file"),
            secret_format=_required(value, "secret_format"),
            model=_required(value, "model"),
            openai_base_url=_required(value, "openai_base_url"),
            anthropic_base_url=_required(value, "anthropic_base_url"),
            responses_bridge_socket_idle_timeout_seconds=_required_int(
                value, "responses_bridge_socket_idle_timeout_seconds"
            ),
            drivers=tuple(str(item).strip() for item in value.get("drivers", ())),
        )
        profile.validate()
        profile.read_secret()
        return profile

    def validate(self) -> None:
        if self.profile_id != "deepseek-v4-flash":
            raise AgentProviderConfigurationError("Unsupported Agent provider profile")
        if self.model != DEEPSEEK_FLASH_MODEL:
            raise AgentProviderConfigurationError(
                "All configured Agent CLIs must use deepseek-v4-flash"
            )
        if self.openai_base_url != DEEPSEEK_OPENAI_BASE_URL:
            raise AgentProviderConfigurationError("Unexpected DeepSeek OpenAI base URL")
        if self.anthropic_base_url != DEEPSEEK_ANTHROPIC_BASE_URL:
            raise AgentProviderConfigurationError(
                "Unexpected DeepSeek Anthropic base URL"
            )
        if not 30 <= self.responses_bridge_socket_idle_timeout_seconds <= 300:
            raise AgentProviderConfigurationError(
                "DeepSeek Responses bridge socket idle timeout must be in [30, 300]"
            )
        if self.secret_format != "shell-export":
            raise AgentProviderConfigurationError("Unsupported Agent secret format")
        if not re.fullmatch(r"[A-Za-z0-9._-]{8,128}", self.credential_generation):
            raise AgentProviderConfigurationError(
                "Agent credential generation is invalid"
            )
        if not self.drivers or set(self.drivers) != SUPPORTED_DRIVERS:
            raise AgentProviderConfigurationError(
                "Agent provider profile must bind Codex, Claude Code, and Kimi Code"
            )
        self._validate_release_registry()
        self.secret_path()

    def _validate_release_registry(self) -> None:
        registry = self.root / "docs" / "flow_v4" / "variables.json"
        if not registry.is_file():
            return
        try:
            raw = json.loads(registry.read_text(encoding="utf-8-sig"))
            variables = raw["variables"]
            defaults = {
                str(item["id"]): item.get("default")
                for item in variables
                if isinstance(item, dict) and item.get("id")
            }
        except (OSError, json.JSONDecodeError, KeyError, TypeError) as exc:
            raise AgentProviderConfigurationError(
                "Flow V4 variable registry is unreadable for Agent provider validation"
            ) from exc
        expected = {
            "agent.provider_profile_id": self.profile_id,
            "agent.provider_model": self.model,
            "agent.provider_openai_base_url": self.openai_base_url,
            "agent.provider_anthropic_base_url": self.anthropic_base_url,
            "agent.provider_credential_generation": self.credential_generation,
            "agent.responses_bridge_socket_idle_timeout_seconds": (
                self.responses_bridge_socket_idle_timeout_seconds
            ),
        }
        mismatched = {
            key: {"profile": value, "registry": defaults.get(key)}
            for key, value in expected.items()
            if defaults.get(key) != value
        }
        if mismatched:
            raise AgentProviderConfigurationError(
                "Agent provider profile does not match the Flow V4 variable registry: "
                + ", ".join(sorted(mismatched))
            )

    def secret_path(self) -> Path:
        path = (self.root / self.secret_file).resolve()
        if path != self.root and self.root not in path.parents:
            raise AgentProviderConfigurationError(
                "Agent secret file must remain inside the workspace root"
            )
        if not path.is_file():
            raise AgentProviderConfigurationError("Agent secret file is missing")
        return path

    def read_secret(self) -> str:
        try:
            lines = [
                line.strip()
                for line in self.secret_path().read_text(
                    encoding="utf-8-sig"
                ).splitlines()
                if line.strip() and not line.lstrip().startswith("#")
            ]
        except OSError as exc:
            raise AgentProviderConfigurationError(
                "Agent secret file is unreadable"
            ) from exc
        if len(lines) != 1:
            raise AgentProviderConfigurationError(
                "Agent secret file must contain exactly one shell export"
            )
        match = _SECRET_LINE.fullmatch(lines[0])
        secret = match.group("secret") if match else ""
        if not secret.startswith("sk-") or len(secret) < 12:
            raise AgentProviderConfigurationError(
                "Agent secret file does not contain a valid DeepSeek API key"
            )
        return secret

    @property
    def contract_digest(self) -> str:
        payload = {
            "schema": PROFILE_SCHEMA,
            "profile_id": self.profile_id,
            "credential_generation": self.credential_generation,
            "secret_file": Path(self.secret_file).as_posix(),
            "secret_format": self.secret_format,
            "model": self.model,
            "openai_base_url": self.openai_base_url,
            "anthropic_base_url": self.anthropic_base_url,
            "responses_bridge_socket_idle_timeout_seconds": (
                self.responses_bridge_socket_idle_timeout_seconds
            ),
            "drivers": sorted(self.drivers),
        }
        return hashlib.sha256(
            json.dumps(
                payload, sort_keys=True, separators=(",", ":")
            ).encode("utf-8")
        ).hexdigest()

    def registration_generation(self, *, driver: str, executable_digest: str) -> str:
        if driver not in self.drivers:
            raise AgentProviderConfigurationError(
                f"Agent driver is not bound to the provider profile: {driver}"
            )
        return hashlib.sha256(
            f"{driver}:{executable_digest}:{self.contract_digest}".encode("utf-8")
        ).hexdigest()

    def public_binding(self) -> dict[str, object]:
        return {
            "schema": PROFILE_SCHEMA,
            "profile_id": self.profile_id,
            "credential_generation": self.credential_generation,
            "model": self.model,
            "contract_digest": self.contract_digest,
            "responses_bridge_socket_idle_timeout_seconds": (
                self.responses_bridge_socket_idle_timeout_seconds
            ),
        }

    def prepare(
        self,
        *,
        driver: str,
        run_root: Path,
        inherited_environment: Mapping[str, str],
    ) -> PreparedDriverRuntime:
        if driver not in self.drivers:
            raise AgentProviderConfigurationError(
                f"Agent driver is not bound to the provider profile: {driver}"
            )
        secret = self.read_secret()
        environment = {
            str(key): str(value)
            for key, value in inherited_environment.items()
            if str(key) not in _CONFLICTING_ENVIRONMENT
        }
        if driver == "claude-code-cli":
            environment.update(self._claude_environment(secret))
            return PreparedDriverRuntime(environment=environment)
        if driver == "kimi-code-cli":
            private_root = provider_private_root(run_root)
            private_root.mkdir(parents=True, exist_ok=True)
            environment.update(self._kimi_environment(secret, private_root))
            return PreparedDriverRuntime(
                environment=environment,
                private_root=private_root,
            )
        private_root = provider_private_root(run_root)
        private_root.mkdir(parents=True, exist_ok=True)
        models_path = private_root / "models.json"
        config_path = private_root / "config.toml"
        bridge = DeepSeekResponsesBridge(
            secret=secret,
            socket_idle_timeout_seconds=(
                self.responses_bridge_socket_idle_timeout_seconds
            ),
        ).start()
        try:
            _write_private_json(models_path, _codex_model_catalog())
            _write_private_text(
                config_path,
                _codex_config(
                    model=self.model,
                    base_url=bridge.base_url,
                    models_path=models_path,
                    secret=secret,
                ),
            )
        except Exception:
            bridge.close()
            _remove_private_tree(private_root)
            raise
        environment["CODEX_HOME"] = str(private_root)
        return PreparedDriverRuntime(
            environment=environment,
            private_root=private_root,
            responses_bridge=bridge,
        )

    def _claude_environment(self, secret: str) -> dict[str, str]:
        return {
            "ANTHROPIC_BASE_URL": self.anthropic_base_url,
            "ANTHROPIC_AUTH_TOKEN": secret,
            "ANTHROPIC_MODEL": self.model,
            "ANTHROPIC_DEFAULT_OPUS_MODEL": self.model,
            "ANTHROPIC_DEFAULT_SONNET_MODEL": self.model,
            "ANTHROPIC_DEFAULT_HAIKU_MODEL": self.model,
            "CLAUDE_CODE_SUBAGENT_MODEL": self.model,
            "CLAUDE_CODE_EFFORT_LEVEL": "max",
            "CLAUDE_CODE_AUTO_COMPACT_WINDOW": "786432",
        }

    def _kimi_environment(
        self, secret: str, private_root: Path
    ) -> dict[str, str]:
        return {
            "KIMI_MODEL_NAME": self.model,
            "KIMI_MODEL_API_KEY": secret,
            "KIMI_MODEL_PROVIDER_TYPE": "openai",
            "KIMI_MODEL_BASE_URL": self.openai_base_url.rstrip("/"),
            "KIMI_MODEL_MAX_CONTEXT_SIZE": "1048576",
            "KIMI_MODEL_CAPABILITIES": "thinking,tool_use",
            "KIMI_MODEL_REASONING_KEY": "reasoning_content",
            "KIMI_CODE_HOME": str(private_root),
            "KIMI_DISABLE_TELEMETRY": "1",
            "KIMI_CODE_NO_AUTO_UPDATE": "1",
            "KIMI_DISABLE_CRON": "1",
            "KIMI_LOOP_MAX_ATTEMPTS_PER_STEP": "1",
            "KIMI_CODE_BACKGROUND_KEEP_ALIVE_ON_EXIT": "0",
        }


def _required(value: dict[str, Any], key: str) -> str:
    result = str(value.get(key) or "").strip()
    if not result:
        raise AgentProviderConfigurationError(
            f"Agent provider profile field is required: {key}"
        )
    return result


def _required_int(value: dict[str, Any], key: str) -> int:
    result = value.get(key)
    if isinstance(result, bool) or not isinstance(result, int):
        raise AgentProviderConfigurationError(
            f"Agent provider profile field must be an integer: {key}"
        )
    return result


def provider_private_root(run_root: Path) -> Path:
    anchor = run_root.parent if run_root.name == "reconcile" else run_root
    return anchor / ".provider-runtime"


def _codex_config(
    *, model: str, base_url: str, models_path: Path, secret: str
) -> str:
    catalog = models_path.resolve().as_posix().replace('"', '\\"')
    token = secret.replace("\\", "\\\\").replace('"', '\\"')
    return (
        f'model = "{model}"\n'
        'model_provider = "deepseek"\n'
        'preferred_auth_method = "apikey"\n'
        'forced_login_method = "api"\n'
        'model_reasoning_effort = "high"\n'
        f'model_catalog_json = "{catalog}"\n\n'
        '[model_providers.deepseek]\n'
        'name = "deepseek"\n'
        f'base_url = "{base_url}"\n'
        'wire_api = "responses"\n'
        f'experimental_bearer_token = "{token}"\n'
    )


def _codex_model_catalog() -> dict[str, Any]:
    path = Path(__file__).with_name("deepseek_models.json")
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AgentProviderConfigurationError(
            "Pinned DeepSeek Codex model catalog is unreadable"
        ) from exc
    models = value.get("models") if isinstance(value, dict) else None
    if not isinstance(models, list) or [item.get("slug") for item in models] != [
        DEEPSEEK_FLASH_MODEL
    ]:
        raise AgentProviderConfigurationError(
            "Pinned DeepSeek Codex model catalog has the wrong model identity"
        )
    return value


def _write_private_json(path: Path, value: object) -> None:
    _write_private_text(
        path,
        json.dumps(value, ensure_ascii=True, separators=(",", ":")) + "\n",
    )


def _write_private_text(path: Path, value: str) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(value, encoding="utf-8")
    os.chmod(temporary, 0o600)
    temporary.replace(path)
    os.chmod(path, 0o600)


def _force_remove(function: Any, path: str, _error: BaseException) -> None:
    os.chmod(path, 0o700)
    function(path)


def _remove_private_tree(path: Path) -> None:
    resolved = path.resolve()
    target = Path(f"\\\\?\\{resolved}") if os.name == "nt" else resolved
    last_error: OSError | None = None
    for _attempt in range(5):
        try:
            shutil.rmtree(target, onexc=_force_remove)
            return
        except OSError as exc:
            last_error = exc
            time.sleep(0.2)
    raise AgentProviderConfigurationError(
        "Unable to remove private Agent provider runtime"
    ) from last_error
