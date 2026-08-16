from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Mapping

from ascendop_protocol.wire_v3 import canonical_digest, validate_envelope
from ascendop_daemon.exchange.flow_v3_payload import materialize_payload
from ascendop_daemon.exchange.git_binary_materializer import (
    enforce_binary_checkout,
    recover_manifest_parts_from_git,
)
from ascendop_daemon.exchange.gitpartner_transport import GitPartnerCanaryTransport
from ascendop_daemon.exchange.runtime_source import (
    apply_transport_runtime_environment,
    load_active_transport_runtime,
)
from ascendop_daemon.exchange.transport_contracts import (
    DeliveryObservation,
    QueryObservation,
    deterministic_receipt_id,
    transport_identity,
)
from ascendop_daemon.registry.system_registry import BackendEndpoint
from ascendop_daemon.runtime.locking import NamedProcessLock
from ascendop_daemon.runtime.process_adapter import (
    process_creation_flags,
    process_startupinfo,
)


REMOTE_TERMINAL_STATES = {"completed", "failed"}


class WireV3ExchangeError(ValueError):
    def __init__(self, message: str, *, failure: Mapping[str, Any]) -> None:
        super().__init__(message)
        self.failure = dict(failure)


class WireV3EndpointTransport:
    """Wire V3 transport only; routing, gate and retry decisions stay central."""

    def __init__(
        self,
        root: Path,
        endpoint: BackendEndpoint,
        *,
        remote: str = "origin",
        command_timeout_seconds: int = 120,
    ) -> None:
        self.root = root.resolve()
        self.endpoint = endpoint
        self.remote = remote
        self.command_timeout_seconds = max(30, int(command_timeout_seconds))
        self.repo = _resolve_under(self.root, endpoint.gitpartner_repo)
        self.transport_runtime = load_active_transport_runtime(self.root)
        self.transport_source = self.transport_runtime.source
        self.result_repo = (
            _resolve_under(self.root, endpoint.result_worktree)
            if endpoint.result_worktree
            else self.repo
        )
        self.return_root = (
            self.root / ".ascendop-work" / "flow-v3" / "returns"
        ).resolve()

    def publish(self, payload: dict[str, Any]) -> DeliveryObservation:
        try:
            envelope = self._envelope(payload)
            completed, observation, _ = self._exchange(
                payload,
                envelope,
                action="accept",
                control_ref=_control_ref(payload, "accept"),
            )
        except subprocess.TimeoutExpired as exc:
            return DeliveryObservation(
                status="uncertain",
                error=f"Wire V3 acceptance timed out after {exc.timeout}s",
            )
        except WireV3ExchangeError as exc:
            visibility = str(exc.failure.get("result_visibility") or "unknown")
            return DeliveryObservation(
                status="uncertain" if visibility == "unknown" else "retry",
                error=str(exc),
                retry_after_seconds=5,
                failure=exc.failure,
            )
        except OSError as exc:
            return DeliveryObservation(
                status="retry",
                error=str(exc),
                retry_after_seconds=5,
                failure=_failure(
                    code="transport-local-preflight",
                    detail=str(exc),
                    retryable=True,
                    pre_publish=True,
                    result_visibility="not-published",
                ),
            )
        except ValueError as exc:
            return DeliveryObservation(
                status="failed",
                error=str(exc),
                failure=_failure(
                    domain="protocol",
                    code="wire-envelope-invalid",
                    detail=str(exc),
                    retryable=False,
                    pre_publish=True,
                    result_visibility="not-published",
                ),
            )
        error = _endpoint_error(completed, observation)
        if error:
            retryable = bool(observation.get("retryable", False))
            endpoint_nack = (
                str(observation.get("schema") or "")
                == "ascendop.flow.endpoint-nack.v3"
            )
            return DeliveryObservation(
                status="retry" if retryable else "failed",
                error=error,
                retry_after_seconds=5,
                failure=_failure(
                    code="endpoint-nack" if endpoint_nack else "endpoint-exchange-error",
                    detail=error,
                    retryable=retryable,
                    pre_publish=endpoint_nack,
                    result_visibility=(
                        "not-published" if endpoint_nack else "unknown"
                    ),
                ),
            )
        if str(observation.get("state") or "") not in {
            "accepted",
            "result-ready",
            "acknowledged",
        }:
            return DeliveryObservation(
                status="uncertain",
                error="Wire V3 accept has no durable endpoint receipt",
            )
        return DeliveryObservation(
            status="accepted",
            receipt=_acceptance(payload, envelope, observation),
        )

    def publish_batch(
        self,
        payloads: list[dict[str, Any]],
    ) -> list[DeliveryObservation]:
        return [self.publish(payload) for payload in payloads]

    def query(self, payload: dict[str, Any]) -> QueryObservation:
        try:
            envelope = self._envelope(payload)
            ordinal = int(payload.get("_transport_poll_ordinal", 1) or 1)
            completed, observation, return_root = self._exchange(
                payload,
                envelope,
                action="query",
                control_ref=_control_ref(payload, "query", ordinal=ordinal),
            )
        except (OSError, ValueError, subprocess.TimeoutExpired) as exc:
            return QueryObservation(error=f"Wire V3 query failed: {exc}")
        error = _endpoint_error(completed, observation)
        if error:
            endpoint_nack = (
                str(observation.get("schema") or "")
                == "ascendop.flow.endpoint-nack.v3"
            )
            not_found = endpoint_nack and error.startswith(
                "unknown endpoint request:"
            )
            return QueryObservation(
                error=error,
                failure=_failure(
                    domain="transport" if not_found else "protocol",
                    code=(
                        "endpoint-request-not-found"
                        if not_found
                        else "endpoint-query-nack"
                    ),
                    detail=error,
                    retryable=not_found,
                    pre_publish=not_found,
                    result_visibility=("not-published" if not_found else "known"),
                ),
            )
        acceptance = _acceptance(payload, envelope, observation)
        result = observation.get("result")
        if not isinstance(result, Mapping):
            return QueryObservation(acceptance=acceptance)
        remote_state = str(result.get("state") or "")
        if remote_state not in REMOTE_TERMINAL_STATES:
            return QueryObservation(acceptance=acceptance)
        if return_root is None:
            return QueryObservation(
                acceptance=acceptance,
                error="terminal Wire V3 result has no returned payload",
            )
        try:
            adapted = self._materialize_result(
                payload,
                envelope,
                dict(result),
                return_root,
            )
        except (OSError, ValueError) as exc:
            return QueryObservation(
                acceptance=acceptance,
                error=f"Wire V3 result materialization failed: {exc}",
            )
        return QueryObservation(acceptance=acceptance, result=adapted)

    def query_batch(
        self,
        payloads: list[dict[str, Any]],
    ) -> list[QueryObservation]:
        return [self.query(payload) for payload in payloads]

    def acknowledge(
        self,
        payload: dict[str, Any],
        returned: dict[str, Any],
    ) -> bool:
        receipt_id = str(returned.get("receipt_id") or "")
        if receipt_id != deterministic_receipt_id(payload):
            return False
        try:
            envelope = self._envelope(payload)
            completed, observation, _ = self._exchange(
                payload,
                envelope,
                action="ack",
                control_ref=_control_ref(
                    payload,
                    "ack",
                    receipt_id=receipt_id,
                ),
                receipt_id=receipt_id,
            )
        except (OSError, ValueError, subprocess.TimeoutExpired):
            return False
        receipt = observation.get("receipt", {})
        return (
            not _endpoint_error(completed, observation)
            and str(observation.get("state") or "") == "acknowledged"
            and str(receipt.get("return_receipt_id") or "") == receipt_id
            and str(observation.get("envelope_digest") or "")
            == canonical_digest(envelope)
        )

    def _envelope(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        if str(payload.get("transport_protocol") or "") != "wire-v3":
            raise ValueError("operator request is missing Wire V3 transport identity")
        path = Path(str(payload.get("wire_envelope_path") or "")).resolve()
        _require_bounded(self.root, path)
        raw = json.loads(path.read_text(encoding="utf-8-sig"))
        if not isinstance(raw, dict):
            raise ValueError("Wire V3 envelope must be an object")
        validated = validate_envelope(raw)
        digest = canonical_digest(validated.envelope)
        if digest != str(payload.get("wire_envelope_digest") or ""):
            raise ValueError("Wire V3 envelope digest changed after publication")
        expected = {
            "request_id": str(payload.get("request_id") or ""),
            "attempt_id": str(payload.get("attempt_id") or ""),
            "endpoint_id": str(payload.get("target_endpoint_id") or ""),
            "endpoint_generation": str(payload.get("target_generation") or ""),
        }
        observed = {
            "request_id": str(validated.envelope["meta"]["request_id"]),
            "attempt_id": str(validated.envelope["meta"]["attempt_id"]),
            "endpoint_id": str(validated.envelope["identity"]["endpoint_id"]),
            "endpoint_generation": str(
                validated.envelope["identity"]["endpoint_generation"]
            ),
        }
        if observed != expected:
            raise ValueError("Wire V3 envelope identity drifted after routing")
        return validated.envelope

    def _exchange(
        self,
        payload: Mapping[str, Any],
        envelope: Mapping[str, Any],
        *,
        action: str,
        control_ref: str,
        receipt_id: str = "",
    ) -> tuple[subprocess.CompletedProcess[str], dict[str, Any], Path | None]:
        if not (self.repo / ".git").exists():
            raise OSError(f"endpoint GP worktree is missing: {self.repo}")
        if not (self.result_repo / ".git").exists():
            raise OSError(
                f"endpoint GP result worktree is missing: {self.result_repo}"
            )
        enforce_binary_checkout(self.repo)
        if self.result_repo != self.repo:
            enforce_binary_checkout(self.result_repo)
        command = self._command(
            payload,
            action=action,
            control_ref=control_ref,
            receipt_id=receipt_id,
        )
        with NamedProcessLock(
            self.root,
            f"gitpartner_endpoint_{self.endpoint.endpoint_id}_{_lane(action)}",
            stale_after_seconds=180,
            wait_timeout_seconds=300,
        ):
            completed = subprocess.run(
                command,
                cwd=self.repo,
                env=self._environment(),
                text=True,
                encoding="utf-8",
                errors="replace",
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
                timeout=self.command_timeout_seconds + 30,
                creationflags=process_creation_flags(),
                startupinfo=process_startupinfo(),
            )
        output = self.result_repo / "output" / "flow-v3" / control_ref
        observation_path = _unique_json(output, "flow_v3_observation.json")
        if observation_path is None:
            detail = (completed.stderr or completed.stdout or "").strip()
            pre_publish = _is_pre_publish_cli_failure(completed)
            raise WireV3ExchangeError(
                "Wire V3 exchange returned no endpoint observation"
                + (f": {detail[-1000:]}" if detail else ""),
                failure=_failure(
                    code=(
                        "gp-cli-prepublish-failure"
                        if pre_publish
                        else "endpoint-observation-missing"
                    ),
                    detail=detail[-4000:] or "endpoint observation missing",
                    retryable=True,
                    pre_publish=pre_publish,
                    result_visibility=(
                        "not-published" if pre_publish else "unknown"
                    ),
                ),
            )
        observation = _read_object(observation_path)
        expected_request_id = str(envelope["meta"]["request_id"])
        expected_attempt_id = str(envelope["meta"]["attempt_id"])
        observed_request_id = str(observation.get("request_id") or "")
        observed_attempt_id = str(observation.get("attempt_id") or "")
        endpoint_nack = (
            str(observation.get("schema") or "")
            == "ascendop.flow.endpoint-nack.v3"
        )
        identity_present = bool(observed_request_id or observed_attempt_id)
        if (not endpoint_nack or identity_present) and (
            observed_request_id != expected_request_id
            or observed_attempt_id != expected_attempt_id
        ):
            raise ValueError("Wire V3 endpoint observation identity mismatch")
        return_dir = (
            _unique_result_directory(output, "flow_v3_return")
            if action == "query"
            else None
        )
        return completed, observation, return_dir

    def _command(
        self,
        payload: Mapping[str, Any],
        *,
        action: str,
        control_ref: str,
        receipt_id: str,
    ) -> list[str]:
        command = [
            sys.executable,
            "-s",
            "-m",
            "limited_remote_partner.gateway.submit_job",
            "--repo",
            str(self.repo),
            "--result-repo",
            str(self.result_repo),
            "--append-request",
            "--commit-push",
            "--wait",
            "--wait-timeout-seconds",
            str(self.command_timeout_seconds),
            "ascendop-flow-v3-exchange",
            "--request-id",
            control_ref,
            "--output-subdir",
            f"flow-v3/{control_ref}",
            "--transport",
            _cli_transport(str(payload.get("target_transport_mode") or "")),
            "--client-work-dir",
            str(payload.get("remote_root") or ""),
            "--engine-root",
            str(payload.get("engine_root") or ""),
            "--target-node",
            str(payload.get("target_node_id") or ""),
            "--target-endpoint-id",
            str(payload.get("target_endpoint_id") or ""),
            "--target-environment-id",
            str(payload.get("target_environment_id") or ""),
            "--target-transport-mode",
            str(payload.get("target_transport_mode") or ""),
            "--registration-generation",
            str(payload.get("target_generation") or ""),
            "--endpoint-generation",
            str(payload.get("target_generation") or ""),
            "--action",
            action,
            "--logical-request-id",
            str(payload.get("request_id") or ""),
            "--attempt-id",
            str(payload.get("attempt_id") or ""),
        ]
        gateway = str(payload.get("target_gateway_id") or "")
        if gateway:
            command.extend(["--target-gateway-id", gateway])
        if action == "accept":
            command.extend(
                [
                    "--envelope",
                    str(payload["wire_envelope_path"]),
                    "--package-root",
                    str(payload["wire_package_root"]),
                ]
            )
        elif action == "ack":
            command.extend(["--receipt-id", receipt_id])
        return command

    def _environment(self) -> dict[str, str]:
        env = os.environ.copy()
        env["GITPARTNER_BRANCH"] = self.endpoint.control_channel
        env["GITPARTNER_RESULT_BRANCH"] = self.endpoint.result_channel
        env["GITPARTNER_REMOTE"] = self.remote
        env["GITPARTNER_ENDPOINT_ID"] = self.endpoint.endpoint_id
        token_file = self.repo / "api.txt"
        if token_file.is_file():
            env["GITPARTNER_TOKEN_FILE"] = str(token_file)
        apply_transport_runtime_environment(env, self.transport_runtime)
        return env

    def _materialize_result(
        self,
        payload: Mapping[str, Any],
        envelope: Mapping[str, Any],
        result: dict[str, Any],
        return_root: Path,
    ) -> dict[str, Any]:
        manifest = result.get("result_payload")
        if not isinstance(manifest, dict):
            raise ValueError("terminal Wire V3 result is missing result_payload")
        recover_manifest_parts_from_git(
            self.result_repo,
            return_root,
            manifest,
            ref=(
                f"refs/remotes/{self.remote}/"
                f"{self.endpoint.result_channel.strip('/')}"
            ),
        )
        destination = (
            self.return_root
            / str(payload["request_id"])
            / str(payload["attempt_id"])
        )
        materialized = materialize_payload(return_root, manifest, destination)
        terminal = _read_object(materialized / "terminal.json")
        state = _read_object(materialized / "state.json")
        engine_state = str(terminal.get("state") or result.get("state") or "")
        failure_domain = str(
            terminal.get("failure_domain") or state.get("failure_domain") or ""
        )
        success = engine_state == "completed"
        identity = transport_identity(dict(payload))
        artifact_relative = _display_path(materialized).relative_to(
            self.root
        ).as_posix()
        return {
            **identity,
            "schema": "ascendop.wire-v3-transport-result.v3",
            "receipt_id": deterministic_receipt_id(dict(payload)),
            "outcome": "success" if success else "failed",
            "error": "" if success else str(
                terminal.get("error")
                or state.get("error")
                or f"Engine terminal state {engine_state}"
            ),
            "failure_domain": failure_domain,
            "terminal_state": (
                "terminal-success"
                if success
                else "terminal-business-failure"
                if failure_domain == "business"
                else "terminal-infrastructure-failure"
            ),
            "workflow_ingest": True,
            "wire_envelope_digest": canonical_digest(envelope),
            "artifact_root": artifact_relative,
            "engine": result.get("engine", {}),
        }


class RoutedEndpointTransport:
    """Dispatch system probes and operator work to their explicit protocol."""

    def __init__(self, canary: GitPartnerCanaryTransport, wire: WireV3EndpointTransport):
        self.canary = canary
        self.wire = wire

    def _target(self, payload: Mapping[str, Any]) -> Any:
        return self.wire if bool(payload.get("workflow_ingest", True)) else self.canary

    def publish(self, payload: dict[str, Any]) -> DeliveryObservation:
        return self._target(payload).publish(payload)

    def publish_batch(self, payloads: list[dict[str, Any]]) -> list[DeliveryObservation]:
        return [self.publish(payload) for payload in payloads]

    def query(self, payload: dict[str, Any]) -> QueryObservation:
        return self._target(payload).query(payload)

    def query_batch(self, payloads: list[dict[str, Any]]) -> list[QueryObservation]:
        return [self.query(payload) for payload in payloads]

    def acknowledge(self, payload: dict[str, Any], returned: dict[str, Any]) -> bool:
        return self._target(payload).acknowledge(payload, returned)

    def close(self) -> None:
        self.canary.close()


def _acceptance(
    payload: Mapping[str, Any],
    envelope: Mapping[str, Any],
    observation: Mapping[str, Any],
) -> dict[str, Any]:
    receipt = observation.get("receipt", {})
    return {
        **transport_identity(dict(payload)),
        "acceptance_id": str(
            receipt.get("engine_receipt", {}).get("receipt_id")
            if isinstance(receipt, Mapping)
            and isinstance(receipt.get("engine_receipt"), Mapping)
            else ""
        )
        or canonical_digest(envelope),
        "remote_state": str(observation.get("state") or ""),
        "envelope_digest": canonical_digest(envelope),
    }


def _control_ref(
    payload: Mapping[str, Any],
    action: str,
    *,
    ordinal: int = 0,
    receipt_id: str = "",
) -> str:
    stable_payload = {
        str(key): value
        for key, value in payload.items()
        if not str(key).startswith("_")
    }
    identity = {
        "schema": "ascendop.flow-control-identity.v2",
        "action": action,
        "ordinal": max(0, ordinal),
        "payload": stable_payload,
        "receipt_id": receipt_id,
    }
    stable = json.dumps(
        identity,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    return f"flowv3-{action}-{hashlib.sha256(stable.encode()).hexdigest()[:24]}"


def _endpoint_error(
    completed: subprocess.CompletedProcess[str],
    observation: Mapping[str, Any],
) -> str:
    if str(observation.get("schema") or "") == "ascendop.flow.endpoint-nack.v3":
        return str(observation.get("error") or "endpoint rejected Wire V3")
    if completed.returncode:
        return str(completed.stderr or completed.stdout or "").strip() or (
            f"Wire V3 exchange exited {completed.returncode}"
        )
    return ""


def _failure(
    *,
    code: str,
    detail: str,
    retryable: bool,
    pre_publish: bool,
    result_visibility: str,
    domain: str = "transport",
) -> dict[str, Any]:
    return {
        "domain": domain,
        "code": code,
        "phase": "publish",
        "detail": detail,
        "retryable": retryable,
        "pre_publish": pre_publish,
        "result_visibility": result_visibility,
    }


def _is_pre_publish_cli_failure(
    completed: subprocess.CompletedProcess[str],
) -> bool:
    detail = str(completed.stderr or completed.stdout or "").lower()
    markers = (
        "error: unrecognized arguments",
        "error: argument kind: invalid choice",
        "the following arguments are required",
        "no module named limited_remote_partner",
    )
    return bool(completed.returncode) and any(marker in detail for marker in markers)


def _unique_json(root: Path, name: str) -> Path | None:
    if not root.is_dir():
        return None
    matches: dict[str, Path] = {}
    for path in root.rglob(name):
        if not path.is_file():
            continue
        try:
            value = _read_object(path)
        except (OSError, ValueError, json.JSONDecodeError):
            continue
        matches[canonical_digest(value)] = path
    if len(matches) > 1:
        raise ValueError(f"conflicting returned {name} documents")
    return next(iter(matches.values())) if matches else None


def _unique_result_directory(root: Path, name: str) -> Path | None:
    if not root.is_dir():
        return None
    matches: dict[str, Path] = {}
    for path in root.rglob(name):
        manifest = path / "RESULT_PAYLOAD.json"
        if path.is_dir() and manifest.is_file():
            value = _read_object(manifest)
            matches[canonical_digest(value)] = path
    if len(matches) > 1:
        raise ValueError(f"conflicting returned {name} directories")
    return next(iter(matches.values())) if matches else None


def _read_object(path: Path) -> dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(raw, dict):
        raise ValueError(f"JSON document is not an object: {path}")
    return raw


def _resolve_under(root: Path, value: str) -> Path:
    path = Path(value)
    return path.resolve() if path.is_absolute() else (root / path).resolve()


def _require_bounded(root: Path, path: Path) -> None:
    if path != root and root not in path.parents:
        raise ValueError(f"Wire V3 path escapes workspace: {path}")


def _display_path(path: Path) -> Path:
    value = str(path)
    if os.name == "nt" and value.startswith("\\\\?\\"):
        return Path(value[4:])
    return path


def _cli_transport(value: str) -> str:
    return "direct" if value in {"direct", "direct-git"} else "relay"


def _lane(action: str) -> str:
    return "ingress" if action == "accept" else "return" if action == "ack" else "watch"
