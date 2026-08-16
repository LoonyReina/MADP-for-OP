from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from ascendop_protocol.agent import (
    AGENT_OUTPUT_CONTRACT_SCHEMA,
    AGENT_OUTPUT_PROMOTION_RECEIPT_SCHEMA,
    AGENT_OUTPUT_SEAL_SCHEMA,
    validate_agent_output_contract,
    validate_solver_candidate_proposal,
)
from ascendop_protocol.workflow import (
    SOLVER_BLOCKER_CONTRACT_REVISION,
    SOLVER_DIAGNOSTIC_CAPABILITIES,
    SOLVER_DIAGNOSTIC_CAPABILITY_GENERATION,
    SOLVER_DIAGNOSTIC_DISPOSITIONS,
    SOLVER_DIAGNOSTIC_EXHAUSTION_SCOPE,
    SOLVER_DIAGNOSTIC_HOLD_OWNERS,
    SOLVER_DIAGNOSTIC_HOLD_RESUME_TRIGGERS,
    validate_solver_diagnostic_request,
)


OUTPUT_DIRECTORY = ".ascendop-output"
OUTPUT_CONTRACT_DOCUMENT_SCHEMA = "ascendop.agent-output-contract-document.v1"
OUTPUT_OBSOLETE_RECOVERY_RECEIPT_SCHEMA = (
    "ascendop.agent-output-obsolete-recovery-receipt.v1"
)


class AgentOutputError(RuntimeError):
    pass


def build_agent_output_contracts(
    root: Path,
    *,
    campaign: str,
    operator: str,
    role: str,
    gate_stage: str,
    next_command: str = "",
    action_descriptor: Mapping[str, Any] | None = None,
    source_digest: str = "",
    proposal_key: str = "",
) -> list[dict[str, Any]]:
    """Derive exact daemon-owned output slots from one effective gate."""

    if role != "solver":
        return []
    pending_reservation = solver_pending_reservation(
        operator=operator,
        gate_stage=gate_stage,
        action_descriptor=action_descriptor,
    )
    if gate_stage == "pending-needs-evidence":
        if not re.fullmatch(r"[0-9a-f]{64}", source_digest):
            raise AgentOutputError("pending evidence source digest is invalid")
        target, pending_version = _pending_evidence_target(
            root,
            operator=operator,
            next_command=next_command,
        )
        knowledge_paths = [
            f"reference/op_knowledge/{operator}/{name}"
            for name in (
                "case_coverage.md",
                "optimization_lessons.md",
                "hypothesis_backlog.md",
            )
        ]
        return [
            _contract(
                root,
                output_id="pending-evidence-repair",
                output_kind="pending-evidence-repair",
                isolated_path=f"{OUTPUT_DIRECTORY}/pending-version.md",
                canonical_path=target,
                required=True,
                must_change=True,
                max_bytes=256 * 1024,
                identity={
                    "campaign": campaign,
                    "operator": operator,
                    "pending_version": pending_version,
                    "execution_source_digest": source_digest,
                    "required_knowledge_paths": knowledge_paths,
                },
            )
        ]
    contracts: list[dict[str, Any]] = []
    if source_digest or proposal_key:
        if not re.fullmatch(r"[0-9a-f]{64}", source_digest):
            raise AgentOutputError("candidate proposal source digest is invalid")
        if not re.fullmatch(r"[0-9a-f]{64}", proposal_key):
            raise AgentOutputError("candidate proposal key is invalid")
        latest_result = _latest_result_identity(root, operator)
        case_version = (
            pending_reservation[1]
            if pending_reservation is not None
            else latest_result[1]
            if latest_result is not None
            else _latest_case_version(root, operator)
        )
        proposal_identity = {
            "campaign": campaign,
            "operator": operator,
            "case_version": case_version,
            "source_before_digest": source_digest,
            "proposal_key": proposal_key,
        }
        if pending_reservation is not None:
            proposal_identity["candidate_version"] = pending_reservation[0]
        contracts.append(
            _contract(
                root,
                output_id="solver-candidate-proposal",
                output_kind="solver-candidate-proposal",
                isolated_path=f"{OUTPUT_DIRECTORY}/candidate-proposal.json",
                canonical_path=(
                    root
                    / ".ascendop-work"
                    / "agent-proposals"
                    / operator
                    / f"{proposal_key}.json"
                ),
                required=False,
                must_change=False,
                max_bytes=256 * 1024,
                identity=proposal_identity,
            )
        )
    descriptor = dict(action_descriptor or {})
    if descriptor.get("operation") == "author-solver-blocker-contract":
        identity = dict(descriptor.get("identity") or {})
        if (
            str(identity.get("campaign") or "") != campaign
            or str(identity.get("operator") or "") != operator
        ):
            raise AgentOutputError("blocker contract descriptor identity mismatch")
        canonical = str(descriptor.get("canonical_path") or "")
        if not canonical:
            raise AgentOutputError("blocker contract descriptor has no target")
        return [
            _contract(
                root,
                output_id="solver-blocker",
                output_kind="solver-blocker",
                isolated_path=f"{OUTPUT_DIRECTORY}/solver-blocker.md",
                canonical_path=root / canonical,
                required=True,
                must_change=True,
                max_bytes=256 * 1024,
                identity=identity,
            )
        ]
    if descriptor.get("operation") == "author-solver-diagnostic-request":
        identity = dict(descriptor.get("identity") or {})
        if (
            str(identity.get("campaign") or "") != campaign
            or str(identity.get("operator") or "") != operator
        ):
            raise AgentOutputError("diagnostic output descriptor identity mismatch")
        canonical = str(descriptor.get("canonical_path") or "")
        if not canonical:
            raise AgentOutputError("diagnostic output descriptor has no target")
        return [
            _contract(
                root,
                output_id="solver-diagnostic-request",
                output_kind="solver-diagnostic-request",
                isolated_path=f"{OUTPUT_DIRECTORY}/solver-diagnostic-request.json",
                canonical_path=root / canonical,
                required=True,
                must_change=bool(descriptor.get("must_change")),
                max_bytes=256 * 1024,
                identity=identity,
            )
        ]
    if gate_stage == "diagnostic-evidence-failed":
        request = _latest_diagnostic_request(root, operator)
        target = request[0]
        payload = request[1]
        if str(payload["campaign"]) != campaign or str(payload["operator"]) != operator:
            raise AgentOutputError(
                "diagnostic request identity does not match the gate"
            )
        identity = {
            key: str(payload[key])
            for key in (
                "campaign",
                "operator",
                "case_version",
                "result_version",
                "blocker_generation",
            )
        }
        identity.update(
            {
                "target_test_version": str(payload["target"]["test_version"]),
                "target_source_sha256": str(payload["target"]["source_sha256"]),
            }
        )
        return [
            _contract(
                root,
                output_id="solver-diagnostic-request",
                output_kind="solver-diagnostic-request",
                isolated_path=f"{OUTPUT_DIRECTORY}/solver-diagnostic-request.json",
                canonical_path=target,
                required=True,
                must_change=True,
                max_bytes=256 * 1024,
                identity=identity,
            )
        ]

    latest_result = _latest_result_identity(root, operator)
    if latest_result is None:
        return contracts
    result_version, case_version = latest_result
    target = (
        root
        / "TestUtils"
        / "casegen"
        / operator
        / "case"
        / case_version
        / "SOLVER_BLOCKER.md"
    )
    return contracts + [
        _contract(
            root,
            output_id="solver-blocker",
            output_kind="solver-blocker",
            isolated_path=f"{OUTPUT_DIRECTORY}/solver-blocker.md",
            canonical_path=target,
            required=False,
            must_change=target.is_file(),
            max_bytes=256 * 1024,
            identity={
                "campaign": campaign,
                "operator": operator,
                "case_version": case_version,
                "result_version": result_version,
                "diagnostic_contract_revision": SOLVER_BLOCKER_CONTRACT_REVISION,
                "diagnostic_capability_generation": (
                    SOLVER_DIAGNOSTIC_CAPABILITY_GENERATION
                ),
            },
        )
    ]


def solver_pending_reservation(
    *,
    operator: str,
    gate_stage: str,
    action_descriptor: Mapping[str, Any] | None,
) -> tuple[str, str] | None:
    """Read one Solver pending reservation from the board's typed command."""

    if gate_stage != "needs-pending-candidate":
        return None
    descriptor = dict(action_descriptor or {})
    if not descriptor:
        return None
    if descriptor.get("operation") != "create-pending":
        raise AgentOutputError(
            "needs-pending-candidate has no typed create-pending reservation"
        )
    positional = descriptor.get("positional")
    options = descriptor.get("options")
    if not isinstance(positional, list) or len(positional) < 2:
        raise AgentOutputError("create-pending reservation positional identity is invalid")
    if not isinstance(options, Mapping):
        raise AgentOutputError("create-pending reservation options are invalid")
    reserved_operator = str(positional[0])
    candidate_version = str(positional[1])
    case_version = str(options.get("case_version") or "")
    if reserved_operator != operator:
        raise AgentOutputError("create-pending reservation operator mismatch")
    if not re.fullmatch(rf"{re.escape(operator)}_V\d+(?:_\d+)?", candidate_version):
        raise AgentOutputError("create-pending reservation candidate version is invalid")
    if not re.fullmatch(r"case_v\d+", case_version, re.IGNORECASE):
        raise AgentOutputError("create-pending reservation case version is invalid")
    return candidate_version, case_version.lower()


def agent_output_contracts_digest(contracts: list[Mapping[str, Any]]) -> str:
    return hashlib.sha256(
        json.dumps(
            contracts,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


class AgentOutputBroker:
    """Seal and atomically promote daemon-authorized non-source Agent outputs."""

    def __init__(self, root: Path, runs_root: Path) -> None:
        self.root = root.resolve()
        self.runs_root = runs_root.resolve()

    def stage(self, action: Mapping[str, Any], workspace: Path) -> Path:
        workspace = self._bounded_workspace(workspace)
        document = self._contract_document(action)
        for contract in document["output_contracts"]:
            target = self._canonical_target(str(contract["canonical_path"]))
            actual = _file_digest(target)
            before = contract["target_before"]
            expected = before["sha256"] if before["state"] == "present" else None
            if actual != expected:
                raise AgentOutputError(
                    f"canonical Agent output changed before delivery: {contract['output_id']}"
                )
            if contract["output_kind"] == "pending-evidence-repair" or (
                contract["output_kind"] == "solver-blocker"
                and bool(contract["required"])
                and before["state"] == "present"
            ):
                isolated = self._isolated_output(
                    workspace,
                    str(contract["isolated_path"]),
                )
                if isolated.exists():
                    if isolated.is_symlink() or not isolated.is_file():
                        raise AgentOutputError(
                            "pending evidence repair output is not a regular file"
                        )
                else:
                    isolated.parent.mkdir(parents=True, exist_ok=True)
                    self._replace_bytes(target.read_bytes(), isolated)
        path = workspace / OUTPUT_DIRECTORY / "CONTRACT.json"
        if path.exists():
            if path.is_symlink() or not path.is_file():
                raise AgentOutputError(
                    "Agent output contract path is not a regular file"
                )
            if path.read_bytes() != _json_bytes(document):
                raise AgentOutputError("staged Agent output contract was modified")
            return path
        self._write_json(path, document)
        return path

    def seal(self, action: Mapping[str, Any], workspace: Path) -> dict[str, Any]:
        workspace = self._bounded_workspace(workspace)
        document = self._contract_document(action)
        contract_path = workspace / OUTPUT_DIRECTORY / "CONTRACT.json"
        if (
            not contract_path.is_file()
            or contract_path.is_symlink()
            or contract_path.read_bytes() != _json_bytes(document)
        ):
            raise AgentOutputError("Agent output contract is missing or modified")
        outputs: list[dict[str, Any]] = []
        for contract in document["output_contracts"]:
            source = self._isolated_output(workspace, str(contract["isolated_path"]))
            if not source.exists():
                if contract["required"]:
                    raise AgentOutputError(
                        f"required Agent output is missing: {contract['output_id']}"
                    )
                continue
            if source.is_symlink() or not source.is_file():
                raise AgentOutputError(
                    "Agent output must be a regular non-symlink file"
                )
            size = source.stat().st_size
            if size <= 0 or size > int(contract["max_bytes"]):
                raise AgentOutputError(
                    f"Agent output size is invalid: {contract['output_id']}={size}"
                )
            payload = source.read_bytes()
            self._validate_payload(contract, payload)
            digest = hashlib.sha256(payload).hexdigest()
            before = contract["target_before"]
            changed = before["state"] == "absent" or digest != before["sha256"]
            if contract["must_change"] and not changed:
                raise AgentOutputError(
                    f"Agent output did not change: {contract['output_id']}"
                )
            if changed:
                outputs.append(
                    {
                        "output_id": str(contract["output_id"]),
                        "output_kind": str(contract["output_kind"]),
                        "isolated_path": str(contract["isolated_path"]),
                        "canonical_path": str(contract["canonical_path"]),
                        "sha256": digest,
                        "size_bytes": size,
                    }
                )
        seal = {
            "schema": AGENT_OUTPUT_SEAL_SCHEMA,
            "action_id": str(action["action_id"]),
            "iteration_id": str(action["iteration_id"]),
            "contracts_digest": _digest(document),
            "outputs": outputs,
            "created_at": _utc_now(),
        }
        self._write_json(
            self._run_root(str(action["action_id"])) / "output-seal.json", seal
        )
        return seal

    def promote(self, seal_path: Path) -> dict[str, Any]:
        seal_path = seal_path.resolve()
        if self.root not in seal_path.parents:
            raise AgentOutputError("Agent output seal is outside the workspace")
        seal = self._read_object(seal_path)
        if seal.get("schema") != AGENT_OUTPUT_SEAL_SCHEMA:
            raise AgentOutputError("unsupported Agent output seal")
        action_id = _token(str(seal.get("action_id") or ""), "action_id")
        run_root = self._run_root(action_id)
        if seal_path != (run_root / "output-seal.json").resolve():
            raise AgentOutputError("Agent output seal path does not match its action")
        stage = self._read_object(run_root / "output-contract.json")
        if _digest(stage) != str(seal.get("contracts_digest") or ""):
            raise AgentOutputError("Agent output seal does not match its contract")
        contracts = {
            str(item["output_id"]): validate_agent_output_contract(item)
            for item in stage.get("output_contracts", [])
        }
        outputs = seal.get("outputs")
        if not isinstance(outputs, list):
            raise AgentOutputError("Agent output seal outputs must be a list")
        if len({str(item.get("output_id") or "") for item in outputs}) != len(outputs):
            raise AgentOutputError("Agent output seal contains duplicate outputs")
        receipt_path = run_root / "output-promotion-receipt.json"
        if receipt_path.is_file():
            receipt = self._read_object(receipt_path)
            self._validate_receipt(receipt, seal, contracts)
            return receipt
        workspace = self._bounded_workspace(run_root / "workspace")
        for item in outputs:
            output_id = str(item.get("output_id") or "")
            contract = contracts.get(output_id)
            if contract is None:
                raise AgentOutputError(f"unknown sealed Agent output: {output_id}")
            source = self._isolated_output(workspace, str(contract["isolated_path"]))
            target = self._canonical_target(str(contract["canonical_path"]))
            if source.is_symlink() or not source.is_file():
                raise AgentOutputError(
                    f"sealed Agent output is unavailable: {output_id}"
                )
            payload = source.read_bytes()
            digest = hashlib.sha256(payload).hexdigest()
            if digest != item.get("sha256"):
                raise AgentOutputError(f"sealed Agent output changed: {output_id}")
            self._validate_payload(contract, payload)
            current_digest = _file_digest(target)
            before = contract["target_before"]
            expected_before = before["sha256"] if before["state"] == "present" else None
            if current_digest not in {expected_before, digest}:
                raise AgentOutputError(
                    f"canonical Agent output has an unknown value: {output_id}"
                )
            if current_digest != digest:
                target.parent.mkdir(parents=True, exist_ok=True)
                self._replace_bytes(payload, target)
        receipt = {
            "schema": AGENT_OUTPUT_PROMOTION_RECEIPT_SCHEMA,
            "action_id": action_id,
            "iteration_id": str(seal["iteration_id"]),
            "contracts_digest": str(seal["contracts_digest"]),
            "outputs": [dict(item) for item in outputs],
            "promoted_at": _utc_now(),
        }
        self._write_json(receipt_path, receipt)
        return receipt

    def recover_obsolete(
        self,
        stale_seal_path: Path,
        prior_seal_path: Path,
    ) -> dict[str, Any]:
        """Restore the exact target-before bytes of one proven stale promotion."""

        stale_seal, stale_root, stale_contracts = self._recovery_evidence(
            stale_seal_path
        )
        prior_seal, prior_root, prior_contracts = self._recovery_evidence(
            prior_seal_path
        )
        stale_action_id = str(stale_seal["action_id"])
        prior_action_id = str(prior_seal["action_id"])
        if stale_action_id == prior_action_id:
            raise AgentOutputError("obsolete recovery requires two distinct actions")
        receipt_path = stale_root / "output-obsolete-recovery-receipt.json"
        if receipt_path.is_file():
            receipt = self._read_object(receipt_path)
            self._validate_recovery_receipt(
                receipt,
                stale_action_id=stale_action_id,
                prior_action_id=prior_action_id,
            )
            return receipt

        prior_outputs = {
            (str(item["canonical_path"]), str(item["sha256"])): item
            for item in prior_seal["outputs"]
        }
        staged: list[tuple[Path, bytes, dict[str, Any]]] = []
        for stale_output in stale_seal["outputs"]:
            output_id = str(stale_output["output_id"])
            stale_contract = stale_contracts.get(output_id)
            if stale_contract is None:
                raise AgentOutputError(
                    f"stale recovery output has no contract: {output_id}"
                )
            before = stale_contract["target_before"]
            if before["state"] != "present":
                raise AgentOutputError(
                    f"stale recovery output has no prior target: {output_id}"
                )
            restored_digest = str(before["sha256"])
            canonical_path = str(stale_output["canonical_path"])
            prior_output = prior_outputs.get((canonical_path, restored_digest))
            if prior_output is None:
                raise AgentOutputError(
                    f"prior promotion does not prove target-before bytes: {output_id}"
                )
            prior_contract = prior_contracts.get(str(prior_output["output_id"]))
            if (
                prior_contract is None
                or str(prior_contract["canonical_path"]) != canonical_path
            ):
                raise AgentOutputError(
                    f"prior recovery contract does not match: {output_id}"
                )
            prior_workspace = self._bounded_workspace(prior_root / "workspace")
            source = self._isolated_output(
                prior_workspace,
                str(prior_contract["isolated_path"]),
            )
            payload = source.read_bytes()
            if hashlib.sha256(payload).hexdigest() != restored_digest:
                raise AgentOutputError(f"prior recovery payload changed: {output_id}")
            target = self._canonical_target(canonical_path)
            current_digest = _file_digest(target)
            stale_digest = str(stale_output["sha256"])
            if current_digest not in {stale_digest, restored_digest}:
                raise AgentOutputError(
                    f"obsolete recovery target has an unknown value: {output_id}"
                )
            staged.append(
                (
                    target,
                    payload,
                    {
                        "canonical_path": canonical_path,
                        "output_id": output_id,
                        "stale_sha256": stale_digest,
                        "restored_sha256": restored_digest,
                        "restored_from_action_id": prior_action_id,
                    },
                )
            )
        for target, payload, item in staged:
            if _file_digest(target) != item["restored_sha256"]:
                self._replace_bytes(payload, target)
        receipt = {
            "schema": OUTPUT_OBSOLETE_RECOVERY_RECEIPT_SCHEMA,
            "stale_action_id": stale_action_id,
            "prior_action_id": prior_action_id,
            "outputs": [item for _, _, item in staged],
            "recovered_at": _utc_now(),
        }
        self._write_json(receipt_path, receipt)
        return receipt

    def _recovery_evidence(
        self,
        seal_path: Path,
    ) -> tuple[dict[str, Any], Path, dict[str, dict[str, Any]]]:
        seal_path = seal_path.resolve()
        if self.root not in seal_path.parents:
            raise AgentOutputError("Agent recovery seal is outside the workspace")
        seal = self._read_object(seal_path)
        if seal.get("schema") != AGENT_OUTPUT_SEAL_SCHEMA:
            raise AgentOutputError("unsupported Agent recovery seal")
        action_id = _token(str(seal.get("action_id") or ""), "action_id")
        run_root = self._run_root(action_id)
        if seal_path != (run_root / "output-seal.json").resolve():
            raise AgentOutputError("Agent recovery seal path does not match its action")
        stage = self._read_object(run_root / "output-contract.json")
        if _digest(stage) != str(seal.get("contracts_digest") or ""):
            raise AgentOutputError("Agent recovery seal does not match its contract")
        outputs = seal.get("outputs")
        if not isinstance(outputs, list) or not outputs:
            raise AgentOutputError("Agent recovery seal has no outputs")
        receipt = self._read_object(run_root / "output-promotion-receipt.json")
        for field in ("action_id", "iteration_id", "contracts_digest", "outputs"):
            if receipt.get(field) != seal.get(field):
                raise AgentOutputError(
                    f"Agent recovery promotion evidence conflicts: {field}"
                )
        if receipt.get("schema") != AGENT_OUTPUT_PROMOTION_RECEIPT_SCHEMA:
            raise AgentOutputError("unsupported Agent recovery promotion receipt")
        contracts = {
            str(item["output_id"]): validate_agent_output_contract(item)
            for item in stage.get("output_contracts", [])
        }
        return seal, run_root, contracts

    def _validate_recovery_receipt(
        self,
        receipt: Mapping[str, Any],
        *,
        stale_action_id: str,
        prior_action_id: str,
    ) -> None:
        if receipt.get("schema") != OUTPUT_OBSOLETE_RECOVERY_RECEIPT_SCHEMA:
            raise AgentOutputError("unsupported obsolete output recovery receipt")
        if receipt.get("stale_action_id") != stale_action_id:
            raise AgentOutputError("obsolete output recovery action conflicts")
        if receipt.get("prior_action_id") != prior_action_id:
            raise AgentOutputError("obsolete output recovery source conflicts")
        outputs = receipt.get("outputs")
        if not isinstance(outputs, list) or not outputs:
            raise AgentOutputError("obsolete output recovery receipt has no outputs")
        for item in outputs:
            target = self._canonical_target(str(item["canonical_path"]))
            if _file_digest(target) != item.get("restored_sha256"):
                raise AgentOutputError(
                    "canonical Agent output changed after obsolete recovery"
                )

    def _contract_document(self, action: Mapping[str, Any]) -> dict[str, Any]:
        contracts = [
            validate_agent_output_contract(item)
            for item in action.get("output_contracts", [])
        ]
        document = {
            "schema": OUTPUT_CONTRACT_DOCUMENT_SCHEMA,
            "action_id": str(action["action_id"]),
            "iteration_id": str(action["iteration_id"]),
            "output_contracts": contracts,
        }
        run_root = self._run_root(str(action["action_id"]))
        path = run_root / "output-contract.json"
        if path.exists():
            if path.is_symlink() or path.read_bytes() != _json_bytes(document):
                raise AgentOutputError("persisted Agent output contract conflicts")
        else:
            self._write_json(path, document)
        return document

    def _validate_receipt(
        self,
        receipt: Mapping[str, Any],
        seal: Mapping[str, Any],
        contracts: Mapping[str, Mapping[str, Any]],
    ) -> None:
        for field in ("action_id", "iteration_id", "contracts_digest", "outputs"):
            if receipt.get(field) != seal.get(field):
                raise AgentOutputError(f"Agent output receipt conflicts: {field}")
        if receipt.get("schema") != AGENT_OUTPUT_PROMOTION_RECEIPT_SCHEMA:
            raise AgentOutputError("unsupported Agent output promotion receipt")
        for item in receipt.get("outputs", []):
            contract = contracts.get(str(item.get("output_id") or ""))
            if contract is None:
                raise AgentOutputError("Agent output receipt has an unknown output")
            target = self._canonical_target(str(contract["canonical_path"]))
            if _file_digest(target) != item.get("sha256"):
                raise AgentOutputError("canonical Agent output changed after promotion")

    def _validate_payload(self, contract: Mapping[str, Any], payload: bytes) -> None:
        kind = str(contract["output_kind"])
        identity = dict(contract["identity"])
        if kind == "pending-evidence-repair":
            try:
                text = payload.decode("utf-8-sig")
            except UnicodeDecodeError as exc:
                raise AgentOutputError(
                    "pending VERSION evidence must be UTF-8"
                ) from exc
            if "\x00" in text:
                raise AgentOutputError("pending VERSION evidence contains NUL bytes")
            expected_heading = f"# {identity['operator']} {identity['pending_version']}"
            if not re.search(rf"(?m)^{re.escape(expected_heading)}\s*$", text):
                raise AgentOutputError("pending VERSION evidence changed its heading")
            fields = {
                key.lower(): value.strip()
                for key, value in re.findall(
                    r"(?m)^(Owner|Season|Source lineage):\s*(.+?)\s*$",
                    text,
                )
            }
            expected = {
                "owner": "solver",
                "season": str(identity["campaign"]),
                "source lineage": "SOURCE_LINEAGE.json",
            }
            if fields != expected:
                raise AgentOutputError(
                    "pending VERSION evidence changed immutable ownership fields"
                )
            for marker in (
                "Observed signal",
                "Primary hypothesis",
                "Counter-hypothesis",
                "Router gap",
                "Consulted evidence",
                "Optimization method decision",
                "Shared knowledge decision",
            ):
                if not _markdown_marker_has_evidence(text, marker):
                    raise AgentOutputError(
                        f"pending VERSION evidence is missing concrete {marker}: evidence"
                    )
            return
        if kind == "solver-diagnostic-request":
            try:
                value = json.loads(payload.decode("utf-8-sig"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise AgentOutputError(
                    "Solver diagnostic output is invalid JSON"
                ) from exc
            try:
                value = validate_solver_diagnostic_request(value)
            except ValueError as exc:
                raise AgentOutputError(
                    f"Solver diagnostic output is invalid: {exc}"
                ) from exc
            for field in (
                "campaign",
                "operator",
                "case_version",
                "result_version",
                "blocker_generation",
            ):
                if str(value[field]) != str(identity[field]):
                    raise AgentOutputError(
                        f"Solver diagnostic output changed immutable identity: {field}"
                    )
            if str(value["target"]["test_version"]) != str(
                identity["target_test_version"]
            ) or str(value["target"]["source_sha256"]) != str(
                identity["target_source_sha256"]
            ):
                raise AgentOutputError("Solver diagnostic output changed its target")
            return
        if kind == "solver-blocker":
            try:
                text = payload.decode("utf-8-sig")
            except UnicodeDecodeError as exc:
                raise AgentOutputError("Solver blocker must be UTF-8") from exc
            if "\x00" in text:
                raise AgentOutputError("Solver blocker contains NUL bytes")
            fields = {
                key.lower(): value.strip()
                for key, value in re.findall(
                    r"(?m)^(Status|Operator|Case version|Result version):\s*(.+?)\s*$",
                    text,
                )
            }
            expected = {
                "status": "active",
                "operator": str(identity["operator"]),
                "case version": str(identity["case_version"]),
                "result version": str(identity["result_version"]),
            }
            if fields != expected:
                raise AgentOutputError(
                    "Solver blocker identity is incomplete or mismatched"
                )
            contract_revision = re.search(
                r"(?m)^Diagnostic contract revision:\s*(.+?)\s*$",
                text,
            )
            expected_revision = str(identity["diagnostic_contract_revision"])
            if (
                not contract_revision
                or contract_revision.group(1).strip() != expected_revision
            ):
                raise AgentOutputError(
                    "Solver blocker diagnostic contract revision is missing or mismatched"
                )
            for marker in (
                "Observed signal",
                "Primary hypothesis",
                "Counter-hypothesis",
                "Required evidence",
                "Consulted evidence",
            ):
                if not _markdown_marker_has_evidence(text, marker):
                    raise AgentOutputError(
                        f"Solver blocker is missing concrete {marker}: evidence"
                    )
            diagnostic = re.search(
                r"(?m)^Diagnostic operation:\s*(.+?)\s*$",
                text,
            )
            disposition = re.search(
                r"(?m)^Diagnostic disposition:\s*(.+?)\s*$",
                text,
            )
            if bool(diagnostic) == bool(disposition):
                raise AgentOutputError(
                    "Solver blocker must declare exactly one diagnostic operation or disposition"
                )
            if diagnostic and diagnostic.group(1).strip() not in {
                "diagnostic-profile",
                "diagnostic-correctness-replay",
            }:
                raise AgentOutputError(
                    "Solver blocker Diagnostic operation is not registered"
                )
            disposition_value = disposition.group(1).strip() if disposition else ""
            if disposition_value and disposition_value not in (
                SOLVER_DIAGNOSTIC_DISPOSITIONS
            ):
                raise AgentOutputError(
                    "Solver blocker Diagnostic disposition is not registered"
                )
            hold_fields = {
                key.lower(): value.strip()
                for key, value in re.findall(
                    r"(?m)^(Hold owner|Hold resume trigger|Hold required capability|"
                    r"Hold evaluated capability generation):\s*(.+?)\s*$",
                    text,
                )
            }
            if diagnostic and hold_fields:
                raise AgentOutputError(
                    "Solver diagnostic operation cannot also declare durable-hold fields"
                )
            exhaustion_fields = {
                key.lower(): value.strip()
                for key, value in re.findall(
                    r"(?m)^(Exhaustion scope|Exhaustion evaluated capability generation):"
                    r"\s*(.+?)\s*$",
                    text,
                )
            }
            if diagnostic and exhaustion_fields:
                raise AgentOutputError(
                    "Solver diagnostic operation cannot also declare exhaustion fields"
                )
            if disposition_value == "durable-hold":
                if exhaustion_fields:
                    raise AgentOutputError(
                        "Solver durable-hold cannot also declare exhaustion fields"
                    )
                if set(hold_fields) != {
                    "hold owner",
                    "hold resume trigger",
                    "hold required capability",
                    "hold evaluated capability generation",
                }:
                    raise AgentOutputError(
                        "Solver durable-hold responsibility fields are incomplete"
                    )
                if hold_fields["hold owner"] not in SOLVER_DIAGNOSTIC_HOLD_OWNERS:
                    raise AgentOutputError(
                        "Solver durable-hold owner is not registered"
                    )
                if (
                    hold_fields["hold resume trigger"]
                    not in SOLVER_DIAGNOSTIC_HOLD_RESUME_TRIGGERS
                ):
                    raise AgentOutputError(
                        "Solver durable-hold resume trigger is not registered"
                    )
                if not re.fullmatch(
                    r"[a-z][a-z0-9]*(?:-[a-z0-9]+)*",
                    hold_fields["hold required capability"],
                ):
                    raise AgentOutputError(
                        "Solver durable-hold required capability is invalid"
                    )
                owner = hold_fields["hold owner"]
                trigger = hold_fields["hold resume trigger"]
                capability = hold_fields["hold required capability"]
                if owner == "daemon-harness" and (
                    trigger != "daemon-evidence-transition"
                    or capability not in SOLVER_DIAGNOSTIC_CAPABILITIES
                ):
                    raise AgentOutputError(
                        "Solver daemon-harness hold is not an executable registered transition"
                    )
                if owner == "external" and trigger != "external-evidence-transition":
                    raise AgentOutputError(
                        "Solver external hold must use external-evidence-transition"
                    )
                if hold_fields["hold evaluated capability generation"] != str(
                    identity["diagnostic_capability_generation"]
                ):
                    raise AgentOutputError(
                        "Solver durable-hold capability generation is mismatched"
                    )
            elif disposition_value == "evidence-exhausted":
                if hold_fields:
                    raise AgentOutputError(
                        "Solver evidence-exhausted cannot assign a hold owner"
                    )
                if exhaustion_fields != {
                    "exhaustion scope": SOLVER_DIAGNOSTIC_EXHAUSTION_SCOPE,
                    "exhaustion evaluated capability generation": str(
                        identity["diagnostic_capability_generation"]
                    ),
                }:
                    raise AgentOutputError(
                        "Solver evidence-exhausted fields are incomplete or mismatched"
                    )
            return
        if kind == "solver-candidate-proposal":
            try:
                value = json.loads(payload.decode("utf-8-sig"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise AgentOutputError(
                    "Solver candidate proposal is invalid JSON"
                ) from exc
            try:
                value = validate_solver_candidate_proposal(value)
            except ValueError as exc:
                raise AgentOutputError(
                    f"Solver candidate proposal is invalid: {exc}"
                ) from exc
            for field in (
                "campaign",
                "operator",
                "case_version",
                "source_before_digest",
            ):
                if str(value[field]) != str(identity[field]):
                    raise AgentOutputError(
                        f"Solver candidate proposal changed immutable identity: {field}"
                    )
            return
        raise AgentOutputError(f"unsupported Agent output kind: {kind}")

    def _isolated_output(self, workspace: Path, relative: str) -> Path:
        raw = workspace / relative
        if raw.is_symlink():
            raise AgentOutputError("Agent output cannot be a symlink")
        path = raw.resolve()
        output_root = (workspace / OUTPUT_DIRECTORY).resolve()
        if output_root not in path.parents:
            raise AgentOutputError("Agent output path escaped its isolated directory")
        return path

    def _canonical_target(self, relative: str) -> Path:
        raw = self.root / relative
        if raw.is_symlink():
            raise AgentOutputError("canonical Agent output cannot be a symlink")
        path = raw.resolve()
        if self.root not in path.parents:
            raise AgentOutputError("canonical Agent output escaped the workspace root")
        if path.exists() and (path.is_symlink() or not path.is_file()):
            raise AgentOutputError("canonical Agent output is not a regular file")
        return path

    def _bounded_workspace(self, workspace: Path) -> Path:
        path = workspace.resolve()
        if self.runs_root not in path.parents or not path.is_dir():
            raise AgentOutputError("Agent output workspace is not action-scoped")
        return path

    def _run_root(self, action_id: str) -> Path:
        path = (self.runs_root / _token(action_id, "action_id")).resolve()
        if path.parent != self.runs_root:
            raise AgentOutputError("Agent output run root escaped")
        path.mkdir(parents=True, exist_ok=True)
        return path

    @staticmethod
    def _write_json(path: Path, value: Mapping[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        AgentOutputBroker._replace_bytes(_json_bytes(value), path)

    @staticmethod
    def _replace_bytes(payload: bytes, target: Path) -> None:
        with tempfile.NamedTemporaryFile(dir=target.parent, delete=False) as handle:
            handle.write(payload)
            temporary = Path(handle.name)
        try:
            os.replace(temporary, target)
        finally:
            if temporary.exists():
                temporary.unlink()

    @staticmethod
    def _read_object(path: Path) -> dict[str, Any]:
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise AgentOutputError(f"invalid Agent output artifact: {path}") from exc
        if not isinstance(value, dict):
            raise AgentOutputError("Agent output artifact must be an object")
        return value


def _contract(
    root: Path,
    *,
    output_id: str,
    output_kind: str,
    isolated_path: str,
    canonical_path: Path,
    required: bool,
    must_change: bool,
    max_bytes: int,
    identity: Mapping[str, Any],
) -> dict[str, Any]:
    target = canonical_path.resolve()
    root = root.resolve()
    if root not in target.parents:
        raise AgentOutputError("Agent output target escaped the workspace root")
    if target.exists() and (target.is_symlink() or not target.is_file()):
        raise AgentOutputError("Agent output target must be a regular file")
    before = _file_digest(target)
    return validate_agent_output_contract(
        {
            "schema": AGENT_OUTPUT_CONTRACT_SCHEMA,
            "output_id": output_id,
            "output_kind": output_kind,
            "isolated_path": isolated_path,
            "canonical_path": target.relative_to(root).as_posix(),
            "required": required,
            "must_change": must_change,
            "max_bytes": max_bytes,
            "target_before": {
                "state": "present" if before else "absent",
                "sha256": before or "",
            },
            "identity": dict(identity),
        }
    )


def _latest_diagnostic_request(
    root: Path, operator: str
) -> tuple[Path, dict[str, Any]]:
    base = root / "TestUtils" / "casegen" / operator / "case"
    candidates = list(base.glob("case_v*/SOLVER_DIAGNOSTIC_REQUEST.json"))
    if not candidates:
        raise AgentOutputError(f"{operator} has no diagnostic request to revise")
    target = max(
        candidates, key=lambda path: (path.stat().st_mtime_ns, path.as_posix())
    )
    try:
        payload = json.loads(target.read_text(encoding="utf-8-sig"))
        payload = validate_solver_diagnostic_request(payload)
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        raise AgentOutputError(
            f"latest {operator} diagnostic request is invalid"
        ) from exc
    return target, payload


def _pending_evidence_target(
    root: Path,
    *,
    operator: str,
    next_command: str,
) -> tuple[Path, str]:
    normalized = str(next_command).replace("\\", "/")
    match = re.search(
        rf"(?<![A-Za-z0-9._/-])"
        rf"(TestUtils/pending/{re.escape(operator)}/"
        rf"({re.escape(operator)}_V[A-Za-z0-9._-]+)/VERSION\.md)",
        normalized,
        re.IGNORECASE,
    )
    if not match:
        raise AgentOutputError(
            f"{operator} pending evidence gate has no exact VERSION.md target"
        )
    relative = Path(match.group(1))
    pending_version = match.group(2)
    target = (root / relative).resolve()
    expected_parent = (
        root / "TestUtils" / "pending" / operator / pending_version
    ).resolve()
    if target.parent != expected_parent or target.name != "VERSION.md":
        raise AgentOutputError(
            "pending evidence target escaped its candidate directory"
        )
    if target.is_symlink() or not target.is_file():
        raise AgentOutputError(
            "pending evidence target must be an existing regular file"
        )
    return target, pending_version


def _markdown_marker_has_evidence(text: str, marker: str) -> bool:
    match = re.search(
        rf"(?ms)^{re.escape(marker)}:\s*(.*?)"
        rf"(?=^[A-Z][A-Za-z ]+:\s*|\Z)",
        text,
    )
    return bool(match and match.group(1).strip())


def _latest_result_identity(root: Path, operator: str) -> tuple[str, str] | None:
    result_root = root / "operators_testresult" / operator
    candidates = list(result_root.glob("*/RESULT.md")) if result_root.is_dir() else []
    if not candidates:
        return None
    result = max(
        candidates, key=lambda path: (path.stat().st_mtime_ns, path.as_posix())
    )
    text = result.read_text(encoding="utf-8-sig", errors="strict")
    match = re.search(r"(?m)^Case version:\s*`?([^`\s]+)`?\s*$", text)
    if not match:
        raise AgentOutputError(f"latest {operator} RESULT has no case identity")
    return result.parent.name, match.group(1)


def _latest_case_version(root: Path, operator: str) -> str:
    base = root / "TestUtils" / "casegen" / operator / "case"
    versions = [
        (int(match.group(1)), path.name)
        for path in base.glob("case_v*")
        if path.is_dir()
        and (match := re.fullmatch(r"case_v(\d+)", path.name, re.IGNORECASE))
    ]
    if not versions:
        raise AgentOutputError(
            f"{operator} has no case version for a candidate proposal"
        )
    return max(versions)[1]


def _file_digest(path: Path) -> str | None:
    if not path.exists():
        return None
    if path.is_symlink() or not path.is_file():
        raise AgentOutputError(f"expected regular file: {path}")
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _json_bytes(value: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")


def _digest(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(_json_bytes(value)).hexdigest()


def _token(value: str, field: str) -> str:
    if not value or any(
        char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"
        for char in value
    ):
        raise AgentOutputError(f"{field} must be a safe token")
    return value


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
