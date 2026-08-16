from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Mapping

from ascendop_daemon.core.atomic_io import write_json_atomic, write_text_atomic
from ascendop_daemon.workflow.case_bundle_contract import (
    require_case_bundle_contract,
)
from ascendop_daemon.workflow.casegen_evidence import casegen_schema_issues


CASE_SOURCE_CONTRACT_SCHEMA = "ascendop.casegen-source-contract.v1"
CASE_PROMOTION_RECEIPT_SCHEMA = "ascendop.agent-case-promotion-receipt.v1"
TASK_CASE_BINDING_SCHEMA = "ascendop.task-case-binding.v1"
CASE_MATERIALIZER_GENERATION = "agent-case-materializer-v3"
DUAL_BAND_PROTOCOL = "sixteen_case_dual_band_v1"
DUAL_BAND_TIERS = (
    ("1k", 1_000, "small", 25.0),
    ("10k", 10_000, "small", 2.5),
    ("50k", 50_000, "small", 0.5),
    ("250k", 250_000, "small", 0.1),
    ("1M", 1_000_000, "large", 10.0),
    ("10M", 10_000_000, "large", 1.0),
    ("50M", 50_000_000, "large", 0.2),
    ("100M", 100_000_000, "large", 0.1),
)


class CaseProposalError(ValueError):
    pass


class CaseProposalPublisher:
    """Mechanically materialize a sealed Tester-authored case specification."""

    def __init__(self, root: Path) -> None:
        self.root = root.resolve()

    def publish(
        self,
        action: Mapping[str, Any],
        source_receipt: Mapping[str, Any],
    ) -> dict[str, Any]:
        action_id = str(action.get("action_id") or "")
        if str(action.get("role") or "") != "tester":
            raise CaseProposalError("case promotion requires a Tester action")
        if (
            source_receipt.get("action_id") != action_id
            or source_receipt.get("source_after_digest") == ""
        ):
            raise CaseProposalError("case promotion source receipt is inconsistent")
        run_root = self.root / ".ascendop-work" / "agent-runs" / action_id
        receipt_path = run_root / "case-promotion-receipt.json"
        if receipt_path.is_file():
            receipt = self._read_object(receipt_path)
            self._validate_receipt(receipt, action, source_receipt)
            target = (self.root / str(receipt["case_dir"])).resolve()
            cases = self._read_list(target / "cases.json")
            task_case = self._sync_task_case_sidecar(action, cases, target)
            contract_result = require_case_bundle_contract(
                task_case,
                target,
                op=str(action["candidate_identity"]["display_name"]),
                case_version=str(action["candidate_version"]),
            )
            receipt = {
                **receipt,
                "materializer_generation": CASE_MATERIALIZER_GENERATION,
                "task_case_sidecar": (
                    task_case / "case_specs.json"
                ).relative_to(self.root).as_posix(),
                "task_case_sidecar_sha256": (
                    contract_result.task_case_sidecar_sha256
                ),
                "task_case_binding": (
                    task_case / "CASE_BINDING.json"
                ).relative_to(self.root).as_posix(),
                "task_case_binding_sha256": hashlib.sha256(
                    (task_case / "CASE_BINDING.json").read_bytes()
                ).hexdigest(),
            }
            write_json_atomic(
                receipt_path,
                receipt,
                ensure_ascii=True,
                sort_keys=True,
            )
            return receipt

        source_root = self._bounded_directory(str(action["origin_workspace"]))
        contract_path = source_root / "case_contract.json"
        contract = self._read_object(contract_path)
        self._validate_contract(contract, action)
        cases = self._emit_cases(source_root, contract)
        readme = (source_root / "README.md").read_text(
            encoding="utf-8", errors="strict"
        )
        plan = self._plan_text(readme, contract)
        meta = self._meta(contract, cases, action, readme)

        operator = str(contract["operator"])
        case_version = str(contract["case_version"])
        case_root = self.root / "TestUtils" / "casegen" / operator / "case"
        case_root.mkdir(parents=True, exist_ok=True)
        target = case_root / case_version
        temp_parent = Path(tempfile.mkdtemp(prefix=".materialize-", dir=case_root))
        stage = temp_parent / case_version
        stage.mkdir()
        try:
            write_json_atomic(stage / "cases.json", cases, ensure_ascii=True)
            write_json_atomic(stage / "meta.json", meta, ensure_ascii=True)
            for case in cases:
                bucket = str(case["bucket"])
                write_json_atomic(
                    stage / f"case_{bucket}.json",
                    case,
                    ensure_ascii=True,
                )
            write_text_atomic(stage / "CASEGEN_PLAN.md", plan)
            write_text_atomic(stage / "MODEL_AUDIT.md", plan)
            for name in ("README.md", "case_specs.py", "test_op.py", "case_contract.json"):
                source = source_root / name
                if not source.is_file() or source.is_symlink():
                    raise CaseProposalError(f"Tester case source is missing: {name}")
                shutil.copy2(source, stage / name)

            structural_issues = casegen_schema_issues(stage, operator)
            if structural_issues:
                raise CaseProposalError(
                    "materialized Tester case is invalid: "
                    + ", ".join(structural_issues)
                )
            stage_digest = _directory_digest(stage)
            if target.exists():
                if not target.is_dir() or _directory_digest(target) != stage_digest:
                    raise CaseProposalError(
                        f"case target already exists with different content: {target}"
                    )
                shutil.rmtree(stage)
            else:
                os.replace(stage, target)
        finally:
            shutil.rmtree(temp_parent, ignore_errors=True)

        coverage_path = self._project_coverage(contract, action_id)
        task_case = self._sync_task_case_sidecar(action, cases, target)
        contract_result = require_case_bundle_contract(
            task_case,
            target,
            op=operator,
            case_version=case_version,
        )
        issues = casegen_schema_issues(target, operator, root=self.root)
        if issues:
            raise CaseProposalError(
                "published Tester case evidence is invalid: " + ", ".join(issues)
            )
        content_digest = _directory_digest(target)
        receipt = {
            "schema": CASE_PROMOTION_RECEIPT_SCHEMA,
            "action_id": action_id,
            "candidate_version": case_version,
            "source_after_digest": str(source_receipt["source_after_digest"]),
            "case_dir": target.relative_to(self.root).as_posix(),
            "case_content_digest": content_digest,
            "case_count": len(cases),
            "coverage_path": coverage_path,
            "materializer_generation": CASE_MATERIALIZER_GENERATION,
            "task_case_sidecar": (
                task_case / "case_specs.json"
            ).relative_to(self.root).as_posix(),
            "task_case_sidecar_sha256": contract_result.task_case_sidecar_sha256,
            "task_case_binding": (
                task_case / "CASE_BINDING.json"
            ).relative_to(self.root).as_posix(),
            "task_case_binding_sha256": hashlib.sha256(
                (task_case / "CASE_BINDING.json").read_bytes()
            ).hexdigest(),
            "materialized_at": str(action.get("created_at") or "unknown"),
        }
        write_json_atomic(receipt_path, receipt, ensure_ascii=True, sort_keys=True)
        return receipt

    def replay_source_receipt(
        self,
        action: Mapping[str, Any],
    ) -> dict[str, Any] | None:
        """Return the sealed source receipt for an already published case."""

        action_id = str(action.get("action_id") or "")
        run_root = self.root / ".ascendop-work" / "agent-runs" / action_id
        source_path = run_root / "promotion-receipt.json"
        case_path = run_root / "case-promotion-receipt.json"
        if not source_path.is_file() or not case_path.is_file():
            return None
        try:
            source_receipt = self._read_object(source_path)
            case_receipt = self._read_object(case_path)
            if (
                source_receipt.get("action_id") != action_id
                or source_receipt.get("candidate_version")
                != action.get("candidate_version")
                or not source_receipt.get("source_after_digest")
            ):
                return None
            self._validate_receipt(case_receipt, action, source_receipt)
        except CaseProposalError:
            return None
        return source_receipt

    def _validate_contract(
        self,
        contract: Mapping[str, Any],
        action: Mapping[str, Any],
    ) -> None:
        display_name = str(
            action.get("candidate_identity", {}).get("display_name")
            or action.get("operator_id")
            or ""
        )
        expected = {
            "schema": CASE_SOURCE_CONTRACT_SCHEMA,
            "action_id": str(action["action_id"]),
            "board_digest": str(action["board_digest"]),
            "campaign": str(action["campaign"]),
            "operator": display_name,
            "role": "tester",
            "case_version": str(action["candidate_version"]),
        }
        mismatches = [
            field for field, value in expected.items() if contract.get(field) != value
        ]
        if mismatches:
            raise CaseProposalError(
                "Tester case contract identity mismatch: " + ", ".join(mismatches)
            )
        if contract.get("case_protocol") != DUAL_BAND_PROTOCOL:
            raise CaseProposalError("unsupported Tester case protocol")
        if contract.get("case_count") != 16 or contract.get("max_usage") != 40:
            raise CaseProposalError("dual-band Tester case lifetime is invalid")
        expected_tiers = [size for _tier, size, _group, _weight in DUAL_BAND_TIERS for _ in range(2)]
        if contract.get("scoring_tiers") != expected_tiers:
            raise CaseProposalError("dual-band scoring tiers are invalid")
        expected_weights = [
            weight / 2.0
            for _tier, _size, _group, weight in DUAL_BAND_TIERS
            for _ in range(2)
        ]
        if contract.get("weights") != expected_weights:
            raise CaseProposalError("dual-band scoring weights are invalid")
        source_identity = contract.get("source_identity")
        action_identity = action.get("candidate_identity")
        if not isinstance(source_identity, Mapping) or not isinstance(
            action_identity, Mapping
        ):
            raise CaseProposalError("Tester source identity is missing")
        if source_identity.get("execution_source_digest") != action_identity.get(
            "execution_source_digest"
        ):
            raise CaseProposalError("Tester execution source identity changed")

    def _emit_cases(
        self, source_root: Path, contract: Mapping[str, Any]
    ) -> list[dict[str, Any]]:
        spec = source_root / "case_specs.py"
        check = source_root / "test_op.py"
        for command in (
            [sys.executable, str(spec), "--emit"],
            [sys.executable, str(check), "--self-check"],
        ):
            completed = subprocess.run(
                command,
                cwd=source_root,
                check=False,
                capture_output=True,
                text=True,
                timeout=60,
                creationflags=int(getattr(subprocess, "CREATE_NO_WINDOW", 0)),
            )
            if completed.returncode != 0:
                raise CaseProposalError(
                    f"Tester case source validation failed: {Path(command[1]).name}: "
                    + completed.stderr[-1000:]
                )
            if command[-1] != "--emit":
                continue
            try:
                raw_cases = json.loads(completed.stdout)
            except json.JSONDecodeError as exc:
                raise CaseProposalError("case_specs.py did not emit JSON") from exc
        if not isinstance(raw_cases, list) or len(raw_cases) != 16:
            raise CaseProposalError("case_specs.py must emit exactly 16 cases")
        cases = [dict(item) if isinstance(item, Mapping) else {} for item in raw_cases]
        tiers = list(contract["scoring_tiers"])
        weights = list(contract["weights"])
        for index, case in enumerate(cases):
            protocol = case.get("protocol")
            if not isinstance(protocol, Mapping):
                raise CaseProposalError(f"case{index + 1} protocol is missing")
            expected = {
                "bucket": f"b{index}",
                "generated_case": f"case{index + 1}",
                "param_count": tiers[index],
            }
            if any(case.get(key) != value for key, value in expected.items()):
                raise CaseProposalError(f"case{index + 1} identity is invalid")
            if (
                protocol.get("case_protocol") != contract["case_protocol"]
                or protocol.get("case_version") != contract["case_version"]
                or protocol.get("weight") != weights[index]
            ):
                raise CaseProposalError(f"case{index + 1} scoring contract is invalid")
        return cases

    def _meta(
        self,
        contract: Mapping[str, Any],
        cases: list[dict[str, Any]],
        action: Mapping[str, Any],
        readme: str,
    ) -> dict[str, Any]:
        weights = {
            f"case{index + 1}": float(value)
            for index, value in enumerate(contract["weights"])
        }
        groups: dict[str, dict[str, Any]] = {}
        for name, ids in {"small": range(1, 9), "large": range(9, 17)}.items():
            groups[name] = {
                "formula": " + ".join(f"case{case_id}*{weights[f'case{case_id}']:g}" for case_id in ids),
                "case_ids": list(ids),
                "weights": {f"case{case_id}": weights[f"case{case_id}"] for case_id in ids},
            }
        tier_by_size = {
            size: (tier, group, tier_weight)
            for tier, size, group, tier_weight in DUAL_BAND_TIERS
        }
        buckets = []
        for index, case in enumerate(cases):
            size = int(case["param_count"])
            tier, group, tier_weight = tier_by_size[size]
            buckets.append(
                {
                    "id": str(case["bucket"]),
                    "generated_case": str(case["generated_case"]),
                    "param_count": str(case.get("bucket_range") or size),
                    "target_param_count": size,
                    "dtype": str(case.get("dtype") or ""),
                    "case_file": f"case_{case['bucket']}.json",
                    "tier": tier,
                    "score_group": group,
                    "tier_weight": tier_weight,
                    "pair_member": "A" if index % 2 == 0 else "B",
                }
            )
        hypothesis = dict(contract.get("hypothesis") or {})
        coverage = dict(contract.get("coverage_delta") or {})
        official_match = re.search(r"Official constraints:\s*`([^`]+)`", readme)
        official_path = official_match.group(1) if official_match else "Tester-authored README evidence"
        return {
            "op": str(contract["operator"]),
            "case_version": str(contract["case_version"]),
            "usage_count": 0,
            "max_usage": 40,
            "usage_history": [],
            "execution_entrypoint": "fused official task_case/test_op.py",
            "case_protocol": DUAL_BAND_PROTOCOL,
            "case_protocol_note": "Exactly two orthogonal cases at each registered scoring tier.",
            "case_selection_strategy": "Tester-authored case_specs.py",
            "model_audit": "MODEL_AUDIT.md",
            "official_case_policy": "drop_official_case1",
            "default_correctness_range": str(contract["correctness_range"]),
            "default_perf_case_range": str(contract["performance_range"]),
            "correctness_repetitions": 1,
            "performance_samples_per_case": 50,
            "generated_at": str(action.get("created_at") or "unknown"),
            "generator": "sealed Tester Agent case source",
            "generator_seed": int(cases[0].get("generator_seed") or 0),
            "case_contract": str(hypothesis.get("statement") or ""),
            "perf_time_unit": "us",
            "perf_time_unit_source": "msprof Task Duration(us), no scaling",
            "perf_weighted_time_formula": "small_weighted_time + large_weighted_time",
            "perf_weighted_time_weights": weights,
            "case_diversity_contract": "official_failure_cluster_v1",
            "diversity_audit": {
                "official_checkpoint_path": official_path,
                "predicted_official_transition": str(hypothesis.get("statement") or ""),
                "strongest_counter_hypothesis": str(hypothesis.get("falsifier") or ""),
                "novel_dimensions": [
                    str(coverage.get("overlap_avoided") or ""),
                    str(coverage.get("same_tier_pair_proof") or ""),
                ],
                "template_profile": "cannjudge-cann90-official-template-v1",
            },
            "perf_score_groups": groups,
            "release_policy": {
                "balanced_improvement_ratio": 0.05,
                "lead_improvement_ratio": 0.08,
                "other_min_improvement_ratio": 0.0,
            },
            "cross_case_regression_budget": 0.05,
            "creation_evidence": json.dumps(
                contract.get("history_basis") or {}, ensure_ascii=True, sort_keys=True
            ),
            "buckets": buckets,
        }

    def _plan_text(self, readme: str, contract: Mapping[str, Any]) -> str:
        shared = contract.get("shared_knowledge")
        citations = shared.get("citations") if isinstance(shared, Mapping) else None
        if not isinstance(citations, list) or not all(
            isinstance(item, str) and item for item in citations
        ):
            raise CaseProposalError("Tester shared-knowledge citations are missing")
        lines = "\n".join(f"- `{item}`" for item in citations)
        return readme.rstrip() + "\n\n## Contract citations\n\n" + lines + "\n"

    def _project_coverage(
        self, contract: Mapping[str, Any], action_id: str
    ) -> str:
        operator = str(contract["operator"])
        knowledge_root = self.root / "reference" / "op_knowledge" / operator
        if not knowledge_root.exists():
            return ""
        coverage_path = knowledge_root / "case_coverage.md"
        if not coverage_path.is_file():
            raise CaseProposalError("operator knowledge case_coverage.md is missing")
        current = coverage_path.read_text(encoding="utf-8", errors="strict")
        case_version = str(contract["case_version"])
        if case_version.lower() not in current.lower():
            delta = dict(contract.get("coverage_delta") or {})
            hypothesis = dict(contract.get("hypothesis") or {})
            record = str(delta.get("case_coverage_record") or "").strip()
            falsifier = str(hypothesis.get("falsifier") or "").strip()
            if not record or not falsifier:
                raise CaseProposalError("Tester coverage projection is incomplete")
            addition = (
                f"\n\n## {case_version} - Tester case promotion\n\n"
                f"- Evidence: Agent action `{action_id}` and sealed `case_contract.json`.\n"
                f"- Coverage: {record}\n"
                f"- Falsifier: {falsifier}\n"
            )
            write_text_atomic(coverage_path, current.rstrip() + addition)
        return coverage_path.relative_to(self.root).as_posix()

    def _sync_task_case_sidecar(
        self,
        action: Mapping[str, Any],
        cases: list[Any],
        case_dir: Path,
    ) -> Path:
        operator = str(
            action.get("candidate_identity", {}).get("display_name")
            or action.get("operator_id")
            or ""
        )
        task_case = (
            self.root
            / "operators"
            / str(action.get("campaign") or "")
            / "case_910b"
            / operator
        ).resolve()
        if self.root not in task_case.parents or not task_case.is_dir():
            raise CaseProposalError(
                "Tester task-case package is unavailable for mechanical sidecar sync"
            )
        write_json_atomic(
            task_case / "case_specs.json",
            cases,
            ensure_ascii=True,
        )
        write_json_atomic(
            task_case / "CASE_BINDING.json",
            {
                "schema": TASK_CASE_BINDING_SCHEMA,
                "campaign": str(action.get("campaign") or ""),
                "operator": operator,
                "case_version": str(action.get("candidate_version") or ""),
                "case_dir": case_dir.relative_to(self.root).as_posix(),
                "cases_sha256": hashlib.sha256(
                    (case_dir / "cases.json").read_bytes()
                ).hexdigest(),
                "source_action_id": str(action.get("action_id") or ""),
                "materializer_generation": CASE_MATERIALIZER_GENERATION,
            },
            ensure_ascii=True,
            sort_keys=True,
        )
        return task_case

    def _validate_receipt(
        self,
        receipt: Mapping[str, Any],
        action: Mapping[str, Any],
        source_receipt: Mapping[str, Any],
    ) -> None:
        if (
            receipt.get("schema") != CASE_PROMOTION_RECEIPT_SCHEMA
            or receipt.get("action_id") != action.get("action_id")
            or receipt.get("candidate_version") != action.get("candidate_version")
            or receipt.get("source_after_digest")
            != source_receipt.get("source_after_digest")
        ):
            raise CaseProposalError("existing case promotion receipt is inconsistent")
        target = (self.root / str(receipt.get("case_dir") or "")).resolve()
        if self.root not in target.parents or not target.is_dir():
            raise CaseProposalError("promoted Tester case directory is unavailable")
        if receipt.get("case_content_digest") != _directory_digest(target):
            raise CaseProposalError("promoted Tester case content changed")

    def _bounded_directory(self, raw: str) -> Path:
        path = (self.root / raw).resolve()
        if self.root not in path.parents or not path.is_dir():
            raise CaseProposalError("Tester source workspace is unavailable")
        return path

    @staticmethod
    def _read_object(path: Path) -> dict[str, Any]:
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise CaseProposalError(f"invalid JSON object: {path}") from exc
        if not isinstance(value, dict):
            raise CaseProposalError(f"JSON value must be an object: {path}")
        return value

    @staticmethod
    def _read_list(path: Path) -> list[Any]:
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise CaseProposalError(f"invalid JSON list: {path}") from exc
        if not isinstance(value, list):
            raise CaseProposalError(f"JSON value must be a list: {path}")
        return value


def _directory_digest(root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        relative = path.relative_to(root).as_posix()
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(hashlib.sha256(path.read_bytes()).digest())
    return digest.hexdigest()
