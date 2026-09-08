"""File-only Solver proposal client; no control database, test submission or carrier.

CLIENT is a trusted host projection. This module validates syntax/identity and
writes requested intent only; trusted intake must still check writer quiescence,
case policy and actual input bytes before accepting any work.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import stat
import uuid
from typing import Any, Mapping

from ascendop_protocol.file_lock import exclusive_file_lock
from ascendop_protocol.filesystem import filesystem_path, sync_directory

CLIENT_SCHEMA = "ascendop.workspace-client.v1"
PROPOSAL_SCHEMA = "ascendop.workspace-proposal.v1"
MAX_BYTES = 16 * 1024
MAX_FILE_BYTES = 1024 * 1024
SAFE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,199}\Z")


def proposal_path(binding: Mapping[str, Any], turn_id: str = "", *, ordinal: int | None = None) -> str:
    action_id = binding.get("action_id")
    if ordinal is not None:
        if isinstance(ordinal, bool) or not isinstance(ordinal, int) or ordinal < 1:
            raise ValueError("workspace proposal requires the original positive native ordinal")
        turn_id = f"native-{ordinal}"
    if not isinstance(action_id, str) or not SAFE.fullmatch(action_id) or not SAFE.fullmatch(turn_id):
        raise ValueError("unsafe workspace proposal action/turn")
    return f".ascendop/proposals/{action_id}/{turn_id}/PROPOSAL.json"


def _bounded(workspace: Path, relative: str) -> Path:
    path = workspace / relative
    if (workspace.resolve() != workspace.absolute() or path.resolve() != path.absolute()
            or not path.resolve().is_relative_to(workspace)):
        raise ValueError("workspace proposal path must be canonical and unlinked")
    return path


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate workspace proposal JSON key")
        result[key] = value
    return result


def _read(path: Path, *, limit: int = MAX_BYTES) -> dict[str, Any]:
    path = filesystem_path(path)
    if not stat.S_ISREG(path.stat(follow_symlinks=False).st_mode):
        raise ValueError("workspace proposal must be an ordinary file")
    with path.open("rb") as stream:
        data = stream.read(limit + 1)
    if len(data) > limit:
        raise ValueError(f"workspace proposal exceeds {limit} bytes")
    value = json.loads(data.decode("utf-8-sig"), object_pairs_hook=_pairs)
    if not isinstance(value, dict):
        raise ValueError("workspace proposal must be a JSON object")
    return value


def _validate(value, context):
    expected = {"schema", "binding", "native_start_id", "native_turn_id", "kind", "summary"}
    case = value.get("kind") == "case-data"
    if case:
        expected.add("case_data")
    gap = value.get("kind") == "capability-gap"
    if gap:
        expected.add("capability_gap")
    if "diagnostic" in value:
        expected.add("diagnostic")
        if (value.get("kind") != "test" or value["diagnostic"] != "native-stack"
                or value["diagnostic"] not in context.get("diagnostics", [])):
            raise ValueError("test diagnostic is not supported by this workspace client")
    if set(value) != expected or value["schema"] != PROPOSAL_SCHEMA or value["kind"] not in {"test", "case-data", "case-revision", "capability-gap"}:
        raise ValueError("unsupported workspace proposal contract")
    for key in ("binding", "native_start_id", "native_turn_id"):
        if value[key] != context[key]:
            raise ValueError(f"workspace proposal {key} differs from the admitted native writer")
    if not isinstance(value["summary"], str) or not value["summary"].strip() or len(value["summary"]) > 2000:
        raise ValueError("workspace proposal requires a summary of 1..2000 characters")
    if gap:
        detail = value["capability_gap"]
        if (context.get("capability_gap") is not True or not isinstance(detail, dict)
                or set(detail) != {"code", "resume_condition"}
                or not isinstance(detail["code"], str) or not SAFE.fullmatch(detail["code"])
                or not isinstance(detail["resume_condition"], str)
                or not detail["resume_condition"].strip() or len(detail["resume_condition"]) > 2000):
            raise ValueError("capability gap requires an available client, code and concrete resume condition")
    elif case:
        contract = context.get("case_data")
        if (context["execution_phase"] != "case-authoring" or not contract
                or not isinstance(value["case_data"], dict)
                or value["case_data"].get("base_case_version") != contract["base_case_version"]):
            raise ValueError("workspace case proposal requires the admitted data case/base contract")
        if len(json.dumps(value["case_data"], ensure_ascii=False, allow_nan=False).encode("utf-8")) > MAX_FILE_BYTES:
            raise ValueError("case proposal data exceeds the existing transport limit")
    elif context["execution_phase"] != "candidate-test":
        raise ValueError("workspace test proposal requires a candidate action")
    elif value["kind"] == "case-revision" and not context.get("case_revision"):
        raise ValueError("workspace case revision requires a supported single Solver correctness action")
    return value


def submit_workspace_proposal(workspace: Path, *, summary: str, action_id: str = "", case_data: bool = False,
        request_revision: bool = False, capability_gap: Mapping[str, str] | None = None,
        diagnostic: str | None = None) -> dict[str, Any]:
    """Only the current proposal directory is written; shared control is read-only."""
    workspace = workspace.absolute()
    if sum((bool(case_data), bool(request_revision), capability_gap is not None)) > 1:
        raise ValueError("select only one proposal: test, case data, revision or capability gap")
    context = _read(_bounded(workspace, ".ascendop/CLIENT.json"))
    if context.get("schema") != CLIENT_SCHEMA or context.get("available") is not True:
        raise ValueError("workspace file client is not available for the current native writer")
    binding = context["binding"]
    relative = proposal_path(binding, context["native_turn_id"], ordinal=context.get("native_ordinal"))
    if context["proposal_path"] != relative or action_id and action_id != binding["action_id"]:
        raise ValueError("workspace client action/path was superseded")
    draft = None
    if case_data:
        case = context.get("case_data") or {}
        expected = f".ascendop/case-drafts/{binding['action_id']}/CASES.json"
        if case.get("draft_path") != expected:
            raise ValueError("workspace case draft is not available for this original action")
        draft = _read(_bounded(workspace, expected), limit=MAX_FILE_BYTES)
    value = _validate({"schema": PROPOSAL_SCHEMA, "binding": binding,
        "native_start_id": context["native_start_id"], "native_turn_id": context["native_turn_id"],
        "kind": "capability-gap" if capability_gap is not None else "case-data" if case_data else "case-revision" if request_revision else "test", "summary": summary,
        **({"case_data": draft} if case_data else {}),
        **({"diagnostic": diagnostic} if diagnostic is not None else {}),
        **({"capability_gap": dict(capability_gap)} if capability_gap is not None else {})}, context)
    limit = MAX_BYTES + MAX_FILE_BYTES if case_data else MAX_BYTES
    target = _bounded(workspace, relative)
    target_io = filesystem_path(target)
    target_io.parent.mkdir(parents=True, exist_ok=True)
    with exclusive_file_lock(target_io.parent / ".proposal-publish.lock", 5):
        if target_io.exists():
            if _read(target_io, limit=limit) != value:
                raise ValueError("this native attempt already has a different semantic proposal")
        else:
            temporary = target_io.with_name(".p-" + uuid.uuid4().hex[:8])
            try:
                with temporary.open("xb") as stream:
                    stream.write((json.dumps(value, ensure_ascii=False, sort_keys=True) + "\n").encode("utf-8"))
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temporary, target_io)
                sync_directory(target_io.parent)
            finally:
                temporary.unlink(missing_ok=True)
    return {"schema": "ascendop.workspace-requested.v1", "state": "requested",
        "action_id": binding["action_id"], "proposal_ref": str(target),
        "next": ("Finish this turn. The harness records the capability gap; no test or automatic unchanged retry is requested."
            if capability_gap is not None else "Finish this turn. The harness confirms writing has stopped, then accepts the selected case and hands off the next action; do not resubmit."
            if case_data else "Finish this turn. The harness hands the accepted base to the same Solver for case authoring; this is not a test."
            if request_revision else "Finish this turn. The harness freezes inputs and tracks the original request; do not wait or resubmit.")}
