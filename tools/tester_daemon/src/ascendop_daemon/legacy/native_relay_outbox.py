from __future__ import annotations

import json
import os
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from hashlib import sha1
from pathlib import Path
from typing import Any, Callable

from ascendop_daemon.runtime.control import read_stop_request

from ascendop_daemon.workflow.casegen_evidence import latest_casegen_evidence_issue
from ascendop_daemon.core.models import utc_now_iso
from ascendop_daemon.automation.trigger_state import (
    codex_cli_resume_process_active,
    codex_cli_resume_process_dead,
    is_transient_native_poll_failure,
    normalized_trigger_status,
    read_tester_trigger_ack_state,
    read_trigger_ack_state,
)


OUTBOX_STATUSES = {"ready", "retry-ready", "needs-native-delivery"}
TERMINAL_TRIGGER_STATUSES = {"completed", "superseded", "cancelled", "inactive", "disabled"}
CLAIM_ACTIVE_STATUSES = {"claimed", "sending"}
ACTIVE_ACK_COVER_SECONDS = 300
RELAY_STATUS_PRIORITY = {
    "ready": 0,
    "needs-native-delivery": 1,
    "retry-ready": 2,
}
NATIVE_RELAY_CONSUMER_STATUS = "native_relay_consumer_status.json"
NATIVE_RELAY_CLAIM_LOCK = "native_relay_claims.lock"
NATIVE_RELAY_CLAIM_LOCK_TIMEOUT_SECONDS = 5.0
NATIVE_RELAY_CLAIM_LOCK_STALE_SECONDS = 30.0


@contextmanager
def native_relay_claim_mutation_lock(root: Path):
    state_dir = root / "TestUtils" / "tester_daemon"
    state_dir.mkdir(parents=True, exist_ok=True)
    lock_path = state_dir / NATIVE_RELAY_CLAIM_LOCK
    token = f"{os.getpid()}:{time.time_ns()}"
    deadline = time.monotonic() + NATIVE_RELAY_CLAIM_LOCK_TIMEOUT_SECONDS
    acquired = False
    while time.monotonic() < deadline:
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            try:
                age = time.time() - lock_path.stat().st_mtime
                if age > NATIVE_RELAY_CLAIM_LOCK_STALE_SECONDS:
                    lock_path.unlink(missing_ok=True)
                    continue
            except OSError:
                pass
            time.sleep(0.02)
            continue
        try:
            os.write(fd, token.encode("ascii"))
        finally:
            os.close(fd)
        acquired = True
        break
    if not acquired:
        raise TimeoutError(f"timed out acquiring native relay claim lock: {lock_path}")
    try:
        yield
    finally:
        try:
            if lock_path.read_text(encoding="ascii", errors="ignore") == token:
                lock_path.unlink(missing_ok=True)
        except OSError:
            pass


def build_native_relay_outbox(root: Path) -> dict[str, Any]:
    state_dir = root / "TestUtils" / "tester_daemon"
    entries: list[dict[str, Any]] = []
    claims = active_claims_by_entry(read_native_relay_claims(root))
    solver_observations = read_json(state_dir / "solver_thread_observations.json")
    for trigger_type, plan_name, ack_name, ack_command in (
        ("solver", "solver_trigger_plan.json", "solver_trigger_ack_state.json", "ack-trigger"),
        ("tester", "tester_trigger_plan.json", "tester_trigger_ack_state.json", "ack-tester-trigger"),
    ):
        plan = read_json(state_dir / plan_name)
        ack_state = read_trigger_ack_state(root) if trigger_type == "solver" else read_tester_trigger_ack_state(root)
        sent = ack_state.get("sent", {}) if isinstance(ack_state.get("sent"), dict) else {}
        plan_updated_at = str(plan.get("updated_at", "") or "")
        triggers = plan.get("triggers", []) if isinstance(plan.get("triggers"), list) else []
        for trigger in triggers:
            if not isinstance(trigger, dict):
                continue
            status = str(trigger.get("status", "") or "")
            key = str(trigger.get("key", "") or "")
            thread_id = str(trigger.get("thread_id", "") or "")
            prompt_rel = str(trigger.get("prompt_path", "") or "")
            if not key or not thread_id or not prompt_rel:
                continue
            ack_record = sent.get(key) if isinstance(sent, dict) else None
            status = effective_outbox_status(
                status,
                ack_record,
                plan_ack_updated_at=str(trigger.get("sent_at", "") or ""),
            )
            if status not in OUTBOX_STATUSES:
                continue
            if not trigger_still_actionable(root, trigger_type, trigger):
                continue
            # The notifier plan and App-side relay ack are updated by independent
            # loops.  Honor an ack-side cooldown here so a stale retry-ready plan
            # cannot be claimed repeatedly before the notifier refreshes it.
            if ack_delivery_retry_pending(ack_record):
                continue
            if ack_has_live_delivery_fence(ack_record) or (
                isinstance(ack_record, dict) and is_transient_native_poll_failure(ack_record)
            ):
                continue
            if (
                status != "retry-ready"
                and ack_is_ide_confirmed(ack_record)
                and not trigger_contract_revision_pending(trigger, ack_record)
                and not trigger_correction_pending(trigger, ack_record)
            ):
                continue
            if trigger_type == "solver" and solver_active_turn_covering_ack(
                sent,
                str(trigger.get("op", "") or ""),
                thread_id,
                solver_observations,
            ):
                continue
            if trigger_type == "solver" and solver_observation_has_active_turn(
                solver_observations,
                str(trigger.get("op", "") or ""),
                thread_id,
            ):
                continue
            if trigger_type == "tester" and thread_observation_has_active_turn(
                solver_observations,
                str(trigger.get("op", "") or ""),
                thread_id,
                role="tester",
            ):
                continue
            if trigger_type == "solver" and solver_recent_active_ack_covering_op(
                sent,
                str(trigger.get("op", "") or ""),
                thread_id,
                solver_observations,
            ):
                continue
            if (
                trigger_type == "tester"
                and status != "retry-ready"
                and tester_casegen_active_covering_ack(
                sent, str(trigger.get("op", "") or ""), key
                )
            ):
                continue
            prompt_path = root / prompt_rel
            prompt_sha1 = ""
            prompt_preview = ""
            if prompt_path.exists():
                try:
                    prompt = prompt_path.read_text(encoding="utf-8", errors="replace")
                    prompt_sha1 = sha1(prompt.encode("utf-8")).hexdigest()
                    prompt_preview = prompt[:240]
                except OSError:
                    prompt = ""
            entry_id = sha1(f"{trigger_type}|{key}".encode("utf-8")).hexdigest()[:16]
            observed_baseline = latest_observed_turn_baseline(
                solver_observations,
                op=str(trigger.get("op", "") or ""),
                role=trigger_type,
                thread_id=thread_id,
            )
            entry = {
                "id": entry_id,
                "type": trigger_type,
                "op": trigger.get("op", ""),
                "gate_stage": trigger.get("gate_stage", ""),
                "key": key,
                "status": status,
                "thread_id": thread_id,
                "model": trigger.get("model", ""),
                "thinking": trigger.get("thinking", ""),
                "prompt_path": prompt_rel,
                "prompt_sha1": prompt_sha1,
                "prompt_preview": prompt_preview,
                **observed_baseline,
                "prompt_chars": trigger.get("prompt_chars", len(prompt) if prompt_path.exists() else 0),
                "prompt_profile": trigger.get("prompt_profile", "standard-delta"),
                "prompt_mode": trigger.get("prompt_mode", ""),
                "delivery_ordinal": trigger.get("delivery_ordinal", 0),
                "contract_revision": trigger.get("contract_revision", ""),
                "reanchor_reason": trigger.get("reanchor_reason", ""),
                "correction_reason": trigger.get("correction_reason", ""),
                "plan_updated_at": plan_updated_at,
                "preferred_delivery": trigger.get("preferred_delivery", "ide_native_relay"),
                "ack_command": (
                    "python tools\\tester_daemon\\daemon.py "
                    f"{ack_command} --key {json.dumps(key)} --thread-id {thread_id} "
                    "--status sent --delivery codex-app-send-message-to-thread --ide-panel-visible "
                    "--turn-id <turn_id>"
                ),
                "claim_command": (
                    "python tools\\tester_daemon\\daemon.py native-relay-claim "
                    "--consumer <relay-consumer-id> --max-items 1 --include-prompt --json"
                ),
                "complete_command": (
                    "python tools\\tester_daemon\\daemon.py native-relay-complete "
                    f"--entry-id {entry_id} --status sent --delivery codex-app-send-message-to-thread "
                    "--ide-panel-visible --turn-id <new_turn_id> "
                    "--pre-delivery-turn-id <old_turn_id_or___none__>"
                ),
            }
            claim = claims.get(entry_id)
            if claim:
                for field in (
                    "pre_delivery_turn_id",
                    "pre_delivery_turn_status",
                    "pre_delivery_observed_at",
                    "pre_delivery_observation_source",
                ):
                    if claim.get(field) not in {None, ""}:
                        entry[field] = claim.get(field)
                entry.update(
                    {
                        "claim_status": "claimed",
                        "claim_id": claim.get("claim_id", ""),
                        "claimed_by": claim.get("claimed_by", ""),
                        "claimed_at": claim.get("claimed_at", ""),
                        "claim_expires_at": claim.get("expires_at", ""),
                    }
                )
            else:
                entry["claim_status"] = "available"
            entries.append(entry)
    claimed_entry_count = sum(1 for item in entries if item.get("claim_status") == "claimed")
    return {
        "updated_at": utc_now_iso(),
        "relay_contract": "codex_app.send_message_to_thread",
        "entry_count": len(entries),
        "claimed_entry_count": claimed_entry_count,
        "available_entry_count": len(entries) - claimed_entry_count,
        "entries": entries,
    }


def write_native_relay_outbox(root: Path) -> dict[str, Any]:
    state_dir = root / "TestUtils" / "tester_daemon"
    state_dir.mkdir(parents=True, exist_ok=True)
    payload = build_native_relay_outbox(root)
    prune_orphan_native_relay_claims(root, payload)
    (state_dir / "native_relay_outbox.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    (state_dir / "NATIVE_RELAY_OUTBOX.md").write_text(render_native_relay_outbox(payload), encoding="utf-8")
    return payload


def latest_observed_turn_baseline(
    observations: Any,
    *,
    op: str,
    role: str,
    thread_id: str,
) -> dict[str, str]:
    """Expose the daemon poller's top-level turn as a relay pre-read fallback."""
    if not isinstance(observations, dict):
        return {}
    threads = observations.get("threads", [])
    if not isinstance(threads, list):
        return {}
    for item in threads:
        if not isinstance(item, dict):
            continue
        if str(item.get("op", "") or "") != op:
            continue
        if str(item.get("role", "solver") or "solver") != role:
            continue
        if str(item.get("thread_id", "") or "") != thread_id:
            continue
        turn_id = str(item.get("latest_turn_id", "") or "")
        if not turn_id:
            return {}
        return {
            "pre_delivery_turn_id": turn_id,
            "pre_delivery_turn_status": str(item.get("latest_turn_status", "") or ""),
            "pre_delivery_observed_at": str(observations.get("updated_at", "") or ""),
            "pre_delivery_observation_source": str(
                item.get("app_server_mode", "daemon-thread-observation")
                or "daemon-thread-observation"
            ),
        }
    return {}


def trigger_contract_revision_pending(
    trigger: dict[str, Any],
    ack_record: dict[str, Any] | None,
) -> bool:
    if str(trigger.get("prompt_mode", "") or "") != "reanchor":
        return False
    revision = str(trigger.get("contract_revision", "") or "")
    if not revision:
        return False
    delivered_revision = (
        str(ack_record.get("contract_revision", "") or "")
        if isinstance(ack_record, dict)
        else ""
    )
    return delivered_revision != revision


def trigger_correction_pending(
    trigger: dict[str, Any],
    ack_record: dict[str, Any] | None,
) -> bool:
    if str(trigger.get("prompt_mode", "") or "") != "correction":
        return False
    correction_reason = str(trigger.get("correction_reason", "") or "")
    if not correction_reason:
        return False
    delivered_reason = (
        str(ack_record.get("correction_reason", "") or "")
        if isinstance(ack_record, dict)
        else ""
    )
    return delivered_reason != correction_reason


def prune_orphan_native_relay_claims(root: Path, outbox: dict[str, Any]) -> None:
    entries = outbox.get("entries", []) if isinstance(outbox.get("entries"), list) else []
    entry_ids = {str(item.get("id", "") or "") for item in entries if isinstance(item, dict)}
    with native_relay_claim_mutation_lock(root):
        claims_payload = read_native_relay_claims(root)
        raw_claims = claims_payload.get("claims", {}) if isinstance(claims_payload, dict) else {}
        if not isinstance(raw_claims, dict):
            raw_claims = {}
        active_claims = active_claims_by_entry(claims_payload)
        pruned = {
            entry_id: claim
            for entry_id, claim in active_claims.items()
            if entry_id in entry_ids or not claim_has_confirmed_ack(root, claim)
        }
        if len(pruned) != len(raw_claims):
            write_native_relay_claims(root, pruned)


def claim_has_confirmed_ack(root: Path, claim: dict[str, Any]) -> bool:
    key = str(claim.get("key", "") or "")
    if not key:
        return False
    trigger_type = str(claim.get("type", "solver") or "solver")
    ack_state = (
        read_tester_trigger_ack_state(root)
        if trigger_type == "tester"
        else read_trigger_ack_state(root)
    )
    sent = ack_state.get("sent", {}) if isinstance(ack_state.get("sent"), dict) else {}
    return ack_is_ide_confirmed(sent.get(key))


def render_native_relay_outbox(payload: dict[str, Any]) -> str:
    lines = [
        "# Native Relay Outbox",
        "",
        f"- updated_at: {payload.get('updated_at', '')}",
        f"- relay_contract: {payload.get('relay_contract', '')}",
        f"- entry_count: {payload.get('entry_count', 0)}",
        "",
    ]
    entries = payload.get("entries", []) if isinstance(payload.get("entries"), list) else []
    if not entries:
        lines.append("- none")
        lines.append("")
        return "\n".join(lines)
    lines.extend(
        [
            "| id | type | op | gate | status | model | thinking | thread | prompt |",
            "|---|---|---|---|---|---|---|---|---|",
        ]
    )
    for item in entries:
        if not isinstance(item, dict):
            continue
        lines.append(
            f"| {item.get('id', '-')} | {item.get('type', '-')} | {item.get('op', '-')} | "
            f"{item.get('gate_stage', '-')} | {item.get('status', '-')}:{item.get('claim_status', 'available')} | "
            f"{item.get('model', '-')} | {item.get('thinking', '-')} | "
            f"{item.get('thread_id', '-')} | `{item.get('prompt_path', '-')}` |"
        )
    lines.append("")
    return "\n".join(lines)


def claim_native_relay_entries(
    root: Path,
    *,
    consumer: str,
    ttl_seconds: int = 120,
    max_items: int = 1,
    include_prompt: bool = False,
) -> dict[str, Any]:
    state_dir = root / "TestUtils" / "tester_daemon"
    state_dir.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc).replace(microsecond=0)
    outbox = write_native_relay_outbox(root)
    entries = [item for item in outbox.get("entries", []) if isinstance(item, dict)]
    # A broken/restarting session must not monopolize the one-item app relay.
    # Deliver fresh gates before retry work while preserving the plan order
    # within each class.
    entries = sorted(
        enumerate(entries),
        key=lambda pair: (
            RELAY_STATUS_PRIORITY.get(str(pair[1].get("status", "") or ""), 99),
            pair[0],
        ),
    )
    entries = [item for _, item in entries]
    stop_request = read_stop_request(root)
    ttl_seconds = max(1, int(ttl_seconds or 120))
    max_items = max(1, int(max_items or 1))
    selected: list[dict[str, Any]] = []
    next_claims: dict[str, dict[str, Any]] = {}
    with native_relay_claim_mutation_lock(root):
        claims_payload = read_native_relay_claims(root)
        active_claims = active_claims_by_entry(claims_payload, now=now)
        if stop_request:
            next_claims = dict(active_claims)
            write_native_relay_claims(root, next_claims, ttl_seconds=ttl_seconds)
        else:
            active_entry_ids = set(active_claims)
            next_claims = dict(active_claims)
            expires_at = (now + timedelta(seconds=ttl_seconds)).isoformat()
            for entry in entries:
                entry_id = str(entry.get("id", "") or "")
                if not entry_id or entry_id in active_entry_ids:
                    continue
                claim_id = sha1(
                    f"{entry_id}|{consumer}|{now.isoformat()}".encode("utf-8")
                ).hexdigest()[:16]
                claim = {
                    "claim_id": claim_id,
                    "entry_id": entry_id,
                    "type": entry.get("type", ""),
                    "op": entry.get("op", ""),
                    "gate_stage": entry.get("gate_stage", ""),
                    "key": entry.get("key", ""),
                    "thread_id": entry.get("thread_id", ""),
                    "model": entry.get("model", ""),
                    "thinking": entry.get("thinking", ""),
                    "prompt_path": entry.get("prompt_path", ""),
                    "prompt_sha1": entry.get("prompt_sha1", ""),
                    "prompt_chars": entry.get("prompt_chars", 0),
                    "prompt_profile": entry.get("prompt_profile", "standard-delta"),
                    "prompt_mode": entry.get("prompt_mode", ""),
                    "delivery_ordinal": entry.get("delivery_ordinal", 0),
                    "contract_revision": entry.get("contract_revision", ""),
                    "reanchor_reason": entry.get("reanchor_reason", ""),
                    "correction_reason": entry.get("correction_reason", ""),
                    "pre_delivery_turn_id": entry.get("pre_delivery_turn_id", ""),
                    "pre_delivery_turn_status": entry.get(
                        "pre_delivery_turn_status", ""
                    ),
                    "pre_delivery_observed_at": entry.get(
                        "pre_delivery_observed_at", ""
                    ),
                    "pre_delivery_observation_source": entry.get(
                        "pre_delivery_observation_source", ""
                    ),
                    "claimed_by": consumer,
                    "claimed_at": now.isoformat(),
                    "expires_at": expires_at,
                    "status": "claimed",
                    "relay_contract": "codex_app.send_message_to_thread",
                    "requires_new_turn_proof": consumer == "app-side-native-relay-heartbeat",
                }
                claim_entry = {**entry, **claim}
                if include_prompt:
                    claim_entry["prompt"] = read_prompt(
                        root, str(entry.get("prompt_path", "") or "")
                    )
                selected.append(claim_entry)
                next_claims[entry_id] = claim
                active_entry_ids.add(entry_id)
                if len(selected) >= max_items:
                    break
            write_native_relay_claims(root, next_claims, ttl_seconds=ttl_seconds)
    if stop_request:
        record_app_side_relay_poll(
            state_dir,
            consumer=consumer,
            claimed_count=0,
            active_claim_count=len(next_claims),
            available_entry_count=0,
            outbox_entry_count=int(outbox.get("entry_count", 0) or 0),
            paused=True,
        )
        return {
            "updated_at": utc_now_iso(),
            "consumer": consumer,
            "paused": True,
            "stop_request": stop_request,
            "claimed_count": 0,
            "active_claim_count": len(next_claims),
            "available_entry_count": 0,
            "entries": [],
        }
    if selected:
        append_claim_events(
            state_dir,
            [
                {
                    key: value
                    for key, value in item.items()
                    if key
                    not in {
                        "prompt",
                        "prompt_preview",
                        "ack_command",
                        "claim_command",
                        "complete_command",
                    }
                }
                for item in selected
            ],
            "native_relay_claimed",
        )
    record_app_side_relay_poll(
        state_dir,
        consumer=consumer,
        claimed_count=len(selected),
        active_claim_count=len(next_claims),
        available_entry_count=max(0, int(outbox.get("entry_count", 0) or 0) - len(next_claims)),
        outbox_entry_count=int(outbox.get("entry_count", 0) or 0),
    )
    return {
        "updated_at": utc_now_iso(),
        "consumer": consumer,
        "claimed_count": len(selected),
        "active_claim_count": len(next_claims),
        "available_entry_count": max(0, int(outbox.get("entry_count", 0) or 0) - len(next_claims)),
        "entries": selected,
    }


def wait_for_native_relay_availability(
    root: Path,
    *,
    wait_seconds: float,
    poll_interval_seconds: float = 0.25,
    outbox_reader: Callable[[Path], dict[str, Any]] | None = None,
    stop_reader: Callable[[Path], dict[str, Any] | None] | None = None,
    clock: Callable[[], float] | None = None,
    sleeper: Callable[[float], None] | None = None,
) -> dict[str, Any]:
    """Long-poll the read-only outbox without creating claim/status churn."""
    reader = outbox_reader or build_native_relay_outbox
    read_stop = stop_reader or read_stop_request
    monotonic = clock or time.monotonic
    sleep = sleeper or time.sleep
    limit = min(300.0, max(0.0, float(wait_seconds or 0.0)))
    interval = min(5.0, max(0.05, float(poll_interval_seconds or 0.25)))
    started = monotonic()
    polls = 0
    reason = "disabled" if limit <= 0 else "timeout"
    preview: dict[str, Any] = {}
    while limit > 0:
        polls += 1
        if read_stop(root):
            reason = "stop-requested"
            break
        preview = reader(root)
        if int(preview.get("available_entry_count", 0) or 0) > 0:
            reason = "available"
            break
        elapsed = max(0.0, monotonic() - started)
        remaining = limit - elapsed
        if remaining <= 0:
            break
        sleep(min(interval, remaining))
    waited = max(0.0, monotonic() - started)
    return {
        "reason": reason,
        "waited_seconds": round(waited, 3),
        "poll_count": polls,
        "available_entry_count": int(preview.get("available_entry_count", 0) or 0),
    }


def record_app_side_relay_poll(
    state_dir: Path,
    *,
    consumer: str,
    claimed_count: int,
    active_claim_count: int,
    available_entry_count: int,
    outbox_entry_count: int,
    paused: bool = False,
) -> None:
    now = utc_now_iso()
    record = {
        "updated_at": now,
        "kind": "native-relay-claim",
        "consumer": consumer,
        "status": "paused" if paused else ("claimed" if claimed_count else "poll"),
        "relay_contract": "codex_app.send_message_to_thread",
        "app_side_required": True,
        "claimed_count": claimed_count,
        "active_claim_count": active_claim_count,
        "available_entry_count": available_entry_count,
        "outbox_entry_count": outbox_entry_count,
        "paused": paused,
    }
    payload = json.dumps(record, ensure_ascii=False, indent=2) + "\n"
    # Consumer liveness must survive a later delivery record.  A successful
    # send proves one turn was visible; it does not prove that anything is
    # still polling the outbox for the next gate.
    (state_dir / NATIVE_RELAY_CONSUMER_STATUS).write_text(payload, encoding="utf-8")
    (state_dir / "native_relay_app_side_status.json").write_text(payload, encoding="utf-8")
    with (state_dir / "native_relay_app_side_events.jsonl").open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=False) + "\n")


def complete_native_relay_claim(
    root: Path,
    *,
    entry_id: str,
    status: str,
    turn_id: str = "",
    delivery: str = "",
    ide_panel_visible: bool = False,
    metadata: dict[str, Any] | None = None,
    claim_hint: dict[str, Any] | None = None,
) -> dict[str, Any]:
    state_dir = root / "TestUtils" / "tester_daemon"
    state_dir.mkdir(parents=True, exist_ok=True)
    claims_payload = read_native_relay_claims(root)
    claims = active_claims_by_entry(claims_payload)
    raw_claims = claims_payload.get("claims", {}) if isinstance(claims_payload, dict) else {}
    if not isinstance(raw_claims, dict):
        raw_claims = {}
    claim = claims.get(entry_id)
    recovered_from_outbox = False
    recovered_from_expired_claim = False
    recovered_from_claim_journal = False
    if not claim:
        raw_claim = raw_claims.get(entry_id)
        if isinstance(raw_claim, dict):
            claim = dict(raw_claim)
            recovered_from_expired_claim = True
    if (
        not claim
        and isinstance(claim_hint, dict)
        and str(claim_hint.get("entry_id") or claim_hint.get("id") or "") == entry_id
    ):
        claim = {
            key: value
            for key, value in claim_hint.items()
            if key not in {"event", "time", "id", "claim_status"}
        }
        claim["entry_id"] = entry_id
        recovered_from_claim_journal = True
    if not claim:
        outbox = write_native_relay_outbox(root)
        outbox_entries = outbox.get("entries", []) if isinstance(outbox.get("entries"), list) else []
        entry = next(
            (
                item
                for item in outbox_entries
                if isinstance(item, dict) and str(item.get("id", "") or "") == entry_id
            ),
            None,
        )
        if not isinstance(entry, dict):
            return {
                "updated_at": utc_now_iso(),
                "entry_id": entry_id,
                "status": "missing-claim",
                "completed": False,
            }
        claim = {
            "entry_id": entry_id,
            "type": entry.get("type", ""),
            "op": entry.get("op", ""),
            "gate_stage": entry.get("gate_stage", ""),
            "key": entry.get("key", ""),
            "thread_id": entry.get("thread_id", ""),
            "model": entry.get("model", ""),
            "thinking": entry.get("thinking", ""),
            "prompt_path": entry.get("prompt_path", ""),
            "prompt_sha1": entry.get("prompt_sha1", ""),
            "prompt_profile": entry.get("prompt_profile", "standard-delta"),
            "prompt_mode": entry.get("prompt_mode", ""),
            "delivery_ordinal": entry.get("delivery_ordinal", 0),
            "contract_revision": entry.get("contract_revision", ""),
            "reanchor_reason": entry.get("reanchor_reason", ""),
            "correction_reason": entry.get("correction_reason", ""),
            "claimed_by": entry.get("claimed_by", ""),
            "claimed_at": entry.get("claimed_at", ""),
            "expires_at": entry.get("claim_expires_at", ""),
            "relay_contract": entry.get("relay_contract", "codex_app.send_message_to_thread"),
        }
        recovered_from_outbox = True
    requested_status = status
    proof_error = ""
    proof = metadata if isinstance(metadata, dict) else {}
    weak_unconfirmed_delivery = (
        status == "delivered"
        and not turn_id
        and proof.get("completion_unconfirmed") is True
        and str(proof.get("error", "") or "")
        == "post-delivery-proof-unavailable"
    )
    if bool(claim.get("requires_new_turn_proof")) and status in {"sent", "active", "completed"}:
        has_pre_delivery_turn = "pre_delivery_turn_id" in proof
        pre_delivery_turn_id = str(proof.get("pre_delivery_turn_id", "") or "")
        if not has_pre_delivery_turn:
            proof_error = "missing-pre-delivery-turn-id"
        elif invalid_nested_item_id(pre_delivery_turn_id):
            proof_error = "invalid-pre-delivery-turn-id"
        elif not turn_id:
            proof_error = "missing-post-delivery-turn-id"
        elif invalid_nested_item_id(turn_id):
            proof_error = "invalid-post-delivery-turn-id"
        elif (
            pre_delivery_turn_id == turn_id
            and proof.get("same_turn_prompt_append_proven") is not True
        ):
            proof_error = "native-turn-id-did-not-change"
        if proof_error:
            status = "failed"
    elif bool(claim.get("requires_new_turn_proof")) and weak_unconfirmed_delivery:
        has_pre_delivery_turn = "pre_delivery_turn_id" in proof
        pre_delivery_turn_id = str(proof.get("pre_delivery_turn_id", "") or "")
        if not has_pre_delivery_turn:
            proof_error = "missing-pre-delivery-turn-id"
        elif invalid_nested_item_id(pre_delivery_turn_id):
            proof_error = "invalid-pre-delivery-turn-id"
        if proof_error:
            status = "failed"

    record = dict(claim)
    record["status"] = status
    if status != requested_status:
        record["requested_status"] = requested_status
        record["failure_kind"] = "native_turn_not_created"
        record["error"] = proof_error
    record["completed_at"] = utc_now_iso()
    if recovered_from_expired_claim:
        record["claim_recovery"] = "expired-claim-after-send"
    elif recovered_from_claim_journal:
        record["claim_recovery"] = "claim-journal-after-send"
    elif recovered_from_outbox:
        record["claim_recovery"] = "outbox-entry-after-claim-expired"
    if turn_id:
        record["turn_id"] = turn_id
    if delivery:
        record["delivery"] = delivery
    if ide_panel_visible and not proof_error:
        record["ide_panel_visible"] = True
        record["ide_panel_visibility"] = "confirmed_by_native_relay"
    if metadata:
        record.update(metadata)
    if proof_error:
        for field in (
            "ide_panel_visible",
            "ide_panel_visibility",
            "completion_unconfirmed",
            "control_plane_unavailable",
            "orphaned_delivery_owner",
            "delivery_started_at",
        ):
            record.pop(field, None)
        record["status"] = "failed"
        record["requested_status"] = requested_status
        record["failure_kind"] = "native_turn_not_created"
        record["error"] = proof_error
    original_claim_id = str(claim.get("claim_id", "") or "")
    with native_relay_claim_mutation_lock(root):
        current_payload = read_native_relay_claims(root)
        current_claims = active_claims_by_entry(current_payload)
        current = current_claims.get(entry_id)
        current_claim_id = str(current.get("claim_id", "") or "") if current else ""
        if current and original_claim_id and current_claim_id != original_claim_id:
            record["claim_replaced_during_delivery"] = True
            record["replacement_claim_id"] = current_claim_id
            remaining = current_claims
        else:
            remaining = {
                key: value for key, value in current_claims.items() if key != entry_id
            }
        write_native_relay_claims(root, remaining)
    append_claim_events(state_dir, [record], "native_relay_completed")
    return {
        "updated_at": record["completed_at"],
        "entry_id": entry_id,
        "type": record.get("type", ""),
        "key": record.get("key", ""),
        "thread_id": record.get("thread_id", ""),
        "status": status,
        "completed": True,
        "record": record,
        "proof_error": proof_error,
        "recovered_from_outbox": recovered_from_outbox,
        "recovered_from_expired_claim": recovered_from_expired_claim,
        "recovered_from_claim_journal": recovered_from_claim_journal,
    }


def invalid_nested_item_id(value: str) -> bool:
    normalized = value.strip().lower()
    return normalized.startswith(("item-", "exec-", "call-"))


def read_native_relay_claims(root: Path) -> dict[str, Any]:
    return read_json(root / "TestUtils" / "tester_daemon" / "native_relay_claims.json")


def active_claims_by_entry(
    claims_payload: dict[str, Any],
    *,
    now: datetime | None = None,
) -> dict[str, dict[str, Any]]:
    now = now or datetime.now(timezone.utc).replace(microsecond=0)
    claims = claims_payload.get("claims", {}) if isinstance(claims_payload, dict) else {}
    if not isinstance(claims, dict):
        return {}
    active: dict[str, dict[str, Any]] = {}
    for entry_id, claim in claims.items():
        if not isinstance(claim, dict):
            continue
        if str(claim.get("status", "") or "") not in CLAIM_ACTIVE_STATUSES:
            continue
        expires_at = parse_timestamp(str(claim.get("expires_at", "") or ""))
        if expires_at is None or expires_at <= now:
            continue
        active[str(entry_id)] = dict(claim)
    return active


def write_native_relay_claims(
    root: Path,
    claims: dict[str, dict[str, Any]],
    *,
    ttl_seconds: int | None = None,
) -> dict[str, Any]:
    state_dir = root / "TestUtils" / "tester_daemon"
    state_dir.mkdir(parents=True, exist_ok=True)
    payload: dict[str, Any] = {
        "updated_at": utc_now_iso(),
        "claim_count": len(claims),
        "claims": claims,
    }
    if ttl_seconds is not None:
        payload["ttl_seconds"] = ttl_seconds
    claims_path = state_dir / "native_relay_claims.json"
    temp_path = state_dir / (
        f".{claims_path.name}.{os.getpid()}.{time.time_ns()}.tmp"
    )
    temp_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    os.replace(temp_path, claims_path)
    (state_dir / "NATIVE_RELAY_CLAIMS.md").write_text(render_native_relay_claims(payload), encoding="utf-8")
    return payload


def release_native_relay_claims(
    root: Path,
    *,
    keys: set[str],
    reason: str,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Release app-side claims whose delivery owner vanished with Codex App."""
    state_dir = root / "TestUtils" / "tester_daemon"
    with native_relay_claim_mutation_lock(root):
        payload = read_native_relay_claims(root)
        claims = payload.get("claims", {}) if isinstance(payload.get("claims"), dict) else {}
        released: list[dict[str, Any]] = []
        kept: dict[str, dict[str, Any]] = {}
        for entry_id, raw_claim in claims.items():
            if not isinstance(raw_claim, dict):
                continue
            claim = dict(raw_claim)
            if str(claim.get("key", "") or "") not in keys:
                kept[str(entry_id)] = claim
                continue
            released.append(
                {
                    **claim,
                    "entry_id": str(entry_id),
                    "status": "released",
                    "released_at": utc_now_iso(),
                    "release_reason": reason,
                }
            )
        if released and not dry_run:
            write_native_relay_claims(root, kept)
            append_claim_events(state_dir, released, "native_relay_claim_released")
    return {
        "updated_at": utc_now_iso(),
        "dry_run": dry_run,
        "released_count": len(released),
        "released": released,
        "remaining_count": len(claims) if dry_run else len(kept),
    }


def render_native_relay_claims(payload: dict[str, Any]) -> str:
    lines = [
        "# Native Relay Claims",
        "",
        f"- updated_at: {payload.get('updated_at', '')}",
        f"- claim_count: {payload.get('claim_count', 0)}",
        "",
    ]
    claims = payload.get("claims", {}) if isinstance(payload.get("claims"), dict) else {}
    if not claims:
        lines.append("- none")
        lines.append("")
        return "\n".join(lines)
    lines.extend(
        [
            "| entry | type | op | gate | claimed_by | expires_at |",
            "|---|---|---|---|---|---|",
        ]
    )
    for entry_id, claim in claims.items():
        if not isinstance(claim, dict):
            continue
        lines.append(
            f"| {entry_id} | {claim.get('type', '-')} | {claim.get('op', '-')} | "
            f"{claim.get('gate_stage', '-')} | {claim.get('claimed_by', '-')} | "
            f"{claim.get('expires_at', '-')} |"
        )
    lines.append("")
    return "\n".join(lines)


def append_claim_events(state_dir: Path, records: list[dict[str, Any]], event: str) -> None:
    with (state_dir / "native_relay_claim_events.jsonl").open("a", encoding="utf-8") as fh:
        for record in records:
            item = {"time": utc_now_iso(), "event": event, **record}
            fh.write(json.dumps(item, ensure_ascii=False) + "\n")


def read_prompt(root: Path, prompt_rel: str) -> str:
    if not prompt_rel:
        return ""
    path = root / prompt_rel
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""


def parse_timestamp(text: str) -> datetime | None:
    if not text:
        return None
    try:
        normalized = text.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).replace(microsecond=0)


def ack_is_ide_confirmed(record: Any) -> bool:
    if not isinstance(record, dict):
        return False
    status = normalized_trigger_status(record, str(record.get("status", "") or ""))
    if status in {"failed", "interrupted", "cancelled", "needs-native-delivery"}:
        return False
    if str(record.get("delivery_retry_reason", "") or "") == "remote_control_not_ready":
        return False
    if str(record.get("failure_kind", "") or "") == "native_turn_no_agent_output":
        return False
    if (
        status != "active"
        and (record.get("latest_user_only_turn") is True or record.get("last_observed_user_only_turn") is True)
    ):
        return False
    native_status = str(record.get("native_status") or record.get("turn_status") or "")
    if status != "active" and native_status in {"failed", "interrupted", "cancelled"}:
        return False
    delivery = str(record.get("delivery", "") or "")
    visibility = str(record.get("ide_panel_visibility", "") or "")
    return record.get("ide_panel_visible") is True and (
        delivery in {"ide-native-relay", "codex-app-send-message-to-thread"}
        or visibility in {
        "confirmed_by_native_relay",
        "live_proxy",
        "live_ws",
        }
    )


def ack_delivery_retry_pending(record: Any) -> bool:
    if not isinstance(record, dict):
        return False
    retry_after = parse_timestamp(str(record.get("delivery_retry_after", "") or ""))
    if retry_after is None:
        return False
    now = datetime.now(timezone.utc).replace(microsecond=0)
    return now < retry_after


def effective_outbox_status(
    plan_status: str,
    ack_record: Any,
    *,
    plan_ack_updated_at: str = "",
) -> str:
    """Recover an expired relay retry even before notifier rewrites its plan."""
    if plan_status == "retry-ready" and isinstance(ack_record, dict):
        ack_status = normalized_trigger_status(
            ack_record, str(ack_record.get("status", "") or "")
        )
        plan_ack_time = parse_timestamp(plan_ack_updated_at)
        current_ack_time = parse_timestamp(
            str(ack_record.get("updated_at", "") or "")
        )
        if (
            ack_status in {"sent", "delivered", "active", "completed"}
            and plan_ack_time is not None
            and current_ack_time is not None
            and current_ack_time > plan_ack_time
        ):
            # The App-side relay ack landed after this plan snapshot. Treat the
            # newer ack as authoritative until the notifier rewrites the plan;
            # otherwise the stale retry-ready row can be claimed every poll.
            return ack_status
    if plan_status != "retry-wait" or not isinstance(ack_record, dict):
        return plan_status
    if ack_delivery_retry_pending(ack_record):
        return plan_status
    ack_status = normalized_trigger_status(
        ack_record, str(ack_record.get("status", "") or "")
    )
    if ack_status == "needs-native-delivery":
        return "needs-native-delivery"
    if ack_status in {"failed", "interrupted", "cancelled"}:
        return "retry-ready"
    return plan_status


def account_usage_limit_cooldown_summary(root: Path) -> dict[str, Any]:
    """Summarize live trigger gates intentionally deferred by account quota."""
    state_dir = root / "TestUtils" / "tester_daemon"
    now = datetime.now(timezone.utc).replace(microsecond=0)
    active: list[dict[str, str]] = []
    cooling: list[dict[str, str]] = []
    for trigger_type, plan_name, ack_name in (
        ("solver", "solver_trigger_plan.json", "solver_trigger_ack_state.json"),
        ("tester", "tester_trigger_plan.json", "tester_trigger_ack_state.json"),
    ):
        plan = read_json(state_dir / plan_name)
        ack_state = read_json(state_dir / ack_name)
        sent = ack_state.get("sent", {}) if isinstance(ack_state.get("sent"), dict) else {}
        triggers = plan.get("triggers", []) if isinstance(plan.get("triggers"), list) else []
        for trigger in triggers:
            if not isinstance(trigger, dict):
                continue
            status = str(trigger.get("status", "") or "")
            key = str(trigger.get("key", "") or "")
            thread_id = str(trigger.get("thread_id", "") or "")
            if status in TERMINAL_TRIGGER_STATUSES or not key or not thread_id:
                continue
            item = {
                "type": trigger_type,
                "op": str(trigger.get("op", "") or ""),
                "key": key,
                "status": status,
                "retry_after": "",
            }
            active.append(item)
            record = sent.get(key) if isinstance(sent, dict) else None
            if not isinstance(record, dict):
                continue
            retry_after_text = str(record.get("delivery_retry_after", "") or "")
            retry_after = parse_timestamp(retry_after_text)
            if (
                str(record.get("delivery_retry_reason", "") or "") == "account_usage_limit"
                and retry_after is not None
                and now < retry_after
            ):
                item["retry_after"] = retry_after.isoformat().replace("+00:00", "Z")
                cooling.append(item)
    retry_times = [item["retry_after"] for item in cooling if item.get("retry_after")]
    return {
        "active_trigger_count": len(active),
        "cooling_trigger_count": len(cooling),
        "all_active_triggers_cooling": bool(active) and len(active) == len(cooling),
        "retry_after": min(retry_times) if retry_times else "",
        "ops": sorted({item["op"] for item in cooling if item.get("op")}),
        "triggers": cooling,
    }


def ack_has_live_delivery_fence(record: Any) -> bool:
    """Prevent duplicate delivery without claiming that storage activity is IDE-visible."""
    if not isinstance(record, dict):
        return False
    turn_id = str(record.get("turn_id") or record.get("native_id") or "")
    if not turn_id:
        return False
    status = normalized_trigger_status(record, str(record.get("status", "") or ""))
    native_status = str(record.get("native_status") or record.get("turn_status") or "")
    if status != "active" or native_status not in {"active", "inProgress", "running"}:
        return False
    observed_at = parse_timestamp(
        str(record.get("last_observed_at") or record.get("updated_at") or "")
    )
    if observed_at is None:
        return False
    now = datetime.now(timezone.utc).replace(microsecond=0)
    if (now - observed_at).total_seconds() > ACTIVE_ACK_COVER_SECONDS:
        return False
    # A PID can be reused days later on Windows.  It is supporting evidence
    # only after the terminal-status and observation-age fences above pass.
    if str(record.get("delivery", "") or "") == "codex-cli-exec-resume":
        return codex_cli_resume_process_active(record)
    return True


def trigger_still_actionable(root: Path, trigger_type: str, trigger: dict[str, Any]) -> bool:
    gate_stage = str(trigger.get("gate_stage", "") or "")
    op = str(trigger.get("op", "") or "")
    if trigger_type == "solver" and op and operator_has_active_resource_lease(root, op):
        return False
    if trigger_type != "tester" or gate_stage != "casegen-evidence-incomplete":
        return True
    if not op:
        return False
    issue = latest_casegen_evidence_issue(
        root,
        op,
        season=str(trigger.get("season", "") or ""),
    )
    if issue is None:
        return False
    if issue.case_version == "missing":
        return True
    case_version = extract_case_version(str(trigger.get("key", "") or ""))
    if case_version and issue.case_version != case_version:
        return False
    return True


def operator_has_active_resource_lease(root: Path, op: str) -> bool:
    leases = read_json(root / "TestUtils" / "tester_daemon" / "leases.json").get("leases", [])
    return any(
        isinstance(item, dict)
        and str(item.get("op", "") or "") == op
        and str(item.get("resource_id", "") or "")
        for item in leases
    )


def tester_casegen_active_covering_record(sent: Any, op: str, current_key: str) -> dict[str, Any] | None:
    if not isinstance(sent, dict) or not op:
        return None
    current_case_version = extract_case_version(current_key)
    prefix = f"{op}|"
    for key, record in sent.items():
        trigger_key = str(key)
        if trigger_key == current_key or not trigger_key.startswith(prefix):
            continue
        if "|needs-case-version|" not in trigger_key and "|casegen-evidence-incomplete|" not in trigger_key:
            continue
        if current_case_version and extract_case_version(trigger_key) != current_case_version:
            continue
        if not isinstance(record, dict):
            continue
        status = normalized_trigger_status(record, str(record.get("status", "") or ""))
        if status not in {"sent", "delivered", "acked", "active"}:
            continue
        if codex_cli_resume_process_dead(record):
            continue
        # IDE visibility proves delivery, not that an agent turn is still
        # running. An old active ack must expire after the observation fence so
        # a Codex restart/interrupted turn can produce a retry outbox entry.
        if status == "active":
            if not ack_has_live_delivery_fence(record):
                continue
        elif not ack_is_ide_confirmed(record):
            continue
        if not str(record.get("turn_id") or record.get("native_id") or ""):
            continue
        return record
    return None


def tester_casegen_active_covering_ack(sent: Any, op: str, current_key: str) -> bool:
    return tester_casegen_active_covering_record(sent, op, current_key) is not None


def solver_active_turn_covering_ack(
    sent: Any,
    op: str,
    thread_id: str,
    solver_observations: Any,
) -> bool:
    if not isinstance(sent, dict) or not op or not thread_id:
        return False
    observed_by_op: dict[str, dict[str, Any]] = {}
    threads = solver_observations.get("threads", []) if isinstance(solver_observations, dict) else []
    if isinstance(threads, list):
        for item in threads:
            if not isinstance(item, dict):
                continue
            observed_op = str(item.get("op", "") or "")
            observed_thread_id = str(item.get("thread_id", "") or "")
            if (
                observed_op
                and str(item.get("role", "solver") or "solver") == "solver"
                and (not observed_thread_id or observed_thread_id == thread_id)
            ):
                observed_by_op[observed_op] = item
    observed = observed_by_op.get(op)
    if not isinstance(observed, dict):
        return False
    if str(observed.get("thread_id", "") or "") != thread_id:
        return False
    if not solver_observation_is_recent(observed):
        return False
    observed_turn_id = str(observed.get("latest_turn_id", "") or "")
    observed_status = str(observed.get("latest_turn_status", "") or "")
    if observed_status not in {"inProgress", "active", "running"} or not observed_turn_id:
        return False
    prefix = f"{op}|"
    for key, record in sent.items():
        if not str(key).startswith(prefix) or not isinstance(record, dict):
            continue
        if str(record.get("thread_id", "") or "") != thread_id:
            continue
        if str(record.get("status", "") or "") not in {"sent", "delivered", "acked", "active"}:
            continue
        turn_id = str(record.get("turn_id") or record.get("native_id") or "")
        if turn_id != observed_turn_id:
            continue
        return True
    return False


def solver_observation_has_active_turn(
    solver_observations: Any,
    op: str,
    thread_id: str,
) -> bool:
    """Fence an active native turn even if its delivery ack was lost."""
    return thread_observation_has_active_turn(
        solver_observations,
        op,
        thread_id,
        role="solver",
    )


def thread_observation_has_active_turn(
    solver_observations: Any,
    op: str,
    thread_id: str,
    *,
    role: str,
) -> bool:
    """Fence any role's active native turn even if its delivery ack was lost."""
    if not isinstance(solver_observations, dict) or not op or not thread_id:
        return False
    threads = solver_observations.get("threads", [])
    if not isinstance(threads, list):
        return False
    for observed in threads:
        if not isinstance(observed, dict):
            continue
        if str(observed.get("op", "") or "") != op:
            continue
        if str(observed.get("role", "solver") or "solver") != role:
            continue
        if str(observed.get("thread_id", "") or "") != thread_id:
            continue
        if not solver_observation_is_recent(observed):
            continue
        if str(observed.get("latest_turn_status", "") or "") not in {
            "inProgress",
            "active",
            "running",
        }:
            continue
        if str(observed.get("latest_turn_id", "") or ""):
            return True
    return False


def solver_recent_active_ack_covering_op(
    sent: Any,
    op: str,
    thread_id: str,
    solver_observations: Any,
) -> bool:
    if not isinstance(sent, dict) or not op or not thread_id:
        return False
    observed_by_turn: dict[str, dict[str, Any]] = {}
    threads = solver_observations.get("threads", []) if isinstance(solver_observations, dict) else []
    if isinstance(threads, list):
        for item in threads:
            if not isinstance(item, dict):
                continue
            if str(item.get("op", "") or "") != op:
                continue
            if str(item.get("thread_id", "") or "") not in {"", thread_id}:
                continue
            turn_id = str(item.get("latest_turn_id", "") or "")
            if turn_id:
                observed_by_turn[turn_id] = item
    prefix = f"{op}|"
    now = datetime.now(timezone.utc).replace(microsecond=0)
    for key, record in sent.items():
        if not str(key).startswith(prefix) or not isinstance(record, dict):
            continue
        if str(record.get("thread_id", "") or "") != thread_id:
            continue
        if normalized_trigger_status(record, str(record.get("status", "") or "")) != "active":
            continue
        if not (ack_is_ide_confirmed(record) or ack_has_live_delivery_fence(record)):
            continue
        turn_id = str(record.get("turn_id") or record.get("native_id") or "")
        if not turn_id:
            continue
        observed = observed_by_turn.get(turn_id)
        if observed is not None and not solver_observation_is_recent(observed):
            continue
        observed_status = str(observed.get("latest_turn_status", "") or "") if observed else ""
        if observed_status in {"completed", "failed", "cancelled"}:
            continue
        native_status = str(record.get("native_status") or record.get("turn_status") or record.get("status") or "")
        if native_status not in {"active", "inProgress", "running"}:
            continue
        updated_at = parse_timestamp(str(record.get("updated_at", "") or ""))
        if updated_at is None:
            continue
        if (now - updated_at).total_seconds() > ACTIVE_ACK_COVER_SECONDS:
            continue
        return True
    return False


def solver_observation_is_recent(observed: dict[str, Any]) -> bool:
    """Reject a repeatedly-polled turn once its real activity has gone stale."""
    idle_seconds = observed.get("idle_seconds")
    if isinstance(idle_seconds, (int, float)):
        return float(idle_seconds) <= ACTIVE_ACK_COVER_SECONDS
    latest_activity_at = parse_timestamp(str(observed.get("latest_activity_at", "") or ""))
    if latest_activity_at is None:
        # Older observation payloads do not carry activity age. Preserve their
        # short ack-based fence rather than turning a schema upgrade into duplicates.
        return True
    now = datetime.now(timezone.utc).replace(microsecond=0)
    return (now - latest_activity_at).total_seconds() <= ACTIVE_ACK_COVER_SECONDS


def extract_case_version(key: str) -> str:
    case_version = ""
    for token in key.replace("\\", " ").replace("/", " ").replace(";", " ").split():
        if token.startswith("case_v"):
            case_version = token.strip("`'\".,:;)")
    return case_version


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}
