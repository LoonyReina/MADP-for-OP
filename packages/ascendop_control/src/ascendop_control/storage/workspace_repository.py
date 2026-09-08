"""Workspace writer CAS in the existing control DB, with its existing outbox.

This is an ownership fence, not an action scheduler or a second result reducer.
The caller supplies the already chosen action and immutable publication plan.
"""
from __future__ import annotations

import json
from pathlib import PurePosixPath, PureWindowsPath
from typing import Any, Mapping

from .errors import ControlRepositoryError
from .outbox_repository import _json, _now, enqueue_control_intent

PUBLICATION_TOPIC = "workspace.action"
PROJECTION_TOPIC = "workspace.project"


def enqueue_workspace_projection(connection, *, workspace: str, source_outbox_id: str, created_at: str) -> str:
    """Invalidate a derived view in the SAME transaction as its source fact."""
    workspace_key(workspace)
    source = connection.execute(
        "SELECT o.origin_id,e.sequence FROM control_outbox_v5 o JOIN control_events e "
        "ON e.entity_type='control-outbox' AND e.entity_id=o.outbox_id "
        "AND e.event_type='control-outbox-enqueued' WHERE o.outbox_id=?",
        (source_outbox_id,),
    ).fetchone()
    if source is None:
        raise ControlRepositoryError("workspace projection requires an accepted source event; historical import is explicit")
    return enqueue_control_intent(
        connection, origin_id=source["origin_id"], attempt_id=source_outbox_id,
        topic=PROJECTION_TOPIC, created_at=created_at,
        payload={"workspace": workspace, "source_outbox_id": source_outbox_id,
                 "required_revision": source["sequence"]},
    )


def workspace_key(path: str) -> str:
    if (not isinstance(path, str) or not path or "\\" in path or ":" in path
            or PureWindowsPath(path).drive or PurePosixPath(path).is_absolute()
            or ".." in PurePosixPath(path).parts or path != PurePosixPath(path).as_posix()
            or path == "."):
        raise ControlRepositoryError("workspace path must be normalized and repository-relative")
    # Portable registration: paths differing only by case cannot own two writers
    # on a Windows host. Keep the original spelling in the binding itself.
    return path.casefold()


def validate_workspace_binding(binding: Mapping[str, Any]) -> dict[str, Any]:
    value = dict(binding)
    if value.get("schema") != "ascendop.workspace-owner.v1":
        raise ControlRepositoryError("unsupported workspace owner binding")
    for field in ("workspace", "campaign_id", "operator_id", "action_id", "attempt_id",
                  "lease_id", "principal_id", "native_session_id"):
        if not isinstance(value.get(field), str) or not value[field].strip():
            raise ControlRepositoryError(f"workspace owner requires {field}")
    workspace_key(value["workspace"])
    if type(value.get("revision")) is not int or value["revision"] < 1:
        raise ControlRepositoryError("workspace owner revision must be positive")
    return value


def workspace_has_owner(connection, action: Mapping[str, Any]) -> bool:
    path = action.get("origin_workspace")
    return bool(path and connection.execute(
        "SELECT 1 FROM workspace_owners_v5 WHERE workspace_key=?", (workspace_key(path),),
    ).fetchone())


class WorkspaceOwnerRepository:
    def workspace_owner(self, workspace: str) -> dict[str, Any] | None:
        with self.connection() as connection:
            row = connection.execute(
                "SELECT binding_json FROM workspace_owners_v5 WHERE workspace_key=?",
                (workspace_key(workspace),),
            ).fetchone()
        return json.loads(row["binding_json"]) if row else None

    def admit_workspace_action(
        self, *, binding: Mapping[str, Any], expected_action_id: str,
        expected_revision: int, plan: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Commit writer transfer and recoverable publication together.

        A replay returns its original admission, even after a newer owner exists;
        it never rewinds the head. Publication/delivery must check the current head.
        """
        value = validate_workspace_binding(binding)
        if (type(expected_revision) is not int or expected_revision < 0
                or value["revision"] != expected_revision + 1
                or not isinstance(expected_action_id, str)
                or bool(expected_action_id) != bool(expected_revision)):
            raise ControlRepositoryError("invalid workspace owner CAS expectation")
        payload = {"binding": value, "expected_action_id": expected_action_id,
                   "expected_revision": expected_revision, "plan": dict(plan)}
        now = _now()
        key = workspace_key(value["workspace"])
        with self.transaction() as connection:
            previous_intent = connection.execute(
                "SELECT payload_json FROM control_outbox_v5 WHERE origin_id=? AND attempt_id=? AND topic=?",
                (value["action_id"], value["attempt_id"], PUBLICATION_TOPIC),
            ).fetchone()
            if previous_intent is not None:
                if previous_intent["payload_json"] != _json(payload):
                    raise ControlRepositoryError("workspace action admission replay changed")
                return value
            head = connection.execute(
                "SELECT * FROM workspace_owners_v5 WHERE workspace_key=?", (key,),
            ).fetchone()
            if head is not None and (head["campaign_id"] != value["campaign_id"]
                    or head["operator_id"] != value["operator_id"]):
                raise ControlRepositoryError("workspace already belongs to another task/operator")
            if ((head is None and expected_revision != 0)
                    or (head is not None and (head["revision"] != expected_revision
                        or head["action_id"] != expected_action_id))):
                raise ControlRepositoryError("workspace owner CAS fenced a stale action plan")
            # Formal V4 work must be drained/retired before standalone ownership
            # is adopted. Expiry alone is not proof that its writer has stopped.
            if connection.execute(
                "SELECT 1 FROM agent_actions_v4 a WHERE "
                "lower(CASE WHEN json_valid(a.action_json) THEN json_extract(a.action_json,'$.origin_workspace') ELSE '' END)=? "
                "AND (a.state IN ('queued','claimed','running','uncertain','retry-pending') "
                "OR EXISTS(SELECT 1 FROM agent_work_leases_v4 l WHERE l.action_id=a.action_id AND l.state='active')) LIMIT 1",
                (key,),
            ).fetchone():
                raise ControlRepositoryError("formal workspace writer must be drained before owner adoption")
            connection.execute(
                "INSERT INTO workspace_owners_v5(workspace_key,campaign_id,operator_id,action_id,"
                "revision,binding_json,updated_at) VALUES(?,?,?,?,?,?,?) "
                "ON CONFLICT(workspace_key) DO UPDATE SET action_id=excluded.action_id,"
                "revision=excluded.revision,binding_json=excluded.binding_json,updated_at=excluded.updated_at",
                (key, value["campaign_id"], value["operator_id"], value["action_id"],
                 value["revision"], _json(value), now),
            )
            publication_id = enqueue_control_intent(
                connection, origin_id=value["action_id"], attempt_id=value["attempt_id"],
                topic=PUBLICATION_TOPIC, payload=payload, created_at=now,
            )
            enqueue_workspace_projection(connection, workspace=value["workspace"],
                source_outbox_id=publication_id, created_at=now)
            self._event(connection, "workspace-owner-admitted", "workspace", key, value)
        return value
