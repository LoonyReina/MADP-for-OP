from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path
from typing import Any


PROFILE_SOURCE_SUFFIXES = {".c", ".cc", ".cpp", ".cxx", ".h", ".hpp"}
PROFILE_ROUNDS_PATTERN = re.compile(
    rb"(\b(?:constexpr\s+)?(?:std::)?int(?:64_t)?\s+"
    rb"kProfileRounds\s*=\s*)([0-9]+)(\s*;)"
)


def discover_profile_round_declarations(task_case: Path) -> list[dict[str, Any]]:
    declarations: list[dict[str, Any]] = []
    if not task_case.is_dir():
        return declarations
    for path in sorted(task_case.rglob("*"), key=lambda item: item.as_posix()):
        if not path.is_file() or path.suffix.lower() not in PROFILE_SOURCE_SUFFIXES:
            continue
        data = path.read_bytes()
        for match in PROFILE_ROUNDS_PATTERN.finditer(data):
            declarations.append(
                {
                    "path": path,
                    "relative_path": path.relative_to(task_case).as_posix(),
                    "rounds": int(match.group(2)),
                    "start": match.start(2),
                    "end": match.end(2),
                }
            )
    return declarations


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def write_receipt(path: Path, receipt: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(
        json.dumps(receipt, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def apply_profile_rounds_override(
    task_case: Path,
    *,
    effective_rounds: int,
    receipt_path: Path,
    expected_anchor_count: int,
) -> dict[str, Any]:
    task_case = task_case.resolve()
    if effective_rounds < 1:
        raise ValueError("effective rounds must be positive")
    if expected_anchor_count < 0:
        raise ValueError("expected anchor count cannot be negative")

    declarations = discover_profile_round_declarations(task_case)
    receipt: dict[str, Any] = {
        "protocol_version": "ascendop-profile-rounds-override-v1",
        "task_case": str(task_case),
        "requested_effective_rounds": effective_rounds,
        "expected_anchor_count": expected_anchor_count,
        "observed_anchor_count": len(declarations),
        "anchors": [
            {
                "path": declaration["relative_path"],
                "rounds": declaration["rounds"],
            }
            for declaration in declarations
        ],
    }
    if len(declarations) != expected_anchor_count:
        receipt["status"] = "anchor-count-mismatch"
        write_receipt(receipt_path, receipt)
        raise RuntimeError(
            "profile-round anchor count changed: "
            f"expected={expected_anchor_count} observed={len(declarations)}"
        )
    if not declarations:
        receipt.update({"status": "not-applicable", "changed": False})
        write_receipt(receipt_path, receipt)
        return receipt
    if len(declarations) != 1:
        receipt["status"] = "ambiguous"
        write_receipt(receipt_path, receipt)
        raise RuntimeError(
            f"profile-round override is ambiguous: anchors={len(declarations)}"
        )

    declaration = declarations[0]
    path = Path(declaration["path"])
    before = path.read_bytes()
    original_rounds = int(declaration["rounds"])
    after = (
        before[: declaration["start"]]
        + str(effective_rounds).encode("ascii")
        + before[declaration["end"] :]
    )
    if after != before:
        path.write_bytes(after)
    receipt.update(
        {
            "status": "changed" if after != before else "already-effective",
            "changed": after != before,
            "path": declaration["relative_path"],
            "original_rounds": original_rounds,
            "effective_rounds": effective_rounds,
            "before_sha256": sha256_bytes(before),
            "after_sha256": sha256_bytes(after),
        }
    )
    write_receipt(receipt_path, receipt)
    return receipt


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Apply an audited profile-round override to an Engine work copy."
    )
    parser.add_argument("--task-case", required=True)
    parser.add_argument("--effective-rounds", type=int, required=True)
    parser.add_argument("--receipt", required=True)
    parser.add_argument("--expected-anchor-count", type=int, required=True)
    args = parser.parse_args()
    try:
        receipt = apply_profile_rounds_override(
            Path(args.task_case),
            effective_rounds=args.effective_rounds,
            receipt_path=Path(args.receipt),
            expected_anchor_count=args.expected_anchor_count,
        )
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"PROFILE_ROUNDS_OVERRIDE_FAILED:{exc}")
        return 2
    print(
        "PROFILE_ROUNDS_OVERRIDE:"
        f"status={receipt['status']}:"
        f"anchors={receipt['observed_anchor_count']}:"
        f"rounds={receipt['requested_effective_rounds']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
