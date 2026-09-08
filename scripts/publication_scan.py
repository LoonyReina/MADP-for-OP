from __future__ import annotations

import json
import re
import sys
import tomllib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ALLOWED_TOP_LEVEL = {
    ".gitattributes",
    ".gitignore",
    "CONTRIBUTING.md",
    "HISTORY.md",
    "LICENSE",
    "README.md",
    "SECURITY.md",
    "docs",
    "packages",
    "publication",
    "pytest.ini",
    "requirements-test.txt",
    "release",
    "scripts",
    "tests",
}
ALLOWED_PACKAGES = {
    "ascendop_protocol",
    "ascendop_control",
    "ascendop_agent_runner",
    "ascendop_daemon",
}
IGNORED_NAMES = {
    ".git",
    ".pytest_cache",
    ".pytest-tmp",
    ".ruff_cache",
    ".venv",
    "__pycache__",
    "artifacts",
    "ascendop_agent_runner.egg-info",
    "ascendop_control.egg-info",
    "ascendop_protocol.egg-info",
    "ascendop_tester_daemon.egg-info",
    "build",
    "dist",
}
FORBIDDEN_MARKERS = (
    "/home/noah/",
    "/home/yangxue/",
    "/home/orange/",
    "DearLoony",
    "openlibing-910b-cann90",
    "githubaccount-910b3-cann9",
    "ascend910b-primary",
    "orange310p",
)
SECRET_PATTERNS = (
    re.compile(r"-----BEGIN (?:RSA |OPENSSH |EC )?PRIVATE KEY-----"),
    re.compile(r"\bgh[opsu]_[A-Za-z0-9]{20,}\b"),
    re.compile(r"\bsk-(?!test-)[A-Za-z0-9_-]{12,}\b"),
)
TEXT_SUFFIXES = {
    "",
    ".cmd",
    ".ini",
    ".json",
    ".md",
    ".ps1",
    ".py",
    ".service",
    ".sh",
    ".toml",
    ".txt",
    ".yaml",
    ".yml",
}


def _walk() -> list[Path]:
    values: list[Path] = []
    for path in ROOT.rglob("*"):
        relative = path.relative_to(ROOT)
        if any(part in IGNORED_NAMES or part.startswith(".madp-sync-") for part in relative.parts):
            continue
        if path.is_file():
            values.append(path)
    return sorted(values)


def main() -> int:
    errors: list[str] = []
    top_level = {
        path.name
        for path in ROOT.iterdir()
        if path.name not in IGNORED_NAMES and not path.name.startswith(".madp-sync-")
    }
    unexpected = sorted(top_level - ALLOWED_TOP_LEVEL)
    if unexpected:
        errors.append("unexpected top-level entries: " + ", ".join(unexpected))
    package_root = ROOT / "packages"
    package_names = {path.name for path in package_root.iterdir() if path.is_dir()}
    if package_names != ALLOWED_PACKAGES:
        errors.append("public package allowlist mismatch: " + ", ".join(sorted(package_names)))

    json_count = 0
    toml_count = 0
    text_count = 0
    for path in _walk():
        relative = path.relative_to(ROOT).as_posix()
        if path.suffix.lower() not in TEXT_SUFFIXES:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            errors.append(f"non-UTF-8 public text file: {relative}")
            continue
        text_count += 1
        if relative != "scripts/publication_scan.py":
            for marker in FORBIDDEN_MARKERS:
                if marker in text:
                    errors.append(f"private deployment marker in {relative}: {marker}")
            for pattern in SECRET_PATTERNS:
                if pattern.search(text):
                    errors.append(
                        f"credential-like value in {relative}: {pattern.pattern}"
                    )
        try:
            if path.suffix.lower() == ".json":
                json.loads(text)
                json_count += 1
            elif path.suffix.lower() == ".toml":
                tomllib.loads(text)
                toml_count += 1
        except (json.JSONDecodeError, tomllib.TOMLDecodeError) as exc:
            errors.append(f"invalid structured file {relative}: {exc}")

    result = {
        "schema": "madp.publication-scan.v1",
        "state": "passed" if not errors else "failed",
        "text_files": text_count,
        "json_files": json_count,
        "toml_files": toml_count,
        "errors": errors,
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
