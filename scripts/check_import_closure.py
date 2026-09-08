"""Read-only dependency check for the public tree or a proposed upstream export.

This does not import modules, copy files, or execute deployment entrypoints.
It checks all literal internal imports, including imports inside functions.
Passing is necessary but not sufficient: wheel installation and runtime tests
must still verify the advertised entrypoints. TYPE_CHECKING imports count too.
"""
from __future__ import annotations

import argparse
import ast
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
INTERNAL_PREFIXES = ("ascendop_", "limited_remote_partner", "official_eval")


def module_name(destination: str, relative: Path) -> str:
    parts = (Path(destination) / relative).parts
    start = parts.index("src") + 1
    names = list(parts[start:])
    names[-1] = Path(names[-1]).stem
    if names[-1] == "__init__":
        names.pop()
    return ".".join(names)


def collect_modules(repository: Path, source_root: Path | None = None) -> dict[str, Path]:
    manifest = json.loads((repository / "publication/core-manifest.json").read_text(encoding="utf-8"))
    modules: dict[str, Path] = {}
    ignored = set(manifest["excluded_names"])
    for component in manifest["components"]:
        destination = component["destination"]
        if "src" not in Path(destination).parts:
            continue
        base = (source_root / component["source"] if source_root else repository / destination)
        excluded = set(component.get("excluded_paths", [])) if source_root else set()
        if not base.is_dir():
            raise ValueError(f"missing component: {component['name']}")
        if source_root and "sync_paths" in component:
            overrides = component.get("source_overrides", {})
            candidates = [(Path(name), base / overrides.get(name, name)) for name in component["sync_paths"]
                          if name.endswith(".py")]
            candidates += [(Path(name), repository / destination / name) for name in component["retained_paths"]
                           if name.endswith(".py")]
        else:
            candidates = [(path.relative_to(base), path) for path in sorted(base.rglob("*.py"))]
        for relative, path in candidates:
            if any(part in ignored for part in relative.parts) or relative.as_posix() in excluded:
                continue
            if path.is_symlink():
                raise ValueError(f"symlink in component: {component['name']}")
            if not path.is_file():
                raise ValueError(f"missing selected file in component: {component['name']}")
            modules[module_name(destination, relative)] = path
    return modules


def missing_imports(modules: dict[str, Path]) -> list[dict[str, object]]:
    # Parent namespaces are valid even when the module uses a namespace package.
    available = set(modules)
    for module in modules:
        parts = module.split(".")
        available.update(".".join(parts[:i]) for i in range(1, len(parts)))
    missing = []
    for name, path in sorted(modules.items()):
        package = name if path.name == "__init__.py" else name.rpartition(".")[0]
        tree = ast.parse(path.read_text(encoding="utf-8-sig"), filename=name)
        for node in ast.walk(tree):
            targets = []
            if isinstance(node, ast.Import):
                targets = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                if node.level:
                    parts = package.split(".")
                    if node.level > len(parts):
                        targets = ["ascendop_invalid_relative_import"]
                    else:
                        base = ".".join(parts[:len(parts) - node.level + 1])
                        targets = [base + ("." + node.module if node.module else "")]
                        if not node.module:
                            # Package attributes can be re-exported by __init__.
                            # Unknown dynamic exports need a separate runtime test.
                            declared = set()
                            if base in modules:
                                package_tree = ast.parse(modules[base].read_text(encoding="utf-8-sig"))
                                for child in package_tree.body:
                                    if isinstance(child, (ast.FunctionDef, ast.ClassDef, ast.AsyncFunctionDef)):
                                        declared.add(child.name)
                                    elif isinstance(child, (ast.Import, ast.ImportFrom)):
                                        declared.update(alias.asname or alias.name.split(".")[0] for alias in child.names)
                                    elif isinstance(child, ast.Assign):
                                        declared.update(target.id for target in child.targets if isinstance(target, ast.Name))
                            targets += [base + "." + alias.name for alias in node.names
                                        if alias.name != "*" and alias.name not in declared]
                else:
                    targets = [node.module or ""]
            for target in targets:
                if target.startswith(INTERNAL_PREFIXES) and target not in available:
                    missing.append({"module": name, "line": node.lineno, "requires": target})
    return missing


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, help="inspect proposed upstream export without copying")
    args = parser.parse_args()
    modules = collect_modules(ROOT, args.source_root.resolve() if args.source_root else None)
    missing = missing_imports(modules)
    print(json.dumps({"schema": "madp.import-closure.v1", "mode": "proposed" if args.source_root else "public",
        "state": "failed" if missing else "passed", "modules": len(modules),
        "missing_count": len(missing), "missing": missing}, indent=2))
    return 1 if missing else 0


if __name__ == "__main__":
    raise SystemExit(main())
