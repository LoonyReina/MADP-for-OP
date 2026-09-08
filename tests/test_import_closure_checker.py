from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("madp_import_check", ROOT / "scripts/check_import_closure.py")
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)


def source(tmp_path, name, content, *, package=False):
    path = tmp_path.joinpath(*name.split("."))
    path = path / "__init__.py" if package else path.with_suffix(".py")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


def test_missing_internal_import_is_reported_without_executing_module(tmp_path):
    modules = {"ascendop_sample.main": source(tmp_path, "ascendop_sample.main",
        "raise AssertionError('must not execute')\nimport ascendop_missing.module\n")}
    assert checker.missing_imports(modules) == [
        {"module": "ascendop_sample.main", "line": 2, "requires": "ascendop_missing.module"}]


def test_function_local_private_dependency_is_not_hidden(tmp_path):
    modules = {"ascendop_sample.main": source(tmp_path, "ascendop_sample.main",
        "def run():\n    from official_eval.service import Service\n")}
    assert checker.missing_imports(modules)[0]["requires"] == "official_eval.service"


def test_relative_import_resolves_existing_sibling(tmp_path):
    modules = {
        "ascendop_sample.main": source(tmp_path, "ascendop_sample.main", "from .helper import value\n"),
        "ascendop_sample.helper": source(tmp_path, "ascendop_sample.helper", "value = 1\n"),
    }
    assert checker.missing_imports(modules) == []


def test_relative_package_import_cannot_hide_missing_module(tmp_path):
    modules = {
        "ascendop_sample": source(tmp_path, "ascendop_sample", "", package=True),
        "ascendop_sample.main": source(tmp_path, "ascendop_sample.main", "from . import absent\n"),
    }
    assert checker.missing_imports(modules)[0]["requires"] == "ascendop_sample.absent"


def test_package_export_and_stdlib_are_allowed(tmp_path):
    modules = {
        "ascendop_sample": source(tmp_path, "ascendop_sample", "value = 1\n", package=True),
        "ascendop_sample.main": source(tmp_path, "ascendop_sample.main", "from . import value\nimport json\n"),
    }
    assert checker.missing_imports(modules) == []


def test_module_name_handles_package_and_module():
    assert checker.module_name("packages/example/src", Path("ascendop_sample/__init__.py")) == "ascendop_sample"
    assert checker.module_name("packages/example/src", Path("ascendop_sample/main.py")) == "ascendop_sample.main"
