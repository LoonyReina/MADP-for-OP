from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("madp_isolated_check", ROOT / "scripts/qualify_isolated.py")
qualifier = importlib.util.module_from_spec(spec)
spec.loader.exec_module(qualifier)


def test_environment_does_not_inherit_provider_or_production_settings(tmp_path, monkeypatch):
    private_names = ("PYTHONPATH", "PYTHONHOME", "CODEX_HOME", "OPENAI_API_KEY", "ANTHROPIC_API_KEY",
                     "GITHUB_TOKEN", "SSH_AUTH_SOCK", "PIP_INDEX_URL", "HTTP_PROXY", "HTTPS_PROXY")
    for name in private_names:
        monkeypatch.setenv(name, "test-private-setting")
    environment = qualifier.isolated_environment(tmp_path)
    assert all(name not in environment for name in private_names)
    assert environment["PYTHONNOUSERSITE"] == "1"
    assert environment["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] == "1"
    assert environment["TEMP"] == str(tmp_path / "tmp")


def test_path_starts_with_new_venv_and_not_inherited_path(tmp_path, monkeypatch):
    monkeypatch.setenv("PATH", "private-tools-directory")
    environment = qualifier.isolated_environment(tmp_path)
    assert "private-tools-directory" not in environment["PATH"]
    assert environment["PATH"].startswith(str(tmp_path / "venv"))


def test_reused_environment_path_is_selected(tmp_path):
    existing = tmp_path / "existing-madp"
    environment = qualifier.isolated_environment(tmp_path / "output", existing)
    assert environment["PATH"].startswith(str(existing))
    assert environment["TEMP"] == str(tmp_path / "output" / "tmp")
