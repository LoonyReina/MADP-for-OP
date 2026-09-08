import importlib.util
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location("selected_sync", Path(__file__).resolve().parents[1] / "scripts/sync_from_ascendop.py")
sync = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sync)


@pytest.mark.parametrize("name", ["../outside", "/absolute", "C:/private", "a\\b", "a/../b", "a//b", "."])
def test_selection_rejects_ambiguous_or_outside_paths(name):
    with pytest.raises(ValueError, match="canonical"):
        sync._relative_file(name)


def test_explicit_sync_preserves_retained_files_and_omits_new_upstream(tmp_path, monkeypatch):
    upstream, public = tmp_path / "upstream", tmp_path / "public"
    upstream.mkdir()
    public.mkdir()
    monkeypatch.setattr(sync, "REPOSITORY_ROOT", public)
    (upstream / "shared.py").write_text("value = 2\n")
    (upstream / "private_new.py").write_text("not_public = True\n")
    (upstream / "retained.py").write_text("private_revision = True\n")
    (public / "retained.py").write_text("public_baseline = True\n")
    sync._copy_component(upstream, public, names=["shared.py"], overrides={})
    assert (public / "shared.py").read_text() == "value = 2\n"
    assert (public / "retained.py").read_text() == "public_baseline = True\n"
    assert not (public / "private_new.py").exists()


def test_source_facade_mapping_has_explicit_destination(tmp_path, monkeypatch):
    upstream, public = tmp_path / "upstream", tmp_path / "public"
    upstream.mkdir()
    public.mkdir()
    monkeypatch.setattr(sync, "REPOSITORY_ROOT", public)
    (upstream / "public_completion.py").write_text("shared = True\n")
    sync._copy_component(upstream, public, names=["agent_completion.py"],
        overrides={"agent_completion.py": "public_completion.py"})
    assert (public / "agent_completion.py").read_text() == "shared = True\n"
    assert not (public / "public_completion.py").exists()


def test_reviewed_manifest_loads_with_no_implicit_directory_exports():
    manifest = sync._load_manifest()
    assert manifest["schema"] == "madp.public-core-manifest.v2"
    assert all("sync_paths" in component and "retained_paths" in component for component in manifest["components"])
