import importlib.util
from pathlib import Path

spec = importlib.util.spec_from_file_location("unified_core_demo", Path(__file__).resolve().parents[1] / "scripts/demo_unified_core.py")
demo = importlib.util.module_from_spec(spec)
spec.loader.exec_module(demo)


def test_synthetic_failure_feedback_revision_success_uses_exported_services(tmp_path):
    report = demo.run_demo(tmp_path / "demo", operators=2)
    assert report["state"] == "passed"
    assert report["pending_ack"] == 4
    assert all(item["local_pass"] and item["owner_revision"] == 2 for item in report["operators"])
    assert report["external_submissions"] == report["model_calls"] == report["hardware_calls"] == 0
