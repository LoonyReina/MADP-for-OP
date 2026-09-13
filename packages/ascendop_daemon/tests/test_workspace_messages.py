import pytest

from ascendop_daemon.workflow.workspace_messages import workspace_entry_prompt


@pytest.mark.parametrize("phase", ["candidate repair", "case data authoring"])
def test_correctness_message_links_files_and_allows_experiments(phase):
    text = workspace_entry_prompt(operator="Demo", workspace="workspaces/demo", phase=phase, action_id="action-1")
    assert len(text) <= 800
    assert "BRIEF.md" in text and "CLIENT.json" in text
    assert "hash" not in text and "sha256" not in text
    assert "do not write control state" in text
    if phase == "candidate repair":
        assert "legal cases or diagnostics" in text
        assert "freely run local CPU analysis" in text


@pytest.mark.parametrize("performance_phase", ["baseline_bootstrap", "baseline_repair", "comparison"])
def test_performance_notification_does_not_require_stale_checklist(performance_phase):
    text = workspace_entry_prompt(operator="Demo", workspace="workspaces/demo",
        phase="performance iteration", mode="performance", performance_phase=performance_phase)
    assert "MAIN_PERFORMANCE.md" not in text
    assert "CPU analysis" in text and len(text) <= 800
    assert "no second outcome or resending accepted requests" in text
    if performance_phase == "baseline_bootstrap":
        assert "source is read-only" in text
    elif performance_phase == "baseline_repair":
        assert "do not claim speedup" in text
    else:
        assert "Improve general performance" in text


def test_notification_never_silently_truncates_identity():
    with pytest.raises(ValueError, match="800"):
        workspace_entry_prompt(operator="Demo", workspace="x" * 900, phase="candidate repair")
