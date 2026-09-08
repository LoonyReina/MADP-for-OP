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
        assert "try unproven hypotheses" in text
        assert "legal case coverage" in text


def test_notification_never_silently_truncates_identity():
    with pytest.raises(ValueError, match="800"):
        workspace_entry_prompt(operator="Demo", workspace="x" * 900, phase="candidate repair")
