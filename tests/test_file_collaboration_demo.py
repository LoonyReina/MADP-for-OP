import json
import subprocess
import sys
from pathlib import Path


def test_two_writers_handoff_and_gateway_recovery(tmp_path):
    script = Path(__file__).resolve().parents[1] / 'scripts/demo_file_collaboration.py'
    root = tmp_path / 'demo'
    completed = subprocess.run([sys.executable, '-I', str(script), '--root', str(root)],
        capture_output=True, text=True, timeout=60)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    report = json.loads(completed.stdout)
    assert [row['state'] for row in report['rounds']] == ['failed', 'completed']
    assert [row['pass_count'] for row in report['rounds']] == [1, 3]
    assert all(row['ack'] == 'delivered' and row['recovered_without_resubmit'] for row in report['rounds'])
    assert report['model_calls'] == report['hardware_calls'] == report['external_submissions'] == 0
    first = root / 'workspaces/DemoScale/runs/demo-round-1/JOURNAL.json'
    assert json.loads(first.read_text())['state'] == 'failed'
    again = subprocess.run([sys.executable, '-I', str(script), '--root', str(root)],
        capture_output=True, text=True, timeout=30)
    assert again.returncode != 0
