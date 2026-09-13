# File handoff demonstration

Install all five packages using the root README (Python 3.11+, dedicated venv).
Run from the repository root:

```bash
python scripts/demo_file_collaboration.py --root artifacts/file-demo-01
```

Use a new output directory; an earlier run will not be overwritten. The script
starts two fixture processes, not model providers. Both use the same workspace
in sequence. Candidate v1 computes `x`; the toy checker expects `2*x` for inputs
`[-3, 0, 7]`. Writer 2 reads the failed result before writing v2, which computes `2*x`.

```text
file-demo-01/
  SUMMARY.json
  workspaces/DemoScale/
    candidates/v1/candidate.json
    candidates/v2/candidate.json
    cases/inputs.json
    EXPERIMENT.json
    HANDOVER.json
    HANDOVER.md
    runs/demo-round-1/   # Original failure and frozen inputs
    runs/demo-round-2/   # Repaired result; does not replace round 1
```

| Round | Writer | Result | ACK |
| --- | --- | --- | --- |
| 1 | fixture-harness-1 | failed, 1/3 | delivered |
| 2 | fixture-harness-2 | completed, 3/3 | delivered |

Each round uses `GatewayRuntime`. The trusted toy bundler snapshots candidate
and inputs; the executor performs CPU arithmetic. Wire identities and artifacts
are synthetic fixtures, not hardware evidence.

For each accepted result a fresh Gateway/transport reads the existing journal
without querying or resubmitting, verifies consumed result bytes, writes a handoff
and separately ACKs. This is object-level reattachment, not an OS/power-loss test.

`submit_decision=hold` expresses local-only intent; no external evaluator port
exists here. This does not test live publication policy. Model calls, accelerator
calls and external submissions stay zero. Compilation, provider intelligence,
concurrent writer admission and filesystem isolation are not demonstrated.

The [managed demo](../architecture/UNIFIED_CORE_DEMO.md) additionally exercises
three synthetic tasks and transactional completion/outbox delivery.
