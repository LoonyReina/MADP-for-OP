# Contributing

Thank you for helping improve MADP for OP.

## Before opening a change

- Keep agent reasoning separate from daemon-owned state and endpoint side effects.
- Add or change a protocol contract before depending on new cross-layer behavior.
- Preserve action identity, lease, receipt, and evidence boundaries.
- Use generic endpoint profiles and paths in public code and documentation.
- Never include live credentials, topology, operator submissions, or benchmark data.

## Validation

For this historical Flow V3 milestone, run:

```bash
python -m compileall -q packages/ascendop_protocol/src
python -m compileall -q tools/tester_daemon/src
python -m compileall -q GitPartner/src
python -m compileall -q engine_runtime
```

Changes to JSON contracts should also parse every schema and example. Changes to a
future milestone should include focused tests for the affected layer and preserve
the milestone provenance record.

## Reporting problems

Use a GitHub issue for reproducible bugs and architecture discussion. Report
security problems privately as described in `SECURITY.md`.
