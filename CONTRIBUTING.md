# Contributing

MADP for OP welcomes changes to the public protocol, durable control model,
Agent runner, daemon control-plane slice, documentation, and tests.

## Design rules

- Keep Agent reasoning separate from control-plane state and external side effects.
- Define or revise a typed contract before relying on cross-layer behavior.
- Preserve action identity, idempotency, lease, heartbeat, receipt, and evidence boundaries.
- Keep roles, providers, transports, endpoint capabilities, and domain policy configurable.
- Keep resident bootstrap, compatibility bridges, concrete GP/Engine deployment,
  operator assets, and live configuration outside this repository.
- Never include credentials, private topology, task identifiers, benchmark data, or runtime state.

## Validation

```bash
python -m pip install -r requirements-test.txt
python -m pip install -e packages/ascendop_protocol
python -m pip install -e packages/ascendop_control
python -m pip install -e packages/ascendop_agent_runner
python -m pip install -e packages/ascendop_daemon
python -m pytest -q
python scripts/publication_scan.py
```

Reuse a dedicated MADP virtual environment for routine regression tests. Verify
package origins and keep private Python path overrides, live configuration and
production services out of the test run; a new environment for every run is not
required. Before publishing each release, also verify clean wheel installation.
`python scripts/qualify_isolated.py --work-root <new-path>` is an optional helper
for that clean check, not a requirement for routine work. Editable/source tests
alone do not demonstrate wheel dependency closure.

For repeated qualification in an existing dedicated environment, add
`--venv <madp-venv> --skip-tooling`. This replaces only its four MADP wheels;
omit `--skip-tooling` if test dependencies need installation. Output directories
are new per run so previous reports are retained.

Changes imported from the private AscendOP workspace must pass
`scripts/sync_from_ascendop.py --check` after synchronization. The allowlist in
`publication/core-manifest.json` is a security boundary; widening it requires an
architecture review.

## Reporting problems

Public issues are currently disabled while the core API is being stabilized.
Use GitHub Security Advisories for security reports.
