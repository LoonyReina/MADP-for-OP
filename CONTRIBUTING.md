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
python -m pip install -e packages/ascendop_protocol
python -m pip install -e packages/ascendop_control
python -m pip install -e packages/ascendop_agent_runner
python -m pip install -e packages/ascendop_daemon
python -m pytest -q
python scripts/publication_scan.py
```

Changes imported from the private AscendOP workspace must pass
`scripts/sync_from_ascendop.py --check` after synchronization. The allowlist in
`publication/core-manifest.json` is a security boundary; widening it requires an
architecture review.

## Reporting problems

Public issues are currently disabled while the core API is being stabilized.
Use GitHub Security Advisories for security reports.
