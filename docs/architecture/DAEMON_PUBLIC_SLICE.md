# Public daemon slice

The public `ascendop-tester-daemon` package is the reusable control plane taken
from the active AscendOP daemon. It is intentionally a component, not a
preconfigured resident service.

## Included

- Agent action admission, delivery coordination, and output validation;
- deterministic scheduling and central retry decisions;
- durable control-database repositories and state readers;
- workflow gates and action materialization;
- endpoint capability registries and route reconciliation;
- typed executor transport interfaces and result ingestion;
- audit, timing, stability, quality, and blocker attribution.

## Excluded

- CLI entrypoints, service launch scripts, and deployment configuration;
- deployment configuration and endpoint identities;
- the historical Flow V3 compatibility bridge;
- four modern modules that still import that bridge: session recovery,
  compatibility status writing, Flow V3 runtime, and Flow V3 migration;
- credentials, databases, queues, payloads, logs, and process state.

The excluded paths are named in both `publication/core-manifest.json` and the
sync program's hard-coded allowlist. This double declaration prevents a manifest
edit from silently exporting a broader daemon tree.

## Consumption

A deployment composes this package with the protocol and control packages, then
provides a domain profile, resident process wrapper, and an implementation of
the executor transport boundary. AscendOP provides one such private composition;
another project can keep the daemon lifecycle while replacing operator-specific
gates and execution adapters.
