# MADP for OP

MADP for OP is a domain-adaptable protocol and control-plane core for
multi-agent development workflows. It was extracted from AscendOP, where
multiple coding agents iterate on operators while scarce test endpoints remain
under deterministic, auditable control.

This repository intentionally publishes the **MADP core**, not the complete
AscendOP system. The public boundary contains typed contracts, durable action
state, leases and receipts, provider-neutral Agent execution, the modern daemon
control plane, and focused tests. GP/Engine architecture and interfaces are
documented publicly; machine bindings, endpoint implementation, operator assets,
and official evaluation stay in the private AscendOP deployment.

The working checkout is preparing the **V5 Unified Core** preview (5.5.0a1).
It shares completion transactions, recoverable outbox delivery, workspace
ownership and accepted-fact views with the reference integration. A file-only
Solver client and short notifications keep detailed interaction in workspace
files. This is not a claim of complete Flow V5 acceptance or a turnkey deployment.

The new candidate is not yet published: both Unified Core and the subsequent
Iteration Runtime milestone must qualify before the first is pushed. See the
[two-release checklist](docs/architecture/V5_TWO_RELEASE_CHECKLIST.md),
[component boundary](docs/architecture/V5_PUBLIC_COMPONENT_MAP.md), and
[synthetic example](docs/architecture/UNIFIED_CORE_DEMO.md). The example exercises
failure, feedback, revision and local success without live models or hardware.

## Core architecture

```text
domain adapter                         GP / Engine port
     |                                       ^
     v                                       |
typed immutable action -> daemon control -> approved external work
     |                         |
     v                         v
Agent runner ------------> typed receipt + evidence
```

The reasoning performed by an Agent is open-ended. MADP limits only the
side-effect boundary: work is admitted as a typed action, claimed with a lease,
completed with a receipt, and advanced by a control-plane gate. An Agent cannot
silently select an endpoint, rewrite queue state, or invent a workflow gate.

## Published packages

| Package | Responsibility |
| --- | --- |
| `ascendop-protocol` | Immutable action, workflow, management, evidence, and wire contracts. |
| `ascendop-control` | SQLite-backed action state, leases, receipts, idempotency, and query/command services. |
| `ascendop-agent-runner` | Serialized Agent execution, workspace isolation, provider drivers, heartbeat, and uncertain-turn recovery. |
| `ascendop-tester-daemon` | Public modern control-plane slice: action coordination, scheduling, retry, storage, transport ports, observability, registries, and gates. |

The `ascendop_*` Python namespace is retained for compatibility with the
reference deployment. The contracts themselves separate role, provider,
transport, endpoint, and domain policy so another domain can replace the
AscendOP profile without replacing the action lifecycle.

## What is generic

- configured Agent roles and provider drivers;
- immutable actions and idempotency identities;
- claim, run, uncertain, retry, completion, failure, and cancellation states;
- leases, heartbeats, receipts, output seals, and evidence references;
- daemon-owned gate authority and typed escalation;
- pluggable domain adapters and external executor transports.

AscendOP supplies one reference profile: Solver and Tester roles, operator
candidate identities, CANN endpoint capabilities, GitPartner transport, and
correctness/performance evidence. Those choices are not requirements of MADP.
See [Generality](docs/architecture/GENERALITY.md) and
[Core boundary](docs/architecture/CORE_BOUNDARY.md). The implementation split is
detailed in [Public daemon slice](docs/architecture/DAEMON_PUBLIC_SLICE.md), and
the remote execution port in [GP and Engine boundary](docs/architecture/GP_ENGINE_BOUNDARY.md).

## Repository boundary

Included:

- `packages/ascendop_protocol`
- `packages/ascendop_control`
- `packages/ascendop_agent_runner`
- `packages/ascendop_daemon`
- architecture documents, package tests, publication manifest, and provenance

Not included:

- daemon resident-service bootstrap and historical compatibility bridge;
- GitPartner and endpoint Engine implementation, live routing, or machine bootstrap;
- live topology, credentials, queues, databases, payloads, or results;
- operator source, testcase collections, profiles, or official evaluation.

The persistent local checkout lives at `AscendOP/code/MADP-for-OP`. Public
updates are one-way, allowlisted exports from AscendOP core sources into this
repository. See [Publication model](docs/architecture/PUBLICATION_MODEL.md).

## Development

Python 3.10 or newer is required.

```bash
python -m pip install -e packages/ascendop_protocol
python -m pip install -e packages/ascendop_control
python -m pip install -e packages/ascendop_agent_runner
python -m pip install -e packages/ascendop_daemon
python -m pytest -q
```

From the private AscendOP workspace, maintainers can check implementation drift
without copying any unlisted component:

```bash
python scripts/sync_from_ascendop.py --ascendop-root .. --check
```

Use `--apply` only after reviewing the manifest and source changes, then run the
tests and publication scan before committing.

## Milestones

- `archive-2026-06-02`: Scheduler V2 source snapshot.
- `archive-2026-08-07-flow-v3`: typed Flow V3 data-plane snapshot.
- `archive-2026-08-16-flow-v4-core`: curated Flow V4 MADP core architecture.
- `preview-2026-08-21-flow-v5-core`: preliminary Flow V5 protocol and control
  checkpoint, before standalone test-gateway development.

Historical tags preserve the evolution of the project. The Flow V4 core tag is
the first milestone with the deliberately narrow public boundary described
above. See [Flow V5 preview](docs/architecture/FLOW_V5_PREVIEW.md) for the
current checkpoint and its explicit incomplete scope.

## License

Apache License 2.0. See [LICENSE](LICENSE).
