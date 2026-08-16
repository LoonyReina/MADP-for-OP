# Public core boundary

The public MADP repository is a reusable kernel extracted from AscendOP. It is
not a deployment bundle and is not expected to run the AscendOP optimization
loop by itself.

## Included layers

### Protocol

The protocol package owns immutable descriptors and validation for Agent work,
workflow requests, management operations, evidence, profiles, and wire
messages. Contracts are data; they do not perform endpoint side effects.

### Durable control

The control package owns the action lifecycle, idempotency, leases, heartbeats,
receipts, registrations, and durable query/command services. It provides the
authority boundary that prevents concurrent Agents from both owning the same
logical work.

### Agent runner

The runner owns provider discovery, isolated workspaces, execution heartbeats,
result collection, uncertain-turn recovery, and typed completion. Provider
drivers are adapters and do not own workflow gates.

### Daemon control plane

The public daemon slice owns action coordination, deterministic scheduling,
central retry arbitration, storage integration, executor transport ports,
observability, endpoint capability registries, and composable workflow gates.
It is real reference implementation code synchronized from AscendOP, not only a
diagram or pseudocode model.

## Excluded layers

The following are AscendOP application or deployment concerns:

- deployment entrypoints and machine bootstrap;
- historical compatibility bridge and migration-only modules;
- GitPartner and remote Engine implementation or live bindings;
- build, correctness, performance, and profiling pipelines;
- operator source and testcase lifetime;
- official evaluation and submission policy;
- deployment topology, credentials, services, and runtime state.

An external project supplies its own resident composition, domain profile, and
executor adapters. The public daemon provides the control-plane implementation
and ports needed to do that without publishing a private deployment snapshot.

## Boundary rule

The allowlist in `publication/core-manifest.json` is executable policy. A source
path outside that manifest cannot enter the repository through the sync tool.
Adding another path is an architecture change and requires review of licensing,
secrets, domain coupling, and test ownership.
