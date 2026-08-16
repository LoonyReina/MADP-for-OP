# MADP for OP

MADP for OP is a protocol-first, multi-agent development framework for hardware
operators. It separates agent reasoning from durable workflow state and from the
machines that build, test, and profile operator candidates.

> Current public milestone: a sanitized reconstruction of the AscendOP Flow V3
> data-plane release created on 2026-08-07 PDT. Publication commits are created
> when released and are not backdated; source dates and hashes are preserved in
> tags and provenance records.

## Core model

```text
Solver / Tester agents
        |
        v
typed protocol contracts and evidence
        |
        v
daemon-owned control plane
        |
        v
Wire V3 request and GitPartner transport
        |
        v
profile-selected endpoint Engine
        |
        v
structured correctness and performance results
```

Agents remain responsible for hypotheses, source changes, and case design. The
framework constrains only side effects that must be reproducible: action identity,
leases, dispatch, endpoint selection, execution, evidence, retry, and release
state.

## What Flow V3 introduced

- Versioned, immutable Python and JSON contracts in `ascendop_protocol`.
- A daemon-owned scheduler and SQLite control plane instead of a shared task file.
- Endpoint-neutral requests carried over a versioned Wire V3 boundary.
- A remote Engine with correctness, performance, and profiling stages.
- Content-addressed release bundles for protocol, daemon, transport, and Engine.
- GitPartner as an isolated transport implementation for heterogeneous endpoints.

Flow V3 established the deployable data plane. It did not yet provide the complete
serialized AgentAction adapter and resident multi-agent control loop later designed
for Flow V4.

## Repository layout

- `packages/ascendop_protocol`: shared Flow V3 schemas and Python contracts.
- `tools/tester_daemon`: scheduler, control plane, exchange, workflow, and telemetry.
- `engine_runtime`: the minimal endpoint Engine bundled with the release.
- `GitPartner`: bounded Git-based transport and endpoint service implementation.
- `docs/architecture`: milestone architecture and known limitations.
- `release/flow-v3`: machine-readable source provenance.

## Local verification

The source requires Python 3.10 or newer. Compilation does not require accelerator
hardware:

```bash
python -m compileall -q packages/ascendop_protocol/src
python -m compileall -q tools/tester_daemon/src
python -m compileall -q GitPartner/src
python -m compileall -q engine_runtime
```

Editable installation of the control-plane packages is optional:

```bash
python -m pip install -e packages/ascendop_protocol
python -m pip install -e tools/tester_daemon
python -m pip install -e GitPartner
```

Endpoint execution additionally requires a deployment-specific topology, secrets,
toolchain, and hardware profile. Those are deliberately absent from this repository.

## Public history

- `archive-2026-06-02`: Scheduler V2 append-only task ledger.
- `archive-2026-08-07-flow-v3`: typed protocol and deployable endpoint data plane.

See [HISTORY.md](HISTORY.md) and the
[Flow V3 milestone note](docs/architecture/FLOW_V3_RELEASE.md) for the architectural
transition.

## Security and license

The public history excludes credentials, endpoint identities, private paths,
operator implementations, benchmark artifacts, production queues, and official
evaluation implementations. See [SECURITY.md](SECURITY.md) before preparing a
deployment.

Licensed under the Apache License 2.0. Ascend and other product names belong to
their respective owners. This independent project is not affiliated with or
endorsed by Huawei or OpenAI.
