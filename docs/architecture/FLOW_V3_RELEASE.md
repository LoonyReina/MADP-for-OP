# Flow V3 release milestone

Flow V3 replaced the Scheduler V2 shared task ledger with a typed, deployable data
plane. This document describes the historical release represented by this commit,
not the later Flow V4 control loop.

## Layer ownership

1. Agents propose source changes, testcase changes, and evidence requests.
2. `ascendop_protocol` defines immutable request, profile, and evidence contracts.
3. `ascendop_daemon` owns scheduling, durable state, retry policy, and routing.
4. GitPartner carries bounded requests to a selected endpoint.
5. The endpoint Engine owns build, correctness, performance, and profile execution.
6. Structured results return through the daemon before another agent decision.

This separation prevents an agent from directly mutating queue state or choosing an
unregistered machine while preserving freedom inside its source workspace.

## Versioned boundaries

- Protocol package version: `3.0.0`
- Wire version: `3`
- Control database schema: `9`
- Release schema: `ascendop.endpoint-release.v3`
- Source release generation:
  `146832cda84837d1a61e210dd0a85af54ad66142f5a08d159e1b320701c44c4f`

The original release manifest bound protocol, daemon, transport, Engine, endpoint
configuration, and registries by SHA-256. The public reconstruction includes only
the source-bearing archives. See `release/flow-v3/PROVENANCE.json`.

## Publication boundary

The following deployment-owned material is intentionally absent:

- endpoint, node, daemon, and topology configuration;
- credential files and authentication material;
- operator workspaces, test cases, payloads, results, and profiles;
- control databases, queues, receipts, and runtime logs;
- official evaluation policy and implementation.

Machine-specific defaults preserved inside source archives were replaced with
`/opt/ascendop`, the service user `ascendop`, and example endpoint identifiers.

## Known limitations

- The release bundle did not contain its development test suite. This public commit
  is verified by archive hashes, JSON validation, source compilation, and import
  smoke checks.
- GitPartner is one transport implementation, not a protocol requirement.
- Flow V3 does not yet serialize all Solver and Tester turns behind typed action
  leases and receipts.
- Deployment is intentionally not turnkey because live endpoint configuration is
  outside the public source boundary.

These limitations are architectural evidence for the later Flow V4 milestone.
