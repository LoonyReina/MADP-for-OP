# GP and Engine boundary

MADP treats remote execution as a typed port. GP is the relay and admission
layer; Engine is the endpoint worker that performs an approved external job.
Their architecture is public even though the current deployment implementation
and machine configuration are not.

```text
daemon outbox
    |
    | immutable request + target generation + idempotency identity
    v
GP relay / admission
    |
    | accepted job identity + bounded payload
    v
endpoint Engine
    |
    | build / correctness / performance / profile stages
    v
sealed result + evidence references
    |
    v
daemon ingestion and gate reconciliation
```

## GP responsibilities

- verify the registered route and target generation;
- preserve request, attempt, and idempotency identities;
- transfer only daemon-approved payloads;
- report acceptance, progress, transport failure, and return identity;
- make retries observable without deciding workflow policy.

## Engine responsibilities

- run inside an isolated endpoint workspace;
- validate runtime and capability compatibility before execution;
- execute only the declared stages and bounded case set;
- seal structured correctness, performance, profile, and diagnostic evidence;
- return correlation identities that the daemon can verify.

## Authority

The daemon selects a gate and endpoint capability. GP does not choose an
operator or retry policy, and Engine does not promote a candidate. A transport
or execution result becomes workflow evidence only after daemon ingestion
validates its identity, schema, and expected generation.

The public daemon package exposes this boundary through
`ascendop_daemon.exchange.transport_contracts` and the wire/protocol packages.
Concrete SSH, repository relay, endpoint commands, credentials, and topology are
deployment concerns and are intentionally absent.
