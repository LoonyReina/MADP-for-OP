# GP and Engine boundary

MADP treats remote execution as a typed port. GP is the relay and admission
layer; Engine is the endpoint worker that performs an approved external job.
Their architecture is public even though the current deployment implementation
and machine configuration are not.

```text
managed daemon outbox / admitted standalone host
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
Gateway retention; host consumption / managed continuation
```

## GP responsibilities

- verify the registered route and target generation;
- preserve request, attempt, and idempotency identities;
- transfer only payloads admitted by the configured trusted host;
- report acceptance, progress, transport failure, and return identity;
- make retries observable without deciding workflow policy.

## Engine responsibilities

- run inside an isolated endpoint workspace;
- validate runtime and capability compatibility before execution;
- execute only the declared stages and bounded case set;
- seal structured correctness, performance, profile, and diagnostic evidence;
- return correlation identities that the daemon can verify.

## Authority

The managed daemon or standalone host admits a request against configured
endpoint capabilities. GP does not choose operator strategy, and Engine does not
promote candidates. Gateway validates and retains the return; the managed host
also commits continuation before authorizing ACK. Independent hosts may consume
and ACK the original request without native daemon-to-harness messaging.
Agents decide research stages and submit/hold; transport success is not external
correctness. See [the current boundary](FILE_FIRST_COLLABORATION.md).

The public daemon package exposes this boundary through
`ascendop_daemon.exchange.transport_contracts` and the wire/protocol packages.
Concrete SSH, repository relay, endpoint commands, credentials, and topology are
deployment concerns and are intentionally absent.
