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

## Local Gateway operation exclusion

`GatewayRuntime.submit`, `status`, `cancel` and `resume_prepublication` serialize
per run directory using the permanent `.gateway-operation.lock` kernel lock.
This is separate from `.journal.lock`: transport calls hold the operation lock,
while individual journal writes still hold their short storage lock. Different
requests remain independent. The independent terminal ACK worker keeps its own
lock and consumption/retention boundary.

A contender waits at most five seconds to acquire the operation lock and raises
`TimeoutError` without transport side effects if it remains busy. This bounds
lock acquisition, not transport execution: deployment adapters must implement
bounded I/O. Cancellation may therefore wait for the active transport call;
a timeout does not prove cancellation was durably requested. Read-only journal
inspection remains available, but lifecycle mutation must use GatewayRuntime.
Transport adapters must not re-enter lifecycle methods for the same request.

Process exit releases the kernel lock; the rendezvous file is never removed.
A new process reconciles durable PUBLISHING/UNCERTAIN state rather than assuming
that publication did not happen. A durable cancellation on PREPARED converges
locally without publication. On PUBLISHING/UNCERTAIN it reconciles/cancels and
suppresses publication retry, even when `retry_uncertain=True`.

All writers sharing a run directory must use this protocol; drain older runtimes
before upgrading. This is local filesystem exclusion, not a distributed lease
or a claim of exactly-once external execution. Transport/endpoint identity and
deduplication remain required, including explicit uncertain-publication retries.
