# Generality model

MADP is generic at the orchestration boundary, not by pretending every domain
has the same work. The stable core controls who may act, on which immutable
input, under which lease, and how the result becomes evidence. A domain adapter
defines what the work means.

| Concern | MADP core contract | AscendOP reference profile |
| --- | --- | --- |
| Participants | Registered roles and provider capabilities | Solver and casegen-only Tester |
| Work identity | Action ID, idempotency key, iteration, immutable context | Campaign, operator, candidate and case version |
| Authority | Daemon gate and leased ownership | Correctness, profile, optimization and case gates |
| Execution | Provider-neutral Agent runner | Codex/Claude/Kimi command adapters |
| External work | Typed request and evidence receipt boundary | CANN build/test/profile on 910B-class endpoints |
| Transport | Replaceable adapter | GitPartner |
| Recovery | Heartbeat, uncertain state, adoption, retry policy | Resume interrupted Solver/Tester turns |
| Escalation | Typed capability-gap request | Main steward extends harness/debug capability |

## Porting test

A new domain should be able to keep the action lifecycle, leases, receipts,
runner, and durable store while replacing:

1. role profiles and output contracts;
2. domain identity and evidence schemas;
3. domain gate policy composed through the daemon interfaces;
4. endpoint capability and transport adapters;
5. prompts and runbooks.

If a port must modify claim semantics or bypass typed completion to run, the
core boundary is too coupled and should be corrected.

## Finite actions, open reasoning

MADP does not reduce an Agent to a fixed list of thoughts or coding techniques.
It provides a finite vocabulary for consequential operations. The Agent may
inspect, reason, edit within its declared workspace, and choose hypotheses
freely; publishing outputs, consuming external resources, changing workflow
state, or asking for a missing capability must cross a typed boundary.

This is closer to an operating-system syscall surface than a catalogue of Agent
skills. New top-level actions should be rare. Most diagnostic growth belongs in
parameters or capability profiles under a stable action kind.
