# File-first collaboration

## Participant boundary

A participant is a model plus its harness: tools, session and execution
environment. MADP does not require that reasoning live inside MADP or share
conversation memory. Files are the durable handoff; a host adapter translates
supported proposals into typed requests. This is collaboration **between agent
environments**, not a system without agents, host integration or control state.

| Concern | Owner |
| --- | --- |
| Hypotheses, source, legal inputs, research stage, submit/hold | Participant |
| Writer admission and transfer | Managed daemon or standalone host |
| Accepted request, execution, retained return | Gateway and executor ports |
| External submission and receipt | Authorized evaluator adapter, when supplied |
| Managed BRIEF/CLIENT/result views | Projector; not agent-editable control state |

Managed: file proposal → writer quiescence → completion/outbox → Gateway →
executor → retained result → continuation view.

Standalone: writer finishes → trusted host calls Gateway → host consumes
retained result → ACK → participant continues. The host may be manually driven;
daemon-to-harness messaging is not required. Recover original accepted requests,
don't create another pending queue or spoof a managed lease.

Correctness/performance research phases live in workspace notes, not additional
competing queue state machines. Case changes require comparable baseline timing.

## Durable facts, small notifications

Notifications identify workspace/action and reference files. Detailed experiments
and logs stay in files. The public helper limits notifications to 800 characters
and permits local CPU analysis without a remote request. Failed hypotheses can
be useful; one business proposal does not mean one allowed local experiment.

Checksums bind accepted payloads and consumed evidence, not unrelated notes or
conversation changes. Acceptance and ACK are separate: a failed ACK must not
erase the accepted result. Byte identity does not replace checking source,
oracle, matrix and invocation parity.

## Implementation boundary

- Public: five packages, file client, lifecycle/policy ports, standalone Gateway
  journal, retained terminal evidence, independent ACK and recovery.
- Demonstrated: two fixture processes, CPU toy checking, a fresh Gateway object
  reading the same journal, failure preserved after repair.
- Private: concrete GP/Engine, device tools, independent Kimi/DSH handoffs and
  manual coordination. These inform design, not shipped adapter certification.
- Not yet exported: generic test-end CPU replay, complete diagnostics, portable
  external-harness bootstrap and migration of all historical auto-submit gates.

Some policy APIs retain automatic decisions/thresholds for compatibility. They
do not prove the latest agent-owned research strategy is implemented. Before
connecting a live evaluator, the host must honor an explicit authorized
submit/hold decision. The local demo does not qualify an evaluator adapter.

See [GP/Engine](GP_ENGINE_BOUNDARY.md) and [participant guide](../guides/PARTICIPANT_HANDOFF.md).
