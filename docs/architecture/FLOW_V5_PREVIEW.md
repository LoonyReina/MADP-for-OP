# Flow V5 preliminary core

This milestone is a sanitized development checkpoint of the MADP control core.
It records the architecture before standalone GP/Engine test-gateway work
begins. It does not claim completion of the full Flow V5 acceptance program.

## Included changes

- versioned actor roles, action kinds, outcomes, blockers, and evidence
  operation catalogs;
- immutable `ActorActionEnvelope`, `RoleBinding`, and typed action receipts;
- `AgentExecutionPort`, `NativeTurnOutcome`, and one daemon-owned completion
  path for native runtimes;
- attempt-scoped delivery and completion identities for bounded retry and
  reconciliation;
- structured Agent outcomes with explicit evidence requests and protocol-gap
  disposition;
- Manager lifecycle contracts, Assistant official-operation contracts, and
  typed Developer capability-gap routing;
- registered correctness, performance, profiling, replay, environment, and
  artifact-recovery evidence-operation contracts;
- multi-facet operator and workflow projections with freshness, lineage, and
  legal-next-command fields;
- focused protocol, control, Agent runner, and public daemon tests.

## Control model

Agents retain open-ended reasoning inside their assigned work. Side effects are
constrained by immutable action identity, effective role, scope, lease,
generation, typed outcome, and daemon validation. Native adapters transport
work and outcomes; they do not promote candidates or mutate workflow gates.

Routine missing evidence is represented by a registered operation rather than
an Agent-authored remote command. A reusable missing capability becomes one
typed Developer action. Manager receives the corresponding notification instead
of relying on a human to discover a stalled board by polling.

## Not yet included

- the standalone `ascendop-test` gateway for using GP/Engine without MADP;
- the Gitee latency characterization and 99% exchange-availability cohort;
- complete App Server and ACP runtime ports;
- the final multi-operator, official-platform, and performance acceptance run;
- concrete GP/Engine endpoint implementation or machine configuration.

The next public implementation milestone is expected to add the standalone
test gateway only after its private integration and transport qualification are
complete.
