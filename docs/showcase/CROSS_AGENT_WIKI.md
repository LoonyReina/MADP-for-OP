# Cross-agent knowledge wiki

The next MADP slice is a workspace-backed wiki rather than a chat transcript
archive. Its purpose is to let a new participant find reusable evidence while
keeping private deployments and provider-specific credentials out of the public
repository.

## Proposed cards

| Card | Required fields | Why it matters |
| --- | --- | --- |
| `TechniqueCard` | mechanism, preconditions, target hardware, expected signal, counterexample | reusable implementation knowledge |
| `DiagnosticCard` | symptom, discriminating input, observation, eliminated hypotheses | prevents repeated blind probes |
| `HandoffCard` | source identity, stage, accepted results, next action, owner | makes continuation explicit |
| `EvidenceCard` | request ID, result ID, matrix, environment, acknowledgement | separates claims from summaries |
| `BoundaryCard` | public/private classification, redaction reason, export rule | keeps open-source packaging safe |

## Card rules

- Every card links to an operator workspace or a synthetic fixture.
- A conclusion is tagged `observed`, `inferred`, or `open`; inference is never
  promoted to a hardware fact without evidence.
- Cards carry source and case identities, but never credentials, endpoint
  addresses, browser sessions or raw private payloads.
- A later participant may append a correction; it does not rewrite the original
  evidence.
- Search indexes summaries and tags, while the source file remains the durable
  authority.

## Incremental implementation

1. Start with Markdown + JSON Schema cards and a local indexer.
2. Add a workspace link checker and redaction scan to the publication scan.
3. Add adapters that export only public cards from private workspaces.
4. Add optional remote wiki synchronization after conflict and provenance
   semantics are qualified.

The wiki is deliberately downstream of the file-first contract. It should make
knowledge easier to discover without becoming another scheduler, hidden state
machine or mandatory agent SDK.

