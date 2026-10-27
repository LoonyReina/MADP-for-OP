# Gateway lifecycle hardening (5.6.1a2)

Integrates contributions from @overdued in PRs #5 and #7, together with
maintainer regression coverage. PR #6 was merged independently.

## Changes

- Serialize submit, status, cancel and prepublication resume per request across
  processes, without conflating the lifecycle lock with the journal write lock.
- Preserve durable cancellation across a restart before local finalization;
  uncertain publication reconciles rather than republishing a cancelled request.
- Reject foreign receipts and reconciled rejection records before persistence.
- Test cancellation lock timeout and successful retry without duplicate submit.
- Distinguish journal file fsync from platform-specific directory fsync.

The selected gateway runtime was ported to private upstream and synced back
through the existing allowlist. No private adapters or deployment data were
exported. Historical release snapshots are unchanged.

The protocol schema registry's retained public release label is advanced with
the five package versions; schema definitions themselves are unchanged. Other
private protocol drift is deliberately not exported.

## Upgrade and evidence boundaries

Drain older writers before upgrading a shared run directory: old versions do not
observe the new operation lock. Transport I/O must remain bounded by adapters.
A lock timeout is not a durable cancellation acknowledgement; retry cancellation
on the same request after the active owner completes. This is not distributed
exactly-once execution, nor a filesystem power-loss guarantee.

Qualification details and component identities are recorded in PROVENANCE.json.
Synthetic tests do not establish live provider, device or external evaluator
behavior. No production service is upgraded by this source release.
