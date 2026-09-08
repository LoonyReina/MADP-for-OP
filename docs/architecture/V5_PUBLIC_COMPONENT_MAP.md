# V5 public component mapping (preparation)

This is a source/extraction record, not an installed-release or deployment ledger.
A and B are qualified locally: 235/235 and 332/332 source/installed-wheel tests.

## A: Unified Core

| Capability | Public implementation | Upstream relationship / remaining work |
| --- | --- | --- |
| Receipt plus continuation transaction | control/storage/repository.py; automation/agent_completion_core.py; managed_completion_commit.py | Typed-action admission and the managed completion transaction are shared. Private compatibility intake subclasses the core and calls the shared commit; domain-specific validation/planning is not exported. |
| Idempotent delivery and ACK independence | control/delivery.py; storage/repositories/gp_terminal_ingest.py | Private continuation loop calls the same delivery function. Terminal, continuation and ACK intents share the existing database transaction. |
| Writer ownership and replay fencing | control/storage/workspace_repository.py | Exact upstream CAS implementation; no second owner table or queue. |
| Current view and historical feedback | automation/workspace_snapshot.py; workspace_projection_port.py | Private projector uses the same snapshot reader. Public host supplies BRIEF/CLIENT/evidence publishing under the owner lock. |
| Solver file proposal | automation/workspace_file_client.py | Private intake reuses this file client; it writes requested intent only. Trusted writer termination and case validation are separate. |
| Short notifications | workflow/workspace_messages.py | Shared renderer, bounded to 800 characters; workspace files carry details. |
| Bad-record isolation | automation/record_intake.py | Preserves malformed source records and isolates quarantine failures. |
| Audit serialization | observability/audit_serialization.py | Shared pure serializers; legacy status renderer is not exported. |
| Filesystem portability | protocol/filesystem.py; protocol/file_lock.py | Shared long-path and locking helpers. No Windows power-loss guarantee is claimed. |

Public `automation/agent_completion.py` is an explicit export facade for the
shared typed-action core, not a byte-for-byte copy of the private compatibility
facade. The core and delivery code are shared; no duplicate receipt/queue
implementation was introduced. Source changes are not automatically installed
into the paused reference deployment.

The synthetic example in `scripts/demo_unified_core.py` uses the actual owner CAS,
file client, shared managed completion transaction, terminal repository, outbox
delivery and snapshot reader. Deterministic Python fixtures supply Solver and local
execution behavior. Two rounds show failure, feedback, revision and local success
while ACK remains pending. This does not qualify a live model, native writer
boundary, NPU execution or external evaluator.

Manifest v2 now records this source-facade mapping and explicit synchronized and
retained paths. No unreviewed upstream module is automatically copied.
A's package/provenance qualification passed (235 source and installed-wheel tests).
Do not describe the generic host ports as a turnkey private deployment.

## B: Iteration Runtime

| Capability | Shared export | Qualification / host boundary |
| --- | --- | --- |
| Multiplexed connection | runtime/app_server_rpc.py | Out-of-order replies, pending original IDs, response/frame bounds and recovery; synthetic JSON-RPC peer. |
| Process hosting | runtime/app_server_process.py; windows_gated_process.py; process_identity.py | Real controlled Windows child processes; no live model or universal OS claim. |
| Writer observation | runtime/resident_writer_observation.py | Actual owned-Job sampling plus injected helper/orphan/unknown cases. Helper allowlist is explicitly Codex-specific. |
| Admission and native receipts | automation/native_commit.py; resident_boundary.py | Original owner/claim transaction and native-terminal outbox; trusted host must observe and validate before committing. Session/journal discovery remains host-owned. |
| Evaluation Gateway | ascendop_test_gateway/runtime.py, ports.py, contracts.py, journal.py, terminal_evidence.py | Same upstream lifecycle with required BundleStorePort; synthetic executor, uncertain publish, cancellation, ACK delay and real crash-cut tests. Concrete GP/Engine deployment is excluded. |
| External evaluation completion | automation/external_completion_core.py | Exact local candidate binding and transactional continuation; required receipt/policy/planning callbacks. Policy rejection is never PASS; no actual browser submission. |
| Case revision/protection | automation/case_data_policy.py | Same active/regression partition policy; only trusted task validator/equivalence may qualify changes. No private cases or oracle assets. |
| Performance comparisons | automation/performance_policy.py | Accepted-fact anchor selection; full raw50/middle20 and matrix/environment/contract checks. Domain retained-artifact loading remains upstream. |
| Delivery lifecycle | automation/delivery_runtime.py | Original loop with injected handlers/intake/stop state; terminal priority, bounded admission, claim renewal and pause/drain/resume on the existing outbox. |

The source-only private wrappers call these shared components. Nothing is installed
into the paused deployment. B remains a local integration preview, not full Flow
V5 qualification. The public manifest explicitly selects every newly shared file.

See the two-release checklist for platform guarantees, synthetic versus real
process evidence, and the separate non-exported private keeper/skill audit findings.
