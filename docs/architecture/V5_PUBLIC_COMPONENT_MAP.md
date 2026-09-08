# V5 public component mapping (preparation)

This is a source/extraction record, not an installed-release or deployment ledger.
The public checkout is being prepared. Neither new milestone has been published.

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
Remaining A acceptance includes final package/provenance qualification and review
of the new Git history.
Do not describe the generic host ports as a turnkey private deployment.

## B: Iteration Runtime

| Capability | Upstream source candidates | Extraction / qualification required |
| --- | --- | --- |
| Multiplexed provider connection | runtime/app_server_rpc.py; app_server_process.py | Isolate process hosting from model policy. Test timeout with original ID, oversized complete frames and subsequent healthy replies. |
| Runtime identity and writer quiescence | runtime/process_identity.py; windows_gated_process.py; resident_writer_observation.py | Real child-process tests and explicit Windows support scope; never interpret unknown as exited. |
| Native admission / receipt binding | automation/resident_terminal.py; native_terminal.py | Replace onboarding lookups with explicit admission/journal ports; keep one original outbox and owner fence. |
| Local executor boundary | standalone Gateway contracts and service adapters | Publish neutral transport contracts and synthetic executor integration, not private GP/Engine topology. |
| External evaluation handoff | automation/official_completion.py; official delivery interfaces | Separate local/remote queues, same candidate identity; inject external receipt/policy ports, no browser account export. |
| Case authoring / regression protection | workspace case intake and standalone case file contract | Separate generic revision/protection rules from operator-specific validators and assets. |
| Performance comparability | automation/performance_evidence.py | Extract accepted-fact baseline selection and measurement validation from private correctness-artifact loading; test environment/matrix mismatch. |
| Fair delivery and pause/drain | completion_continuations.py; resident lifecycle | Keep terminal priority and lease renewal on existing outbox; exercise slow side effects and pause without accessing production. |

These rows are a concrete preparation list, not a claim that B has already been
exported or passed. They must be implemented and tested on top of A before A is
pushed. B remains local until a separate publication authorization.

The initial upstream runtime/performance precheck passed 35 tests: RPC bounds and
recovery, controlled real Windows process startup/disposal, writer observation
classification and performance evidence. This is input evidence for extraction,
not installed B package qualification.
