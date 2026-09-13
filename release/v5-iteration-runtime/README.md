# V5 Iteration Runtime

Version: `5.6.0a1`. Release tag: `preview-2026-09-13-v5-iteration-runtime`.
This second milestone builds on the published Unified Core, without a parallel state store.

The candidate adds the generic Gateway runtime and trusted host ports for native
admission/receipt commits, external completion, data-only case revision and
performance evidence. The existing delivery loop is shared with the reference
implementation, including terminal priority, claim renewal and pause/drain.

Provider transport is JSON-RPC over stdio. The included process host and writer
inventory are Windows-specific; the writer helper policy is explicitly the
Codex app-server adapter, not a claim of provider-independent process topology.
No Kimi live model, real accelerator, browser submission or production daemon
is used for qualification.

The host still owns admission policy, session/journal discovery, trusted domain
materialization, oracle validation, external submission allowance and successor
planning. Required callbacks expose these boundaries; they are not automatic
no-op implementations. This is an integration preview, not a turnkey deployment
or complete Flow V5 acceptance.

Qualification passed: 332 source tests and 332 installed-wheel tests; five wheels,
two CLI help checks, package origins/dependencies and the synthetic iteration demo.
Exact hashes are in PROVENANCE.json. See the two-release checklist for the separate
non-exported private keeper issue; this qualification does not claim to fix it.
