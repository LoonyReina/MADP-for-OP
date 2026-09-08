# V5 two-release preparation

Status: A and B qualified locally. Neither is published yet.
A: 235 source + 235 installed-wheel tests. B: 332 source + 332 installed-wheel tests.
The existing V5 preliminary tag remains the current public release.

Both milestones must pass before publishing the first. The second remains local
until separately authorized. These are consecutive milestones, not maintained
forks, and neither is a declaration of complete private Flow V5 acceptance.

## Milestone A: Unified Core

Proposed tag: `preview-2026-09-08-v5-unified-core` (use the actual publication date
if publication occurs later). The tag is annotated and points to the tested
commit. Package versions must be selected together with dependency constraints;
the internal V5 work-package numbers are not package version numbers.

- [x] Identify the previous public baseline and inspect source drift read-only.
- [x] Preserve the public/private one-way publication model and immutable old tags.
- [x] Add a read-only internal import-closure check for current/proposed exports.
- [x] Replace the obsolete directory-level export boundary with reviewed,
  dependency-closed components; do not include private planners to silence imports.
- [x] Port shared typed-action and managed completion transactions and delivery
  behavior; domain intake validation stays behind trusted host integration.
- [x] Port workspace ownership CAS, replay protection and the shared read-only
  snapshot behind a serialized trusted publication port (domain publication is
  still supplied by the host).
- [x] Expose the file-only proposal client and short notification renderer;
  trusted native-terminal/case acceptance remains a host integration boundary.
- [x] Demonstrate failure -> feedback -> revision -> success with synthetic inputs,
  using the real exported completion/ownership/outbox services, not a parallel ledger.
- [x] Demonstrate ACK delay cannot retract an accepted result or duplicate a successor
  through the real terminal repository and shared delivery adapter.
- [x] Cover stale owners, conflicting replay, bad records, crash recovery and pause.
- [x] Keep identifiers/concurrency control separate from boundary byte validation.
- [x] Run all public tests, build wheels, install outside the source tree and execute
  every advertised entrypoint without private PYTHONPATH, files, credentials or network.
- [x] Update README, compatibility notes, CURRENT and public provenance together
  as a qualified local candidate (no tag or push is claimed).
- [x] Review the sanitized aggregate outcome summary and candidate source files;
  publication scan and whitespace checks pass. Review final pushed refs again at publication.

## Milestone B: Iteration Runtime

Proposed tag: `preview-2026-09-08-v5-iteration-runtime`; local only at this stage.
This explicitly proposes public generic runtime ports beyond the previous narrow
core slice. It does not authorize exporting the concrete competition browser,
account configuration, production Engine/GP deployment or operator assets.

- [x] Inherit A without a second implementation lineage or completion authority.
- [x] Export the resident transport/lifecycle components behind provider-neutral ports.
- [x] Test turn terminal versus writer quiescence, process birth identity, disconnected
  responses and restart attachment; label platform-specific guarantees accurately.
- [x] Export test gateway/executor contracts and synthetic executor integration.
- [x] Separate local-test and external-evaluation queues, retaining one candidate identity.
- [x] Exercise rejection, exhausted policy allowance and uncertain external submission;
  never retry an uncertain side effect under a new identity.
- [x] Exercise active cases plus protected regressions and same-matrix performance
  comparisons; reject environment or measurement-contract mismatches.
- [x] Exercise slow publication, lease renewal, terminal priority and bounded delivery.
- [x] Demonstrate pause/drain/resume without restarting operator production.
- [x] Final isolated installation, 332 source/332 wheel tests, five wheel builds,
  dependency/origin checks, CLI and synthetic demo all pass; sanitized scan passes.
- [x] List real-process versus simulated-provider evidence; do not claim Kimi live
  qualification, full cross-platform parity or large-scale reliability without tests.

## Release order

1. Freeze source inputs and record a private-to-public mapping outside the public tree.
2. Prepare and test A; preserve its exact commit and artifacts locally.
3. Prepare and test B on top of A; preserve its exact commit and artifacts locally.
4. Recheck A's wheel hashes, tests, documentation, tag target and publication scan.
5. Push only A's commit to the public branch and A's annotated tag. B must not be
   reachable from any pushed ref. Verify the remote branch/tag after the push.
6. Keep B locally and report its ready/unpublished status with remaining limitations.

## Publication security

Export source and synthetic fixtures only. Exclude credentials, login state,
machine/user paths, endpoint identifiers, live databases, request/session/receipt
identities, logs, submissions, private cases, profiles and raw benchmark results.
Review third-party licenses separately. Redaction is not permission to redistribute.
Scan all publishable files, and inspect the staged diff and reachable new commits;
an old scanner passing the old tree does not qualify a new export.

## Qualification environment

Reuse an existing dedicated MADP test environment for routine regressions after
checking package origins and updating test dependencies. Physical placement inside
the public checkout is acceptable: isolation means separate dependencies, test
state and configuration, not a mandatory separate machine or new directory per run.
Do not load private packages through Python path overrides or use live state.

Before publishing each release, verify clean wheel installation once for its final
candidate. The optional helper
`python scripts/qualify_isolated.py --venv <existing-madp-venv> --skip-tooling --work-root <new-output-directory>`
reuses the existing environment and replaces its MADP wheels. Omit `--venv`
to create a fresh environment for a final clean-install check. The new output
directory preserves each test report, not a requirement to rebuild dependencies.
The helper copies only Git-selected public files,
scrubs provider credentials and Python path overrides, builds wheels and verifies
installed package origins. No environment is deleted automatically.
Production daemons, model sessions, competition accounts and hardware endpoints
remain paused and are not used. Package-tool installation uses the public Python
package index; test evidence stays in the isolated work directory.

### Initial findings (not release acceptance)

Latest A qualification supersedes the earlier partial runs below: **235 source
tests + 235 installed-wheel tests passed**, all four wheel builds/installations,
dependency checks, both installed CLI help entrypoints and the three-task
synthetic iteration demo passed. Provenance records the tested wheel hashes.
No model, hardware or external-evaluation call was made.

- Previous public source baseline: 110 existing tests plus 6 import-checker tests
  passed after selecting a short writable temporary directory. This run was in the
  public checkout and is not independent wheel qualification.
- Existing public tree: 259 modules, one missing literal dependency from audit_log
  to the excluded status_writer. Do not export legacy status code just to fix it.
- Proposed uncurated current-source export: 320 modules, 64 missing literal import
  references, including private onboarding, Gateway/Plan and Official components.
  These are dependency references, not 64 independent runtime failure incidents.
- Read-only source drift: 182 paths across 12 allowlisted components differ.
- Reused-environment preparation run: 223 source tests and 223 installed-wheel
  tests passed; the provenance test remains red while CURRENT names the previous
  release. This is not milestone acceptance. Newer extraction tests must also be
  included in the next whole-package run.
- Three packaging defects were fixed: a model catalog omitted from the runner
  wheel, a source module hidden by a broad credential filename ignore, and audit
  serialization importing an excluded legacy renderer.
- Shared typed completion/delivery extraction passed 31 upstream regression tests.
  Shared workspace snapshot extraction passed 39 upstream contract tests, including
  original file proposals and external-feedback projection. These are fixture/process
  tests, not new production runs.
- See [component mapping](V5_PUBLIC_COMPONENT_MAP.md) for what is shared, what is
  still host-owned, and the second milestone's extraction work.
- The subsequent private file-client/case/gap/filesystem regression run had 50
  passes and one failure: the existing private skill manifest requires obsolete
  text markers in unrelated optimization/version skills. No production rule or
  skill was changed to hide it; this failure is distinct from public package tests.

## Outcome presentation

Allowed scope after evidence review: the private reference deployment exercised six
operator tasks; five have historical local + external correctness success; all five
entered local performance iteration. One task's external correctness remains open.
These are reference-deployment observations, not public-package reproduction results,
not certification of every later performance candidate and not autonomous-operation
benchmarks. Use aggregate facts only, no raw performance tables or private artifacts.

Do not describe all intervention as autonomous. Maintenance and operator diagnosis
included human-directed support. The public synthetic demo and private observations
must be labeled separately.

## Runtime evidence boundary

B exports shared runtime mechanisms with mandatory trusted host callbacks; it does
not export the private deployment launcher or session/journal discovery layer.
The original admission/terminal database commits, external result binding and
priority/lease loop are shared, not independent demonstration state machines.

The test matrix separates real Windows process launch/exit and process-death ACK
recovery from synthetic RPC/provider responses, trusted case validators and
external policy decisions. Allowance exhaustion is injected by the trusted
policy adapter; this core neither grants quota nor contacts a submission site.
No live Kimi/provider interoperability or large-scale load claim is made.

Upstream extraction regression found a separate, non-exported legacy Job keeper
issue when launched through a Windows virtualenv redirector: its saved keeper
identity differs from the actual interpreter process identity during crash
recovery. The three failing legacy recovery fixtures are not included in B's
export or acceptance claim. The private installed deployment was not changed.
A prior skill-manifest marker mismatch also remains a separate private issue.
