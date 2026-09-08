# V5 Unified Core — public preview

Package version: 5.5.0a1. Tag: `preview-2026-09-08-v5-unified-core`.
Both milestones qualified before this release: Unified Core 235/235 tests;
Iteration Runtime 332/332 tests (source/installed wheel). Only Unified Core is
published here; Iteration Runtime remains a separate local candidate.

Local qualification passed: 235 source tests, 235 installed-wheel tests, four
wheel builds/installations, dependency and import-origin checks, both installed
CLI help entrypoints and a three-task synthetic iteration demo. Exact wheel
hashes and scope are recorded in PROVENANCE.json.

This milestone carries shared transactional completion and outbox delivery,
workspace writer CAS and accepted-fact snapshots, a file-only Solver proposal
client, short notifications, bad-record isolation and independent package checks.
It includes a runnable synthetic iteration example, not a private deployment.

The four package versions and pinned dependencies move together. The control
schema advances to 17; back up application-owned databases before migration and
do not reopen a migrated database with older code. New managed workspace ownership
fences incompatible formal writers; existing writers must be drained before
adoption. Deployment-specific migration/installation is not performed by this
public package release.

Trusted host ports still own native writer validation, domain case/oracle rules,
input freezing and external evaluation. Compatibility modules retained from the
previous public release are explicitly listed in the manifest; they are not a
claim that every private runtime component was ported.

See the component mapping, two-release checklist, synthetic demo instructions
and aggregate reference-deployment observations in docs/architecture. Five of six
reference tasks had historical matching local/external correctness success;
these private observations are separate from public synthetic test evidence.
