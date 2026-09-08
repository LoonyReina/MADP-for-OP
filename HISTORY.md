# Project history

This repository preserves sanitized architecture milestones from AscendOP MADP.
Publication commits are not backdated; source dates and release identities are
recorded in annotated tags and provenance files.

## 2026-06-02: Scheduler V2 source snapshot

The earliest preserved design used a shared task ledger, one logical writer per
operator, and a serial hardware lock. It proved the submit, execute, receipt,
and follow-up loop, while recovery still depended on polling and local scripts.

Tag: `archive-2026-06-02`.

## 2026-08-07: Flow V3 data plane

Flow V3 introduced typed protocol packages, endpoint-neutral daemon operations,
and an Engine transport boundary. The data plane was deployable, but Agent
execution and workflow recovery were not yet fully serialized behind durable
actions.

Tag: `archive-2026-08-07-flow-v3`.

## 2026-08-16: Flow V4 public core

Flow V4 places Agent work behind immutable actions, leases, heartbeats, receipts,
and explicit uncertain-turn recovery. Workflow gates remain control-plane
authority; Agents may request capabilities or escalation but cannot move those
gates themselves.

This milestone also changes the publication model. GitHub now carries the MADP
core packages, the modern daemon control-plane slice, focused tests, and the
GP/Engine architecture boundary. Resident bootstrap, concrete transport and
Engine deployment, endpoint configuration, operators, and evaluation assets
remain outside the public boundary.

Tag: `archive-2026-08-16-flow-v4-core`.

## 2026-08-21: Flow V5 preliminary core

This preview introduces versioned role and action catalogs, Manager and
Assistant authority contracts, adapter-neutral Agent execution and completion,
attempt-scoped delivery identity, registered evidence operations, typed
capability-gap routing, and multi-facet workflow projections.

It is intentionally published before implementation of the standalone
GP/Engine operator-test gateway. The preview preserves the public-core boundary
and does not include endpoint bindings, live workflow state, operator assets,
or official-evaluation implementation. It is a development checkpoint, not the
final Flow V5 acceptance release.

Tag: `preview-2026-08-21-flow-v5-core`.

## 2026-09-08: V5 Unified Core

Tag: `preview-2026-09-08-v5-unified-core`. Packages: 5.5.0a1.

Shared completion transactions, independent ACK delivery, workspace writer CAS,
file proposals and short notifications replace duplicated interaction boundaries.
Qualification: 235 source/235 wheel tests, four packages and a three-task synthetic
iteration example. Reference observations are aggregate, not reproduced benchmarks.
The next Iteration Runtime milestone qualified locally first but is not published
by this tag. No private deployment, endpoints, accounts or operator assets are included.
