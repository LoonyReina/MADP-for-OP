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
