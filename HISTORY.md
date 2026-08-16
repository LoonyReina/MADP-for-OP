# Project history

This repository imports sanitized historical milestones of AscendOP MADP. The Git
commits are created at publication time and are not backdated. Annotated tags and
release notes preserve the dates and provenance of the source snapshots.

## 2026-06-02: Scheduler V2 source snapshot

The earliest preserved architecture used `task.md` as a shared queue and receipt
ledger, one logical writer per operator, and a serial hardware lock. It proved the
basic submit, execute, receipt, and follow-up loop, but recovery and endpoint
coordination still depended on polling and operator-specific scripts.

## 2026-08-07: Flow V3 data-plane release

Flow V3 introduced typed protocol packages, endpoint-neutral daemon operations,
and an engine transport boundary. Its data plane was versioned and deployable, but
the resident control loop was not yet complete enough to justify a production
cutover. That limitation drove the later Flow V4 design.

The public milestone is reconstructed from release generation
`146832cda84837d1a61e210dd0a85af54ad66142f5a08d159e1b320701c44c4f`,
created at `2026-08-08T00:29:17Z` (`2026-08-07` PDT). Its manifest declares
`ascendop.endpoint-release.v3`, Wire V3, and control-database schema 9. The exact
included archive hashes are recorded in `release/flow-v3/PROVENANCE.json`.

For publication, deployment configuration, endpoint identities, credentials,
runtime receipts, and machine paths were removed or replaced with documented
generic placeholders. The protocol and control-flow behavior were otherwise kept
at the historical release boundary.

## Next public milestone: Flow V4

Flow V4 moves Agent execution behind typed, leased actions and receipts, separates
workflow gates from agent decisions, and adds a resident steward path for genuine
capability gaps. It will be published as a later, independently reviewed milestone
rather than folded into this historical commit.
