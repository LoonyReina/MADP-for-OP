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
