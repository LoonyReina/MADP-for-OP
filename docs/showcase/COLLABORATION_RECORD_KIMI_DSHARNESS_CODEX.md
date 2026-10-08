# A sanitized collaboration record: Kimi, DSHarness and Codex

This is a public case study of a private AscendOP campaign. It is deliberately
written as an evidence pattern rather than a provider benchmark. It omits
accounts, endpoint addresses, credentials, raw logs, private source and
contest payloads. “DSHarness” names an independent harness role, not a claim of
an official provider integration.

## Roles and handoff boundary

| Participant | Kept in its native environment | Wrote to the shared workspace |
| --- | --- | --- |
| Codex / Main | architecture, admission, contract and evidence review | `BRIEF`, stage decision, handoff and release notes |
| Kimi | long-running per-operator experiments and local diagnostics | candidate source, case matrix, probe notes and result interpretation |
| DSHarness | independent host-side experiments and performance/correctness probes | baseline comparisons, GP/Engine observations and continuation notes |

The framework did not require daemon-to-harness messaging. When a harness was
not reachable through the managed session, the authorized host continued via
the same file-backed Gateway lifecycle. This is the key interoperability
property: a participant can resume from files without replaying a chat.

## Representative sequence

1. Codex records a candidate, a baseline and a discriminating question in the
   operator workspace.
2. Kimi runs the local correctness/performance matrix and writes the original
   result, not just a summary. A failure remains a usable research artifact.
3. DSHarness compares an independent execution or external-evaluator result
   with the same candidate identity. If the local improvement does not transfer,
   it records that as a geometry or invocation question rather than silently
   changing the baseline.
4. Codex reviews the evidence, decides whether the next transition is a case
   revision, a diagnostic probe, a performance experiment, a hold, or a
   handoff. The framework executes only the authorized request and returns a
   retained result plus a separate acknowledgement.

## Publicly reportable snapshot

The September 2026 six-operator campaign is included only as a sanitized usage
record. At the snapshot below, four operators had an official correctness PASS;
two still had an external correctness disagreement. The numbers describe
correctness points, not leaderboard score or rank.

| Work item | Snapshot evidence | Lesson carried into MADP |
| --- | --- | --- |
| MhcExpand | external 8/8 PASS; a repeated same-source draw showed timing variance | preserve original receipts and do not treat a redraw as a code change |
| MhcSinkhorn | external 5/5 PASS; local optimization arms did not transfer reliably | keep local proxy cases separate from evaluator geometry |
| MhcHeadCollapse | external 5/5 PASS; a local streaming candidate passed hard gates but was not yet released | separate local qualification from publication |
| SparseFlashAttention | external 6/6 PASS; packed-row experiments regressed selected cases | retain rejected hypotheses and close a route with evidence |
| LightningIndexer | 7/8 external points passed; one precision-ratio point remained WA | use direction-changing probes before more submissions |
| SparseLightningIndexerGradKLLoss | residual precision disagreements remained after an equivalence-class fix | compare repaired cases with the base signature before charging a regression |

This table is a dated snapshot, not a current service status or a reproducible
public benchmark. It demonstrates why interleaving is useful: a local PASS,
external receipt, case geometry and handoff decision are different facts.

## What is public and what is not

Public: protocol packages, synthetic Gateway demos, contracts, test results and
this sanitized workflow record. Private: concrete GP/Engine deployment,
operator source and cases, endpoint configuration, browser accounts, raw
competition artifacts and machine paths. Contributors can replace the toy
fixture with a licensed operator fixture without changing the evidence model.

