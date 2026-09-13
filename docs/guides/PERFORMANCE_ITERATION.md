# Agent-owned performance iteration strategy

This sanitized strategy is not a daemon admission rule. Agents decide experiments,
legal case revisions, phases and submit/hold. Defaults should not change without
a strong recorded reason and validation arrangement. User pauses, real evaluator
quotas and source/checker authority still apply.

After external correctness PASS, start with about 30 diverse legal cases, small
and large, covering relevant dtype/layout/tail variation and regressions. Measure
baseline/candidate on the same matrix, environment and timing method. Baseline
means the latest externally accepted source, remeasured locally; never compare
totals from different matrices.

Let B be baseline total time and C candidate total time.

| Phase | Focus | Default external publication trigger |
| --- | --- | --- |
| Overall optimization | Remove large structural costs across a diverse matrix | B/C >= 2: at least 2x speedup / 50% less time |
| External-case alignment | After large gains plateau, revise work sizes while retaining diversity to probe external timing scale | C <= 0.8B: at least 20% less time |
| Targeted optimization | Remaining shape/layout/bottleneck directions | Continue aligned trigger or document a justified exception |

Gains are cumulative since release baseline, not required of every local
experiment. Failures and flat timings may guide further work. Correctness PASS
alone does not automatically publish every performance candidate.

Compare external per-case times with leading times where available. Similar
latency does not reveal hidden shapes: alignment is a probe, not reconstruction.
If a corresponding case improves locally by >50% but externally by <20%, examine
whether it is a poor proxy, noise or another bottleneck. Explore orthogonal
workload dimensions rather than one guessed shape. Repeated local stagnation may
also justify case revision. Rebaseline whenever the matrix changes.

Record stage/evidence in operator logs, not another central state machine.
External publication identifies actual tested source and original receipt.
Diagnostic submissions remain distinct from full correctness success.

**Runtime caveat:** historical exported policy classes still contain automatic
publication/threshold behavior. A live integration must reconcile them with the
agent's explicit decision; this document does not implement that migration.
