# Reference deployment observations

Draft for the two V5 milestones. This is not a benchmark of the public package.

The private AscendOP integration exercised six operator development tasks during
the September 2026 V5 iteration. At the recorded pause:

| Observation | Scope |
| --- | --- |
| Five of six tasks had local and external correctness success | Historical matching-candidate acceptance; one task's external correctness remained open. |
| All six had full local correctness results | Local coverage is not equivalent to undisclosed external test coverage. |
| The five completed tasks entered local performance iteration | Later source versions require their own correctness evidence; old external success is not inherited. |
| Performance experiments produced improvements and regressions | Comparable baselines were used; this document publishes no private timings or universal speedup claim. |
| Local results and external feedback drove subsequent Solver actions | Actual automatic handoffs occurred, but maintenance and diagnosis also involved human-directed assistance. |

The important system result is a working path from an Agent's proposed change
through managed evaluation to the next revision, with retained request identity
and evidence. The observation does not establish complete V5 acceptance,
unattended reliability over an arbitrary duration, or performance leadership.

## Lessons being carried into the public milestones

- Durable acceptance and continuation intent belong together; an ACK is not a
  reason to discard an already accepted business result.
- Workspace files can carry the detail while messages identify the current task
  and next action. A conversation transcript is not a second control database.
- Giving a Solver input-case authoring capability helped expose local defects;
  the trusted oracle and acceptance thresholds remained outside that capability.
- External rejection, delayed feedback and exhausted submission policy require
  distinguishable responses. None should be confused with a numerical failure.
- A resident process being alive is not evidence that an iteration is advancing.
- Baseline comparability and regression preservation matter more than counting
  completed experiments or treating every small timing change as a speedup.

The public synthetic examples must have their own reproducible commands and
qualification reports. They must not be presented as reproductions of these
private hardware or external-evaluation observations. Operator implementations,
test inputs, receipts, account identifiers and raw measurement artifacts are not
part of this publication.
