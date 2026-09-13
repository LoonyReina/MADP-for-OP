# Evidence and limitations

## Public reproducibility

Release B (`preview-2026-09-13-v5-iteration-runtime`) qualified five packages with
332 source and 332 installed-wheel tests in a dedicated Windows environment.
They test the same behaviors through different installation paths, not 664
distinct features. No live model, accelerator or external evaluator was used.
[B provenance](../../release/v5-iteration-runtime/PROVENANCE.json).

The next edition adds two-process handoff, short-prompt regression and long-path
artifact coverage. [Candidate qualification](../../release/file-first-collaboration/README.md)
records the actual results. This tests protocols, not multi-model intelligence
or cross-platform production availability.

## Private experience informing the design

AscendOP has involved Codex, Kimi and an independent collaborator called DSH.
DSH is a collaboration label here, not a publicly certified provider adapter.
Work included correctness diagnosis, performance experiments, handoff and human
intervention; it was not a controlled model comparison or fully unattended run.

| Difficulty | Lesson | Public status |
| --- | --- | --- |
| Independent harness has no native daemon messaging | Allow manually driven host with the same test lifecycle | Generic Gateway public; GP/Engine deployment private |
| Evidence lives in another conversation | Keep source, diagnostics and next steps in workspace | Guide and synthetic demo |
| Rigid prompts repeat stale checklists | Short file references and freedom for local analysis | Helper and tests |
| Failure may involve invocation rather than arithmetic | Keep direct-call evidence and controlled repeats | Lesson only; hardware reproduction not exported |
| Local speedup misses external gains | Comparable baselines and deliberate proxy-case revision | Strategy, not a hidden-case reconstruction tool |
| Paths/ACK block consumption | Verify consumed files; separate acceptance from ACK | Gateway fix and tests |

No private source, accounts, machines, credentials, request IDs, raw logs or
contest payloads are published. Private optimization results are not public
reproducible benchmarks, so no numerical speedup/ranking claim is made here.

## Stronger evidence next

A licensed operator fixture and portable executor; two real independent harness
adapters; measured duplicate-work/recovery/manual-intervention rates; independent
reproduction on another platform. These are planned, not achieved claims.
[Roadmap](ROADMAP.md).
