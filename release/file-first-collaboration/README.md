# File-first Collaboration — prepared edition

Version: 5.6.1a1. Status: prepared locally, not published or tagged.
Parent published release: V5 Iteration Runtime (5.6.0a1), commit
`c04f60bcbdd0dab754bb0070ab5ca67d52c2d884`.

## Scope

- File-first positioning, English/Chinese README, architecture, handoff and
  performance strategy, evidence boundaries and a project showcase.
- A two-process synthetic handoff demo using the real public Gateway.
- Selected upstream fixes: compact workflow prompts and long-path retained-file IO.
- No bulk export of private diagnostics, GP/Engine, operator assets or deployment.

The latest agent-owned research strategy is documented; migration of every old
auto-publication policy path is not included. A concrete independent harness
adapter and real device demo remain follow-up work.

## Qualification

Passed on 2026-09-13: **339 source tests and 339 installed-wheel tests** across
five packages. Publication scan, dependency closure, installed import origins,
two CLI entry points, the three-task managed demo and two-writer file demo pass.
The first qualification caught stale schema release metadata; after correcting
it, both full suites were rerun successfully.

Qualification reused the dedicated MADP venv on Windows / Python 3.14 with
scrubbed configuration and public-only imports. No private service was deployed
or contacted. No live model, hardware or external-evaluator qualification is
claimed. Component and wheel digests: [PROVENANCE.json](PROVENANCE.json).

Release B remains the latest remote release; this candidate is only a local
preparation and has no publication tag.
