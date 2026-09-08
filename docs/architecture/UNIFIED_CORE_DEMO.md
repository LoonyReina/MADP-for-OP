# Synthetic Unified Core example

Install the four public packages in the dedicated MADP environment, then run:

```bash
python scripts/demo_unified_core.py --root /tmp/madp-demo-new --operators 3
```

Use a new writable output directory (a short path is useful on Windows). The
example refuses to overwrite an existing directory. All databases and generated
workspace files stay there; no provider, endpoint or external account is used.

Each deterministic Solver first writes an incorrect doubling function, receives
its local mismatch, then revises the implementation. The trusted fixture host
uses the shared managed completion transaction and terminal repository; the
existing outbox performs the next owner handoff. Both local results retain their
ACK intent without blocking revision two. Reopening the database and repeated
delivery do not create a duplicate successor.

Inspect REPORT.json and each workspace's .ascendop/ITERATION.json, BRIEF.md,
CLIENT.json and proposal files. Short notifications point to these files.
The report explicitly records no model calls, hardware calls or external
submissions. Local success is not a dual PASS or a benchmark result.

The publication callback in this example is deliberately minimal. A real host
must enforce native writer quiescence, immutable input capture, trusted oracle,
receipt validation, permissions and domain case policy before accepting work.
Those responsibilities cannot be delegated to the Solver's file client.
