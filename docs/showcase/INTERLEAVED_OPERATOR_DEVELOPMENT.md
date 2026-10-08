# Interleaved operator development

MADP treats correctness and performance work as a **vertical interleave**, not
as two permanently separated queues. A participant may move from a numerical
probe to a performance experiment, back to an invocation or case-shape
diagnostic, and then hand the evidence to another participant. The workspace
records the stage and evidence; the framework does not decide the research
strategy.

## Why interleave

An operator can look locally optimal because the current case matrix, tiling
regime, or oracle hides a different failure mode. A strictly sequential policy
(``finish all correctness work, then optimize one fixed matrix``) tends to
reinforce the same local assumptions. A bounded interleave keeps three kinds of
information moving:

1. **Correctness evidence**: invocation, shape, dtype, numerical and external
   disagreement signals.
2. **Performance evidence**: same-case timing, regime coverage and a comparable
   baseline.
3. **Research-state evidence**: failed hypotheses, handoff notes and the next
   discriminating experiment.

Interleave does not mean running uncontrolled experiments. Each transition is
still owned by the participant, and every accepted test keeps its request,
result and acknowledgement separately.

## A small Markov model

The script [`interleave_markov_demo.py`](../../scripts/interleave_markov_demo.py)
uses a deliberately illustrative finite-state Markov chain:

| State | Meaning |
| --- | --- |
| `B` | broad case or hypothesis search |
| `N` | narrow optimization around the current regime |
| `D` | diagnostic or cross-participant handoff |
| `L` | local optimum / stale assumption |
| `E` | externally validated improvement |

The transition probabilities are not measurements of an LLM or an accelerator.
They are a transparent toy model for comparing policies. The sequential policy
spends most transitions in `N`; the interleaved policy periodically visits `D`
and returns to `B` when the evidence stops transferring. Starting from `B`, the
script computes the distribution after 12 transitions and prints the probability
of reaching `E` and remaining in `L`.

For a row-vector distribution `p_t` and transition matrix `P`,

```text
p_(t+1) = p_t P
```

The useful claim is therefore conditional and testable: **if** diagnostic
handoffs have a non-zero probability of escaping a stale local regime, then a
policy that periodically interleaves them lowers the stationary mass of `L` and
raises the probability of reaching `E`. The model does not prove a speedup; it
specifies what an empirical study must measure.

## How this maps to MADP

- The workspace `BRIEF`/stage notes identify `B`, `N`, `D`, `L` and `E`-like
  states without creating a second scheduler state machine.
- A Solver may revise legal cases when evidence exposes a missing dimension.
- A different harness can continue from the same candidate and evidence files.
- The Gateway records execution facts; it does not convert a local PASS into an
  external PASS or force an optimization submission.

The public demo is intentionally synthetic. A real operator fixture and an
independent reproduction study are roadmap items, not implied by this model.

