# Project positioning

## One sentence

MADP lets independently operated AI coding environments collaborate on operator
engineering through durable workspaces and verifiable test evidence.

## Short introduction

MADP chooses the **model plus its harness** as the integration boundary. Rather
than replacing coding environments, it connects their work through files,
explicit ownership and an execution/evidence core. Operator correctness and
performance are a demanding concrete domain: experiments are expensive,
numerical failures subtle, and local timing may disagree with external results.
The public preview makes the core testable without devices or model accounts,
separating private experience from publicly reproducible claims.

This design contrast is not a claim that other tools cannot support multiple
providers, that similar projects don't exist, or that MADP is the first.

| Choice | Value | Evidence entry |
| --- | --- | --- |
| Model + harness as participant | Retain each collaborator's tools | Guide and private handoff lessons |
| Files as handoff | Continue without another chat transcript | Two-process demo |
| Research separate from execution | Revise hypotheses/cases without encoding each experiment in scheduler logic | Strategy with runtime caveats |
| Durable results and separate ACK | Recover instead of duplicating tests or hiding failures | Gateway tests and demo |
| Operator correctness/performance focus | Concrete, measurable engineering work | Private experience, not public speedup benchmarks |

## 90-second showcase script

1. Show two fixture participants and one workspace; real harnesses need adapters.
2. Run the demo; open the failed result, then the repaired result.
3. Show the handoff, original request IDs and independent ACK state.
4. Explain fresh-Gateway recovery without resubmit.
5. Close on boundaries: runnable public core, separate real-device deployment.

Avoid claims of full autonomy, universal compatibility, a fully open private
system, reproducible device benchmarks or superiority over other frameworks.
An operator speedup does not establish that MADP caused it without a study.

The strongest current pitch is an **explicit interoperability boundary with
executable recovery/evidence mechanics**, informed by engineering use.

The deeper research pattern is **vertical interleave**: correctness, case
coverage, performance and handoff are allowed to inform one another while the
workspace keeps their evidence distinct. This is a research-policy claim, not
a claim that MADP replaces a model's reasoning or that the public toy model is
a device benchmark. See [the Markov explanation](INTERLEAVED_OPERATOR_DEVELOPMENT.md)
and the [sanitized collaboration record](COLLABORATION_RECORD_KIMI_DSHARNESS_CODEX.md).
