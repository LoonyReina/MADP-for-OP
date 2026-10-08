# Codex for Open Source — application draft

This is a preparation document, not a submitted application. Replace the
bracketed identity fields after confirming the public GitHub repository and
the OpenAI Organization ID.

Official form: <https://openai.com/form/codex-for-oss/>. The current form asks
for a public GitHub repository, maintainer role, a short qualification statement
(500 characters), intended support, Organization ID and a short API-credit use
description (500 characters).

## Identity fields

- First name: `[fill in]`
- Last name: `[fill in]`
- Email associated with ChatGPT: `[fill in]`
- GitHub username: `LoonyReina`
- Repository URL: `https://github.com/LoonyReina/MADP-for-OP`
- Role: `Primary maintainer` (change to `Core maintainer` if that is the accurate
  description)
- OpenAI Organization ID: `[fill in from platform.openai.com]`

## Why this repository qualifies (under 500 characters)

MADP is an active open-source core for collaboration between independently
operated coding agents and their harnesses. File-backed handoffs, durable
results and explicit evidence make operator correctness/performance work
resumable. The repository includes a runnable synthetic Gateway, isolated
qualification, recovery tests and a sanitized Kimi–DSHarness–Codex case study.
It addresses a practical gap without requiring one agent SDK.

## Intended support

- API credits for project maintenance automation and release workflows.
- Six months of ChatGPT Pro with Codex for day-to-day triage, review and
  maintenance.
- Codex Security only if the repository becomes eligible and the review supports
  it.

## How API credits would be used (under 500 characters)

Credits would support Codex-assisted pull-request review, regression-test
maintenance, release qualification, documentation/wiki updates and portable
adapter work. MADP will keep provider credentials and private accelerator
deployments outside the public repository. Public CI and synthetic fixtures
will remain the reproducible baseline; any live-provider or hardware result
will be separately identified and redacted where necessary.

## Anything else (under 500 characters)

MADP is intentionally provider-neutral: a model plus its harness is a
participant, while the workspace and Gateway preserve source lineage and
evidence. The project is informed by real operator engineering, but it does not
claim that the public preview contains private GP/Engine adapters or benchmark
reproduction. The next planned contribution is a cross-agent wiki with
provenance-aware Technique, Diagnostic, Handoff and Evidence cards.
