# AscendOP integration profile

AscendOP is the first application of MADP and remains the private integration
environment for the public core.

## Mapping

- `Solver` proposes operator candidates and diagnostic evidence requests.
- `Tester` owns testcase generation within the configured case-version lifetime.
- the MADP daemon materializes actions and owns workflow gates;
- the Agent runner executes only a claimed action in an isolated workspace;
- GitPartner carries daemon-approved work to a selected endpoint Engine;
- the Engine returns structured correctness, performance, or profile evidence;
- the daemon validates that evidence before allowing the next transition.

The public packages contain some `ascendop.*` schema identifiers and Python
namespaces for compatibility. These names record origin, not a requirement that
all MADP adopters use Ascend hardware or operator workflows.

## Dependency direction

AscendOP composes the MADP packages and public daemon slice with its private
resident bootstrap, GitPartner/Engine adapters, operator repositories, endpoint
configuration, and official evaluation implementation. Public code must not
import those private deployment layers.

Generic fixes are made in AscendOP's core source directories first, validated in
the private integration environment, then synchronized into the public core.
Public-only documentation and packaging metadata remain in this repository.

## First-run admission

An endpoint/operator pair should pass a private admission workflow before being
added to the resident optimization loop. Admission proves build, correctness,
profile, artifact return, and retry/recovery behavior for that profile. It is an
AscendOP deployment policy and therefore is documented conceptually here but not
implemented in the public core.
