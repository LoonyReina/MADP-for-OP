# Publication model

MADP uses two repositories with one implementation lineage.

## Private upstream

The AscendOP workspace is the integration authority. It contains the public core
source directories together with the complete resident daemon, transports,
endpoint topology, operators, tests, profiles, evaluation policy, and runtime
state.

## Public core

`AscendOP/code/MADP-for-OP` is a persistent independent Git repository and the
checkout published to GitHub. It contains only allowlisted core implementation,
the modern daemon control-plane slice, core tests, public packaging metadata,
GP/Engine architecture documents, and provenance.

## One-way release flow

1. Freeze and test a private AscendOP integration point.
2. Run `sync_from_ascendop.py --check` to inspect core drift.
3. Run `sync_from_ascendop.py --apply` to copy only manifest-listed trees.
4. Review the diff and update public docs/package versions.
5. Run core tests, package builds, and `publication_scan.py`.
6. Record source and release identities in provenance.
7. Commit, tag, and push the public repository.

There is no automatic two-way merge. A useful public change should be
implemented or consciously ported into the private core first, exercised by the
full AscendOP integration, and then included in a later public export. This
prevents the two implementations from silently diverging.

Historical public tags are immutable. A new milestone appends a commit and tag;
it does not rewrite earlier architecture history.
