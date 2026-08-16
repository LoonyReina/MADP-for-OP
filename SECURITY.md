# Security policy

Please report vulnerabilities privately through GitHub's security advisory feature
instead of opening a public issue.

This repository intentionally excludes endpoint credentials, private hostnames,
remote paths, operator submissions, test artifacts, and production queue state.
Never commit secrets or live endpoint configuration. Use local environment files
or a secret manager, and keep those files outside version control.

In particular, do not commit:

- `api.txt`, SSH keys, access tokens, cookies, or credential-helper exports;
- endpoint and node registries containing hostnames, addresses, users, or paths;
- daemon configuration copied from a live deployment;
- control databases, queues, receipts, payloads, results, profiles, or crash dumps;
- operator submissions or private correctness and benchmark cases.

The example service files use the generic account `ascendop` and installation root
`/opt/ascendop`. Replace them through deployment automation; do not patch public
source with machine-specific values.
