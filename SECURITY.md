# Security policy

Report vulnerabilities privately through GitHub Security Advisories.

This repository is a curated public core, not a deployment mirror. Its
allowlisted synchronization boundary includes only the modern daemon
control-plane slice. It excludes deployment entrypoints, compatibility bridges,
concrete transport and Engine runtimes, live endpoint registries, operator
workspaces, official evaluation, and generated state.

Never commit:

- API keys, access tokens, cookies, SSH material, or credential-helper exports;
- hostnames, addresses, user names, remote roots, or live endpoint identities;
- daemon configuration, control databases, queues, leases, receipts, or logs;
- operator submissions, private tests, profiles, payloads, or benchmark results.

Tests may use obvious non-secret values beginning with `sk-test-`. Publication
scanning treats any other key-like token as a failure. Example paths and endpoint
identifiers must be generic and non-routable.
