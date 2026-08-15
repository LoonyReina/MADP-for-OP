# Security policy

Please report vulnerabilities privately through GitHub's security advisory feature
instead of opening a public issue.

This repository intentionally excludes endpoint credentials, private hostnames,
remote paths, operator submissions, test artifacts, and production queue state.
Never commit secrets or live endpoint configuration. Use local environment files
or a secret manager, and keep those files outside version control.
