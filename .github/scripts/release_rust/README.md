# Rust package publication

The main `open-arrow-access` crate is the user-facing facade. The publishable
workspace components share one version and release tag. Services and formats
remain opt-in Cargo features; `open-arrow-access-test-support` is not published.

This follows OpenDAL's Rust publication workflow: dependency planning, separate
name bootstrap, OIDC-only release credentials, temporary development-dependency
removal, retrying registry rate limits, and resumable publication. The adapted
helpers retain their Apache-2.0 notices; their license is local to this directory.

## Inspect the plan

Python 3.11 or newer is required:

```sh
python3 .github/scripts/release_rust/plan.py
python3 -m unittest discover -s .github/scripts/release_rust -p 'test_*.py'
```

The planner discovers workspace members, resolves inherited/target-specific local
runtime and build dependencies, and sorts them deterministically. Development
dependencies do not participate in publication ordering. Every released package
and its internal registry requirements must match the workspace version.

## Publish a release

See [the release procedure](../../../docs/releasing.md) for bootstrap, version
preparation, tag creation, CI verification, and reruns. Live publication is only
available through the OIDC-enabled `release_rust.yml` job in the `release`
environment. The normal workflow does not consume a long-lived Cargo token.

Local packaging checks do not request OIDC credentials:

```sh
python3 .github/scripts/release_rust/publish.py --dry-run
```

This per-package command requires sibling versions to be available in the
registry. Before a first release, use the workspace-wide package verification in
the release procedure instead.

The publisher queries the public sparse index before uploading, skips an already
published exact version, and refuses to treat a yanked version as complete. Every
new attempt gets a fresh OIDC-derived token and revokes it afterward. Registry
rate-limit dates are honored, including their explanatory URL suffix. Waiting
uses intervals of at most 60 seconds and remains interruptible. An unrelated
failure stops publication; rerunning the same tag resumes the remaining packages.

Manifests and lockfiles are restored after every attempt, including failures.
Tests retain local development dependencies; only the package being uploaded has
those dependencies temporarily removed.
