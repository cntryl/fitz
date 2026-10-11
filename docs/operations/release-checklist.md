# Release Checklist

Use this checklist before approving a Fitz release.

## Build and Test

1. All CI checks green.
2. Required unit, integration, and benchmark suites completed.
3. No unresolved critical or high-severity regressions.

## Documentation

1. [docs/README.md](../README.md) links validated.
2. API and admin behavior changes documented.
3. Migration notes added for any compatibility impacts.
4. Broker and all supported client codecs validated against each breaking wire contract.

## Operations Readiness

1. [operations-runbook.md](operations-runbook.md) reviewed and current.
2. Alerts and dashboards updated for new signals.
3. Rollback plan verified.
4. Required storage export/replay or reset rehearsed for breaking persisted-format changes.

## Sign-off

1. Engineering sign-off.
2. Operations sign-off.
3. Security sign-off for auth or policy changes.

## Promote the release candidate

1. Open a pull request from `develop` to `main` only when the release is ready.
2. Confirm the `backend` and `frontend` CI jobs passed on the exact pull request head. Review the separate GitHub default CodeQL analysis for that head.
3. Merge with a merge commit to preserve ancestry for the next release promotion. This update to `main` starts the `Containers` workflow below.

## Publish

1. Every push to `main` automatically runs `Containers` and updates the
   moving `ghcr.io/cntryl/fitz:main` and `:latest` multi-architecture aliases.
   A green run must report both `linux/amd64` and `linux/arm64` in its manifest.
   Every push to `develop` likewise updates the moving
   `ghcr.io/cntryl/fitz:develop` alias for integration testing; never deploy it
   as a release.
2. Dispatch the `Publish` workflow from `main` only for a stable release. Its
   called `Containers` workflow reads the package version from `Cargo.toml` via
   `version.yml`; bump that manifest version before dispatching a new release.
   Repeating the same publish for the same source SHA is idempotent:
   the existing image and source tag are verified and reused. A conflicting
   existing `v<semver>` tag still fails safely.
3. Confirm the called `Containers` workflow published the multi-architecture
   `ghcr.io/cntryl/fitz:<semver>` manifest. That tag is immutable: publishing
   different image content under the same SemVer fails.
4. Confirm the workflow created the annotated `v<semver>` repository tag only
   after the container manifest succeeded. A failed container build does not
   tag the source commit.
5. For rollback, deploy the previous immutable SemVer image. Do not move or
   replace an existing image or repository version tag.

Use the standalone `Containers` workflow to publish the moving `main` or
`develop` branch image aliases. It does not publish a versioned image or create
a repository release tag.

## Midge 0.3 cloud storage release gate

Before upgrading a broker from Midge 0.2.0 to the 0.3 release line, confirm every
cloud-mode deployment (`FITZ_STORAGE_MODE=cloud`) has chosen logical migration
or a fresh prefix, and has preserved its original prefix and cache for
rollback. Rehearse the chosen path on a copy, including restart recovery and
recovery after local-cache loss. Local-mode deployments only need a verified
copy of `FITZ_STORAGE_PATH`. Follow the procedure in the
[migration guide](migration-guide.md).

For the 0.3.1 to 0.3.2 dependency update, preserve a verified pre-upgrade copy
and qualify reads, writes, shutdown, and recovery separately; include empty-cache
recovery for cloud deployments. No logical export/import is required solely by
this update. Verify that lease-loss storage errors remain terminal and do not
become successful reads or automatic mutation retries.

The 0.3.2 to 0.3.3 update follows the same procedure. Fitz pins Midge 0.3.3 to
an untagged git rev; before release, confirm the pin matches a published 0.3.3
tag or record why the release ships an untagged storage dependency.

## Stream error envelope generation 2 release gate

For issue #238, record the released .NET, TypeScript, Go, Python, and Rust SDK
versions that decode status 2 before releasing the broker change. Verify legacy
status-1 decoding, APPEND and COMMIT code `2001` over a real broker, unrelated
wording with `2001`, misleading wording with another code, and backend `2012`.
Requalify both linked Portia assertions using the exact broker and client
artifacts, including original failure and pending-batch preservation through
cleanup failures. Follow the client-first upgrade and broker-first rollback in
[migration guidance](../operations/migration-guide.md).
