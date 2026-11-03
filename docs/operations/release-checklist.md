# Release Checklist

Use this checklist for a Fitz release candidate.

## Contract and compatibility

- Review the changes against the [domain contracts](../development/domain-boundaries-spec.md).
- Update the user, operations, and protocol documentation affected by the release.
- Document wire and storage compatibility requirements in the
  [migration guide](migration-guide.md).
- For a wire change, validate the broker and supported SDKs against the shared
  [client scenarios](../clients/cross-language-conformance-suite.yaml).
- For the Wire 3 cutover, record the deployed 0.1.0 / Wire 1 baseline and exact
  broker plus Rust, .NET, TypeScript, Go, and Python SDK versions. Require
  shared golden-vector and full conformance results, same-hardware performance
  evidence, a no-mixed-generation cutover plan, and a paired broker/client
  rollback plan. Keep any storage migration as a separate qualification.

## Validation

- Run the required local checks from [Contributing](../../CONTRIBUTING.md).
- Confirm the required CI checks pass on the exact release candidate commit.
- For storage changes, verify the applicable write, shutdown, restart, and
  recovery paths using the supported storage modes.
- Review open critical defects and record any release-blocking limitations.

## Publish

- Confirm the package and image versions match the intended release.
- Publish through the repository's release workflow.
- Verify the published artifact and immutable version tag before announcing it.
- Keep the previous release artifact available for rollback.
