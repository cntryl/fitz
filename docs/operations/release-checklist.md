# Release Checklist

Use this checklist for a Fitz release candidate.

## Contract and compatibility

- Review the changes against the [domain contracts](../development/domain-boundaries-spec.md).
- Update the user, operations, and protocol documentation affected by the release.
- Document wire and storage compatibility requirements in the
  [migration guide](migration-guide.md).
- For a wire change, validate the broker and supported SDKs against the shared
  [client scenarios](../clients/cross-language-conformance-suite.yaml).

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
