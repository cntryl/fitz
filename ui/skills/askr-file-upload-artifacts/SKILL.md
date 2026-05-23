---
name: askr-file-upload-artifacts
description: Use when building Askr file uploads, generated artifacts, progress, previews, validation, storage adapters, downloads, open flows, virus-scan or processing states, and event-sourced artifact readiness.
---

# Askr File Upload Artifacts

Use this for uploads, downloads, previews, and generated files. The goal is one clear upload workflow, explicit processing truth, and no loss of progress or readiness state.

## Use This When

- You need file selection, upload, processing, preview, or download flows.
- Artifacts may be generated asynchronously after upload.
- You need progress, retry, cancellation, or readiness reconciliation.
- Validation rules include type, size, count, privacy, or retention.

## Inspect First

- Adapter support for upload URLs, multipart uploads, or artifact APIs
- Feature workflow for create, upload, process, and download
- File validation rules: type, size, count, privacy, retention
- Artifact processing states and event stream support

## Choose The Boundary

- `src/adapters`: upload transport, signed URL calls, artifact downloads.
- `src/features/<feature>`: validation, upload workflow, and artifact query or mutation state.
- `src/components/shared`: reusable file picker, progress list, and preview shell.
- `src/shared`: size formatting, safe filename helpers, and error normalization.

## Do This In Order

1. Keep file validation explicit before upload starts.
2. Model selection, uploading, processing, ready, and failure as separate states.
3. Preserve upload ID, artifact ID, processing job ID, and last event ID when readiness is asynchronous.
4. Treat upload completion and artifact readiness as separate truths.
5. Allow retry or refresh without duplicating uploads where possible.

## Copy This Shape

```ts
type ArtifactStatus =
  | "selected"
  | "uploading"
  | "processing"
  | "ready"
  | "failed-validation"
  | "failed-upload"
  | "failed-processing";
```

## Never Do These

- Pretending an artifact is ready immediately after upload when processing is asynchronous.
- Client-only validation as the only protection.
- Losing progress and error state on route-local rerenders.
- Download links without accessible names or file metadata.

## Validate

- Validation, upload, processing, ready, and failure states are visible.
- Upload cancellation or retry behavior is explicit.
- Large files and unsupported types fail clearly.
- Artifact readiness is reconciled from server state.

## Done When

- Upload completion and artifact readiness are modeled separately.
- Progress and error state survive the owning workflow.
- Artifact metadata is preserved for retries and reconciliation.
- Users can understand whether a file is uploaded, processing, ready, or failed.

## Handoff

- Use `askr-api-integration` when signed URLs or artifact APIs are the hard part.
- Use `askr-error-loading-empty` when the hard part is presenting processing and failure truth.
- Use `askr-testing-determinism` before closing upload and retry behavior.
