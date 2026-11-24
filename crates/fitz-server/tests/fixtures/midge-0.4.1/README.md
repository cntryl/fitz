# Midge 0.4.1 local-store fixture

These files were written by the released Fitz 0.2.0 container
`ghcr.io/cntryl/fitz@sha256:6d62d9ed9d9fd32186391e6f5944a05a5894e7bd2c7e117288c5ce87acb3035c`,
which embeds Midge 0.4.1. The broker synchronously committed and read back key
`upgrade-check` with value `preserve-this-value` at
`kv://migration/reference/state`, then shut down cleanly. The fixture retains
the FORMAT marker, manifest, WAL ledger, and WAL segment; process-local writer
lease and lock files are deliberately excluded.

The compatibility test opens a copy, checks that the current broker rejects it,
and verifies that these persisted files remain unchanged. This fixture does not
establish a complete application migration or cloud-prefix compatibility.
