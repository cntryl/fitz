package integration

// diag_test.go — Diagnostic test for raw frame debugging.
//
// This file previously used a legacy TLV transport abstraction that
// was removed during the domain refactor. It is intentionally empty
// until a new diagnostic helper is needed. The debug logging package
// (internal/core/debug) now provides frame-level diagnostics at runtime
// via FITZ_DEBUG=1 environment variable.
