# Support and Validation Matrix

Fitz is pre-GA and is already used in production deployments. Production use and
product release stage are separate facts: these deployments do not certify every
configuration or establish a general service-level commitment.

This matrix records the environments and compatibility expectations that have
been explicitly validated. It is not a certification of every combination of
runtime, transport, storage provider, and client version.

## Runtime Support

- Linux x86_64: primary target
- Linux arm64: secondary target after validation
- macOS and Windows: exercised by development and test workflows

## Transport Support

- WebSocket: supported
- TCP framed: supported
- HTTP admin surface: supported

## Client SDK Status

See sibling repositories for language-specific release versions and maintenance
status. The cross-language conformance ledger is a dated verification snapshot;
passing that suite does not imply a support SLA for every client version.
