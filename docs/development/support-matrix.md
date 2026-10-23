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

## Wire Format

- Fitz 0.3.0 and later speak wire format 2 only; it is the format Fitz intends
  to freeze as 1.0.0. Fitz 0.2.x speaks wire format 1 only.
- No broker serves both. A 0.3.0 broker requires the 2.0 line of every SDK, and
  a 0.2.x broker requires the 1.x lines; the migration guide states the
  flag-day order.
- Later 0.x and 1.x releases extend wire format 2 only through capability bits
  and new verb identifiers; the message header, handshake, and error envelope
  do not change.

## Client SDK Status

See sibling repositories for language-specific release versions and maintenance
status. The cross-language conformance ledger is a dated verification snapshot;
passing that suite does not imply a support SLA for every client version.
