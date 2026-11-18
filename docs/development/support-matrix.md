# Support and Validation Matrix

This matrix records the runtime, transport, and wire compatibility expectations
documented by the project. It is not a certification of every combination of
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

- Fitz 0.3.0 speaks wire format 2 only. Fitz 0.2.x speaks wire format 1 only.
- Fitz 0.4.0 and 0.5.0 both use Wire 3. Network clients that connect to
  the 0.4.0 broker can use 0.5.0 over TCP or WebSocket with the same SDK
  and configuration.
- No broker serves multiple generations. A 0.3.0 broker requires the 2.0 line
  of every SDK, and a 0.2.x broker requires the 1.x lines; the migration guide
  states the flag-day order.
- Deployments still on Wire 1 or Wire 2 require a coordinated broker and SDK
  upgrade. See the [migration guide](../operations/migration-guide.md).

## Client SDK Status

Language-specific SDK versions and maintenance status are maintained in their
respective repositories. Cross-language conformance is described in the client
conformance guide.
