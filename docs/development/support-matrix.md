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

- Fitz 0.3.x speaks wire format 2 only. Fitz 0.2.x speaks wire format 1 only.
- No broker serves both. A 0.3.0 broker requires the 2.0 line of every SDK, and
  a 0.2.x broker requires the 1.x lines; the migration guide states the
  flag-day order.
- Wire 3 is a broker draft for the coordinated cutover described in
  [issue #508](https://github.com/cntryl/fitz/issues/508); it is not a released
  or supported client/broker combination. Its planned release starts from the
  deployed 0.1.0 / Wire 1 client cohort and requires all five SDKs to qualify
  with it together.

## Client SDK Status

Language-specific SDK versions and maintenance status are maintained in their
respective repositories. Cross-language conformance is described in the client
conformance guide.
