# Stream error codes: issue #238 qualification

Local qualification on 2026-09-06. These results describe uncommitted working
changes on `fix/stream-error-codes-238`, based on broker
`6f6e7c3ca71d8a9615e7f795cfa01329b3e23f95`. No package or broker release was
published by this qualification.

## Contract

Non-READ Stream errors use status 2, followed by a big-endian u32 code and a
length-prefixed UTF-8 message. READ retains its status-1 coded envelope.
Both generations are decoded by the coordinated .NET, TypeScript, Go, Python,
and Rust SDK changes. Legacy uncoded errors remain unclassified.

APPEND and COMMIT expose 2001 for OCC; backend failures use 2012. Explicit
codes survive unrelated wording and misleading conflict wording. Ingress
backpressure/timeout responses use the same Stream envelope and retain their
existing numeric codes. Subscription errors preserve 2010/2011.

## Broker validation

- `cargo fmt --all -- --check`: passed.
- `cargo test --locked --workspace`: 2,192 passed, zero failed, one existing
  ignored benchmark-artifact acceptance test.
- Strict workspace Clippy with all targets/features: passed.
- Stream transport suite: 93 passed across TCP and WebSocket.
- Wire-contract suite: five passed, including a real store commit conflict.

A single resource permits only one active network append session. The
commit-time regression therefore stages two actors against one real store,
commits the first, and verifies the second store commit fails and encodes 2001.
It also verifies that the losing actor retains its active session for explicit
cleanup. This does not claim that two network clients can stage simultaneously.
Stale APPEND is exercised over both TCP and WebSocket.

## SDK validation

- .NET: 304 unit/Stream tests passed before adding three legacy-envelope cases;
  all 13 dedicated envelope cases passed afterward. The full run passed 330
  tests but failed the reconnect conformance aggregate: its fixture restarts
  Docker Compose while the qualification endpoint is a separately started
  local binary. Full restart qualification remains a release gate.
- TypeScript: format, lint, build, 616 unit tests, 44 Stream integration tests,
  361 full integration tests, and 18 conformance tests passed.
- Go: `go test ./...` passed.
- Python: Ruff format/lint and 156 unit tests passed; integration reported six
  passed and two pre-existing prerequisite skips.
- Rust: all-target/all-feature tests reported 100 passed and three existing
  ignored broker tests; formatting, strict Clippy, and test-policy validation
  passed.

## Portia packed-consumer qualification

An isolated copy of Portia `a3913d539108f975636d014bc74c9f5f69900c99` used locally
packed `Cntryl.Fitz` and `Cntryl.Fitz.Abstractions` version `0.1.2-issue238`.
NuGet source mapping selected those packages from a private local folder;
the original Portia checkout was unchanged. The endpoint was the newly built
broker binary at `ws://127.0.0.1:4190/ws`, with memory storage.

- `ShouldThrowConcurrencyExceptionWhenAppendingWithStaleExpectedVersion`:
  passed (one case).
- `CompetingRaisedEventsStillConflictAndKeepPendingChanges` plus
  `FitzPersistenceFailureTests`: passed (10 cases).

The failure tests preserve the original exception as the translated conflict's
inner exception, retain the pending batch, and preserve the original append or
commit failure through rollback/disposal failures. Misleading text with another
code and uncoded text are not translated. No automatic command retry was added.

## Release boundary

These local artifacts are not released versions. Publish and record all five
SDK versions before releasing the broker; then repeat the Portia assertions
against the exact released SDK packages and broker image digest. Follow the
client-first upgrade and broker-first rollback in the migration guide. Issue
#238 must remain open until coordinated release qualification is recorded.
