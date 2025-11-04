# Gap Analysis: Core Engine, Transport Multiplexing, TLV Framing, and Tests

Date: 2025-11-04
Author: automated analysis

## Objective

Assess the repository's current readiness to support high concurrency across multiplexed transports, end-to-end routing and auth pipelines, and a zero-copy/zero-allocation TLV framing path. Produce an actionable gap list and prioritized recommendations with next steps and test requirements.

## High-level goals

- Support many concurrent clients (WebSocket/TCP/TLS) using multiplexed logical channels.
- Ensure each operation flows through route/auth/execution pipelines correctly and efficiently.
- Provide a TLV framing implementation that allows zero-copy parsing and minimizes allocations.
- Maintain strong unit and system-level integration test coverage verifying correctness and performance.

## Files inspected (representative)

- `src/core/engine.rs` — EngineCommand enum, EngineHandle, single-task engine model using an mpsc channel to serialize store access and manage subscriptions.
- `src/core/stream.rs` — Stream helper wrapping EngineHandle for append/peek/consume.
- `src/protocol/frame.rs` — Frame/TLV parsing helpers: `parse_frame` returns a `ParsedFrame` with a borrowed payload slice; `find_tlv` returns slices; builders use `Vec<u8>` (copies); CRC handling included.
- `src/core/router.rs` — In-memory router and subscription registry; `dispatch` clones `route`, `body` into the subscriber send and uses `try_send` with `mpsc::Sender` to avoid blocking.
- `src/transport/*` — transports exist (WS/TCP/HTTP) and a `channel_id` is already embedded in frames; transports not exhaustively reviewed here but present.

## Current strengths (what we have)

1. TLV design supports borrowed parsing
   - `parse_frame` returns `ParsedFrame` with `payload: &'a [u8]`, and `find_tlv` returns slices into that payload. This already allows zero-copy parsing provided the transport keeps the buffer alive.

2. Clear framing format and helpers
   - `build_frame`, `build_tlv`, `parse_pub`, `parse_reg` and `build_notification_frame_ex` provide concrete canonical encoders/decoders. CRC32 TLV verification exists.

3. Router and subscription model
   - `Router` provides route pattern matching (exact, trailing `*`, hierarchical) and a working subscription registry with tests.

4. Engine API is extensive
   - `EngineCommand` and `EngineHandle` expose many operations (publish, reserve, stream append/peek, kv operations), and responses are oneshot channels — easy to reason about correctness.

5. Tests and meta-tests exist
   - Unit tests are present in places (e.g., `router.rs`), and the repo includes test guideline enforcement (meta-test). Good foundations for expanding tests.

## Identified gaps and risks

1. Engine serialization bottleneck
   - `engine.rs` is documented as "single task that serializes store access and manages subscriptions" and uses a single `mpsc::Sender<EngineCommand>`. This centralizes correctness but is a potential throughput bottleneck for high concurrency (many clients issuing publishes, kv operations, stream appends, etc.).
   - Risk: single-threaded command dispatch cannot exploit parallelism for independent operations (e.g., read-only peeks, subscription dispatch) and may increase latency.

2. Router dispatch clones data on send
   - `Router::dispatch` currently builds new owned values when calling `sub.sender.try_send((route.to_string(), msg_id.map(...), body.to_vec(), ...))`. This allocates and clones for each recipient, which is costly under large fan-out.
   - Risk: excessive allocations and copies on high-volume notifications; memory pressure & reduced throughput.

3. Subscription send type uses heavy types
   - `SubSender` is `mpsc::Sender<(String, Option<String>, Vec<u8>, Option<String>, Option<u32>, bool)>`. Passing `Vec<u8>` triggers clones; String allocations too. No use of zero-copy shared buffers (e.g., `Bytes`/`Arc<[u8]>`).

4. Builder-side allocations when producing frames
   - `build_frame` and `build_notification_frame_ex` create `Vec<u8>` and copy payload bytes. This is fine for tests and small scale, but for high throughput it's better to use a pooled buffer/BytesMut+Bump or at least `bytes::Bytes` to avoid repeated allocations.

5. Transport-buffer ownership & lifecycle not explicitly enforced
   - While `parse_frame` returns borrowed slices, the transport must ensure the underlying buffer stays alive until the parsed slices are consumed. Current helpers support this, but there is no documented pattern or helper types (e.g., `Bytes` / arenas) to make this safe and ergonomic across async tasks.

6. Backpressure & subscriber send policy is best-effort
   - `dispatch` uses `try_send` and drops on `Full` (backpressure). This is intentionally best-effort but needs explicit policy & tests covering message loss vs. blocking semantics.

7. Tests missing for zero-copy semantics and high concurrency
   - There are unit tests for components (router) but few or no unit tests that assert zero-copy behavior (i.e., that a `find_tlv` slice points into the original buffer and remains valid) or stress tests validating behavior under many concurrent clients.

8. Integration tests / harness absent for multiplexed clients
   - No visible system-level integration tests that establish many multiplexed channels over a transport (e.g., multiple websocket connections with many logical channels) and measure correctness and latency under load.

9. Type-safety and ergonomics for TLV tags and payloads
   - Tag constants and low-level helpers exist, but higher-level typed frames (e.g., typed enums for frame types with automatic (de)serialization) would reduce mistakes and duplicate verification code.

## Recommendations (prioritized)

Priority: High (must address early)

1. Reduce expensive cloning in Router/dispatch
   - Change subscription payload to use reference-counted zero-copy buffers (e.g., change `SubSender` to send `Arc<bytes::Bytes>` or `Arc<Vec<u8>>` for body + `Arc<str>` or `String` for route/id). Better: send a small struct that references `Arc<Bytes>` and `Option<Arc<str>>`.
   - Benefit: eliminates per-subscriber body copies and drastically reduces allocations on large fan-out.
   - Effort: small — modify `SubSender` typedef, update call sites in `engine.rs` and transports, update tests.

2. Replace per-frame `Vec<u8>` allocations with `bytes::BytesMut` / `Bytes`
   - Use `bytes::BytesMut` as the mutable buffer the transport fills and pass `Bytes` (cheap clone) to readers/parsers. Update `build_frame` to accept an existing buffer or return `Bytes`.
   - Benefit: efficient buffer reuse and cheap cloning for zero-copy slices.
   - Effort: small-medium — add bytes dependency and update builders and tests.

3. Rework engine concurrency model for hot vs cold paths
   - Keep a single sequencer for operations that require strict serialization (e.g., single-writer KV append) but allow concurrent paths for read-only or subscription dispatch.
   - Options:
     a) Shard the engine into logical components (store shard(s) + subscription/router task). Use a command router to forward operations to the appropriate task.
     b) Keep the current engine but offload subscription dispatch and non-mutating reads to worker tasks (engine pushes notifications to a bounded queue consumed by dispatch workers).
   - Benefit: improves throughput while preserving correctness where needed.
   - Effort: medium — requires careful design and tests.

Priority: Medium

4. Introduce typed frame enums and small helpers
   - Provide typed decoders for PUB/REG/DAT frames that return lightweight borrowed structs referencing `Bytes`.
   - Benefit: safer parsing and fewer ad-hoc parse errors.
   - Effort: small.

5. Add explicit transport buffer lifecycle pattern
   - Document and provide helpers that read bytes into a `BytesMut` and hand out `Bytes` clones to parser/handlers. Create tests validating the borrow semantics.
   - Effort: small.

Priority: Medium-Low

6. Define subscriber backpressure policy and tests
   - Decide on drop-on-full vs. blocking vs. per-subscriber buffers and add tests to verify behavior under backpressure.
   - Effort: small.

Priority: Low

7. Performance benchmarks and stress tests
   - Add benches (or a small harness) that simulate N clients each publishing M messages over multiplexed channels and measure throughput/latency.
   - Effort: medium.

## Concrete next steps / Implementation plan (milestones)

Short (1–3 days)

- [ ] Replace `SubSender` payload to use `Arc<Bytes>` (or `Bytes`) for bodies and `Arc<str>` for route/id; update `Router::dispatch` to avoid copying body bytes for each recipient. Update tests in `router.rs` to use the new shapes.
- [ ] Add `bytes` crate to dependencies and update `build_frame`/`build_tlv` helpers to accept/return `Bytes`/`BytesMut` where appropriate. Provide compatibility shims for existing tests.

Medium (1–2 weeks)

- [ ] Design and implement engine sharding or partial offload for subscription dispatch. Start with a minimal change that offloads notifications to a per-engine bounded queue drained by a worker.
- [ ] Add unit tests for zero-copy TLV behavior (assert payload slices are valid and point into original buffer using `Bytes` semantics).
- [ ] Add integration test harness that spins up multiple transports (WS/TCP) locally and validates end-to-end publish/subscribe across multiplexed channels.

Longer (2–6 weeks)

- [ ] Create benchmarks and refine sharding strategy as needed.
- [ ] Add CI jobs to run integration tests and benchmarks (optionally gated on separate runner due to resource needs).

## Test matrix (unit + integration)

Unit tests (fast)

- Frame/TLV
  - Parse a frame from a single buffer and assert `ParsedFrame.payload` is a subslice of the original buffer (zero-copy invariant).
  - Build a frame using `BytesMut` and parse it back.
  - CRC TLV present and mismatch -> Error::Invalid.
- Router
  - Keep current router test coverage; add tests that dispatch with `Bytes` body and verify no cloning occurs (e.g., using `Bytes::from_static` or using weak counting of `Arc`).
- Engine
  - Add unit tests for EngineHandle behavior for common commands.

Integration/system tests (slower)

- Multiplexed connections
  - Start an in-process broker instance and create many simulated client connections using the transport code paths (e.g., multiple websockets or TCP clients). Each client opens several logical channels (distinct channel_id values) and performs publish/subscribe traffic. Verify message delivery and ordering per-channel.
- Backpressure
  - Simulate slow subscribers and verify configured backpressure policy (drop/block) behaves as expected.
- Stress smoke
  - Run a smaller scale stress test in CI that asserts no panics, reasonable latencies, and resource bounds.

## Low-risk, high-value immediate changes (quick wins)

1. Add `bytes` dependency and switch frame builders to `BytesMut`.
2. Change subscription message body to `Bytes` (cheap clone) and update `Router::dispatch` to pass `Bytes` directly.
3. Add unit tests to assert zero-copy parsing invariants and update README/docs to document buffer ownership patterns.

## Risks and open questions

- How strongly do you want to preserve the single-engine serialized model? If it's a hard correctness requirement for the product, we should focus on improving dispatch and buffer sharing rather than sharding the store. If high throughput is prioritized, sharding is recommended.
- Backpressure policy: prefer drop-on-full (best-effort, current behavior) or stronger delivery guarantees? This will affect memory and latency trade-offs.
- Operational constraints for tests: CI capacity to run integration/stress tests may be limited; plan for staged CI (unit tests always, heavier integration/stress on nightly or dedicated runners).

## Suggested immediate PRs

1. PR: `zero-copy-bodies`
   - Add `bytes` crate, change `SubSender` to send `(String, Option<String>, Bytes, Option<String>, Option<u32>, bool)`, update router and engine sites, and update tests.
2. PR: `bytes-frame-builders`
   - Add helpers that accept `BytesMut`/return `Bytes` and update `build_frame`/`build_notification_frame_ex` to use them.
3. PR: `frame-zero-copy-tests`
   - Add tests that explicitly assert zero-copy semantics and CRC validation.

## Next steps (what I can do next)

- Implement the `zero-copy-bodies` change (small, low-risk). I can prepare a small patch that updates `SubSender`, `Router::dispatch`, and call sites in `engine.rs`, plus updated tests in `router.rs`.
- After that, implement `bytes`-based builders and add unit tests for zero-copy invariants.

---

End of analysis.
