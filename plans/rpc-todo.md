# RPC domain — TODO

Summary
- Ensure RPC streaming/unary semantics, correlation matching, chunk ordering, and two‑phase responses are fully tested and bench‑covered.

Goals
- Harden streaming chunk-ordering tests and error propagation.
- Add benches for unary hot-path and streaming chunk reassembly.

Files to inspect
- `src/domains/rpc/rpc_route_actor.rs`
- `src/domains/rpc/reply_inbox.rs`
- `src/domains/rpc/protocol.rs`
- `src/domains/rpc/errors.rs`
- Tests: `tests/rpc_*`
- Benches: `benches/tier1_hotpath_rpc.rs`, `benches/tier3_system_rpc.rs`

Required unit tests (examples)
- `should_match_rpc_responses_to_correlation_id()`
- `should_reject_out_of_order_streaming_chunks()`
- `should_handle_rpc_accepted_two_phase_response()`

Integration tests to add/verify
- `should_stream_large_payloads_reliably()`
- `should_handle_concurrent_rpc_requests_same_route()`

Bench targets
- Tier1: unary request/response hot-path (low-latency)
- Tier1 (streaming): chunk ingestion and reassembly hot-path
- Tier3/4: end-to-end streaming throughput under concurrency

PR plan (2 commits)
1. Add/strengthen streaming tests & reply_inbox invariants (2–3 hr).
2. Add streaming hot-path benches and validate system-level behavior (2–4 hr).

Acceptance criteria
- Streaming ordering and correlation invariants covered by unit tests
- Tier1 bench validates low-latency unary RPC path and streaming chunk path
