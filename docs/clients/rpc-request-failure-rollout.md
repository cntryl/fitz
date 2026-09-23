# RPC REQUEST failure-reply compatibility and rollout

This is the migration plan for the REQUEST (302) failure contract in
[the wire specification](spec/queue-rpc-kv.md). It is recorded before the broker
wire change in issue #267. This change does not publish a broker or client.

## Baseline and compatibility

Before this change, ingress authorization rejection and domain-level rejection
answer a valid REQUEST with a terminal RESPONSE (303) containing its UUID.
Ingress mailbox backpressure, actor-reply timeout, and unavailable-domain
failures instead send a REQUEST (302) error envelope without the UUID. Frame
correlation, when negotiated, may accompany that 302, but the RPC caller's UUID
does not. Successful submission has no immediate broker success frame.

The current .NET, TypeScript, Python, Go, and Rust caller implementations in
`fitz-clients` send REQUEST without awaiting a 302 reply and route calls by UUID
on RESPONSE (303). A bare 302 error therefore cannot reliably complete one
concurrent call. The target broker sends all five failure classes as terminal
303 responses with the request UUID, sequence 0, and `stream_end` set. It does
not add a success acknowledgement or change worker REQUEST/RESPONSE frames.
The established client 303 decoders already handle the target frame shape;
older clients that specifically expect a 302 error on these three ingress
failures must be upgraded before using the new broker.

## Rollout and rollback

1. Add client compatibility tests for the 303 terminal errors and, if clients
   must run against an older broker, treat an unidentifiable 302 rejection as a
   connection-wide/indeterminate failure. Never attribute its code to one of
   several outstanding UUIDs or automatically retry an ambiguous call.
2. Validate the broker's TCP and WebSocket behavior for each admission stage,
   including concurrent UUID demultiplexing, before coordinated release.
3. Release compatible clients and broker together after the backlog's explicit
   publish hold is lifted. No package or container publication is part of this
   PR.
4. For broker rollback, retain the client old-broker fallback above. A 302
   backpressure frame lacks enough identity for a multiplexed caller to prove
   which call was rejected; only a UUID-bearing 303 with code 6003 establishes
   that a particular request was never enqueued and is safe to retry with
   backoff.

The response body remains the standard RPC terminal error envelope. Code 6003
means pre-enqueue rejection and permits a deliberate retry. Code 6010 means
dispatch timeout or unavailable domain: the outcome may be unknown, and the
client must not blindly retry. Authorization uses 6009; domain validation and
routing rejections retain their existing RPC codes.
