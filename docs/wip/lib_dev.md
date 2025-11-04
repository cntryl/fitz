Library development guide — high level API, wire contract, and implementation notes
===================================================================

This document describes the recommended high-level API that language-specific client libraries should expose, the wire-level TLV framing and tags they must encode, delivery-token semantics, and other design decisions that keep client libraries portable and the server simple.

1. Goals
- Provide small, idiomatic high-level APIs for clients (publish/reserve/consume/extend/peek/subscribe/etc.)
- Keep the wire format minimal and language-agnostic (FTZ/TLV framing with raw bytes for bodies)
- Server translates wire messages into authoritative engine commands (leases, HMAC token validation, subscriptions)

2. Architectural overview
- Client library (language-specific): exposes the high-level functions. It encodes requests into TLV frames and sends them over the chosen transport (WebSocket, HTTP upgrade, raw TCP).
- Wire protocol: FTZ header-v2 + TLV records. TAG_BODY is raw bytes. All other metadata is small TLV tags (strings, integers, tokens).
- Server: a central engine task (single-threaded async loop) receives decoded TLV requests and maps them to internal engine commands (Publish, Reserve, ExtendLease, Peek, Consume, ListResources, ListAreas, FetchStatus, FetchResourceStatus, Subscribe, Unsubscribe). The engine owns the store and enforces semantics.

3. Minimal high-level contract
These are the operations client libraries should support and the mapping to wire TLVs and engine commands.

- Publish
  - High level: publish(route: String, id?: String, body: bytes) -> Result<(), Error>
  - Wire TLV: TAG_ROUTE, optional TAG_ID, TAG_BODY
  - Engine command: Publish(route, id, body)

- Reserve
  - High level: reserve(route: String, lease_secs: u32) -> Result<(id, body, delivery_token), Error>
  - Wire TLV: TAG_ROUTE, TAG_LEASE
  - Engine command: Reserve(route, lease_secs) -> (id, body, token)

- ExtendLease
  - High level: extend_lease(route: String, id: String, token: String, add_secs: u32) -> Result<remaining_secs, Error>
  - Wire TLV: TAG_ROUTE, TAG_ID, TAG_DELIVERY_TOKEN, TAG_LEASE_EXTEND
  - Engine command: ExtendLease(route, id, token, add_secs)

- Consume / Ack
  - High level: consume(route: String, id: String, token: String) or ack(route, id, token)
  - Wire TLV: TAG_ROUTE, TAG_ID, TAG_DELIVERY_TOKEN, TAG_CONSUME
  - Engine command: Consume(route, id, token)

- Peek
  - High level: peek(route: String) -> Option<(id, body)>
  - Wire TLV: TAG_ROUTE, TAG_PEEK
  - Engine command: Peek(route) -> Option<(id, body)>

- ListResources / ListAreas
  - High level: list_resources(route) -> Vec<String>
  - Wire: TAG_LIST_RESOURCES
  - Engine command: ListResources

- Subscribe / Unsubscribe
  - High level: subscribe(route) -> subscription_id; unsubscribe(subscription_id)
  - Wire: TAG_ROUTE + TAG_SUBSCRIBE / TAG_UNSUBSCRIBE
  - Engine: Subscribe(route) -> sub_id; Unsubscribe(sub_id)
  - Notifications are pushed as TLV messages: TAG_NOTIFICATION with nested tags (TAG_ROUTE, TAG_ID, TAG_BODY, TAG_DELIVERY_TOKEN?)

- FetchStatus / FetchResourceStatus
  - High level: fetch_status() / fetch_resource_status(resource)
  - Wire: control tags
  - Engine: return runtime status or resource health

4. TLV tag recommendations
- Numeric tag assignments are up to the implementation, but keep a documented, stable list in this repo. Suggested tags (symbolic):
  - TAG_ROUTE — string (route)
  - TAG_ID — string
  - TAG_BODY — bytes
  - TAG_LEASE — u32 (seconds)
  - TAG_LEASE_EXTEND — u32 (seconds to add)
  - TAG_DELIVERY_TOKEN — string (base64 HMAC token)
  - TAG_SUBSCRIBE / TAG_UNSUBSCRIBE — flags
  - TAG_NOTIFICATION — wrapper for server->client event
  - TAG_ERROR — structured error (code + message)
  - TAG_PROTOCOL_VERSION — protocol version
  - TAG_KEY_ID — key id for HMAC (optional)

5. Delivery token format and security
- Server generates delivery tokens to protect extend/consume operations. Recommended format:
  - data = route || "|" || id || "|" || expiry_unix_secs || "|" || nonce
  - token = base64( key_id || ":" || HMAC_SHA256(server_secret_key, data) )
  - Server validates HMAC and expiry on ExtendLease/Consume
  - Include TAG_KEY_ID if rotating keys; if absent, server assumes default key

6. Framing, versioning, and negotiation
- FTZ header-v2 remains the outer frame. Each message is a sequence of TLV records. Clients must set TAG_PROTOCOL_VERSION during handshake.
- Server may reject clients with incompatible versions. Keep tags unordered (decode by tag) to allow optional tags.

7. Subscription semantics and reliability choices
- Two options:
  - Best-effort: notifications are pushed without ack. Simple and fast.
  - Reliable: notifications include delivery tokens and require ack/consume; server retries or queues until ack. More complex but ensures delivery.
- Implementation recommendation: start with best-effort and add reliable notifications later if required.

8. Error handling and codes
- Define TAG_ERROR to contain a machine code + human message. Example codes:
  - 100: EngineStopped
  - 101: InvalidToken
  - 102: NotFound
  - 103: LeaseExpired
  - 104: PermissionDenied
  - 110: NotImplemented

9. Content typing
- The server never parses JSON automatically. Client libraries may provide helpers to serialize typed bodies, but TAG_BODY is always opaque bytes.
- Optionally include TAG_CONTENT_TYPE for the client to mark encoding (application/json, application/protobuf).

10. Practical steps & tooling recommendations
- Create a spec file (YAML or JSON) in the repo that enumerates operations, TLV tags, and field names. This will let us auto-generate small encoder/decoder stubs.
- Provide a reference Rust client in this repo that implements the high-level API and TLV encoding over WebSocket. That reference will serve as concrete guidance for other languages.
- Add a conformance test harness (simple server-side tests) so client libraries can run the same tests against the running server.

11. Next steps I can implement
- Draft the wire spec (Markdown + concrete TLV numeric assignments and examples).  (Recommended first step)
- Create a small YAML IDL and a generator script to produce simple encoder/decoder stubs.
- Add a reference Rust client in `examples/client_ref` demonstrating the publish/reserve/extend flows.

Notes
- This document is intentionally minimal where possible to keep the wire format small and stable. When in doubt, prefer adding a new TLV tag rather than changing an existing tag's semantics.

If you'd like, I can now produce the numeric TLV tag assignments and a couple of example frames (Publish, Reserve) in hex + the equivalent pseudo-code for client libs. Reply with which you'd like next.
