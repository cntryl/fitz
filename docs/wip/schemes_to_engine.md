Mapping notice / rpc / stream / queue schemes to core Engine commands
=================================================================

This document breaks down four high-level usage schemes (notice, rpc, stream, queue) and maps them to the core engine commands and the high-level API surface client libraries should expose. For each scheme we show: the high-level client contract, TLV tag usage, engine commands required, token/lease semantics (where applicable), and example flows.

Common engine commands (reference)
- Publish(route, id, body)
- Reserve(route, lease_secs) -> (id, body, delivery_token)
- ExtendLease(route, id, token, add_secs) -> remaining_secs
- Peek(route) -> Option<(id, body)>
- Consume(route, id, token) -> Result
- ListResources(route) -> Vec<String>
- ListAreas() -> Vec<String>
- Subscribe(route) -> sub_id
- Unsubscribe(sub_id)
- FetchStatus()/FetchResourceStatus(resource)

TLV tags used (repeated summary)
- TAG_ROUTE, TAG_ID, TAG_BODY, TAG_LEASE, TAG_LEASE_EXTEND, TAG_DELIVERY_TOKEN, TAG_SUBSCRIBE, TAG_UNSUBSCRIBE, TAG_NOTIFICATION, TAG_ERROR, TAG_PROTOCOL_VERSION

1) Notice scheme
-----------------
Purpose: server pushes informational events to subscribers. No delivery guarantees (best-effort) in the basic form.

Client API
- subscribe(route) -> subscription_id
- unsubscribe(subscription_id)
- on_notification(callback(notification)) — invoked when server sends a TAG_NOTIFICATION frame

Wire / TLV
- Client -> Server: TAG_ROUTE + TAG_SUBSCRIBE
- Server -> Client notification: TAG_NOTIFICATION containing TAG_ROUTE, TAG_BODY, optional TAG_ID, optional TAG_METADATA

Engine mapping
- On subscribe: EngineCommand::Subscribe(route) -> sub_id
- On unsubscribe: EngineCommand::Unsubscribe(sub_id)
- When Publish occurs for matching route, engine should iterate matching subscribers and push TAG_NOTIFICATION frames (transport-level push) — no EngineCommand variant required for push itself; Publish already exists and should trigger notifications.

Semantics and edge cases
- Best-effort: if client disconnects without unsubscribing, server removes subscription (or keeps it for a timeout) depending on desired model.
- Optional reliable mode: server attaches delivery tokens to notifications and expects explicit ack (Consume) from client; requires extra complexity (track outstanding deliveries, retry policy).

Example (subscribe -> publish -> notification)
- Client A: Subscribe(route="notice://realm/area/resource")
- Client B: Publish(route, id, body)
- Engine handles Publish -> append to store (if stored) -> notify subscribers with TAG_NOTIFICATION (BODY + optional ID)

2) RPC scheme
-----------------
Purpose: request/response pairs with tight request lifetime (synchronous), often with per-call reply route or correlation id.

Client API
- rpc_call(route, request_body, timeout) -> Result<response_body, Error>

Wire / TLV
- Client -> Server: TAG_ROUTE, TAG_BODY, optional TAG_ID (correlation id), TAG_PROTOCOL_VERSION
- Server -> Client response: TAG_ROUTE (reply route) or TAG_ID (correlation id) and TAG_BODY; or TAG_ERROR

Engine mapping
- Map an RPC request to an engine-level Publish into a service-specific route (or a direct EngineCommand::Publish + internal routing logic)
- The server may implement an internal RPC dispatcher that calls a handler (which may itself publish a response). The engine must support:
  - Publish
  - Optionally: Subscribe for server-side worker services to accept incoming RPC (workers subscribe to request route and reserve/consume)

Semantics and edge cases
- Correlation id: client can provide TAG_ID (or server will assign) and expect response with same id.
- Timeouts: client must enforce RPC timeouts; server should not retain request state indefinitely.
- Authentication/Authorization: RPC may require stricter permission checks.

Example (client -> server -> worker -> response)
- Client: publish route `rpc://realm/service` with TAG_ID=cid and TAG_BODY=request
- Worker (service) has subscribed/reserved the route via Reserve/Consume to pick up the request
- Worker processes request and publishes response to reply route or sends a direct response frame containing TAG_ID=cid and TAG_BODY=response

RPC: high-level API design and reply-routing options
---------------------------------------------------
To make RPC ergonomically usable from a client-library perspective we recommend a small, well-documented surface that sits on top of the engine primitives. Below are two practical reply-routing options (reply-queue and direct-transport) and suggested helper signatures along with TLV/correlation conventions and concrete flows.

Common conventions
- Correlation id: use TAG_ID as the request correlation id (client-supplied or server-assigned). This value MUST be copied to the response's TAG_ID so the client can correlate replies.
- Reply route: a reply queue route is an ordinary engine route where the client subscribes/reserves to receive responses. Example reply route: `rpc/reply/<client-id>`.
- Timeouts: client libraries MUST enforce RPC timeouts locally; the engine does not track RPC timeouts specially.

Option A — Reply-queue (recommended initial approach)
---------------------------------------------------
Overview
- Client creates a short-lived reply route (e.g. `rpc/reply/<client-id>`), subscribes (or reserves) on it, then publishes the request to the target service route with TAG_ID=cid and an extra TLV telling workers where to reply (TAG_ROUTE=reply_route). Worker processes the request and publishes the response to the reply route using the same TAG_ID.

Advantages
- Simple to implement using existing engine primitives (Publish, Subscribe, Reserve, Consume).
- Works over disconnected transports and across process boundaries.
- No engine-level per-connection state required.

Helper signatures (client library)
- async fn rpc_call(route: &str, body: &[u8], timeout: Duration) -> Result<Vec<u8>, RpcError>
  - Implementation overview: create ephemeral reply route (or reuse a client-scoped reply route), Subscribe/Reserve on it, Publish request to `route` with TAG_ID=cid and TAG_ROUTE=reply_route, wait for a response record on reply route with matching TAG_ID, return body or error.

Worker pattern
- Worker Reserve/Consume on `rpc://realm/service` to obtain requests; read TAG_ROUTE or TAG_ID to know where to publish the response; Publish(response_route, cid, response_body).

Example flow (reply-queue)
- Client: subscribe to `rpc/reply/client-42` and wait.
- Client: Publish(route=`rpc://realm/service`, TAG_ID=`cid-123`, TAG_ROUTE=`rpc/reply/client-42`, TAG_BODY=request)
- Worker: Reserve(route=`rpc://realm/service`) -> receives id/body, reads TAG_ROUTE=rpc/reply/client-42 and TAG_ID=cid-123
- Worker: Publish(route=`rpc/reply/client-42`, TAG_ID=cid-123, TAG_BODY=response)
- Client: receive response with TAG_ID=cid-123 on its reply route and correlate

Option B — Direct-transport replies (advanced)
---------------------------------------------
Overview
- Engine tracks per-transport session reply channels so a worker can Publish a response that the engine routes directly to the originating transport connection (no reply-queue required). This requires transports to register a reply endpoint with the engine when a session starts and to include a reply-token or reply-hint with the request.

Pros and cons
- Pros: lower-latency, fewer resources used (no reply queue per client), simpler client-side API (rpc_call can be one call which publishes and awaits a direct reply).
- Cons: more complex engine state (per-connection routing), increased coupling between transport and engine, more complex reconnection/resumption semantics.

Suggested helper signatures (engine/transport)
- Transport registers: engine_handle.register_session(session_id, reply_sender)
- Request: Publish includes TAG_REPLY_HINT (opaque value the engine understands to map reply to a session)
- Engine: exposes async fn rpc_request(route, id, body, reply_hint, timeout) that publishes the request and awaits a response routed back over the registered session's channel.

Worker pattern and reply routing
- Worker processes request as usual. To reply directly, the worker may Publish to a special internal reply route or use a control path that includes the reply_hint; the engine then routes the response to the registered session's channel.

Implementation note
- Because option B requires cross-cutting changes across transports and engine, start with Option A (reply queues) and iterate to Option B if low-latency direct replies are required.

Error handling, timeouts and corner cases
- If a response does not arrive before the client timeout, client returns RpcError::Timeout and may optionally attempt to cancel the request by publishing a cancelation to the service route (this is application-level and not enforced by the engine).
- If a worker crashes after consuming a request but before publishing a response, the request is lost unless the worker uses queue semantics (reserve + extend_lease + consume) instead of subscribe; choose Reserve/Consume when worker crash safety is required.
- Provide clear error codes: NotFound, InvalidToken, LeaseExpired, PermissionDenied, Backpressure, Timeout.

Suggested docs additions
- Small code snippet for a client `rpc_call` using reply-queue (pseudocode) and a worker example showing how to read TAG_ROUTE and TAG_ID and Publish a response.
- A short FAQ about when to use Subscribe vs Reserve for workers (Subscribe for fire-and-forget or broadcast; Reserve/Consume for queue-backed, crash-resilient request processing).

Decision recommendation
- Start with the reply-queue pattern (Option A) as it reuses current engine primitives and requires minimal new code. Implement Publish->notification dispatch first. Later, add a direct-transport reply path (Option B) if low-latency or simpler client ergonomics are required.

3) Stream scheme
-----------------
Purpose: ordered, possibly long-lived sequence of messages (server->client or client->server). Can be used for logs, telemetry, or continuous data streams.

Client API
- open_stream(route) -> StreamHandle
- stream_send(handle, body) -> Result
- stream_recv(handle) -> async iterator of bodies / event callbacks
- close_stream(handle)

Wire / TLV
- Stream establishment: TAG_ROUTE + TAG_SUBSCRIBE (or a dedicated TAG_STREAM_OPEN)
- Messages: TAG_NOTIFICATION (for server->client stream items) or TAG_BODY frames with sequence numbers (optional)
- Close: TAG_STREAM_CLOSE

Engine mapping
- Subscribe for stream consumers: EngineCommand::Subscribe(route)
- Publish for producers: EngineCommand::Publish(route, id, body)
- If ordered delivery is required, engine/store must preserve ordering semantics; this may require appending to a store queue per resource and delivering in order to subscribers.

Semantics and edge cases
- Ordering: explicit sequence numbers in TLV help clients and engine verify or resume streams.
- Resumption: include stream cursor/token (opaque) to allow clients to resume from last-seen position.
- Backpressure: if producers outpace consumers, engine should either buffer (bounded) or reject/pause producers with an error (TAG_ERROR:Backpressure).

Example (stream producer -> server queues -> subscriber receives in order)
- Producer publishes messages to `stream://realm/area/resource`
- Subscriber subscribes to same route; engine delivers events as TAG_NOTIFICATION frames preserving append order

4) Queue scheme
-----------------
Purpose: classic message queue semantics where consumers reserve messages and explicitly consume/ack with lease semantics for processing.

Client API
- produce(route, id?, body) -> Result
- reserve(route, lease_secs) -> Result<Option<(id, body, delivery_token)>>
- extend_lease(route, id, token, add_secs) -> Result<remaining_secs>
- consume(route, id, token) -> Result (ack)
- peek(route) -> Option<(id, body)>

Wire / TLV
- Produce: TAG_ROUTE, TAG_ID (optional), TAG_BODY
- Reserve: TAG_ROUTE, TAG_LEASE (seconds)
- Reserve response: TAG_ID, TAG_BODY, TAG_DELIVERY_TOKEN
- Extend: TAG_ROUTE, TAG_ID, TAG_DELIVERY_TOKEN, TAG_LEASE_EXTEND
- Consume/Ack: TAG_ROUTE, TAG_ID, TAG_DELIVERY_TOKEN, TAG_CONSUME (or TAG_ACK)

Engine mapping
- Use EngineCommand::Publish for produce
- EngineCommand::Reserve to get next available message and create delivery token
- EngineCommand::ExtendLease to prolong processing time
- EngineCommand::Consume to ack and remove/mark message completed
- Store responsibilities: append, reserve_next, extend_lease, remove/mark consumed

Semantics and edge cases
- Exactly-once vs at-least-once: with delivery tokens and proper consume/ack, you can achieve at-most-once (if discard on deliver) or at-least-once (if redelivery on lease expiry). Decide semantics early.
- Delivery token: server-issued HMAC protecting route+id+expiry; server validates on extend/consume.
- Duplicates: dedupe by id at produce time if client provides id.

Example flow (consume with lease extend)
- Consumer: reserve(route, lease_secs=30)
- Engine/store: finds next record -> sets lease_expiry -> returns id, body, token
- Consumer: processes; if requires more time, calls extend_lease(route, id, token, add_secs)
- When done, consumer calls consume(route, id, token) to ack and remove

Engine features required across schemes
- Subscription registry and notification dispatch
- Message store with append/reserve_next/extend_lease/remove
- Delivery token generation & verification (HMAC + expiry)
- List/Fetch introspection APIs for resources and runtime status
- Optional: replay/resume tokens and sequence numbers for streams

Error codes & handling
- Define common error codes (TAG_ERROR) for: NotFound, InvalidToken, LeaseExpired, PermissionDenied, Backpressure, NotImplemented

Wrap-up / next steps
- Pick concrete TLV tag numeric assignments and example frames for each scheme.
- Decide which subscription model to support initially (best-effort notifications vs reliable delivery with acks).
- Implement store listing APIs used by ListResources/ListAreas.
