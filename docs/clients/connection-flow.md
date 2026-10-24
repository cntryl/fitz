# Client Connection and Operation Flow

This document describes the Fitz lifecycle from the client's point of view.
It intentionally omits broker-internal routing, shard assignment, and worker
dispatch details. Clients interoperate through transports, `CONNECT(jwt)`, and
documented domain payloads only.

## Overview

Every Fitz session follows the same high-level flow:

1. Open a WebSocket or TCP connection.
2. Send `CONNECT` with the JWT as the first Fitz message.
3. Treat a still-open connection as success and a close frame/socket close as failure.
4. Send domain requests that contain routes plus documented payload fields.
5. Decode synchronous responses and asynchronous deliveries.
6. On reconnect, authenticate again and rebuild any connection-scoped state.

## Transport Setup

### WebSocket

- Connect to `ws://` or `wss://`.
- Complete the normal WebSocket upgrade handshake.
- Send Fitz payloads in binary frames only.
- Reject or ignore text-frame based integrations; Fitz is binary only.

### TCP

- Connect to the configured broker port.
- Frame each Fitz message as `[u32 BE length][payload bytes]`.
- Buffer reads until the full payload is available.
- Reject frames whose declared length exceeds the configured maximum.

### Transport Equivalence

Both transports carry the same Fitz payloads. A client should decode the same
message body the same way regardless of whether it arrived over WebSocket or TCP.

## Authentication Flow

### Step 1: Open the Connection

The client first establishes the transport connection:

```python
client = FitzClient.connect("wss://broker.example/ws")
```

At this stage the transport is open, but the Fitz session is not authenticated yet.

### Step 2: Send `CONNECT`

The first Fitz message on a new connection must be `CONNECT`, carrying the
client's wire version range, its capability bits, the compact JWT, and
optional display metadata:

```python
client.connect(jwt_token)
```

Wire shape (header, then value):

```text
[type=1][flags=0][correlation=0][length]
[u16 min_version][u16 max_version][u32 client_caps][string jwt][string service_name][string client_build]
```

Client requirements:

- `CONNECT` MUST be the first Fitz message on a new connection.
- The client MUST NOT send extra shard or routing metadata.
- The client MUST treat the JWT as authentication input, not as a source of client-side dispatch logic.

### Step 3: Read HELLO or CONNECT_REJECTED

The broker answers every `CONNECT` with exactly one message:

- `HELLO` (type 2): the wire version in force, the broker's capability bits,
  `max_message_bytes`, `max_in_flight`, and a diagnostic session id. The
  session is live.
- `CONNECT_REJECTED` (type 3): a protocol-block code and a message, then the
  broker closes. `0001` is an invalid or missing JWT; `0050` means no wire
  version in common; `0051` means the identity maps to a route family this
  broker does not serve.

A client MAY pipeline domain requests behind `CONNECT`; they are served only
after a successful `CONNECT` and are lost with the connection on rejection.
A close with neither answer is a transport failure, not a rejection.

See `spec/wire-routing.md`, Connection Lifecycle, for both layouts.

### Step 4: Internal Broker Setup

After a valid `CONNECT`, the broker may attach internal session metadata and
authorization state. In authenticated mode the broker resolves route family
server-side from the configured identity claim and `FITZ_ROUTE_FAMILY_MAP`;
anonymous mode always uses internal family `1`.
Clients do not observe or manage that state directly.

## Request and Response Flow

Once authenticated, the client sends domain requests. Each request is self-contained
and includes the route plus the fields documented for that message type.

### Example: KV `BEGIN`

User-facing call:

```python
tx = client.kv_begin(
    route="kv://prod/app/users",
    mode=TxMode.ReadWrite,
)
```

Wire payload shape:

```text
[MessageType=100][Length=N][u32 route_len][route][u8 mode]
```

Client-side processing:

1. Encode the route string.
2. Encode the operation fields in documented order.
3. Send the frame over WebSocket or TCP.
4. Wait for the response frame.
5. Decode the response and return a transaction object or error.

Response handling:

```python
tx.put(b"user:123", b"alice")
tx.commit(CommitPersistence.Sync)
```

The transaction object may store route and transaction identifiers internally so the
public API stays ergonomic, but each wire operation still carries its documented fields.

### General Invariants

- Routes are opaque strings from the client's perspective.
- Clients MUST NOT derive broker dispatch behavior from JWT claims or route segments.
- Clients MUST NOT add undocumented routing fields to request payloads.
- Clients SHOULD surface domain errors as typed client errors when possible.

## Async Deliveries

Some domains deliver messages asynchronously after a client subscribes or registers:

- Notice: `NOTIFY`
- RPC: inbound worker requests and responses
- Stream: subscription deliveries
- Schedule: notifications

Typical flow:

1. Client sends a `SUBSCRIBE`-style request.
2. Broker acknowledges according to that domain's response contract.
3. Client keeps a handler or callback registered for future deliveries on that connection.
4. Incoming delivery frames are decoded using the same protocol layer as normal responses.

Example:

```typescript
const sub = await client.notice.subscribe("notice://prod/app/*", handler);
```

The client should track these registrations per connection because they are not preserved across reconnects.

## Reconnect Flow

Disconnects create a new Fitz session. On reconnect, the client must rebuild any
connection-scoped state explicitly.

Recommended reconnect sequence:

1. Detect socket close, read failure, or write failure.
2. Open a new WebSocket or TCP connection.
3. Send `CONNECT(jwt)` again.
4. Re-create subscriptions and worker registrations.
5. Resume normal request flow.

State handling rules:

- Subscription state is lost on disconnect.
- Worker registrations are lost on disconnect.
- In-flight KV transactions are lost on disconnect.
- In-flight queue lease handling must be restarted according to the domain contract.
- The client SHOULD assume every reconnect is a brand-new authenticated session.

## Failure Cases

Clients should handle these cases explicitly:

- `CONNECT` rejected: read `CONNECT_REJECTED`, surface its code and message, and expect the close.
- `GOAWAY` received: the broker is draining; finish in-flight work and reconnect within `grace_ms`.
- Domain frame sent before `CONNECT`: expect connection close.
- Malformed or unsupported stream `Read` filter: expect a typed stream error response (2055 or 2056) and keep the connection open.
- Partial TCP frame: keep buffering until complete or the socket closes.
- Oversized frame length: fail fast before allocating.
- Reconnect while work is in flight: treat in-flight work as interrupted unless the domain contract says otherwise.

## Implementation Checklist

- Support both WebSocket and TCP transports.
- Send `CONNECT` first on every new connection.
- Treat routes as opaque strings.
- Encode only the documented domain fields.
- Decode both synchronous responses and asynchronous deliveries.
- Re-subscribe or re-register after reconnect.
- Never expose broker-internal shard or routing state in the public client API.
