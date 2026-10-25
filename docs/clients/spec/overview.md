# Client Model

Fitz clients encode the documented messages and carry them over WebSocket or
TCP. The broker owns authorization, routing, domain state, and domain behavior.
The client specification is split into this overview, the [wire and routing
contract](wire-routing.md), and one specification per domain.

## Connection and session

Each connection creates an ephemeral broker session. The client sends `CONNECT`
first and waits for `HELLO` or `CONNECT_REJECTED`. `HELLO` supplies the active
wire version and connection limits. Disconnect ends the session; after
reconnecting, the client authenticates again and restores connection-owned
subscriptions and worker registrations. Old transactions, leases, pending
calls, and delivery handles are no longer valid.

## Request handling

- Encode each message according to its domain specification and enforce the
  message limit reported in `HELLO`.
- Match correlated responses by the header correlation value; do not depend on
  response arrival order.
- Route notifications by message type and domain subscription state.
- Preserve error effect semantics. A timeout or lost connection can leave an
  operation's outcome indeterminate; retry only when the operation is safe to
  repeat or its outcome has been checked.
- Rebuild client state after reconnect. The broker does not transfer session
  ownership to a replacement connection.

## API shape

SDKs may expose transactions, subscriptions, route helpers, or other
language-native conveniences. These are client-side ergonomics: they must
preserve the wire and domain contracts, and they must not assign a business
meaning to `realm` or derive it from `route_family`.

The broker remains authoritative for route validation and permissions. A
client may reject malformed inputs locally but must surface broker rejections
without changing their meaning.

## Transports

WebSocket uses binary messages. TCP prefixes each message with a four-byte
big-endian length. Both transports carry the same Fitz message format; see the
[wire contract](wire-routing.md) for headers, handshake, limits, and routing.
