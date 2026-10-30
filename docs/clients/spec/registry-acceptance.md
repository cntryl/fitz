# Message Registry

Message identifiers are fixed by wire format 2. Reserved identifiers are not
valid operations. The broker manifest in `src/protocol/manifest.rs` is the
implementation source of truth.

## Message identifiers

**Control (0–99):**
| Value | Name | Direction |
|---:|---|---|
| 1 | CONNECT | Client→Server |
| 2 | HELLO | Server→Client |
| 3 | CONNECT_REJECTED | Server→Client |
| 4 | GOAWAY | Server→Client |
**KV Domain (100–111):**
| Value | Name |
|---:|---|
| 100 | BEGIN |
| 101 | COMMIT |
| 102 | ROLLBACK |
| 103 | GET |
| 104 | PUT |
| 105 | INSERT |
| 106 | DELETE |
| 107 | DELETE_RANGE |
| 108 | SCAN |
| 109 | SUBSCRIBE |
| 110 | UNSUBSCRIBE |
| 111 | NOTIFY |
**Queue Domain (200–209):**
| Value | Name |
|---:|---|
| 200 | ENQUEUE |
| 202 | RESERVE |
| 203 | EXTEND |
| 204 | COMPLETE |
| 207 | SUBSCRIBE |
| 208 | UNSUBSCRIBE |
| 209 | NOTIFY |
**RPC Domain (300–303):**
| Value | Name |
|---:|---|
| 300 | SUBSCRIBE_WORKER |
| 301 | UNSUBSCRIBE_WORKER |
| 302 | REQUEST |
| 303 | RESPONSE |
**Lease messages:**
| Value | Name |
|---:|---|
| 400 | ACQUIRE |
| 401 | RENEW |
| 402 | RELEASE |
| 403 | QUERY |
| 407 | SUBSCRIBE |
| 408 | UNSUBSCRIBE |
| 409 | NOTIFY |
| 410 | LIST |
**Notice Domain (500–504):**
| Value | Name |
|---:|---|
| 500 | PUBLISH |
| 501 | SUBSCRIBE |
| 502 | UNSUBSCRIBE |
| 503 | UNSUBSCRIBE_ALL |
| 504 | NOTIFY |
**Stream Domain (600–609):**
| Value | Name |
|---:|---|
| 600 | BEGIN |
| 601 | APPEND |
| 602 | COMMIT |
| 603 | ROLLBACK |
| 604 | READ |
| 605 | LAST |
| 606 | GET_METADATA |
| 607 | SUBSCRIBE |
| 608 | UNSUBSCRIBE |
| 609 | NOTIFY |
**Schedule Domain (700–706):**
| Value | Name |
|---:|---|
| 700 | CREATE |
| 701 | CANCEL |
| 702 | LIST |
| 703 | SUBSCRIBE |
| 704 | UNSUBSCRIBE |
| 705 | NOTIFY |
| 706 | CREATE_BATCH (broker extension) |

### Message dispatch

The listed identifiers are exact assignments. Gaps are reserved and are not
implicitly assigned to a domain. The broker dispatches by the exact message
manifest entry; numeric ranges do not grant authorization or select a decoder.

### Error Code Allocation (Authoritative)

The authoritative allocation, with class, effect, and the old-to-new mapping,
is [error-codes.md](error-codes.md). In summary: blocks are decimal thousands
per domain (`0xxx` protocol, `1xxx` KV, `2xxx` Stream, `3xxx` Notice, `4xxx`
Queue, `5xxx` Lease, `6xxx` RPC, `7xxx` Schedule). Slots `x001`–`x010` carry
the same shared meaning in every block (`x001` UNAUTHORIZED, `x002`
INVALID_ROUTE, `x003` INVALID_PATTERN, `x004` REGISTRATION_LIMIT, `x005` BUSY,
`x006` BACKEND, `x007` TIMEOUT, `x008` BAD_REQUEST, `x009` NOT_FOUND, `x010`
UNSUPPORTED); domain-specific codes start at `x050`. Codes are append-only and
MUST NOT be reused across domains.

### Channel IDs (Broker-Internal Reference)

Clients do NOT encode these; listed for reference:
| ChannelId | Value | Purpose |
| --------- | ----: | ---------------------- |
| Control | 0 | Control/handshake |
| Pub | 1 | Publishing/notice |
| Sub | 2 | Subscriptions/delivery |
| Rpc | 3 | RPC request/response |
| Lease | 4 | Lease domain |

### Type Encoding Rules

`type` is a plain `u16 BE` in the fixed header. There is no escape byte and no
variable-width form: CONNECT is `[0x00 0x01]`, KV BEGIN is `[0x00 0x64]`,
Notice PUBLISH is `[0x01 0xF4]`.

## Related contracts

- [Canonical operations](operations.md) defines the operations and shared
  message behavior.
- Each [domain specification](../client-spec.md) defines its payloads and
  lifecycle rules.
- [Error codes](error-codes.md) defines error classes and effects.
- [Cross-language scenarios](../cross-language-conformance-suite.yaml) defines
  shared observable client behavior.
