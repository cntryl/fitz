# Error Codes (wire format 2)

Status: **authoritative for 0.3.0 and the 1.0.0 wire format**. Codes are
append-only: a code is never renamed, reused, or given a new meaning. The
broker's catalog in `src/protocol/wire2/catalog.rs` is the implementation of
this page; the two must agree and the conformance vectors prove it.

Every error travels in the common envelope from
[wire-format-1.0-design.md](../../development/wire-format-1.0-design.md):

```
[u8 1][u16 BE code][u8 class][u8 effect][string message]
```

## Class

`class` is explicit so an SDK never derives it from the code by arithmetic.
It is what a generic retry policy keys on.

| Class | Name | Meaning |
| ---: | --- | --- |
| 0 | DOMAIN | A domain-specific condition; consult the code |
| 1 | UNAUTHORIZED | Permission denied for this route and verb, or wrong ownership |
| 2 | INVALID_ROUTE | Route shape, scheme, realm, or family mismatch |
| 3 | INVALID_PATTERN | Registration or selector pattern grammar |
| 4 | REGISTRATION_LIMIT | A per-session or per-domain cap on registrations or definitions |
| 5 | BUSY | The broker declined to accept the request; nothing applied |
| 6 | BACKEND | Storage or runtime failure after acceptance |
| 7 | TIMEOUT | Accepted but not finished before its deadline |
| 8 | BAD_REQUEST | The payload decodes but violates the verb contract |
| 9 | NOT_FOUND | The addressed object does not exist |
| 10 | UNSUPPORTED | Version, capability, option, or flag not negotiated |

## Effect

| Effect | Name | Retry guidance |
| ---: | --- | --- |
| 0 | NOT_APPLIED | Re-sending is safe |
| 1 | INDETERMINATE | The request may have taken effect; retry only idempotent work, or verify first |
| 2 | APPLIED | The operation happened; the error describes a follow-on failure |

The table below gives the typical effect. A broker may report a different
effect for a specific failure when it knows better, which is why the field is
on the wire rather than in this table.

## Numbering

Blocks are decimal thousands per domain. In every block, codes `x001` through
`x010` are the shared conditions and carry the same class as their slot.
Domain-specific codes start at `x050`. Codes `x011` through `x049` are never
assigned, so a future shared slot can never collide with a domain code.

| Block | Owner |
| --- | --- |
| 0xxx | Protocol and session |
| 1xxx | KV |
| 2xxx | Stream |
| 3xxx | Notice |
| 4xxx | Queue |
| 5xxx | Lease |
| 6xxx | RPC |
| 7xxx | Schedule |

### Shared slots, present in every domain block

| Slot | Name | Class | Typical effect |
| --- | --- | --- | --- |
| x001 | UNAUTHORIZED | 1 | NOT_APPLIED |
| x002 | INVALID_ROUTE | 2 | NOT_APPLIED |
| x003 | INVALID_PATTERN | 3 | NOT_APPLIED |
| x004 | REGISTRATION_LIMIT | 4 | NOT_APPLIED |
| x005 | BUSY | 5 | NOT_APPLIED |
| x006 | BACKEND | 6 | INDETERMINATE |
| x007 | TIMEOUT | 7 | INDETERMINATE |
| x008 | BAD_REQUEST | 8 | NOT_APPLIED |
| x009 | NOT_FOUND | 9 | NOT_APPLIED |
| x010 | UNSUPPORTED | 10 | NOT_APPLIED |

## Protocol block (0xxx)

Used by `CONNECT_REJECTED`, `GOAWAY`, and errors the session layer answers
before a request reaches a domain.

| Code | Name | Class | Effect | Meaning |
| --- | --- | --- | --- | --- |
| 0001 | UNAUTHORIZED | 1 | NOT_APPLIED | JWT invalid, expired, or missing in authenticated mode |
| 0005 | BUSY | 5 | NOT_APPLIED | `max_in_flight` reached for this connection |
| 0008 | BAD_REQUEST | 8 | NOT_APPLIED | Malformed header or payload; length mismatch; trailing bytes |
| 0010 | UNSUPPORTED | 10 | NOT_APPLIED | Unknown verb id, reserved flag set, or capability not negotiated |
| 0050 | VERSION_MISMATCH | 10 | NOT_APPLIED | No wire version in common (`CONNECT_REJECTED`) |
| 0051 | FAMILY_NOT_PROVISIONED | 2 | NOT_APPLIED | Identity maps to a route family the broker does not serve |
| 0052 | CONNECT_TIMEOUT | 8 | NOT_APPLIED | No `CONNECT` within the broker's window |
| 0053 | MESSAGE_TOO_LARGE | 8 | NOT_APPLIED | `length` exceeds the advertised `max_message_bytes` |
| 0054 | DRAINING | 5 | NOT_APPLIED | `GOAWAY`: planned drain; reconnect within `grace_ms` |
| 0055 | FORCED_CLOSE | 6 | INDETERMINATE | `GOAWAY`: the broker is closing now; in-flight outcomes unknown |
| 0056 | LIMIT_CHANGED | 10 | NOT_APPLIED | `GOAWAY`: a negotiated limit changed; reconnect to renegotiate |

## KV (1xxx)

| Code | Name | Class | Effect | Was | Meaning |
| --- | --- | --- | --- | --- | --- |
| 1001 | UNAUTHORIZED | 1 | NOT_APPLIED | 1011 | |
| 1002 | INVALID_ROUTE | 2 | NOT_APPLIED | 1007 | |
| 1003 | INVALID_PATTERN | 3 | NOT_APPLIED | 1012 | |
| 1004 | REGISTRATION_LIMIT | 4 | NOT_APPLIED | 1013 | |
| 1005 | BUSY | 5 | NOT_APPLIED | 1014 | |
| 1006 | BACKEND | 6 | INDETERMINATE | 1009 | |
| 1007 | TIMEOUT | 7 | INDETERMINATE | new | |
| 1008 | BAD_REQUEST | 8 | NOT_APPLIED | new | |
| 1009 | NOT_FOUND | 9 | NOT_APPLIED | new | Generic; prefer the specific codes below |
| 1010 | UNSUPPORTED | 10 | NOT_APPLIED | new | |
| 1050 | TRANSACTION_NOT_FOUND | 9 | NOT_APPLIED | 1001 | Unknown or expired transaction id |
| 1051 | INVALID_MODE | 8 | NOT_APPLIED | 1002 | Unknown transaction mode on BEGIN |
| 1052 | KEY_NOT_FOUND | 9 | NOT_APPLIED | 1003 | |
| 1053 | ISOLATION_CONFLICT | 0 | NOT_APPLIED | 1004 | Commit lost to a concurrent writer; begin again |
| 1054 | WRITE_IN_READONLY | 8 | NOT_APPLIED | 1005 | |
| 1055 | KEY_EXISTS | 0 | NOT_APPLIED | 1006 | INSERT on an existing key |
| 1056 | REALM_MISMATCH | 2 | NOT_APPLIED | 1008 | Operation targets a realm other than the transaction's |
| 1057 | TRANSACTION_ABORTED | 0 | NOT_APPLIED | 1010 | Transaction was rolled back by the broker |

## Stream (2xxx)

| Code | Name | Class | Effect | Was | Meaning |
| --- | --- | --- | --- | --- | --- |
| 2001 | UNAUTHORIZED | 1 | NOT_APPLIED | 2009 | |
| 2002 | INVALID_ROUTE | 2 | NOT_APPLIED | new | Includes route family mismatch, which was 2012 |
| 2003 | INVALID_PATTERN | 3 | NOT_APPLIED | 2010 | |
| 2004 | REGISTRATION_LIMIT | 4 | NOT_APPLIED | 2011 | |
| 2005 | BUSY | 5 | NOT_APPLIED | 2014 | |
| 2006 | BACKEND | 6 | INDETERMINATE | 2012 | |
| 2007 | TIMEOUT | 7 | INDETERMINATE | new | |
| 2008 | BAD_REQUEST | 8 | NOT_APPLIED | new | |
| 2009 | NOT_FOUND | 9 | NOT_APPLIED | new | |
| 2010 | UNSUPPORTED | 10 | NOT_APPLIED | new | |
| 2050 | CONCURRENCY_CONFLICT | 0 | NOT_APPLIED | 2001 | `expected_offset` mismatch; the session stays open |
| 2051 | SESSION_ALREADY_ACTIVE | 0 | NOT_APPLIED | 2002 | |
| 2052 | SESSION_NOT_FOUND | 9 | NOT_APPLIED | 2003 | |
| 2053 | INVALID_READ_BOUND | 8 | NOT_APPLIED | 2004 | |
| 2054 | RESOURCE_NOT_FOUND | 9 | NOT_APPLIED | 2005 | |
| 2055 | FILTER_UNSUPPORTED_VERSION | 10 | NOT_APPLIED | 2006 | |
| 2056 | FILTER_INVALID_PAYLOAD | 8 | NOT_APPLIED | 2007 | |
| 2057 | READ_RESPONSE_TOO_LARGE | 8 | NOT_APPLIED | 2013 | One record exceeds `max_message_bytes`; narrow the read |

## Notice (3xxx)

| Code | Name | Class | Effect | Was | Meaning |
| --- | --- | --- | --- | --- | --- |
| 3001 | UNAUTHORIZED | 1 | NOT_APPLIED | 3009 | |
| 3002 | INVALID_ROUTE | 2 | NOT_APPLIED | 3001 | |
| 3003 | INVALID_PATTERN | 3 | NOT_APPLIED | 3002 | Every pattern grammar error, including the malformed wildcard shapes that were 3005 |
| 3004 | REGISTRATION_LIMIT | 4 | NOT_APPLIED | 3003 | |
| 3005 | BUSY | 5 | NOT_APPLIED | 3006 | Also the answer to an `ACK_REQUESTED` PUBLISH the broker dropped |
| 3006 | BACKEND | 6 | INDETERMINATE | 3005 | |
| 3007 | TIMEOUT | 7 | INDETERMINATE | new | |
| 3008 | BAD_REQUEST | 8 | NOT_APPLIED | new | |
| 3009 | NOT_FOUND | 9 | NOT_APPLIED | new | |
| 3010 | UNSUPPORTED | 10 | NOT_APPLIED | new | |
| 3050 | TRANSPORT_CLOSED | 0 | NOT_APPLIED | 3004 | The subscriber's transport closed before delivery |

## Queue (4xxx)

| Code | Name | Class | Effect | Was | Meaning |
| --- | --- | --- | --- | --- | --- |
| 4001 | UNAUTHORIZED | 1 | NOT_APPLIED | 4009 | |
| 4002 | INVALID_ROUTE | 2 | NOT_APPLIED | new | |
| 4003 | INVALID_PATTERN | 3 | NOT_APPLIED | 4010 | |
| 4004 | REGISTRATION_LIMIT | 4 | NOT_APPLIED | 4011 | |
| 4005 | BUSY | 5 | NOT_APPLIED | 4005 | Was named QUEUE_FULL; same number, same meaning |
| 4006 | BACKEND | 6 | INDETERMINATE | 4007 | |
| 4007 | TIMEOUT | 7 | INDETERMINATE | new | |
| 4008 | BAD_REQUEST | 8 | NOT_APPLIED | 4006 | |
| 4009 | NOT_FOUND | 9 | NOT_APPLIED | new | |
| 4010 | UNSUPPORTED | 10 | NOT_APPLIED | new | |
| 4050 | INVALID_TOKEN | 8 | NOT_APPLIED | 4001 | Inflight token invalid or wrong |
| 4051 | INFLIGHT_EXPIRED | 0 | NOT_APPLIED | 4002 | The reservation lapsed; the message may be redelivered |
| 4052 | MESSAGE_NOT_FOUND | 9 | NOT_APPLIED | 4003 | |
| 4053 | QUEUE_NOT_FOUND | 9 | NOT_APPLIED | 4004 | |

## Lease (5xxx)

| Code | Name | Class | Effect | Was | Meaning |
| --- | --- | --- | --- | --- | --- |
| 5001 | UNAUTHORIZED | 1 | NOT_APPLIED | 5009 | |
| 5002 | INVALID_ROUTE | 2 | NOT_APPLIED | new | |
| 5003 | INVALID_PATTERN | 3 | NOT_APPLIED | 5010, 5012 | SUBSCRIBE and LIST selector grammar |
| 5004 | REGISTRATION_LIMIT | 4 | NOT_APPLIED | new | |
| 5005 | BUSY | 5 | NOT_APPLIED | 5007 (mailbox case) | |
| 5006 | BACKEND | 6 | INDETERMINATE | new | |
| 5007 | TIMEOUT | 7 | INDETERMINATE | new | Broker-side deadline after acceptance; not the acquire wait |
| 5008 | BAD_REQUEST | 8 | NOT_APPLIED | 5008 | |
| 5009 | NOT_FOUND | 9 | NOT_APPLIED | new | |
| 5010 | UNSUPPORTED | 10 | NOT_APPLIED | new | |
| 5050 | HELD | 0 | NOT_APPLIED | 5001 | Immediate rejection with `wait_seconds=0` |
| 5051 | INVALID_FENCE | 8 | NOT_APPLIED | 5002 | Fencing token invalid or out of order |
| 5052 | EXPIRED | 0 | NOT_APPLIED | 5003 | Lease no longer valid |
| 5053 | LEASE_NOT_FOUND | 9 | NOT_APPLIED | 5004 | |
| 5054 | INVALID_TOKEN | 8 | NOT_APPLIED | 5005 | |
| 5055 | ACQUIRE_WAIT_EXPIRED | 0 | NOT_APPLIED | 5006 | The caller's `wait_seconds` elapsed without a grant |
| 5056 | WAITERS_LIMIT | 5 | NOT_APPLIED | 5007 (waiters case) | Too many pending waiters on the key; back off |
| 5057 | INVALID_LIST_CURSOR | 8 | NOT_APPLIED | 5011 | Cursor unknown, from another selector, or from a previous broker lifetime |

## RPC (6xxx)

| Code | Name | Class | Effect | Was | Meaning |
| --- | --- | --- | --- | --- | --- |
| 6001 | UNAUTHORIZED | 1 | NOT_APPLIED | 6009 | |
| 6002 | INVALID_ROUTE | 2 | NOT_APPLIED | 6011 | |
| 6003 | INVALID_PATTERN | 3 | NOT_APPLIED | 6012 | |
| 6004 | REGISTRATION_LIMIT | 4 | NOT_APPLIED | 6013 | |
| 6005 | BUSY | 5 | NOT_APPLIED | 6003 | Was RPC_BACKPRESSURE |
| 6006 | BACKEND | 6 | INDETERMINATE | 6010 (unavailable case) | |
| 6007 | TIMEOUT | 7 | INDETERMINATE | 6001, 6010 (timeout case) | The two meanings that shared 6010 are now distinct |
| 6008 | BAD_REQUEST | 8 | NOT_APPLIED | new | |
| 6009 | NOT_FOUND | 9 | NOT_APPLIED | new | |
| 6010 | UNSUPPORTED | 10 | NOT_APPLIED | new | |
| 6050 | WORKER_NOT_FOUND | 9 | INDETERMINATE | 6002 | A dispatched worker may have applied the operation before disconnecting; retry only idempotent work or verify first |
| 6051 | ROUTE_NOT_REGISTERED | 9 | NOT_APPLIED | 6004 | No worker registered for the route |
| 6052 | CORRELATION_NOT_FOUND | 9 | NOT_APPLIED | 6005 | Response, ACK, or cancel named an unknown or finished request |
| 6053 | INVALID_SEQUENCE | 8 | NOT_APPLIED | 6006 | Chunks must start at 0 and advance contiguously |
| 6054 | DUPLICATE_CORRELATION | 8 | NOT_APPLIED | 6007 | Caller reused a live request identifier |
| 6055 | WRONG_WORKER | 1 | NOT_APPLIED | 6008 | Response or ACK from a worker that does not own the request |

## Schedule (7xxx)

Every Schedule verb now returns coded errors. The uncoded messages that
production clients matched on map as follows:

| Old uncoded message | New code |
| --- | --- |
| `wildcard subscription limit exceeded (128 per session)` | 7004 |
| `total subscription limit exceeded (1024 per session)` | 7004 |
| invalid cron | 7051 |
| unknown delivery mode | 7054 |
| invalid subscription pattern | 7003 |

| Code | Name | Class | Effect | Was | Meaning |
| --- | --- | --- | --- | --- | --- |
| 7001 | UNAUTHORIZED | 1 | NOT_APPLIED | 7009 | |
| 7002 | INVALID_ROUTE | 2 | NOT_APPLIED | new | |
| 7003 | INVALID_PATTERN | 3 | NOT_APPLIED | 7006 | |
| 7004 | REGISTRATION_LIMIT | 4 | NOT_APPLIED | 7007 | |
| 7005 | BUSY | 5 | NOT_APPLIED | 7010 | Was named BACKEND_ERROR but meant saturation; the mismatch ends here |
| 7006 | BACKEND | 6 | INDETERMINATE | new | |
| 7007 | TIMEOUT | 7 | INDETERMINATE | 7011 | |
| 7008 | BAD_REQUEST | 8 | NOT_APPLIED | 7004 | Was PARSE_ERROR |
| 7009 | NOT_FOUND | 9 | NOT_APPLIED | new | |
| 7010 | UNSUPPORTED | 10 | NOT_APPLIED | new | |
| 7050 | SCHEDULE_NOT_FOUND | 9 | NOT_APPLIED | 7001 | |
| 7051 | INVALID_CRON | 8 | NOT_APPLIED | 7002 | |
| 7052 | DEFINITION_LIMIT | 4 | NOT_APPLIED | 7003 | Was SCHEDULE_LIMIT |
| 7053 | INVALID_TARGET | 8 | NOT_APPLIED | 7005 | |
| 7054 | INVALID_DELIVERY_MODE | 8 | NOT_APPLIED | 7008 | |

## Rules for SDK authors

1. Branch on `class` for generic policy and on `code` for specific handling.
   Never branch on `message`.
2. Retry only when `effect` is NOT_APPLIED, or when the operation is
   idempotent by the domain's own rules.
3. An unknown code with a known class is handled by class. An unknown class
   is treated as BACKEND with the reported effect.
4. Codes a broker version does not emit are still valid identifiers; never
   remove one from an SDK enum.
