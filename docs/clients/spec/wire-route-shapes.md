# Wire Route Shapes

Route acceptance rules, global route rules, per-domain route shapes, and the lock-in rule, moved from [wire-routing.md](wire-routing.md).

## Route Acceptance Criteria (Authoritative)

A request is valid **only if**:

1. The route shape is valid for the domain
2. Wildcards appear only in allowed positions (per domain)
3. The method permits those wildcards
4. The route depth matches the method's plane
   **Violations are protocol errors.** Broker MUST reject; clients **MAY** perform local route shape validation for ergonomics, but the broker is authoritative. Clients **MUST** accept broker rejection as the source of truth and MUST NOT rely on local validation as a substitute for server-side checks.

## Global Route Rules (Normative)

- Routes are opaque strings with a fixed, domain-defined shape
- `{realm}` may be a whole-segment wildcard for registration operations that support patterns
- `*` MAY appear only in positions explicitly allowed by the domain
- Extra path segments are **forbidden**
- Route shape validation occurs **before** permission or dispatch checks

### Wildcard Support by Domain

**Domains supporting wildcards (`*` and `**` patterns):**
- **KV:** SUBSCRIBE and UNSUBSCRIBE accept patterns capable of matching a three-segment route; mutations remain concrete
- **Stream:** READ patterns and SUBSCRIBE/UNSUBSCRIBE registration patterns are supported; writes remain concrete
- **Queue:** RESERVE, SUBSCRIBE, and UNSUBSCRIBE accept patterns capable of
  matching a three-segment route; other Queue operations remain concrete
- **Notice:** Full wildcard support in SUBSCRIBE patterns (`notice://realm/area/*`, `notice://realm/**`)
- **RPC:** Worker registrations accept `*` and `**`; calls remain concrete
- **Schedule:** SUBSCRIBE and UNSUBSCRIBE accept patterns capable of matching a four-segment route; CREATE and CANCEL remain concrete

**Domains requiring concrete routes only (no wildcards):**
- **Lease:** All operations use concrete routes only (`lease://realm/area/resource`)
- **KV mutations:** use concrete routes only (`kv://realm/area/resource`)
- **Queue mutations:** ENQUEUE, EXTEND, and COMPLETE use concrete routes only
  (`queue://realm/area/resource`)
- **Stream writes:** use concrete routes only (`stream://realm/area/resource`)
- **RPC calls:** use concrete routes only (`rpc://realm/area/resource/operation`)
- **Schedule definitions:** `CREATE` and `CANCEL` use concrete routes only (`schedule://realm/area/resource/operation`)

**Pattern matching semantics:**
- `*` matches exactly one path segment
- `**` matches zero or more path segments (greedy)
- Concrete routes (no wildcards) match exactly
- Each registration domain permits at most 1,024 retained registrations per
  session, counting exact and wildcard registrations together, plus at most
  128 wildcard registrations per session. Duplicate `(session, original
  registration string)` requests are checked before either limit
- Wildcards never cross `RouteFamily` or permission boundaries, and overlapping registrations have no exact-pattern precedence
- Notifications carry the matching `subscription_id` and the exact concrete
  route, never the registration pattern

## Route Shapes by Domain

### KV Domain

**Valid Route Shapes:**

- `kv://{realm}/{area}`
- `kv://{realm}/{area}/{resource}`
- `kv://{realm}/{area}/*`
- `kv://{realm}/*/*`
- `kv://{realm}/**`
- `kv://*/{area}/{resource}`
  **Method Acceptance:**
  | Method | Accepted Route Shapes |
  | ---------------- | ----------------------------------------------- |
  | `LIST` | `{realm}/{area}`, `{realm}/*/*` |
  | `CREATE` | `{realm}/{area}` |
  | `DELETE` (admin) | `{realm}/{area}` |
  | `BEGIN` | `{realm}/{area}/{resource}` |
  | `GET` | `{realm}/{area}/{resource}` |
  | `PUT` | `{realm}/{area}/{resource}` |
  | `INSERT` | `{realm}/{area}/{resource}` |
  | `DELETE` | `{realm}/{area}/{resource}` |
  | `DELETE_RANGE` | `{realm}/{area}/{resource}` |
  | `SCAN` | `{realm}/{area}/{resource}`, `{realm}/{area}/*` |
  | `SUBSCRIBE` | `{realm}/{area}/{resource}`, `{realm}/{area}/*`, `{realm}/*/*` |
  | `UNSUBSCRIBE` | same as `SUBSCRIBE` |
  | `CREATE_BATCH` | broker extension; repeated canonical CREATE entries |
  | `LIST_V2` | broker extension; cursor pagination, not a replacement for LIST 702 |
  | `COMMIT` | `{realm}/{area}/{resource}` |
  | `ROLLBACK` | `{realm}/{area}/{resource}` |
  **Note:** `LIST`, `CREATE`, and `DELETE` (admin) operations are broker-internal management operations not currently exposed in the client wire protocol. Clients should focus on data operations: BEGIN, GET, PUT, INSERT, DELETE, DELETE_RANGE, SCAN, SUBSCRIBE, UNSUBSCRIBE, COMMIT, ROLLBACK.

### Stream Domain

**Valid Route Shapes:**

- `stream://{realm}/{area}/{resource}`
- `stream://{realm}/{area}/*`
- `stream://{realm}/*/*`
- `stream://{realm}/**`
- `stream://*/{area}/{resource}`
  **Method Acceptance:**
  | Method | Accepted Route Shapes |
  | ---------------- | -------------------------------------------------------------- |
  | `LIST` | `{realm}/{area}`, `{realm}/*/*` |
  | `CREATE` | `{realm}/{area}` |
  | `DELETE` (admin) | `{realm}/{area}` |
  | `BEGIN` | `{realm}/{area}/{resource}` |
  | `APPEND` | session_id established by `BEGIN({realm}/{area}/{resource})` |
  | `READ` | `{realm}/{area}/{resource}`, `{realm}/{area}/*`, `{realm}/*/*` |
  | `SUBSCRIBE` | `{realm}/{area}/{resource}`, `{realm}/{area}/*`, `{realm}/*/*` |
  | `UNSUBSCRIBE` | same as `SUBSCRIBE` |
  | `COMMIT` | session_id established by `BEGIN({realm}/{area}/{resource})` |
  | `ROLLBACK` | session_id established by `BEGIN({realm}/{area}/{resource})` |
  **Note:** `LIST`, `CREATE`, and `DELETE` (admin) operations are broker-internal management operations not currently exposed in the client wire protocol. Clients should focus on stream operations: BEGIN, APPEND, READ, SUBSCRIBE, UNSUBSCRIBE, COMMIT, ROLLBACK.

### Queue Domain

**Valid Route Shapes:**

- `queue://{realm}/{area}/{resource}`
- `queue://{realm}/{area}/*`
- `queue://{realm}/*/*`
- `queue://{realm}/**`
- `queue://*/{area}/{resource}`

**Route format:** For per-resource isolation, use the 3-segment form `queue://{realm}/{area}/{resource}`. Each distinct resource has its own queue and lease state.

RESERVE response items are selector-dependent without adding a negotiation
field: an exact request returns the established route-less item shape, while a
request containing a whole-segment wildcard returns the matched concrete route
before each item. The client always knows which decoder to use from the request
it sent.

**Lease expiry:** Servers process lease expiry lazily (e.g. when the next RESERVE or other operation runs). A reserved message whose lease has expired is returned to the ready queue on the next operation that touches that queue. Clients that rely on lease expiry (e.g. to re-reserve) should allow for this delay (e.g. wait a few seconds after lease TTL before re-reserving).

  **Method Acceptance:**
  | Method | Accepted Route Shapes |
  | ---------- | ----------------------------------------------- |
  | `LIST` | `{realm}/{area}`, `{realm}/*/*` |
  | `ENQUEUE` | `{realm}/{area}/{resource}` |
  | `RESERVE` | exact route or whole-segment pattern capable of matching three segments |
  | `COMPLETE` | `{realm}/{area}/{resource}` |
  | `EXTEND` | `{realm}/{area}/{resource}` |
  | `SUBSCRIBE` | exact route or whole-segment pattern capable of matching three segments |
  | `UNSUBSCRIBE` | same as `SUBSCRIBE` |

  **Note:** `LIST` is a broker-internal management operation not currently exposed in the client wire protocol. Clients should use: ENQUEUE, RESERVE, COMPLETE, EXTEND as documented in the wire format section.

### Schedule Domain

**Valid Route Shapes:**

- concrete route: `schedule://{realm}/{area}/{resource}/{operation}`
  **Method Acceptance:**
  | Method | Accepted Route Shapes |
  | -------- | ----------------------------------------------- |
  | `CREATE` | `{realm}/{area}/{resource}/{operation}` |
  | `CANCEL` | `{realm}/{area}/{resource}/{operation}` |
  | `LIST` | no route payload; optional `[offset][limit]` pagination fields only |
  | `SUBSCRIBE` | exact route or whole-segment pattern capable of matching four segments |
  | `UNSUBSCRIBE` | same as `SUBSCRIBE` |

  **Note:** `DELETE` (admin) and `TRIGGER` operations are broker-internal. Portable clients should use CREATE, CANCEL, LIST, SUBSCRIBE, and UNSUBSCRIBE as documented in the wire format section. The broker additionally advertises CREATE_BATCH 706 and LIST_V2 707; LIST 702 remains the canonical cross-client operation and returns a single response payload containing `total_count` plus zero or more schedule entries.

### Lease Domain

**Valid Route Shapes:**

- `lease://{realm}/{area}/{resource}`
  **Method Acceptance:**
  | Method | Accepted Route Shapes |
  | --------- | --------------------------- |
  | `ACQUIRE` | `{realm}/{area}/{resource}` |
  | `RENEW` | `{realm}/{area}/{resource}` |
  | `RELEASE` | `{realm}/{area}/{resource}` |
  | `QUERY` | `{realm}/{area}/{resource}` |
  | `SUBSCRIBE` | `{realm}/{area}/{resource}`, or the generic depth-3 `*`/`**` grammar |
  | `UNSUBSCRIBE` | same as `SUBSCRIBE` |
  | `LIST` | same as `SUBSCRIBE` |

`ACQUIRE`, `RENEW`, `RELEASE`, and `QUERY` accept only an exact route: any
wildcard, wrong scheme, empty segment, or route with fewer or more than three
segments is rejected. `SUBSCRIBE`, `UNSUBSCRIBE`, and `LIST` additionally
accept the complete literal-or-`*` matrix over the three segments plus valid
generic, non-adjacent `**` compositions capable of matching depth three (for
example `lease://{realm}/**`, `lease://**`, and
`lease://**/renderers/**`). A partial wildcard token, adjacent `**` segments,
wrong scheme, empty segment, or pattern incapable of matching three segments
is rejected with 5010 (5012 for `LIST` specifically). Accepting a wildcard
here never grants, renews, or releases the matched lease — only the exact-route
mutation operations change ownership.

### Notice Domain

**Valid Route Shapes:**

- `notice://{realm}/{area}/{resource}`
- `notice://{realm}/{area}/*`
- `notice://{realm}/*/*`
  **Method Acceptance:**
  | Method | Accepted Route Shapes |
  | ------------- | -------------------------------------------------------------- |
  | `SUBSCRIBE` | `{realm}/{area}/{resource}`, `{realm}/{area}/*`, `{realm}/*/*` |
  | `UNSUBSCRIBE` | same as `SUBSCRIBE` |
  | `PUBLISH` | `{realm}/{area}/{resource}` |

### RPC Domain

**Route Shape Guidance:**

- Calls use concrete route strings. Worker registrations accept strict
  whole-segment `*` and `**` patterns, including wildcard realm.
- Every registration owns independent concurrency credit across all concrete
  routes it matches. Exact and wildcard overlaps are equal candidates.
- Ready concrete routes rotate fairly within one `RouteFamily`; matching never
  crosses a family boundary.
- A session may retain at most 1,024 RPC registrations total and 128 wildcard
  registrations. Duplicate `(session, pattern)` registration is idempotent and
  retains its original credit.
- The common operation-style form is `rpc://{realm}/{area}/{resource}/{operation}`.
  **Method Acceptance:**
  | Method | Accepted Route Shapes |
  | ------------- | ------------------------------------------------------------ |
  | `CALL` | exact route (commonly `{realm}/{area}/{resource}/{operation}`) |
  | `SUBSCRIBE` | exact route or whole-segment wildcard pattern |
  | `UNSUBSCRIBE` | same as `SUBSCRIBE` |

## Lock-In Rule

**If a route shape is not explicitly listed for a method, it is invalid.**
This specification is the **single source of truth** for:

- Broker validation
- SDK conformance testing
- Permission enforcement
- Long-term protocol stability
