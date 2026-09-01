# Migration Guide

This guide covers safe upgrades between Fitz releases.

## Upgrade Strategy

1. Read [development/format-compatibility.md](../development/format-compatibility.md).
2. Review [development/release-policy.md](../development/release-policy.md).
3. Test the upgrade path with representative data and a recoverable copy.
4. Plan a bounded maintenance replacement for each Fitz node. Fitz does not coordinate rolling state transfer between nodes.

## Route Family Identity Map Migration

The hardened broker resolves route family server-side from verified identity
context. Configure `FITZ_ROUTE_FAMILIES=1,2,...` with the contiguous,
provisioned families that the node may serve, then configure
`FITZ_ROUTE_FAMILY_MAP=identity=family,...` for the identity values accepted by
the node. The default identity claim is `tid`; set
`FITZ_ROUTE_FAMILY_CLAIM=org_id` for Auth0 Organizations.

For Auth0, configure Fitz with the Auth0 API audience, the Auth0 JWKS URL, and a
route-family map keyed by Auth0 organization IDs. See
[../user-guides/auth0.md](../user-guides/auth0.md).

Update token issuers to emit identity context and one supported permission
source, and stop emitting all removed legacy Fitz auth shapes:
`fitz.route_family`, `fitz.permissions`, JWT `realm`, JWT `areas`, and JWT
`scopes`. A hardened node rejects authenticated `CONNECT` when the configured
identity claim is missing, unmapped, or maps to an unprovisioned family.
Anonymous mode always uses route family `1`.

## Breaking Admin Route Migration

Admin domain routes now require a concrete family path segment. Replace every
`/api/v1/{domain}/...` request with `/api/v1/{family}/{domain}/...`; do not use
`family` or `route_family` query parameters as a fallback. The removed
domain-first paths return `404`. Route-family values are `u32` identifiers:
wire and admin values above `u32::MAX` are rejected rather than clamped.

### Routed Queue reserve and Stream read upgrade

Existing concrete Queue RESERVE clients remain wire compatible: each item is
still `[message_id][lease_token][body]`. Clients that send the new wildcard
RESERVE form must decode each returned item as
`[concrete_route][message_id][lease_token][body]` and use that route for EXTEND
or COMPLETE. Stream READ now prefixes every event, filtered marker, and filtered
range with `concrete_route`; mixed broker/client versions cannot decode Stream
responses. The byte-exact response layouts are defined in
[the Queue wire contract](../clients/spec/queue-rpc-kv.md) and
[the Stream wire contract](../clients/spec/notice-stream.md).

Existing Stream stores require an offline event export/replay into a fresh
store, or an intentional clear and rebuild of persisted Stream state. The
promotion-frontier generation marker changes to D4 (`[0, 0xD4, 2]`); startup
rejects the D3 marker with reset-required guidance before decoding or scanning
any D3 pages. Compact area/realm pages now require persisted route identity and the old
route-less markers are rejected. Take and retain a pre-upgrade snapshot. To roll
back, restore that snapshot and the prior broker/client versions together.

### Schedule delivery-mode client upgrade

Upgrade every client Schedule codec atomically with the broker. CREATE entries
are now `[route][cron][mode][payload]`; CREATE_BATCH repeats that shape; LIST
entries return `[route][cron][mode][payload]`. Use `0` for broadcast and `1` for
single delivery. Unknown values fail with `ERR_INVALID_DELIVERY_MODE` (`7008`).
SCHEDULE_NOTIFY (`705`) is a clean wire break from
`[subscription_id][payload]` to `[subscription_id][exact_route][payload]`.
Upgrade every Schedule client decoder before routing traffic to the new broker.
Rollback requires restoring the prior broker and prior client codec together;
mixed versions cannot safely decode 705 frames.

Schedule backend unavailability and saturation now use the dedicated
`ERR_BACKEND_ERROR` (`7010`) wire code. Upgrade clients to preserve and classify
that code as transient, subject to operation replay safety. Do not map these
failures to `ERR_PARSE_ERROR` (`7004`), which incorrectly tells callers that
their cron or payload is malformed.

### Subscription registration contract

KV, Queue, Notice, Stream, RPC, and Schedule now share strict whole-segment
`*`/`**` registration validation and a 128-wildcard-registration session limit.
Exact registrations do not count, and duplicates remain idempotent. Clients
must surface the domain-specific validation and limit codes: KV 1012/1013,
Stream 2010/2011, Notice 3002/3003, Queue 4010/4011, RPC 6012/6013, and Schedule
7006/7007.

Lease `SUBSCRIBE` and `UNSUBSCRIBE` now accept the shared generic
three-segment selector grammar, including whole-segment `*` and valid
non-adjacent `**` forms. Existing exact subscriptions remain wire compatible;
new wildcard callers must use an updated SDK that applies the same grammar as
the broker and handles the 128-wildcard-registration session limit. `LIST`
(message 410) is a clean protocol addition with typed 5011/5012
cursor/selector failures, and each supported SDK now provides a high-level
subscribe-before-list inventory observer. Upgrade broker and observer clients
together before enabling patterned fleet observation. ACQUIRE now rejects an
`owner_id` longer than 512 bytes so every legal holder can fit in a LIST item.

Queue availability notifications now carry the concrete three-segment Queue
resource route rather than a synthetic `/ready` suffix. Update Queue
notification routing before upgrading the broker.

### Schedule cron day-field compatibility

Schedule evaluation now follows standard cron semantics when both day-of-month
and day-of-week are restricted: a date fires when either field matches.
Previously, Fitz required both fields to match. Review existing schedules that
restrict both fields before upgrading because they can fire more often after
the change. The broker also rejects calendar-impossible expressions during
CREATE and startup recovery instead of fabricating a later fire time.

Update admin grants to either `*` or canonical decimal family IDs. Symbolic,
non-canonical, and overflowed grants are rejected when a session is created or
validated.

## Stream `**` Selector Aliases Now Reach Authorization

The two canonical `**` aliases in routing-design.md §8.1, `stream://**` and
`stream://{realm}/**`, were rejected during ingress authorization because Stream
routes were canonicalized through the generic realm/area/resource parser, which
requires three segments. Clients running with authorization enabled could not
use either alias; with authorization disabled they worked, because the rejection
happened only on the authorization path.

Stream selectors now canonicalize through the §8.1 grammar and fold to their
expanded spelling, so `stream://acme/**` authorizes identically to
`stream://acme/*/*` as §11.2 requires. Two consequences for operators:

- Permissions written either way now grant the same concrete-route language. No
  permission rewrite is required, but a grant that was previously unreachable
  will now take effect. Review Stream grants containing `**` before upgrading.
- Noncanonical spellings such as `stream://acme/**/orders`, `stream://*/**`, and
  `stream://**/orders` are now rejected at ingress instead of passing the
  generic depth check and failing later in the domain. Requests using them
  change from a domain-level error to an authorization-level rejection.

Concrete Stream routes are unaffected: `BEGIN` still addresses
`{realm}/{area}/{resource}/{operation}` and still authorizes against its
resource identity.

## Removed Runtime Matcher And Routing Helpers

The following previously `#[deprecated]` `fitz::runtime` items have been
removed:

| Removed | Replacement |
| --- | --- |
| `runtime::matcher::extract_route_segments` | `runtime::matcher::extract_route_segments_borrowed` |
| `runtime::matcher::match_pattern_segments` | `Pattern::matches` / `Pattern::matches_str` |
| `runtime::matcher::match_pattern_segments_borrowed` | `Pattern::matches` / `Pattern::matches_str` |
| `runtime::routing::RouteFamily::from_u32` | `RouteFamily::new` (identical behavior) |
| `runtime::router::Router::resolve_domain_sink` | `Router::route_to_domain` |

The `match_pattern_segments` helpers compared path segments only and did not
enforce a pattern's scheme; `Pattern::matches` does. Callers that relied on the
segment-only comparison must add their own scheme check, or they will accept
routes from other domains.

`Router::route_to_domain` resolves the sink and delivers in one step, so the
route-miss path stays instrumented; resolving the sink separately bypassed the
mismatch counter.

`runtime::Scheduler::new` no longer takes a worker-thread count. Each spawned
actor already gets its own processing thread, so the argument had no effect;
call `Scheduler::new()`.

## Removed KV Authorization And Metrics Facades

The public `fitz::domains::kv::SessionActor` authorization helper has been
removed. Send KV frames through runtime ingress, which authorizes BEGIN against
the exact `kv://{realm}/{area}/{resource}` route and keeps subsequent
transaction operations session-owned. Direct state-machine tests may continue
to use `fitz::domains::kv::KvActor`, but application authorization must not be
reimplemented around it.

The public `fitz::domains::kv::KvMetrics` path has also been removed. Configure
KV metrics through `KvDomainSink::with_metrics` before registering the sink with
the router. The consuming configuration method rebuilds the sink's private
actor and returns the configured sink.

## Breaking: Single-Generation Storage Formats

**This upgrade cannot read any store written by an earlier broker.** Every
prior-generation storage reader has been removed across Queue, Schedule, and
Stream. Old rows are not migrated, and in the Schedule case they are not even
detected — they are silently ignored.

Queue enqueues now write the versioned split header plus a separate body row.
Previously the enqueue path wrote the older embedded-header encoding and rows
were only rewritten into the split layout when a message was redelivered or
dead-lettered.

See [../development/format-compatibility.md](../development/format-compatibility.md)
for the per-domain format detail.

**Required procedure:** drain every queue, stream, and schedule before
upgrading, then start the new broker on a fresh storage path. There is no
in-place upgrade and no rewrite tool. Rollback means restoring the pre-upgrade
store snapshot together with the old broker binary.

`FITZ_STREAM_STORAGE_LAYOUT` no longer accepts the `legacy`, `legacy-covering`,
or `covering` aliases; `promotion-frontier` is the only layout.

## New Domain Delivery-Drop Metrics

RPC previously discarded undeliverable client responses, including terminal
error responses, with no log or counter. Dashboards can now alert on:

- `fitz_rpc_response_drops_total` (new)
- `fitz_kv_notify_drops_total` (now exposed through `DomainStats`)

Existing drop-counter names are unchanged, including
`fitz_notice_delivery_drops_total`, which keeps its `delivery` spelling rather
than the `notify` spelling used by the other domains.

## Stream Rust API Cleanup

New construction code should call `StreamDomainSink::try_new` and handle
`StreamSinkInitError`. `StreamDomainSink::new` remains as a compatibility
wrapper and retains its historical panic-on-initialization behavior.

The client-facing `StreamWriteMode` now contains only `Buffered` and `Sync`.
Cloud provider acknowledgement remains a broker storage-policy choice for
`Sync`; callers must replace `StreamWriteMode::CloudStrict` with `Sync` and
configure cloud-strict write options when constructing the sink.

The unused `StreamEvent`, `parse_stream_route`, and public `StreamMetrics`
paths were removed. Use protocol `StreamMessage` values, the typed
three-segment Stream selector grammar, and `StreamDomainSink::with_metrics`,
respectively.

## Pre-Upgrade Checklist

1. Back up durability-sensitive state.
2. Validate rollback image is available.
3. Confirm client compatibility for target version.
4. Freeze nonessential schema or config changes.
5. Confirm the broker has `FITZ_ROUTE_FAMILY_MAP` entries for every identity value expected in incoming tokens.
6. Drain every queue, stream, and schedule, then provision a fresh storage path — persisted state from an earlier broker is unreadable.

## During Upgrade

1. Stop traffic to the single broker node and allow graceful shutdown to drain active sessions.
2. Replace the node and wait for readiness validation before restoring traffic.
3. Monitor auth errors, route mismatches, and tail latency.
4. Stop rollout on sustained error growth.

## Post-Upgrade

1. Execute smoke tests for each domain.
2. Verify metrics continuity.
3. Record upgrade notes and any required mitigations.
