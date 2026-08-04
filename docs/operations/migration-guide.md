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
promotion-frontier generation marker changes from `0xD1` to `0xD2`; startup
rejects `0xD1` with reset-required guidance before decoding or scanning any V1
pages. Compact area/realm pages now require persisted route identity and the old
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

### Subscription registration contract

KV, Queue, Notice, Stream, RPC, and Schedule now share strict whole-segment
`*`/`**` registration validation and a 128-wildcard-registration session limit.
Exact registrations do not count, and duplicates remain idempotent. Clients
must surface the domain-specific validation and limit codes: KV 1012/1013,
Stream 2010/2011, Notice 3002/3003, Queue 4010/4011, RPC 6012/6013, and Schedule
7006/7007.

Lease watches are exact-only. Rename Lease client request fields from `pattern`
to `route` and reject non-concrete `lease://realm/area/resource` input locally
when convenient; the broker returns 5010 authoritatively. The encoded string
payload is unchanged. Queue availability notifications now carry the concrete
three-segment Queue resource route rather than a synthetic `/ready` suffix.
Update Queue notification routing before upgrading the broker.

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

## Pre-Upgrade Checklist

1. Back up durability-sensitive state.
2. Validate rollback image is available.
3. Confirm client compatibility for target version.
4. Freeze nonessential schema or config changes.
5. Confirm the broker has `FITZ_ROUTE_FAMILY_MAP` entries for every identity value expected in incoming tokens.

## During Upgrade

1. Stop traffic to the single broker node and allow graceful shutdown to drain active sessions.
2. Replace the node and wait for readiness validation before restoring traffic.
3. Monitor auth errors, route mismatches, and tail latency.
4. Stop rollout on sustained error growth.

## Post-Upgrade

1. Execute smoke tests for each domain.
2. Verify metrics continuity.
3. Record upgrade notes and any required mitigations.
