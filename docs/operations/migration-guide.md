# Migration Guide

This guide covers safe upgrades between Fitz releases.

## Upgrade Strategy

1. Read [development/format-compatibility.md](../development/format-compatibility.md).
2. Review [development/release-policy.md](../development/release-policy.md).
3. Test upgrade path in staging with production-like data.
4. Plan a bounded maintenance replacement for each Fitz node. Fitz does not coordinate rolling state transfer between nodes.

## Route Family Identity Map Migration

The hardened broker resolves route family server-side from verified identity
context. Configure `FITZ_ROUTE_FAMILIES=1,2,...` with the contiguous,
provisioned families that the node may serve, then configure
`FITZ_ROUTE_FAMILY_MAP=identity=family,...` for the identity values accepted by
the node. The default identity claim is `tid`; set
`FITZ_ROUTE_FAMILY_CLAIM=org_id` for Auth0 Organizations.

Update token issuers to emit identity context and route-shaped permissions, but
do not emit `fitz.route_family` by default. A hardened node rejects authenticated
`CONNECT` when the configured identity claim is missing, unmapped, or maps to an
unprovisioned family. Anonymous mode always uses route family `1`.

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
