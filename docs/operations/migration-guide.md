# Migration Guide

This guide covers safe upgrades between Fitz releases.

## Upgrade Strategy

1. Read [development/format-compatibility.md](../development/format-compatibility.md).
2. Review [development/release-policy.md](../development/release-policy.md).
3. Test upgrade path in staging with production-like data.
4. Plan a bounded maintenance replacement for each Fitz node. Fitz does not coordinate rolling state transfer between nodes.

## Route Family JWT Migration

The hardened broker requires every authenticated JWT to include `fitz.route_family`
as a non-zero integer. Configure `FITZ_ROUTE_FAMILIES=1,2,...` with the contiguous,
provisioned families that the node may serve. The default allowlist is `1`.

Update token issuers before deploying the hardened broker. A hardened node rejects
authenticated `CONNECT` when `fitz.route_family` is missing, zero, or absent from
its provisioned allowlist. Anonymous mode always uses route family `1`.

## Pre-Upgrade Checklist

1. Back up durability-sensitive state.
2. Validate rollback image is available.
3. Confirm client compatibility for target version.
4. Freeze nonessential schema or config changes.
5. Confirm token issuers emit provisioned `fitz.route_family` claims before replacing the broker.

## During Upgrade

1. Stop traffic to the single broker node and allow graceful shutdown to drain active sessions.
2. Replace the node and wait for readiness validation before restoring traffic.
3. Monitor auth errors, route mismatches, and tail latency.
4. Stop rollout on sustained error growth.

## Post-Upgrade

1. Execute smoke tests for each domain.
2. Verify metrics continuity.
3. Record upgrade notes and any required mitigations.
