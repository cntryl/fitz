import type { LeaseStats, RealmEntry } from "@/adapters";
import type { LeaseOverview, LeaseRealmSummary, LeaseStatsSummary } from "./lease-models";

export function mapLeaseRealm(dto: RealmEntry): LeaseRealmSummary {
  return {
    realm: dto.realm,
  };
}

export function mapLeaseStats(dto: LeaseStats): LeaseStatsSummary {
  return {
    acquireTimeoutsTotal: dto.acquire_timeouts_total,
    forcedReleasesTotal: dto.forced_releases_total,
    invalidTokenRejectsTotal: dto.invalid_token_rejects_total,
    leasesActive: dto.leases_active,
    operationsPerSecond: dto.operations_per_second,
    oldestLeaseAgeSeconds: dto.oldest_lease_age_seconds,
    waiterDepth: dto.waiter_depth,
  };
}

export function mapLeaseOverview(realms: RealmEntry[], stats: LeaseStats): LeaseOverview {
  return {
    realms: realms.map(mapLeaseRealm),
    stats: mapLeaseStats(stats),
  };
}
