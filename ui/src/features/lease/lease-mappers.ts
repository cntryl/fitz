import type { LeaseStats, RealmEntry } from "@/adapters";
import type { LeaseOverview, LeaseRealmSummary, LeaseStatsSummary } from "./lease-models";

export function mapLeaseRealm(dto: RealmEntry): LeaseRealmSummary {
  return {
    realm: dto.realm,
  };
}

export function mapLeaseStats(dto: LeaseStats): LeaseStatsSummary {
  return {
    leasesActive: dto.leases_active,
    operationsPerSecond: dto.operations_per_second,
  };
}

export function mapLeaseOverview(realms: RealmEntry[], stats: LeaseStats): LeaseOverview {
  return {
    realms: realms.map(mapLeaseRealm),
    stats: mapLeaseStats(stats),
  };
}
