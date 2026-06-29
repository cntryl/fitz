import type {
  LeaseSearchItem,
  LeaseSearchResponse,
  LeaseStats,
  RealmEntry,
  ResourceEntry,
} from "@/adapters";
import type {
  LeaseAreaResourceRows,
  LeaseAreaSummary,
  LeaseOverview,
  LeaseRealmInventory,
  LeaseRemainingLifetime,
  LeaseSearchState,
  LeaseStatsSummary,
  LeaseOwnershipSearchResult,
  LeaseOwnershipSearchRow,
} from "./lease-models";

const MS_PER_SECOND = 1000;

function parseExpirySeconds(expiresAt: string | null) {
  if (!expiresAt) {
    return null;
  }

  const parsed = Date.parse(expiresAt);
  return Number.isNaN(parsed) ? null : parsed;
}

export function deriveLeaseRemainingLifetime(expiresAt: string | null, now = Date.now()) {
  const expires = parseExpirySeconds(expiresAt);

  if (expires === null) {
    return {
      label: "--",
      remainingSeconds: null,
      status: "missing",
    } as LeaseRemainingLifetime;
  }

  const remainingSeconds = Math.floor((expires - now) / MS_PER_SECOND);

  if (remainingSeconds <= 0) {
    return {
      label: "expired",
      remainingSeconds: 0,
      status: "expired",
    } as LeaseRemainingLifetime;
  }

  return {
    label: `${remainingSeconds}s`,
    remainingSeconds,
    status: "active",
  } as LeaseRemainingLifetime;
}

function mapLeaseSearchAgeSeconds(acquiredAt: string | null, now = Date.now()) {
  if (!acquiredAt) {
    return null;
  }

  const acquired = Date.parse(acquiredAt);
  if (Number.isNaN(acquired)) {
    return null;
  }

  return Math.floor((now - acquired) / MS_PER_SECOND);
}

export function mapLeaseRealmSummary(dto: RealmEntry): LeaseOverview["realms"][number] {
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
    realms: realms.map(mapLeaseRealmSummary),
    stats: mapLeaseStats(stats),
  };
}

export function mapLeaseAreaSummary(
  realm: string,
  area: string,
  resources: ResourceEntry[],
): LeaseAreaSummary {
  return {
    area,
    realm,
    resources: resources.map((resource) => resource.resource),
  };
}

export function mapLeaseRealmInventory(
  realm: string,
  areas: LeaseAreaSummary[],
): LeaseRealmInventory {
  return {
    realm,
    areas,
  };
}

export function mapLeaseAreaResourceRows(
  realm: string,
  area: string,
  resources: ResourceEntry[],
): LeaseAreaResourceRows {
  return {
    area,
    realm,
    resources: resources.map((resource) => resource.resource),
  };
}

export function mapLeaseOwnershipSearchRow(
  dto: LeaseSearchItem,
  now = Date.now(),
): LeaseOwnershipSearchRow {
  return {
    acquiredAt: dto.acquired_at,
    ageSeconds: mapLeaseSearchAgeSeconds(dto.acquired_at, now),
    area: dto.area,
    expiresAt: dto.expires_at,
    ownerId: dto.owner_id,
    ownerSessionId: dto.owner_session_id,
    pendingWaiters: dto.pending_waiters,
    queuedToken: dto.queued_token,
    realm: dto.realm,
    resource: dto.resource,
    routeFamily: dto.route_family,
    state: dto.state as LeaseSearchState,
  };
}

export function mapLeaseOwnershipSearchResult(
  dto: LeaseSearchResponse,
  now = Date.now(),
): LeaseOwnershipSearchResult {
  return {
    items: dto.items.map((item) => mapLeaseOwnershipSearchRow(item, now)),
    limit: dto.limit,
    routeFamily: dto.route_family,
  };
}
