export interface LeaseRealmSummary {
  realm: string;
}

export interface LeaseStatsSummary {
  acquireTimeoutsTotal: number;
  forcedReleasesTotal: number;
  invalidTokenRejectsTotal: number;
  leasesActive: number;
  operationsPerSecond: number;
  oldestLeaseAgeSeconds: number;
  waiterDepth: number;
}

export interface LeaseOverview {
  realms: LeaseRealmSummary[];
  stats: LeaseStatsSummary;
}

export interface LeaseAreaSummary {
  area: string;
  realm: string;
  resources: string[];
}

export interface LeaseRealmInventory {
  realm: string;
  areas: LeaseAreaSummary[];
}

export interface LeaseAreaResourceRows {
  area: string;
  realm: string;
  resources: string[];
}

export type LeaseSearchState = "owned" | "waiting" | "contention";

export interface LeaseSearchRequest {
  area?: string;
  limit?: number;
  owner?: string;
  realm?: string;
  resource?: string;
  state?: LeaseSearchState;
  routeFamily?: number | string;
}

export type LeaseOwnershipSearchRequest = LeaseSearchRequest;

export interface LeaseOwnershipSearchRow {
  acquiredAt: string | null;
  ageSeconds: number | null;
  area: string;
  expiresAt: string | null;
  ownerId: string | null;
  ownerSessionId: string | null;
  pendingWaiters: number;
  queuedToken: number | null;
  realm: string;
  resource: string;
  routeFamily: number;
  state: LeaseSearchState;
}

export interface LeaseRemainingLifetime {
  label: string;
  remainingSeconds: number | null;
  status: "active" | "expired" | "missing";
}

export interface LeaseOwnershipSearchResult {
  items: LeaseOwnershipSearchRow[];
  limit: number;
  routeFamily: number;
}

export interface LeaseOwnershipSearchScope {
  area: string;
  realm: string;
  resource: string;
}
