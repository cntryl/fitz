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

export type LeaseSearchState = "owned" | "waiting" | "contention";

export interface LeaseSearchRequest {
  area?: string;
  limit?: number;
  owner?: string;
  realm?: string;
  resource?: string;
  routeFamily: number;
  state?: LeaseSearchState;
}
