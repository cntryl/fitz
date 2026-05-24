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
