export interface LeaseRealmSummary {
  realm: string;
}

export interface LeaseStatsSummary {
  leasesActive: number;
  operationsPerSecond: number;
}

export interface LeaseOverview {
  realms: LeaseRealmSummary[];
  stats: LeaseStatsSummary;
}
