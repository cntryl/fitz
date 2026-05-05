export interface StreamRealmSummary {
  realm: string;
}

export interface StreamStatsSummary {
  eventsTotal: number;
  operationsPerSecond: number;
  streamsActive: number;
  subscriptionsActive: number;
}

export interface StreamOverview {
  realms: StreamRealmSummary[];
  stats: StreamStatsSummary;
}
