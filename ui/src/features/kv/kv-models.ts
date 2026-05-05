export interface KvRealmSummary {
  realm: string;
}

export interface KvStatsSummary {
  keysTotal: number;
  operationsPerSecond: number;
  transactionsActive: number;
}

export interface KvOverview {
  realms: KvRealmSummary[];
  stats: KvStatsSummary;
}
