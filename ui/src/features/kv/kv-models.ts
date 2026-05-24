export interface KvRealmSummary {
  realm: string;
}

export interface KvStatsSummary {
  commitsFailedTotal: number;
  invalidTransactionRejectsTotal: number;
  keysTotal: number;
  operationsPerSecond: number;
  rollbacksTotal: number;
  transactionsActive: number;
}

export interface KvOverview {
  realms: KvRealmSummary[];
  stats: KvStatsSummary;
}
