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

export type KvKeyEncoding = "utf8" | "base64";

export interface KvByteValue {
  base64: string;
  lenBytes: number;
  utf8: string | null;
}

export interface KvCommittedValueResult {
  area: string;
  found: boolean;
  key: KvByteValue;
  realm: string;
  resource: string;
  routeFamily: number;
  value: KvByteValue | null;
}

export interface KvCommittedPair {
  key: KvByteValue;
  value: KvByteValue;
}

export interface KvPrefixScanResult {
  area: string;
  hasMore: boolean;
  items: KvCommittedPair[];
  limit: number;
  prefix: KvByteValue;
  realm: string;
  resource: string;
  routeFamily: number;
}

export interface KvCommittedResourceScope {
  area: string;
  realm: string;
  resource: string;
  routeFamily: number;
}
