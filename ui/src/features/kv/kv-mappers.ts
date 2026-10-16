import type {
  KvByteValue as KvByteValueDto,
  KvCommittedPair as KvCommittedPairDto,
  KvCommittedValueResponse,
  KvPrefixScanResponse,
  KvRowsResponse,
  KvStats,
  KvTransaction as KvTransactionDto,
  KvTransactionsList,
  KvResourceDetail as KvResourceDetailDto,
  RealmEntry,
} from "@/adapters";
import type {
  KvByteValue,
  KvCommittedPair,
  KvCommittedValueResult,
  KvOverview,
  KvPrefixScanResult,
  KvRealmSummary,
  KvRowsResult,
  KvStatsSummary,
  KvTransaction,
  KvResourceDetail,
} from "./kv-models";

export function mapKvRealm(dto: RealmEntry): KvRealmSummary {
  return {
    realm: dto.realm,
  };
}

export function mapKvStats(dto: KvStats): KvStatsSummary {
  return {
    commitsFailedTotal: dto.commits_failed_total,
    invalidTransactionRejectsTotal: dto.invalid_transaction_rejects_total,
    keysTotal: dto.keys_total,
    operationsPerSecond: dto.operations_per_second,
    transactionsActive: dto.transactions_active,
  };
}

export function mapKvOverview(realms: RealmEntry[], stats: KvStats): KvOverview {
  return {
    realms: realms.map(mapKvRealm),
    stats: mapKvStats(stats),
  };
}

export function mapKvResourceDetail(dto: KvResourceDetailDto): KvResourceDetail {
  return {
    estimatedRecordCount: dto.estimated_record_count,
    estimatedStorageBytes: dto.estimated_storage_bytes,
    estimateComplete: dto.estimate_complete,
    measurementsAvailable: dto.route_family !== undefined && dto.route_family !== null,
    readLatencyAvgMs: dto.read_latency_avg_ms,
    readLatencyP95Ms: dto.read_latency_p95_ms,
    transactionsActive: dto.transactions_active,
    writeLatencyAvgMs: dto.write_latency_avg_ms,
    writeLatencyP95Ms: dto.write_latency_p95_ms,
  };
}

function mapKvTransaction(dto: KvTransactionDto): KvTransaction {
  return {
    idleSeconds: dto.idle_seconds,
    mode: dto.mode,
    operationsCount: dto.operations_count,
    startedAt: dto.started_at,
    txId: dto.tx_id,
  };
}

export function mapKvTransactions(dto: KvTransactionsList): KvTransaction[] {
  return dto.transactions.map(mapKvTransaction);
}

export function mapKvByteValue(dto: KvByteValueDto): KvByteValue {
  return {
    base64: dto.base64,
    lenBytes: dto.len_bytes,
    utf8: dto.utf8,
  };
}

export function mapKvCommittedPair(dto: KvCommittedPairDto): KvCommittedPair {
  return {
    key: mapKvByteValue(dto.key),
    value: mapKvByteValue(dto.value),
  };
}

export function mapKvCommittedValue(dto: KvCommittedValueResponse): KvCommittedValueResult {
  return {
    area: dto.area,
    found: dto.found,
    key: mapKvByteValue(dto.key),
    realm: dto.realm,
    resource: dto.resource,
    routeFamily: dto.route_family,
    value: dto.value ? mapKvByteValue(dto.value) : null,
  };
}

export function mapKvPrefixScan(dto: KvPrefixScanResponse): KvPrefixScanResult {
  return {
    area: dto.area,
    hasMore: dto.has_more,
    items: dto.items.map(mapKvCommittedPair),
    limit: dto.limit,
    prefix: mapKvByteValue(dto.prefix),
    realm: dto.realm,
    resource: dto.resource,
    routeFamily: dto.route_family,
  };
}

export function mapKvRows(dto: KvRowsResponse): KvRowsResult {
  return {
    area: dto.area,
    hasMore: dto.has_more,
    items: dto.items.map(mapKvCommittedPair),
    limit: dto.limit,
    nextCursor: dto.next_cursor ?? null,
    realm: dto.realm,
    resource: dto.resource,
    routeFamily: dto.route_family,
    startsWith: mapKvByteValue(dto.starts_with),
  };
}
