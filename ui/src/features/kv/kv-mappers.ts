import type {
  KvByteValue as KvByteValueDto,
  KvCommittedPair as KvCommittedPairDto,
  KvCommittedValueResponse,
  KvPrefixScanResponse,
  KvStats,
  RealmEntry,
} from "@/adapters";
import type {
  KvByteValue,
  KvCommittedPair,
  KvCommittedValueResult,
  KvOverview,
  KvPrefixScanResult,
  KvRealmSummary,
  KvStatsSummary,
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
    rollbacksTotal: dto.rollbacks_total,
    transactionsActive: dto.transactions_active,
  };
}

export function mapKvOverview(realms: RealmEntry[], stats: KvStats): KvOverview {
  return {
    realms: realms.map(mapKvRealm),
    stats: mapKvStats(stats),
  };
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

export function mapKvCommittedValue(
  dto: KvCommittedValueResponse,
): KvCommittedValueResult {
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
