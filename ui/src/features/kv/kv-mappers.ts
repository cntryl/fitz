import type { KvStats, RealmEntry } from "@/adapters";
import type { KvOverview, KvRealmSummary, KvStatsSummary } from "./kv-models";

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
