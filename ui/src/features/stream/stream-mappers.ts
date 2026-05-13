import type { RealmEntry, StreamStats } from "@/adapters";
import type { StreamOverview, StreamRealmSummary, StreamStatsSummary } from "./stream-models";

export function mapStreamRealm(dto: RealmEntry): StreamRealmSummary {
  return {
    realm: dto.realm,
  };
}

export function mapStreamStats(dto: StreamStats): StreamStatsSummary {
  return {
    eventsTotal: dto.events_total,
    operationsPerSecond: dto.operations_per_second,
    watermarkLagBuckets: {
      caughtUp: dto.watermark_lag_buckets.caught_up,
      over100: dto.watermark_lag_buckets.over_100,
      under10: dto.watermark_lag_buckets.under_10,
      under100: dto.watermark_lag_buckets.under_100,
    },
    streamsActive: dto.streams_active,
    subscriptionsActive: dto.subscriptions_active,
  };
}

export function mapStreamOverview(realms: RealmEntry[], stats: StreamStats): StreamOverview {
  return {
    realms: realms.map(mapStreamRealm),
    stats: mapStreamStats(stats),
  };
}
