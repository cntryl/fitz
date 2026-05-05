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
