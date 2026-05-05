import type { NoticeStats, RealmEntry } from "@/adapters";
import type { NoticeOverview, NoticeRealmSummary, NoticeStatsSummary } from "./notice-models";

export function mapNoticeRealm(dto: RealmEntry): NoticeRealmSummary {
  return {
    realm: dto.realm,
  };
}

export function mapNoticeStats(dto: NoticeStats): NoticeStatsSummary {
  return {
    publishesPerSecond: dto.publishes_per_second,
    subscriptionsActive: dto.subscriptions_active,
  };
}

export function mapNoticeOverview(realms: RealmEntry[], stats: NoticeStats): NoticeOverview {
  return {
    realms: realms.map(mapNoticeRealm),
    stats: mapNoticeStats(stats),
  };
}
