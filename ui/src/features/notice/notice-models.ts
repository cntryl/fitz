export interface NoticeRealmSummary {
  realm: string;
}

export interface NoticeStatsSummary {
  publishesPerSecond: number;
  deliveryDropsTotal: number;
  routesActive: number;
  wildcardLimitRejectsTotal: number;
  subscriptionsActive: number;
  maxRouteSubscribers: number;
}

export interface NoticeOverview {
  realms: NoticeRealmSummary[];
  stats: NoticeStatsSummary;
}
