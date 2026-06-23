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

export interface NoticeDeliverySearchRequest {
  area?: string;
  limit?: number;
  query?: string;
  realm?: string;
  resource?: string;
  routeFamily: number;
}
