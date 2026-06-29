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

export interface NoticeRealmAreaSummary {
  area: string;
  realm: string;
  resources: string[];
}

export interface NoticeRealmInventory {
  realm: string;
  areas: NoticeRealmAreaSummary[];
}

export interface NoticeAreaResourceRows {
  area: string;
  realm: string;
  resources: string[];
}

export interface NoticeDeliveryRow {
  area: string;
  notificationsReceived: number;
  publishesPerMinute: number;
  publishesTotal: number;
  realm: string;
  resource: string;
  route: string;
  sessionId: string | null;
  status: string;
  subscriptionId: number | null;
}

export interface NoticeDeliveryRows {
  area: string;
  limit: number;
  observations: NoticeDeliveryRow[];
  realm: string;
  routeFamily: number;
}

export interface NoticeResourceOperationRow {
  operation: string;
  activeSubscribers: number;
  rollingMessageCount: number;
  latencyMs: number | null;
}

export interface NoticeResourceOperationRows {
  area: string;
  limit: number;
  operations: NoticeResourceOperationRow[];
  realm: string;
  resource: string;
  routeFamily: number;
}

export interface NoticeDeliverySearchRequest {
  area?: string;
  limit?: number;
  query?: string;
  realm?: string;
  resource?: string;
  operation?: string;
  routeFamily?: number | string;
}
