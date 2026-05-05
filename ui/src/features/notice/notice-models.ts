export interface NoticeRealmSummary {
  realm: string;
}

export interface NoticeStatsSummary {
  publishesPerSecond: number;
  subscriptionsActive: number;
}

export interface NoticeOverview {
  realms: NoticeRealmSummary[];
  stats: NoticeStatsSummary;
}
