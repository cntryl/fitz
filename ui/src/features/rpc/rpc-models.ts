export interface RpcRealmSummary {
  realm: string;
}

export interface RpcStatsSummary {
  operationsPerSecond: number;
  requestsPending: number;
  workersRegistered: number;
}

export interface RpcOverview {
  realms: RpcRealmSummary[];
  stats: RpcStatsSummary;
}
