export interface RpcRealmSummary {
  realm: string;
}

export interface RpcStatsSummary {
  invalidSequenceErrorsDroppedTotal: number;
  invalidSequenceErrorsForwardedTotal: number;
  invalidSequenceResponsesTotal: number;
  operationsPerSecond: number;
  requestsPending: number;
  responsesDroppedClosedCallerTotal: number;
  responsesMissingPendingTotal: number;
  workersRegistered: number;
}

export interface RpcOverview {
  realms: RpcRealmSummary[];
  stats: RpcStatsSummary;
}
