import type { GlobalTroubleshootingDiagnostics } from "@/adapters";

export interface BrokerStatsSummary {
  connections: number;
  messagesPerSecond: number;
  realms: string[];
  sessions: number;
  uptimeSeconds: number;
}

export interface SystemDomainStatsSummary {
  kv: {
    commitsFailedTotal: number;
    invalidTransactionRejectsTotal: number;
    keysTotal: number;
    operationsPerSecond: number;
    rollbacksTotal: number;
    transactionsActive: number;
  };
  lease: {
    acquireTimeoutsTotal: number;
    failureTotal: number;
    forcedReleasesTotal: number;
    invalidTokenRejectsTotal: number;
    leasesActive: number;
    oldestLeaseAgeSeconds: number;
    operationsPerSecond: number;
    requestsTotal: number;
    successTotal: number;
    waiterDepth: number;
  };
  notice: {
    deliveryDropsTotal: number;
    failureTotal: number;
    publishesPerSecond: number;
    requestsTotal: number;
    successTotal: number;
    subscriptionsActive: number;
    unsubscribesTotal: number;
    wildcardLimitRejectsTotal: number;
  };
  queue: {
    inflightActive: number;
    messagesDeadLettered: number;
    messagesDelayed: number;
    messagesPending: number;
    messagesReady: number;
    operationsPerSecond: number;
  };
  rpc: {
    acksRejectedWrongWorkerTotal: number;
    backpressureRejectsTotal: number;
    duplicateCorrelationRejectsTotal: number;
    failureTotal: number;
    invalidSequenceErrorsDroppedTotal: number;
    invalidSequenceErrorsForwardedTotal: number;
    invalidSequenceResponsesTotal: number;
    operationsPerSecond: number;
    requestTimeoutsTotal: number;
    requestsPending: number;
    requestsTotal: number;
    responsesDroppedClosedCallerTotal: number;
    responsesMissingPendingTotal: number;
    successTotal: number;
    wrongWorkerRejectsTotal: number;
    workersRegistered: number;
  };
  schedule: {
    ackFailuresTotal: number;
    cancelPersistenceFailuresTotal: number;
    createPersistenceFailuresTotal: number;
    executionsPerMinute: number;
    notifyFailuresTotal: number;
    overdueNormalizationsTotal: number;
    pendingFireClaims: number;
    schedulesActive: number;
    subscriptionsActive: number;
    upsertPersistenceFailuresTotal: number;
  };
  stream: {
    appendConflictsTotal: number;
    failureTotal: number;
    eventsTotal: number;
    notifyDropsTotal: number;
    operationsPerSecond: number;
    requestsTotal: number;
    successTotal: number;
    streamsActive: number;
    subscriptionsActive: number;
  };
}

export interface MetricsPreview {
  raw: string;
  lines: string[];
  lineCount: number;
}

export interface SystemOverview {
  broker: BrokerStatsSummary;
  diagnostics: GlobalTroubleshootingDiagnostics;
  domains: SystemDomainStatsSummary;
  metrics: MetricsPreview;
}
