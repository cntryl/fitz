export interface BrokerStatsSummary {
  connections: number;
  messagesPerSecond: number;
  realms: string[];
  sessions: number;
  uptimeSeconds: number;
}

export interface SystemDomainStatsSummary {
  kv: {
    keysTotal: number;
    operationsPerSecond: number;
    transactionsActive: number;
  };
  lease: {
    leasesActive: number;
    operationsPerSecond: number;
  };
  notice: {
    publishesPerSecond: number;
    subscriptionsActive: number;
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
    operationsPerSecond: number;
    requestsPending: number;
    workersRegistered: number;
  };
  schedule: {
    ackFailuresTotal: number;
    executionsPerMinute: number;
    notifyFailuresTotal: number;
    overdueNormalizationsTotal: number;
    pendingFireClaims: number;
    schedulesActive: number;
    subscriptionsActive: number;
  };
  stream: {
    eventsTotal: number;
    operationsPerSecond: number;
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
  domains: SystemDomainStatsSummary;
  healthStatus: string;
  metrics: MetricsPreview;
}
