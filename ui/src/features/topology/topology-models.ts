import type { DiagnosticSnapshot, GlobalTroubleshootingDiagnostics } from "@/adapters";
import type { ActiveSession } from "@/features/session/session-models";

export type TopologyDomain = "queue" | "rpc" | "notice" | "schedule" | "stream" | "lease" | "kv";
export type TopologyState = "quiet" | "flowing" | "pressure" | "blocked";
export type TrendDirection = "falling" | "rising" | "stable";

export interface TopologyBroker {
  connections: number;
  messagesPerSecond: number;
  realms: string[];
  routerBackpressureTotal: number;
  routerHighLaneBackpressureTotal: number;
  sessions: number;
  uptimeSeconds: number;
}

export interface TopologyCounter {
  key: string;
  label: string;
  value: number;
}

export interface TopologyScope {
  area?: string;
  operation?: string;
  pattern?: string;
  realm?: string;
  resource?: string;
  route?: string;
  routeFamily?: number;
  sessionId?: string;
}

export interface TopologySessionGroup {
  maxIdleSeconds: number;
  messagesReceived: number;
  messagesSent: number;
  representativeSessions: ActiveSession[];
  routeFamily: number;
  sessions: number;
  transports: string[];
}

export interface TopologyScopedResource {
  counters: TopologyCounter[];
  domain: TopologyDomain;
  href: string;
  id: string;
  label: string;
  scope: TopologyScope;
  state: TopologyState;
}

export interface TopologyLane {
  activityPerSecond: number;
  consumers: number;
  counters: TopologyCounter[];
  diagnostics: DiagnosticSnapshot;
  href: string;
  id: TopologyDomain;
  observers: number;
  resources: TopologyScopedResource[];
  state: TopologyState;
  title: string;
}

export interface TopologyConnection {
  counters: TopologyCounter[];
  domain: TopologyDomain;
  href: string;
  id: string;
  kind: string;
  label: string;
  scope: TopologyScope;
  source: string;
  state: TopologyState;
  target: string;
}

export interface TopologyConnectionPage {
  items: TopologyConnection[];
  limit: number;
  total: number;
  truncated: boolean;
}

export interface MessagingTopologyOverview {
  broker: TopologyBroker;
  connections: TopologyConnectionPage;
  diagnostics: GlobalTroubleshootingDiagnostics;
  fetchedAt: string;
  generatedAt: string;
  lanes: TopologyLane[];
  sessionGroups: TopologySessionGroup[];
}

export interface TopologyTrendPoint {
  generatedAt: string;
  lanePressure: Record<TopologyDomain, number>;
  messagesPerSecond: number;
  sessions: number;
}

export type TopologySelection =
  | {
      counters: TopologyCounter[];
      description: string;
      href?: string;
      id: string;
      kind: "broker";
      state: TopologyState;
      title: string;
    }
  | {
      counters: TopologyCounter[];
      description: string;
      id: string;
      kind: "session_group";
      state: TopologyState;
      title: string;
    }
  | {
      counters: TopologyCounter[];
      description: string;
      href: string;
      id: string;
      kind: "lane";
      scope?: TopologyScope;
      state: TopologyState;
      title: string;
    }
  | {
      counters: TopologyCounter[];
      description: string;
      href: string;
      id: string;
      kind: "resource";
      scope: TopologyScope;
      state: TopologyState;
      title: string;
    }
  | {
      counters: TopologyCounter[];
      description: string;
      href: string;
      id: string;
      kind: "connection";
      scope: TopologyScope;
      state: TopologyState;
      title: string;
    };
