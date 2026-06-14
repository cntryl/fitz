import type {
  MessagingTopology,
  SessionInfo,
  TopologyConnection as TopologyConnectionDto,
  TopologyLane as TopologyLaneDto,
  TopologyScope as TopologyScopeDto,
} from "@/adapters";
import {
  type MessagingTopologyOverview,
  type TopologyConnection,
  type TopologyCounter,
  type TopologyDomain,
  type TopologyLane,
  type TopologyScope,
  type TopologyScopedResource,
  type TopologySelection,
  type TopologyState,
  type TopologyTrendPoint,
  type TrendDirection,
} from "./topology-models";

const LANE_ORDER: TopologyDomain[] = [
  "queue",
  "rpc",
  "notice",
  "schedule",
  "stream",
  "lease",
  "kv",
];
const TREND_HISTORY_LIMIT = 12;

const selectionPrefix = {
  connection: "connection:",
  lane: "lane:",
  resource: "resource:",
  sessionGroup: "session-family:",
} as const;

export const topologyDomainDescriptions: Record<TopologyDomain, string> = {
  kv: "Current authoritative state",
  lease: "Ephemeral ownership coordination",
  notice: "Live ephemeral fanout",
  queue: "Durable work delivery",
  rpc: "Live request/response",
  schedule: "Durable timing intent",
  stream: "Durable history and replay",
};

const domainHref: Record<TopologyDomain, string> = {
  kv: "/kv",
  lease: "/lease",
  notice: "/notice",
  queue: "/queue",
  rpc: "/rpc",
  schedule: "/schedule",
  stream: "/stream",
};

const stateRank: Record<TopologyState, number> = {
  quiet: 0,
  flowing: 1,
  pressure: 2,
  blocked: 3,
};

export function isTopologyDomain(value: string | undefined): value is TopologyDomain {
  return LANE_ORDER.some((domain) => domain === value);
}

function mapCounter(counter: { key: string; label: string; value: number }): TopologyCounter {
  return {
    key: counter.key,
    label: counter.label,
    value: counter.value,
  };
}

function mapScope(scope: TopologyScopeDto): TopologyScope {
  return {
    area: scope.area,
    operation: scope.operation,
    pattern: scope.pattern,
    realm: scope.realm,
    resource: scope.resource,
    route: scope.route,
    routeFamily: scope.route_family,
    sessionId: scope.session_id,
  };
}

function mapSession(session: SessionInfo) {
  return {
    connectedAt: session.connected_at,
    idleSeconds: session.idle_seconds,
    identityClaim: session.identity_claim,
    identityValue: session.identity_value,
    key: session.session_id ?? `${session.route_family ?? "unknown"}:${session.connected_at ?? ""}`,
    messagesReceived: session.messages_received,
    messagesSent: session.messages_sent,
    remoteAddress: session.remote_addr,
    routeFamily: session.route_family,
    sessionId: session.session_id,
    subject: session.subject,
    transport: session.transport,
  };
}

export function topologyLaneSelectionId(laneId: TopologyDomain) {
  return `${selectionPrefix.lane}${laneId}`;
}

export function topologyLaneIdFromSelectionId(selectionId: string) {
  if (!selectionId.startsWith(selectionPrefix.lane)) {
    return null;
  }

  const laneId = selectionId.slice(selectionPrefix.lane.length);
  return isTopologyDomain(laneId) ? laneId : null;
}

export function topologyResourceSelectionId(resourceId: string) {
  return `${selectionPrefix.resource}${resourceId}`;
}

export function topologyConnectionSelectionId(connectionId: string) {
  return `${selectionPrefix.connection}${connectionId}`;
}

export function topologySessionGroupSelectionId(routeFamily: number) {
  return `${selectionPrefix.sessionGroup}${routeFamily}`;
}

function domainForConnection(connection: TopologyConnectionDto): TopologyDomain {
  if (connection.kind.startsWith("queue_")) return "queue";
  if (connection.kind.startsWith("rpc_")) return "rpc";
  if (connection.kind.startsWith("notice_")) return "notice";
  if (connection.kind.startsWith("schedule_")) return "schedule";
  if (connection.kind.startsWith("stream_")) return "stream";
  if (connection.kind.startsWith("lease_")) return "lease";
  if (connection.kind.startsWith("kv_")) return "kv";

  const brokerFlowTarget = connection.target.replace("domain:", "");
  return isTopologyDomain(brokerFlowTarget) ? brokerFlowTarget : "queue";
}

export function topologyScopeHref(domain: TopologyDomain, scope: TopologyScope): string {
  const base = domainHref[domain];

  if (!scope.realm || !scope.area || !scope.resource) {
    return base;
  }

  return `${base}/${encodeURIComponent(scope.realm)}/${encodeURIComponent(
    scope.area,
  )}/${encodeURIComponent(scope.resource)}`;
}

function mapResource(
  domain: TopologyDomain,
  resource: TopologyLaneDto["top_scoped_resources"][number],
): TopologyScopedResource {
  const scope = mapScope(resource.scope);

  return {
    counters: resource.counters.map(mapCounter),
    domain,
    href: topologyScopeHref(domain, scope),
    id: resource.id,
    label: resource.label,
    scope,
    state: resource.state,
  };
}

function mapLane(lane: TopologyLaneDto): TopologyLane {
  const domain: TopologyDomain = lane.id;

  return {
    activityPerSecond: lane.activity_per_second,
    consumers: lane.consumers,
    counters: lane.counters.map(mapCounter),
    diagnostics: lane.diagnostics,
    href: domainHref[domain],
    id: domain,
    observers: lane.observers,
    resources: lane.top_scoped_resources.map((resource) => mapResource(domain, resource)),
    state: lane.state,
    title: lane.title,
  };
}

function mapConnection(connection: TopologyConnectionDto): TopologyConnection {
  const domain = domainForConnection(connection);
  const scope = mapScope(connection.scope);

  return {
    counters: connection.metrics.map(mapCounter),
    domain,
    href: topologyScopeHref(domain, scope),
    id: connection.id,
    kind: connection.kind,
    label: connection.label,
    scope,
    source: connection.source,
    state: connection.state,
    target: connection.target,
  };
}

export function mapMessagingTopology(dto: MessagingTopology): MessagingTopologyOverview {
  return {
    broker: {
      connections: dto.broker.connections,
      messagesPerSecond: dto.broker.messages_per_second,
      realms: dto.broker.realms,
      routerBackpressureTotal: dto.broker.router_backpressure_total,
      routerHighLaneBackpressureTotal: dto.broker.router_high_lane_backpressure_total,
      sessions: dto.broker.sessions,
      uptimeSeconds: dto.broker.uptime_seconds,
    },
    connections: {
      items: dto.connections.items.map(mapConnection),
      limit: dto.connections.limit,
      total: dto.connections.total,
      truncated: dto.connections.truncated,
    },
    diagnostics: dto.diagnostics,
    fetchedAt: new Date().toISOString(),
    generatedAt: dto.generated_at,
    lanes: dto.lanes.map(mapLane),
    sessionGroups: dto.session_groups.map((group) => ({
      maxIdleSeconds: group.max_idle_seconds,
      messagesReceived: group.messages_received,
      messagesSent: group.messages_sent,
      representativeSessions: group.representative_sessions.map(mapSession),
      routeFamily: group.route_family,
      sessions: group.sessions,
      transports: group.transports,
    })),
  };
}

function lanePressureScore(lane: TopologyLane) {
  return (
    stateRank[lane.state] * 1_000 + lane.counters.reduce((sum, counter) => sum + counter.value, 0)
  );
}

function counterScore(counters: TopologyCounter[]) {
  return counters.reduce((sum, counter) => sum + counter.value, 0);
}

function compareTopologyPressure(
  left: { counters: TopologyCounter[]; label?: string; state: TopologyState },
  right: { counters: TopologyCounter[]; label?: string; state: TopologyState },
) {
  const rankDelta = stateRank[right.state] - stateRank[left.state];
  if (rankDelta !== 0) return rankDelta;

  const scoreDelta = counterScore(right.counters) - counterScore(left.counters);
  if (scoreDelta !== 0) return scoreDelta;

  return (left.label ?? "").localeCompare(right.label ?? "");
}

export function topTopologyResources(topology: MessagingTopologyOverview, limit = 6) {
  return topology.lanes
    .flatMap((lane) => lane.resources)
    .sort(compareTopologyPressure)
    .slice(0, limit);
}

export function topologyConnectionKindLabel(kind: string) {
  return kind.split("_").join(" ");
}

export function topologyTrendPoint(topology: MessagingTopologyOverview): TopologyTrendPoint {
  return {
    generatedAt: topology.generatedAt,
    lanePressure: Object.fromEntries(
      topology.lanes.map((lane) => [lane.id, lanePressureScore(lane)]),
    ) as Record<TopologyDomain, number>,
    messagesPerSecond: topology.broker.messagesPerSecond,
    sessions: topology.broker.sessions,
  };
}

export function appendTopologyTrendPoint(
  history: TopologyTrendPoint[],
  topology: MessagingTopologyOverview,
): TopologyTrendPoint[] {
  const point = topologyTrendPoint(topology);
  const last = history[history.length - 1];

  if (last?.generatedAt === point.generatedAt) {
    return history;
  }

  return [...history, point].slice(-TREND_HISTORY_LIMIT);
}

function trendFromValues(previous: number, current: number): TrendDirection {
  const delta = current - previous;

  if (Math.abs(delta) < 0.01) return "stable";
  return delta > 0 ? "rising" : "falling";
}

export function topologyTrendDirection(
  history: TopologyTrendPoint[],
  metric: "messagesPerSecond" | "sessions",
): TrendDirection {
  if (history.length < 2) return "stable";

  return trendFromValues(history[0][metric], history[history.length - 1][metric]);
}

export function laneTrendDirection(
  history: TopologyTrendPoint[],
  laneId: TopologyDomain,
): TrendDirection {
  if (history.length < 2) return "stable";

  return trendFromValues(
    history[0].lanePressure[laneId] ?? 0,
    history[history.length - 1].lanePressure[laneId] ?? 0,
  );
}

function strongestLane(topology: MessagingTopologyOverview) {
  return topology.lanes.reduce<TopologyLane | undefined>(
    (best, lane) => (!best || compareTopologyPressure(lane, best) < 0 ? lane : best),
    undefined,
  );
}

export function defaultTopologySelectionId(topology: MessagingTopologyOverview) {
  const bottleneckDomain = topology.diagnostics.top_bottleneck?.domain;
  const bottleneckLane = isTopologyDomain(bottleneckDomain)
    ? topology.lanes.find((lane) => lane.id === bottleneckDomain)
    : undefined;

  if (bottleneckLane && bottleneckLane.state !== "quiet") {
    return topologyLaneSelectionId(bottleneckLane.id);
  }

  const lane = strongestLane(topology);
  return lane && lane.state !== "quiet" ? topologyLaneSelectionId(lane.id) : "broker";
}

function selectionDescriptionForState(state: TopologyState) {
  if (state === "blocked") return "Bottleneck visible in the current broker snapshot.";
  if (state === "pressure") return "Pressure visible in the current broker snapshot.";
  if (state === "flowing") return "Current broker-visible activity is flowing.";
  return "No current broker-visible activity or pressure.";
}

export function resolveTopologySelection(
  topology: MessagingTopologyOverview,
  requestedId: string | null | undefined,
): TopologySelection {
  const selectionId = requestedId ?? defaultTopologySelectionId(topology);

  if (selectionId === "broker") {
    return {
      counters: [
        { key: "sessions", label: "Sessions", value: topology.broker.sessions },
        { key: "connections", label: "Connections", value: topology.broker.connections },
        {
          key: "messages_per_second",
          label: "Messages/sec",
          value: topology.broker.messagesPerSecond,
        },
        { key: "realms", label: "Realms", value: topology.broker.realms.length },
        {
          key: "router_backpressure",
          label: "Router backpressure",
          value: topology.broker.routerBackpressureTotal,
        },
      ],
      description: topology.diagnostics.incident_summary?.explanation ?? "Broker snapshot.",
      id: "broker",
      kind: "broker",
      state:
        topology.diagnostics.incident_summary?.severity === "informational"
          ? "flowing"
          : "pressure",
      title: "Fitz broker",
    };
  }

  if (selectionId.startsWith(selectionPrefix.sessionGroup)) {
    const routeFamily = Number(selectionId.slice(selectionPrefix.sessionGroup.length));
    const group = topology.sessionGroups.find((entry) => entry.routeFamily === routeFamily);

    if (group) {
      return {
        counters: [
          { key: "sessions", label: "Sessions", value: group.sessions },
          { key: "messages_received", label: "Received", value: group.messagesReceived },
          { key: "messages_sent", label: "Sent", value: group.messagesSent },
          { key: "max_idle_seconds", label: "Max idle", value: group.maxIdleSeconds },
        ],
        description: `${group.transports.join(", ") || "No transports reported"} in route family ${group.routeFamily}.`,
        id: selectionId,
        kind: "session_group",
        state: group.sessions > 0 ? "flowing" : "quiet",
        title: `Route family ${group.routeFamily}`,
      };
    }
  }

  if (selectionId.startsWith(selectionPrefix.lane)) {
    const laneId = topologyLaneIdFromSelectionId(selectionId);
    const lane = laneId ? topology.lanes.find((entry) => entry.id === laneId) : undefined;

    if (lane) {
      return {
        counters: lane.counters,
        description:
          lane.diagnostics.explanation_hints?.[0] ?? selectionDescriptionForState(lane.state),
        href: lane.href,
        id: selectionId,
        kind: "lane",
        state: lane.state,
        title: lane.title,
      };
    }
  }

  if (selectionId.startsWith(selectionPrefix.resource)) {
    const resourceId = selectionId.slice(selectionPrefix.resource.length);
    const resource = topology.lanes
      .flatMap((lane) => lane.resources)
      .find((entry) => entry.id === resourceId);

    if (resource) {
      return {
        counters: resource.counters,
        description: selectionDescriptionForState(resource.state),
        href: resource.href,
        id: selectionId,
        kind: "resource",
        scope: resource.scope,
        state: resource.state,
        title: resource.label,
      };
    }
  }

  if (selectionId.startsWith(selectionPrefix.connection)) {
    const connectionId = selectionId.slice(selectionPrefix.connection.length);
    const connection = topology.connections.items.find((entry) => entry.id === connectionId);

    if (connection) {
      return {
        counters: connection.counters,
        description: topologyConnectionKindLabel(connection.kind),
        href: connection.href,
        id: selectionId,
        kind: "connection",
        scope: connection.scope,
        state: connection.state,
        title: connection.label,
      };
    }
  }

  return resolveTopologySelection(topology, "broker");
}
