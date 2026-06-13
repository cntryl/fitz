import type {
  DiagnosticSnapshot,
  GlobalTroubleshootingDiagnostics,
  TopologyLane as TopologyLaneDto,
} from "@/adapters";
import type {
  MessagingTopologyOverview,
  TopologyCounter,
  TopologyDomain,
  TopologyLane,
  TopologyState,
} from "@/features/topology/topology-models";

export const healthyDiagnostics: DiagnosticSnapshot = {
  confidence: 1,
  confidence_justification: {
    rationale: "No pressure detected",
    signals_matched: [],
    signals_missing: [],
  },
  contention_count: 0,
  current_stage: "healthy",
  explanation_hints: [],
  failure_count: 0,
  recent_transition_count: 0,
  severity: "informational",
  trend: "steady",
  waiter_count: 0,
};

export const healthyGlobalDiagnostics: GlobalTroubleshootingDiagnostics = {
  hotspots: [],
  incident_summary: {
    confidence: 1,
    explanation: "No active pressure detected",
    recommended_next_query: "No follow-up needed",
    severity: "informational",
    suggested_next_queries: [],
    status: "healthy",
    title: "Healthy",
  },
  last_significant_transition_at: null,
};

export const dashboardDiagnostics: GlobalTroubleshootingDiagnostics = {
  ...healthyGlobalDiagnostics,
  hotspots: [
    {
      ...healthyDiagnostics,
      area: "ops",
      current_stage: "healthy",
      domain: "queue",
      likely_bottleneck: null,
      realm: "default",
      resource: "primary",
      severity: "informational",
    },
  ],
  incident_summary: {
    ...healthyGlobalDiagnostics.incident_summary,
    explanation: "No active pressure detected",
    recommended_next_query: "Check queue",
    title: "Healthy",
  },
  top_bottleneck: undefined,
};

export function topologyDtoLane(
  id: TopologyDomain,
  state: TopologyState,
  value: number,
): TopologyLaneDto {
  return {
    activity_per_second: value,
    consumers: value,
    counters: [{ key: "pressure", label: "Pressure", value }],
    diagnostics: {
      ...healthyDiagnostics,
      current_stage: state === "blocked" ? "dead_letter_pressure" : "healthy",
      severity: state === "blocked" ? "high" : "informational",
    },
    id,
    observers: 0,
    state,
    title: id.toUpperCase(),
    top_scoped_resources:
      id === "queue"
        ? [
            {
              counters: [{ key: "ready", label: "Ready", value: 4 }],
              id: "queue:4:prod:jobs:worker",
              label: "prod / jobs / worker",
              scope: {
                area: "jobs",
                realm: "prod",
                resource: "worker",
                route_family: 4,
              },
              state: "blocked",
            },
          ]
        : [],
  };
}

export function topologyAppLane(
  id: TopologyDomain,
  title: string,
  state: TopologyState = "flowing",
  counters: TopologyCounter[] = [],
): TopologyLane {
  return {
    activityPerSecond: 1,
    consumers: 1,
    counters,
    diagnostics: {
      ...healthyDiagnostics,
      current_stage: state === "blocked" ? "dead_letter_pressure" : "healthy",
      explanation_hints: ["Current broker-visible activity."],
      severity: state === "blocked" ? "high" : "informational",
    },
    href: `/${id}`,
    id,
    observers: 1,
    resources:
      id === "queue"
        ? [
            {
              counters: [
                { key: "ready", label: "Ready", value: 4 },
                { key: "dead_letters", label: "Dead letters", value: 1 },
              ],
              domain: "queue",
              href: "/queue/default/ops/primary",
              id: "queue:default:ops:primary",
              label: "default / ops / primary",
              scope: {
                area: "ops",
                realm: "default",
                resource: "primary",
                routeFamily: 1,
              },
              state: "blocked",
            },
          ]
        : [],
    state,
    title,
  };
}

export const topologyOverview: MessagingTopologyOverview = {
  broker: {
    connections: 2,
    messagesPerSecond: 3.25,
    realms: ["default"],
    routerBackpressureTotal: 0,
    routerHighLaneBackpressureTotal: 0,
    sessions: 1,
    uptimeSeconds: 120,
  },
  connections: {
    items: [
      {
        counters: [{ key: "attempts", label: "Attempts", value: 2 }],
        domain: "queue",
        href: "/queue/default/ops/primary",
        id: "queue-inflight:1:42",
        kind: "queue_inflight_consumer",
        label: "ops / primary inflight",
        scope: {
          area: "ops",
          realm: "default",
          resource: "primary",
          routeFamily: 1,
          sessionId: "session-1",
        },
        source: "domain:queue",
        state: "flowing",
        target: "session:session-1",
      },
    ],
    limit: 250,
    total: 1,
    truncated: false,
  },
  diagnostics: dashboardDiagnostics,
  fetchedAt: "2026-05-21T13:10:00.000Z",
  generatedAt: "2026-05-21T13:10:00.000Z",
  lanes: [
    topologyAppLane("queue", "Queue", "blocked", [
      { key: "ready", label: "Ready", value: 4 },
      { key: "inflight", label: "Inflight", value: 2 },
      { key: "dead_letters", label: "Dead letters", value: 1 },
    ]),
    topologyAppLane("rpc", "RPC", "flowing", [
      { key: "pending", label: "Pending", value: 1 },
      { key: "workers", label: "Workers", value: 4 },
    ]),
    topologyAppLane("notice", "Notice", "flowing", [
      { key: "subscriptions", label: "Subscriptions", value: 7 },
      { key: "delivery_drops", label: "Drops", value: 0 },
    ]),
    topologyAppLane("schedule", "Schedule", "pressure", [
      { key: "schedules", label: "Schedules", value: 5 },
      { key: "pending_claims", label: "Pending claims", value: 1 },
      { key: "subscriptions", label: "Subscriptions", value: 6 },
    ]),
    topologyAppLane("stream", "Stream", "flowing", [
      { key: "events", label: "Events", value: 200 },
      { key: "streams", label: "Streams", value: 8 },
      { key: "subscriptions", label: "Subscriptions", value: 9 },
    ]),
    topologyAppLane("lease", "Lease", "flowing", [
      { key: "leases", label: "Leases", value: 3 },
      { key: "waiters", label: "Waiters", value: 0 },
      { key: "oldest_lease_age_seconds", label: "Oldest lease age", value: 42 },
    ]),
    topologyAppLane("kv", "KV", "flowing", [
      { key: "keys", label: "Keys", value: 12 },
      { key: "transactions", label: "Transactions", value: 1 },
    ]),
  ],
  sessionGroups: [
    {
      maxIdleSeconds: 12,
      messagesReceived: 2,
      messagesSent: 3,
      representativeSessions: [],
      routeFamily: 1,
      sessions: 1,
      transports: ["ws"],
    },
  ],
};
