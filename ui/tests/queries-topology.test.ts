import { describe, expect, it } from "vite-plus/test";
import { mapStructuredMetrics } from "@/features/metrics/metrics-mappers";
import {
  appendTopologyTrendPoint,
  defaultTopologySelectionId,
  laneTrendDirection,
  mapMessagingTopology,
  resolveTopologySelection,
  topologyScopeHref,
  topologyTrendDirection,
} from "@/features/topology/topology-mappers";
import { healthyDiagnostics, healthyGlobalDiagnostics, topologyDtoLane } from "./fixtures/topology";

describe("Data query layer", () => {
  it("maps structured metrics into searchable families", () => {
    const mapped = mapStructuredMetrics({
      family: 7,
      generated_at: 123,
      samples: [
        {
          help: "RPC requests",
          kind: "counter",
          labels: { area: "api", realm: "prod" },
          name: "fitz_rpc_requests_total",
          value: 42,
        },
        {
          help: "Queue ready messages",
          kind: "gauge",
          labels: { area: "jobs", realm: "prod", resource: "emails" },
          name: "fitz_queue_ready",
          value: 7,
        },
      ],
      scope: "family",
    });

    expect(mapped.generatedAt).toBe(123);
    expect(mapped.scope).toBe("family");
    expect(mapped.families).toEqual([
      {
        help: "Queue ready messages",
        name: "fitz_queue_ready",
        samples: [
          {
            labels: { area: "jobs", realm: "prod", resource: "emails" },
            name: "fitz_queue_ready",
            value: 7,
          },
        ],
        type: "gauge",
      },
      {
        help: "RPC requests",
        name: "fitz_rpc_requests_total",
        samples: [
          {
            labels: { area: "api", realm: "prod" },
            name: "fitz_rpc_requests_total",
            value: 42,
          },
        ],
        type: "counter",
      },
    ]);
  });
  it("maps topology DTOs and derives selection links and trends", () => {
    const topology = mapMessagingTopology({
      broker: {
        connections: 3,
        messages_per_second: 2,
        realms: ["prod"],
        router_backpressure_total: 0,
        router_high_lane_backpressure_total: 0,
        sessions: 2,
        uptime_seconds: 60,
      },
      connections: {
        items: [
          {
            id: "queue-inflight:4:99",
            kind: "queue_inflight_consumer",
            label: "jobs / worker inflight",
            metrics: [{ key: "attempts", label: "Attempts", value: 2 }],
            scope: {
              area: "jobs",
              realm: "prod",
              resource: "worker",
              route_family: 4,
              session_id: "11",
            },
            source: "domain:queue",
            state: "flowing",
            target: "session:11",
          },
        ],
        limit: 250,
        total: 1,
        truncated: false,
      },
      diagnostics: {
        ...healthyGlobalDiagnostics,
        top_bottleneck: {
          ...healthyDiagnostics,
          area: "jobs",
          current_stage: "dead_letter_pressure",
          domain: "queue",
          realm: "prod",
          resource: "worker",
          severity: "high",
        },
      },
      generated_at: "2026-05-21T13:10:00Z",
      lanes: [
        topologyDtoLane("queue", "blocked", 4),
        topologyDtoLane("rpc", "flowing", 1),
        topologyDtoLane("notice", "quiet", 0),
        topologyDtoLane("schedule", "pressure", 2),
        topologyDtoLane("stream", "flowing", 3),
        topologyDtoLane("lease", "quiet", 0),
        topologyDtoLane("kv", "flowing", 1),
      ],
      session_groups: [
        {
          max_idle_seconds: 12,
          messages_received: 5,
          messages_sent: 7,
          representative_sessions: [
            {
              messages_received: 5,
              messages_sent: 7,
              route_family: 4,
              session_id: "11",
              transport: "websocket",
            },
          ],
          route_family: 4,
          sessions: 1,
          transports: ["websocket"],
        },
      ],
    });

    expect(topology.lanes.map((entry) => entry.id)).toEqual([
      "queue",
      "rpc",
      "notice",
      "schedule",
      "stream",
      "lease",
      "kv",
    ]);
    expect(topology.sessionGroups[0].representativeSessions[0].routeFamily).toBe(4);
    expect(topology.connections.items[0].scope.routeFamily).toBe(4);
    expect(topologyScopeHref("queue", topology.connections.items[0].scope)).toBe(
      "/admin/4/queue/prod/jobs/worker",
    );
    expect(defaultTopologySelectionId(topology)).toBe("lane:queue");
    expect(resolveTopologySelection(topology, "lane:queue").title).toBe("QUEUE");

    const history = appendTopologyTrendPoint([], topology);
    const nextTopology = {
      ...topology,
      broker: {
        ...topology.broker,
        messagesPerSecond: 5,
      },
      generatedAt: "2026-05-21T13:10:10Z",
      lanes: topology.lanes.map((entry) =>
        entry.id === "queue"
          ? {
              ...entry,
              counters: [{ key: "pressure", label: "Pressure", value: 8 }],
            }
          : entry,
      ),
    };
    const nextHistory = appendTopologyTrendPoint(history, nextTopology);

    expect(topologyTrendDirection(nextHistory, "messagesPerSecond")).toBe("rising");
    expect(laneTrendDirection(nextHistory, "queue")).toBe("rising");
  });
});
