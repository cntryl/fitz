import { describe, expect, it } from "vite-plus/test";
import { mockFitzResponse } from "../dev/mock-api";
import { domains } from "../dev/mock-api/fixtures";

function jsonBody(response: ReturnType<typeof mockFitzResponse>) {
  if (!response) {
    throw new Error("Expected mock response");
  }

  return JSON.parse(response.body);
}

function scopedRouteParts(route: string) {
  return route.replace(/^[a-z]+:\/\//, "").split("/");
}

function laneStates(body: { lanes: { id: string; state: string }[] }) {
  return Object.fromEntries(body.lanes.map((lane) => [lane.id, lane.state]));
}

describe("Vite mock API", () => {
  it("returns each domain's own resource metrics instead of queue-shaped rows", () => {
    // Arrange
    const expected: Record<string, { has: string[]; lacks: string[] }> = {
      kv: { has: ["estimated_record_count", "read_latency_p95_ms"], lacks: ["messages_ready"] },
      lease: {
        has: ["active_leases", "waiters", "oldest_lease_age_seconds"],
        lacks: ["messages_ready"],
      },
      notice: { has: ["subscriptions_active", "publishes_per_minute"], lacks: ["messages_ready"] },
      queue: { has: ["messages_ready", "messages_dead_lettered"], lacks: ["read_latency_p95_ms"] },
      rpc: { has: ["workers_registered", "requests_pending"], lacks: ["messages_ready"] },
      schedule: {
        has: ["schedules_active", "pending_claims", "next_run"],
        lacks: ["messages_ready"],
      },
      stream: { has: ["committed_event_count", "size_bytes"], lacks: ["messages_ready"] },
    };

    for (const [domain, fields] of Object.entries(expected)) {
      // Act
      const body = jsonBody(
        mockFitzResponse("GET", `/api/v1/2/${domain}/realms/acme/areas/payments/resources`),
      );

      // Assert
      for (const resource of body.resources) {
        for (const field of fields.has)
          expect(resource, `${domain}.${field}`).toHaveProperty(field);
        for (const field of fields.lacks)
          expect(resource, `${domain}.${field}`).not.toHaveProperty(field);
      }
    }
  });

  it("reports pending handoffs on schedule observations", () => {
    // Act
    const body = jsonBody(
      mockFitzResponse(
        "GET",
        "/api/v1/2/schedule/realms/acme/areas/payments/resources/invoices/executions",
      ),
    );

    // Assert
    expect(body.observations[0]).toHaveProperty("pending_handoffs");
    expect(body.observations[0]).toHaveProperty("delivery_mode");
  });

  it("returns typed structured family metrics", () => {
    const response = mockFitzResponse("GET", "/api/v1/2/metrics");

    expect(response?.status).toBe(200);
    const body = jsonBody(response);
    expect(body.scope).toBe("family");
    expect(body.family).toBe(2);
    expect(body.samples).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ name: "fitz_queue_messages_pending", kind: "gauge" }),
      ]),
    );
  });

  it("hides route families until a protected session is authenticated", () => {
    const features = jsonBody(mockFitzResponse("GET", "/api/v1/features"));
    const session = jsonBody(mockFitzResponse("GET", "/api/v1/session"));

    expect(features.route_families).toEqual([]);
    expect(features.admin_auth_mode).toBe("protected");
    expect(features.admin_auth_required).toBe(true);
    expect(session.route_families).toEqual(["1", "2", "3", "4", "5"]);
  });

  it("returns not found for family-scoped endpoints outside the provisioned set", () => {
    for (const path of ["/api/v1/6/metrics", "/api/v1/6/topology", "/api/v1/6/queue/realms"]) {
      const response = mockFitzResponse("GET", path);

      expect(response?.status).toBe(404);
      expect(jsonBody(response).error).toBe("Route family is not provisioned");
    }
  });

  it("accepts only the documented mock admin credentials", () => {
    const accepted = mockFitzResponse("POST", "/api/v1/session", {
      password: "pwd123",
      username: "root",
    });
    const rejected = mockFitzResponse("POST", "/api/v1/session", {
      password: "wrong",
      username: "root",
    });

    expect(accepted?.status).toBe(204);
    expect(accepted?.body).toBe("");
    expect(rejected?.status).toBe(401);
  });

  it("returns domain inventory payloads", () => {
    const response = mockFitzResponse(
      "GET",
      "/api/v1/2/queue/realms/acme/areas/payments/resources",
    );
    const body = jsonBody(response);

    expect(body.realm).toBe("acme");
    expect(body.area).toBe("payments");
    expect(body.resources.length).toBeGreaterThan(1);
  });

  it("returns operation metadata for operation-domain inventory routes", () => {
    for (const domain of ["notice", "rpc", "schedule"]) {
      const response = mockFitzResponse(
        "GET",
        `/api/v1/2/${domain}/realms/acme/areas/payments/resources`,
      );
      const body = jsonBody(response);

      expect(body.resources[0].operation).toBe("ReconcileInvoice");
    }
  });

  it("returns four-part FITZ routes for operation domains", () => {
    const topology = jsonBody(mockFitzResponse("GET", "/api/v1/2/topology"));

    for (const domain of ["notice", "rpc", "schedule"]) {
      const lane = topology.lanes.find((entry: { id: string }) => entry.id === domain);
      const route = lane.top_scoped_resources[0].scope.route;

      expect(scopedRouteParts(route)).toHaveLength(4);
      expect(lane.top_scoped_resources[0].scope.operation).toBe("ReconcileInvoice");
    }

    const noticeDeliveries = jsonBody(mockFitzResponse("GET", "/api/v1/2/notice/deliveries"));

    expect(scopedRouteParts(noticeDeliveries.observations[0].route)).toHaveLength(4);
  });

  it("makes Route Family 1 completely idle", () => {
    const stats = jsonBody(mockFitzResponse("GET", "/api/v1/1/stats"));
    const topology = jsonBody(mockFitzResponse("GET", "/api/v1/1/topology"));
    const sessions = jsonBody(mockFitzResponse("GET", "/api/v1/1/sessions"));
    const metrics = jsonBody(mockFitzResponse("GET", "/api/v1/1/metrics"));

    expect(stats.broker).toEqual(
      expect.objectContaining({
        connections: 0,
        messages_per_second: 0,
        sessions: 0,
      }),
    );
    expect(stats.diagnostics.incident_summary).toEqual(
      expect.objectContaining({
        status: "healthy",
        title: "Idle Route Family",
      }),
    );
    expect(Object.values(laneStates(topology))).toEqual(domains.map(() => "quiet"));
    expect(topology.connections.items).toEqual([]);
    expect(topology.session_groups).toEqual([]);
    expect(sessions.sessions).toEqual([]);
    expect(metrics.samples.every((sample: { value: number }) => sample.value === 0)).toBe(true);

    for (const domain of domains) {
      const inventory = jsonBody(mockFitzResponse("GET", `/api/v1/1/${domain}/realms`));

      expect(inventory.realms).toEqual([]);
    }
  });

  it("makes Route Family 2 healthy across every domain", () => {
    const stats = jsonBody(mockFitzResponse("GET", "/api/v1/2/stats"));
    const topology = jsonBody(mockFitzResponse("GET", "/api/v1/2/topology"));
    const deadLetters = jsonBody(
      mockFitzResponse(
        "GET",
        "/api/v1/2/queue/realms/acme/areas/payments/resources/invoices/dead-letters",
      ),
    );

    expect(stats.diagnostics.incident_summary.title).toBe("Healthy activity");
    expect(domains.map((domain) => stats.domains[domain].diagnostics.severity)).toEqual(
      domains.map(() => "informational"),
    );
    expect(Object.values(laneStates(topology))).toEqual(domains.map(() => "flowing"));
    expect(
      topology.connections.items.every(
        (connection: { state: string }) => connection.state === "flowing",
      ),
    ).toBe(true);
    expect(topology.diagnostics.hotspots).toEqual([]);
    expect(topology.session_groups[0].sessions).toBeGreaterThan(0);
    expect(deadLetters.messages).toEqual([]);
  });

  it("makes Route Family 3 a mix of healthy and unhealthy domains", () => {
    const topology = jsonBody(mockFitzResponse("GET", "/api/v1/3/topology"));

    expect(laneStates(topology)).toEqual({
      kv: "flowing",
      lease: "pressure",
      notice: "flowing",
      queue: "pressure",
      rpc: "pressure",
      schedule: "flowing",
      stream: "flowing",
    });
    expect(
      topology.diagnostics.hotspots.map((hotspot: { domain: string }) => hotspot.domain),
    ).toEqual(["queue", "lease", "rpc"]);
    expect(topology.diagnostics.incident_summary.title).toBe("Mixed domain health");
  });

  it("makes Route Family 4 unhealthy across every domain", () => {
    const topology = jsonBody(mockFitzResponse("GET", "/api/v1/4/topology"));

    expect(Object.values(laneStates(topology))).toEqual(domains.map(() => "pressure"));
    expect(topology.diagnostics.hotspots).toHaveLength(domains.length);
    expect(
      topology.lanes.every(
        (lane: { diagnostics: { severity: string } }) => lane.diagnostics.severity === "high",
      ),
    ).toBe(true);
    expect(topology.diagnostics.incident_summary.title).toBe("All domains unhealthy");
  });

  it("makes Route Family 5 a critical chaos state", () => {
    const unhealthyMetrics = jsonBody(mockFitzResponse("GET", "/api/v1/4/metrics"));
    const chaosMetrics = jsonBody(mockFitzResponse("GET", "/api/v1/5/metrics"));
    const topology = jsonBody(mockFitzResponse("GET", "/api/v1/5/topology"));
    const metricValue = (body: { samples: { name: string; value: number }[] }, name: string) => {
      const sample = body.samples.find((entry) => entry.name === name);

      if (!sample) throw new Error(`Expected metric ${name}`);
      return sample.value;
    };

    expect(Object.values(laneStates(topology))).toEqual(domains.map(() => "blocked"));
    expect(
      topology.connections.items.every(
        (connection: { state: string }) => connection.state === "blocked",
      ),
    ).toBe(true);
    expect(topology.diagnostics.incident_summary).toEqual(
      expect.objectContaining({
        severity: "critical",
        status: "stalled",
        title: "Route Family chaos",
      }),
    );
    expect(topology.session_groups[0].sessions).toBe(8);
    expect(metricValue(chaosMetrics, "fitz_queue_messages_pending")).toBeGreaterThan(
      metricValue(unhealthyMetrics, "fitz_queue_messages_pending"),
    );
  });
});
