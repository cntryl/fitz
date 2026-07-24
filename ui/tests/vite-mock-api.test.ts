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

  it("returns credential-protected features with multiple route families", () => {
    const response = mockFitzResponse("GET", "/api/v1/features");
    const body = jsonBody(response);

    expect(body.route_families).toEqual(["1", "2", "3", "4", "5"]);
    expect(body.admin_auth_mode).toBe("protected");
    expect(body.admin_auth_required).toBe(true);
  });

  it("accepts only the documented mock admin credentials", () => {
    const accepted = mockFitzResponse("POST", "/api/v1/session", {
      password: "pwd123",
      username: "admin",
    });
    const rejected = mockFitzResponse("POST", "/api/v1/session", {
      password: "wrong",
      username: "admin",
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
