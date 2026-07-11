import { describe, expect, it } from "vite-plus/test";
import { mockFitzResponse } from "../dev/mock-api";

function jsonBody(response: ReturnType<typeof mockFitzResponse>) {
  if (!response) {
    throw new Error("Expected mock response");
  }

  return JSON.parse(response.body);
}

function scopedRouteParts(route: string) {
  return route.replace(/^[a-z]+:\/\//, "").split("/");
}

describe("Vite mock API", () => {
  it("returns typed structured family metrics", () => {
    const response = mockFitzResponse("GET", "/api/v1/7/metrics");

    expect(response?.status).toBe(200);
    const body = jsonBody(response);
    expect(body.scope).toBe("family");
    expect(body.family).toBe(7);
    expect(body.samples).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ name: "fitz_queue_messages_pending", kind: "gauge" }),
      ]),
    );
  });

  it("returns global stats with multiple route families", () => {
    const response = mockFitzResponse("GET", "/api/v1/features");
    const body = jsonBody(response);

    expect(body.route_families).toEqual(["1", "7", "42"]);
    expect(body.admin_auth_required).toBe(false);
  });

  it("returns domain inventory payloads", () => {
    const response = mockFitzResponse(
      "GET",
      "/api/v1/7/queue/realms/acme/areas/payments/resources",
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
        `/api/v1/7/${domain}/realms/acme/areas/payments/resources`,
      );
      const body = jsonBody(response);

      expect(body.resources[0].operation).toBe("ReconcileInvoice");
    }
  });

  it("returns four-part FITZ routes for operation domains", () => {
    const topology = jsonBody(mockFitzResponse("GET", "/api/v1/topology"));

    for (const domain of ["notice", "rpc", "schedule"]) {
      const lane = topology.lanes.find((entry: { id: string }) => entry.id === domain);
      const route = lane.top_scoped_resources[0].scope.route;

      expect(scopedRouteParts(route)).toHaveLength(4);
      expect(lane.top_scoped_resources[0].scope.operation).toBe("ReconcileInvoice");
    }

    const noticeDeliveries = jsonBody(mockFitzResponse("GET", "/api/v1/7/notice/deliveries"));

    expect(scopedRouteParts(noticeDeliveries.observations[0].route)).toHaveLength(4);
  });
});
