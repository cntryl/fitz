import { describe, expect, it } from "vite-plus/test";
import { mockFitzResponse } from "../dev/mock-api";

function jsonBody(response: ReturnType<typeof mockFitzResponse>) {
  if (!response) {
    throw new Error("Expected mock response");
  }

  return JSON.parse(response.body);
}

describe("Vite mock API", () => {
  it("returns typed Prometheus metrics", () => {
    const response = mockFitzResponse("GET", "/metrics");

    expect(response?.status).toBe(200);
    expect(response?.body).toContain("# TYPE fitz_queue_messages_pending gauge");
    expect(response?.body).toContain("# TYPE fitz_schedule_latency_ms histogram");
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
});
