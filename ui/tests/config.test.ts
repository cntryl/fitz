import { describe, expect, it } from "vite-plus/test";
import { parseDashboardPollIntervalMs } from "@/shared/config";

describe("app config", () => {
  it("parses the dashboard polling interval", () => {
    expect(parseDashboardPollIntervalMs(undefined)).toBe(10_000);
    expect(parseDashboardPollIntervalMs("2500")).toBe(2500);
    expect(() => parseDashboardPollIntervalMs("0")).toThrow(
      "Invalid VITE_FITZ_DASHBOARD_POLL_INTERVAL_MS",
    );
  });
});
