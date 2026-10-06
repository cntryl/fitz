import { describe, expect, it, vi } from "vite-plus/test";
import { click, queryState, submit, type } from "@askrjs/askr/testing";
import { mountRoute, pageSmokeMocks, queryOptions } from "./page-smoke/harness";
import { diagnostics, systemOverview, topologyOverview } from "./page-smoke/fixtures";

const mocks = pageSmokeMocks();

describe("admin infrastructure smoke tests", () => {
  it("renders the status-first dashboard sections", async () => {
    const { default: Home } = await import("@/pages/app/home");

    const root = await mountRoute("/", "/", Home);
    const text = root.textContent ?? "";

    expect(text).toContain("Fitz status");
    expect(text).toContain("Current status");
    expect(text).toContain("Issues");
    expect(text).toContain("Actionable signals only");
    expect(text).toContain("Queue blocked");
    expect(text).toContain("Schedule pending claims");
    expect(text).toContain("Open Queue");
    expect(root.querySelector('[aria-label="Domain health"]')).toBeNull();
    expect(text).toContain("Broker vitals");
    expect(text).toContain("Router pressure");
    expect(text).not.toContain("Messaging flow");
    expect(text).not.toContain("Flow inspector");

    const orderedSections = ["Current status", "Issues", "Broker vitals"];
    let cursor = -1;
    for (const section of orderedSections) {
      const index = text.indexOf(section, cursor + 1);
      expect(index).toBeGreaterThan(cursor);
      cursor = index;
    }
  });
  it("renders diagnostics as the infrastructure-internals console", async () => {
    const diagnosticsWithSuggestion = {
      ...diagnostics,
      incident_summary: {
        ...diagnostics.incident_summary,
        confidence: 0.82,
        explanation: "Queue backlog is increasing while RPC workers are saturated.",
        recommended_next_query: "Inspect queues",
        severity: "medium",
        status: "degraded",
        suggested_next_queries: [
          {
            endpoint: "/api/v1/queue/stats",
            priority: 1,
            rationale: "Queue backlog is the top broker-visible pressure signal.",
            remediation: "Open Queue and inspect ready, inflight, and DLQ pressure.",
            title: "Inspect queue pressure",
          },
        ],
        title: "Queue pressure",
      },
    };

    mocks.queryStates.system = queryState.fresh(
      {
        ...systemOverview,
        diagnostics: diagnosticsWithSuggestion,
      },
      queryOptions(),
    );
    mocks.queryStates.topology = queryState.fresh(
      {
        ...topologyOverview,
        diagnostics: diagnosticsWithSuggestion,
      },
      queryOptions(),
    );

    const { default: DiagnosticsPage } = await import("@/pages/app/diagnostics");
    const root = await mountRoute(
      "/admin/1/diagnostics",
      "/admin/{family}/diagnostics",
      DiagnosticsPage,
    );
    const text = root.textContent ?? "";

    expect(text).toContain("Diagnostics console");
    expect(text).toContain("Infrastructure signals");
    expect(text).toContain("Domain internals");
    expect(text).toContain("Structured metrics");
    expect(text).toContain("Storage health");
    expect(text).toContain("Not exposed");
    expect(text).toContain("Hotspots");
    expect(text).toContain("Suggested queries");
    expect(text).toContain("Inspect queue pressure");
    expect(text).toContain("/api/v1/queue/stats");
    expect(text).toContain("Metric families");
    expect(text).toContain("fitz_rpc_latency_histogram");
    expect(root.querySelector('a[href="/admin/1/sessions"]')).toBeTruthy();
    expect(root.querySelector('a[href="/admin/1/metrics"]')).toBeTruthy();
    expect(root.querySelector('a[href="/api/v1/queue/stats"]')).toBeTruthy();
  });
  it("folds diagnostics prose into row subtitles instead of columns", async () => {
    // Arrange
    const { default: DiagnosticsPage } = await import("@/pages/app/diagnostics");

    // Act
    const root = await mountRoute(
      "/admin/1/diagnostics",
      "/admin/{family}/diagnostics",
      DiagnosticsPage,
    );
    const proseHeaders = ["detail", "internals", "evidence", "remediation", "help"].filter(
      (id) => root.querySelector(`th[data-column-id="${id}"]`) !== null,
    );

    // Assert
    expect(proseHeaders).toEqual([]);
    expect(root.querySelectorAll(".titled-cell-subtitle").length).toBeGreaterThan(0);
  });
  it("refreshes every diagnostics source from the page action", async () => {
    const { default: DiagnosticsPage } = await import("@/pages/app/diagnostics");
    mocks.refresh.mockClear();
    const root = await mountRoute(
      "/admin/1/diagnostics",
      "/admin/{family}/diagnostics",
      DiagnosticsPage,
    );

    const refresh = root.querySelector<HTMLButtonElement>(
      'button[aria-label="Refresh diagnostics"]',
    );
    if (refresh) click(refresh);

    expect(mocks.refresh).toHaveBeenCalledTimes(3);
  });
  it("marks diagnostics partial when secondary inputs fail", async () => {
    const { default: DiagnosticsPage } = await import("@/pages/app/diagnostics");
    mocks.queryStates.metrics = queryState.error(
      new Error("metrics unavailable"),
      undefined,
      queryOptions(),
    );
    mocks.queryStates.topology = queryState.error(
      new Error("topology unavailable"),
      undefined,
      queryOptions(),
    );
    const root = await mountRoute(
      "/admin/1/diagnostics",
      "/admin/{family}/diagnostics",
      DiagnosticsPage,
    );
    expect(root.textContent).toContain("Partial");
    expect(root.textContent).toContain("metrics unavailable");
    expect(root.textContent).toContain("topology unavailable");
  });
  it("searches diagnostics through the URL and opens a family-scoped result", async () => {
    const { default: DiagnosticsPage } = await import("@/pages/app/diagnostics");
    const root = await mountRoute(
      "/admin/1/diagnostics",
      "/admin/{family}/diagnostics",
      DiagnosticsPage,
    );
    const input = root.querySelector(
      'input[aria-label="Search diagnostics"]',
    ) as HTMLInputElement | null;
    const form = root.querySelector('form[role="search"]');

    if (input) {
      type(input, "orders");
    }
    if (form instanceof HTMLFormElement) submit(form);

    await vi.waitFor(() => {
      const searchResults = root.querySelector<HTMLElement>("[data-diagnostics-search-results]");

      expect(window.location.search).toBe("?q=orders");
      expect(searchResults?.hidden).toBe(false);
      expect(searchResults?.textContent).toContain("Search results");
      expect(
        searchResults?.querySelector(
          '[aria-label="Admin search results"] a[href="/admin/1/sessions"]',
        ),
      ).toBeTruthy();
      expect(
        searchResults?.querySelector(
          '[aria-label="Admin search results"] a[href="/admin/1/kv?realm=acme"]',
        ),
      ).toBeTruthy();
    });
  });
  it("keeps dashboard behavior visible while refresh is in flight", async () => {
    const { default: Home } = await import("@/pages/app/home");

    mocks.queryStates.topology = queryState.refreshing(topologyOverview, queryOptions());

    const root = await mountRoute("/", "/", Home);
    const text = root.textContent ?? "";

    expect(text).toContain("Refreshing");
    expect(text).toContain("Issues");
    expect(root.querySelector('[aria-label="Domain health"]')).toBeNull();
    expect(text).toContain("Broker vitals");
    expect(text).toContain("Queue");
  });
});
