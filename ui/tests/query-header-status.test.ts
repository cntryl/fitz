import { describe, expect, it } from "vite-plus/test";
import { queryFreshness, queryHeaderStatus } from "@/components/shared/query-header-status";

describe("query header status", () => {
  it("presents an initial query as loading", () => {
    const query = {
      data: null,
      error: null,
      loading: true,
      refreshing: false,
      stale: false,
    };

    expect(queryHeaderStatus(query)).toEqual({
      label: "Loading",
      tone: "info",
    });
    expect(queryFreshness(query)).toBe("Loading");
  });

  it("presents an initial query error as unavailable", () => {
    const query = {
      data: null,
      error: new Error("offline"),
      loading: false,
      refreshing: false,
      stale: true,
    };

    expect(queryHeaderStatus(query)).toEqual({
      label: "Unavailable",
      tone: "warning",
    });
    expect(queryFreshness(query)).toBe("Unavailable");
  });

  it("does not present retained data as live after a refresh error", () => {
    const query = {
      data: { rows: 1 },
      error: new Error("offline"),
      loading: false,
      refreshing: false,
      stale: true,
    };

    expect(queryHeaderStatus(query)).toEqual({
      freshness: {
        label: "Update unavailable",
        tone: "warning",
      },
    });
    expect(queryFreshness(query)).toBe("Stale");
  });

  it("shows no badge for fresh data without a page-specific status", () => {
    // Arrange
    const query = {
      data: { rows: 1 },
      error: null,
      loading: false,
      refreshing: false,
      stale: false,
    };

    // Act
    const status = queryHeaderStatus(query);

    // Assert
    expect(status).toBeUndefined();
  });

  it("preserves page-specific evidence while the query refreshes", () => {
    const query = {
      data: { rows: 1 },
      error: null,
      loading: true,
      refreshing: true,
      stale: false,
    };

    expect(queryHeaderStatus(query, { label: "Waiters present", tone: "warning" })).toEqual({
      freshness: { label: "Refreshing", tone: "info" },
      label: "Waiters present",
      tone: "warning",
    });
    expect(queryFreshness(query)).toBe("Refreshing");
  });
});
