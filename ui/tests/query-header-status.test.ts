import { describe, expect, it } from "vite-plus/test";
import { queryFreshness, queryHeaderStatus } from "@/components/shared/query-header-status";

const copy = {
  loading: "Loading evidence.",
  ready: "Evidence is current.",
  unavailable: "Evidence is unavailable.",
};

describe("query header status", () => {
  it("presents an initial query as loading", () => {
    const query = {
      data: null,
      error: null,
      loading: true,
      refreshing: false,
      stale: false,
    };

    expect(queryHeaderStatus(query, copy)).toEqual({
      detail: "Loading evidence.",
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

    expect(queryHeaderStatus(query, copy)).toEqual({
      detail: "Evidence is unavailable.",
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

    expect(queryHeaderStatus(query, copy)).toEqual({
      detail: "Evidence is unavailable.",
      label: "Update unavailable",
      tone: "warning",
    });
    expect(queryFreshness(query)).toBe("Stale");
  });

  it("preserves a page-specific ready status only for fresh data", () => {
    const query = {
      data: { rows: 1 },
      error: null,
      loading: false,
      refreshing: false,
      stale: false,
    };

    expect(queryHeaderStatus(query, copy, { label: "Enabled", tone: "success" })).toEqual({
      detail: "Evidence is current.",
      label: "Enabled",
      tone: "success",
    });
    expect(queryFreshness(query)).toBe("Live");
  });
});
