import { describe, expect, it } from "vite-plus/test";
import {
  aggregateDomainMetricRows,
  areaRollupRows,
  realmRollupRows,
} from "@/components/shared/domain-inventory-rollup";
import type { DomainResourceInventoryRow } from "@/components/shared/domain-resource-inventory-table";

const rows: DomainResourceInventoryRow[] = [
  {
    area: "payments",
    realm: "acme",
    resource: "orders",
    messagesReady: 4,
    messagesDeadLettered: 1,
    oldestBacklogAgeSeconds: 30,
    readLatencyP95Ms: 12,
  },
  {
    area: "payments",
    realm: "acme",
    resource: "refunds",
    messagesReady: 6,
    messagesDeadLettered: 0,
    oldestBacklogAgeSeconds: 900,
    readLatencyP95Ms: 4,
  },
  {
    area: "support",
    realm: "acme",
    resource: "tickets",
    messagesReady: 1,
    messagesDeadLettered: 0,
    oldestBacklogAgeSeconds: 5,
    readLatencyP95Ms: 40,
  },
  {
    area: "payments",
    realm: "globex",
    resource: "billing",
    messagesReady: 2,
    messagesDeadLettered: 3,
    oldestBacklogAgeSeconds: 60,
    readLatencyP95Ms: 8,
  },
];

describe("domain inventory rollup", () => {
  it("sums counts and takes the worst latency and age across a scope", () => {
    expect(aggregateDomainMetricRows(rows)).toEqual({
      messagesDeadLettered: 4,
      messagesReady: 13,
      oldestBacklogAgeSeconds: 900,
      readLatencyP95Ms: 40,
    });
  });

  it("leaves a field absent when no resource reported it", () => {
    const rollup = aggregateDomainMetricRows([{ resource: "orders" }]);

    expect(rollup).toEqual({});
    expect("messagesReady" in rollup).toBe(false);
  });

  it("keeps the earliest next run and requires every estimate to be complete", () => {
    expect(
      aggregateDomainMetricRows([
        { resource: "a", nextRun: "2026-09-13T10:00:00Z", estimateComplete: true },
        { resource: "b", nextRun: "2026-09-12T10:00:00Z", estimateComplete: false },
      ]),
    ).toEqual({
      estimateComplete: false,
      nextRun: "2026-09-12T10:00:00Z",
    });
  });

  it("treats a reported-but-null next run as a scope with no next run", () => {
    expect(aggregateDomainMetricRows([{ resource: "a", nextRun: null }])).toEqual({
      nextRun: null,
    });
  });

  it("rolls resources up into realm rows with area and resource counts", () => {
    expect(realmRollupRows(rows)).toEqual([
      {
        areaCount: 2,
        messagesDeadLettered: 1,
        messagesReady: 11,
        oldestBacklogAgeSeconds: 900,
        readLatencyP95Ms: 40,
        realm: "acme",
        resourceCount: 3,
      },
      {
        areaCount: 1,
        messagesDeadLettered: 3,
        messagesReady: 2,
        oldestBacklogAgeSeconds: 60,
        readLatencyP95Ms: 8,
        realm: "globex",
        resourceCount: 1,
      },
    ]);
  });

  it("rolls the resources of one realm up into area rows", () => {
    const acmeRows = rows.filter((row) => row.realm === "acme");

    expect(areaRollupRows(acmeRows)).toEqual([
      {
        area: "payments",
        messagesDeadLettered: 1,
        messagesReady: 10,
        oldestBacklogAgeSeconds: 900,
        readLatencyP95Ms: 12,
        realm: "acme",
        resourceCount: 2,
      },
      {
        area: "support",
        messagesDeadLettered: 0,
        messagesReady: 1,
        oldestBacklogAgeSeconds: 5,
        readLatencyP95Ms: 40,
        realm: "acme",
        resourceCount: 1,
      },
    ]);
  });

  it("preserves structural realms and areas that contain no resources", () => {
    expect(
      realmRollupRows(rows, [
        { realm: "acme", areas: [{ area: "payments" }, { area: "support" }] },
        { realm: "empty", areas: [] },
      ]),
    ).toContainEqual({
      areaCount: 0,
      realm: "empty",
      resourceCount: 0,
    });

    expect(areaRollupRows([], [{ area: "quiet" }], "empty")).toEqual([
      {
        area: "quiet",
        realm: "empty",
        resourceCount: 0,
      },
    ]);
  });
});
