import type { ResourceInventoryResource } from "@/features/resource/resource-models";

/**
 * Any row a domain metric column can read. Resource rows carry their own values;
 * realm and area rows carry the rollup of their children in the same field names,
 * so one column definition renders identically at every drilldown tier.
 */
export type DomainMetricRow = Partial<ResourceInventoryResource>;

export interface DomainScopeRow extends DomainMetricRow {
  area?: string;
  areaCount?: number;
  realm: string;
  resourceCount: number;
}

export interface DomainRealmScope {
  areas: readonly { area: string }[];
  realm: string;
}

type NumericField = {
  [Key in keyof ResourceInventoryResource]-?: NonNullable<
    ResourceInventoryResource[Key]
  > extends number
    ? Key
    : never;
}[keyof ResourceInventoryResource];

/** Counts, sizes, and rates add up across a scope. */
const SUM_FIELDS = [
  "activeLeases",
  "committedEventCount",
  "estimatedRecordCount",
  "estimatedStorageBytes",
  "messagesDeadLettered",
  "messagesDelayed",
  "messagesInflight",
  "messagesReady",
  "notificationsReceived",
  "pendingClaims",
  "publishesPerMinute",
  "requestsPending",
  "schedulesActive",
  "sessionsActive",
  "sizeBytes",
  "subscriptionsActive",
  "transactionsActive",
  "waiters",
  "workersRegistered",
] as const satisfies readonly NumericField[];

/**
 * Latencies and ages cannot be summed or averaged without weights the API does not
 * report, so a scope shows the worst value any of its resources reported.
 */
const MAX_FIELDS = [
  "oldestBacklogAgeSeconds",
  "oldestLeaseAgeSeconds",
  "readLatencyAvgMs",
  "readLatencyP95Ms",
  "slowestWorkerAverageLatencyMs",
  "writeLatencyAvgMs",
  "writeLatencyP95Ms",
] as const satisfies readonly NumericField[];

function numericValue(row: DomainMetricRow, field: NumericField) {
  const value = row[field];

  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function earliestNextRun(rows: readonly DomainMetricRow[]) {
  let earliest: string | null = null;
  let earliestMs = Number.POSITIVE_INFINITY;
  let reported = false;

  for (const row of rows) {
    if (!("nextRun" in row)) continue;
    reported = true;

    const parsed = row.nextRun ? Date.parse(row.nextRun) : Number.NaN;
    if (!Number.isNaN(parsed) && parsed < earliestMs) {
      earliestMs = parsed;
      earliest = row.nextRun ?? null;
    }
  }

  return reported ? { nextRun: earliest } : {};
}

/**
 * Folds child rows into one rollup row. A field stays absent unless at least one
 * child reported it, so a domain that reports no metrics still renders no columns.
 */
export function aggregateDomainMetricRows(rows: readonly DomainMetricRow[]): DomainMetricRow {
  const rollup: DomainMetricRow = {};

  for (const field of SUM_FIELDS) {
    let total = 0;
    let reported = false;

    for (const row of rows) {
      const value = numericValue(row, field);
      if (value !== null) {
        total += value;
        reported = true;
      }
    }

    if (reported) rollup[field] = total;
  }

  for (const field of MAX_FIELDS) {
    let worst: number | null = null;

    for (const row of rows) {
      const value = numericValue(row, field);
      if (value !== null && (worst === null || value > worst)) {
        worst = value;
      }
    }

    if (worst !== null) rollup[field] = worst;
  }

  const estimates = rows
    .map((row) => row.estimateComplete)
    .filter((value): value is boolean => value !== undefined);

  if (estimates.length > 0) {
    rollup.estimateComplete = estimates.every(Boolean);
  }

  return { ...rollup, ...earliestNextRun(rows) };
}

function groupBy<Row>(rows: readonly Row[], key: (row: Row) => string) {
  const groups = new Map<string, Row[]>();

  for (const row of rows) {
    const group = groups.get(key(row));

    if (group) {
      group.push(row);
    } else {
      groups.set(key(row), [row]);
    }
  }

  return groups;
}

export function realmRollupRows<Row extends DomainMetricRow & { area: string; realm: string }>(
  rows: readonly Row[],
  realms?: readonly DomainRealmScope[],
): DomainScopeRow[] {
  const rowsByRealm = groupBy(rows, (row) => row.realm);
  const scopes =
    realms ??
    Array.from(rowsByRealm, ([realm, realmRows]) => ({
      areas: Array.from(new Set(realmRows.map((row) => row.area)), (area) => ({ area })),
      realm,
    }));

  return scopes.map(({ areas, realm }) => {
    const realmRows = rowsByRealm.get(realm) ?? [];

    return {
      ...aggregateDomainMetricRows(realmRows),
      areaCount: areas.length,
      realm,
      resourceCount: realmRows.length,
    };
  });
}

export function areaRollupRows<Row extends DomainMetricRow & { area: string; realm: string }>(
  rows: readonly Row[],
  areas?: readonly { area: string }[],
  realm?: string,
): DomainScopeRow[] {
  const rowsByArea = groupBy(rows, (row) => row.area);
  const scopes = areas ?? Array.from(rowsByArea, ([area]) => ({ area }));

  return scopes.map(({ area }) => {
    const areaRows = rowsByArea.get(area) ?? [];

    return {
      ...aggregateDomainMetricRows(areaRows),
      area,
      realm: realm ?? areaRows[0]?.realm ?? "",
      resourceCount: areaRows.length,
    };
  });
}
