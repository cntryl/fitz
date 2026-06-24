import { state } from "@askrjs/askr";
import { For } from "@askrjs/askr/control";
import { Link } from "@askrjs/askr/router";
import { Button } from "@askrjs/themes/controls";
import { Flex, Stack } from "@askrjs/themes/layouts";
import {
  Badge,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@askrjs/themes/surfaces";
import { Input, Label, VirtualTable, type VirtualTableColumn } from "@askrjs/ui";
import type { KvByteValue, StreamAdminRecord, StreamRecordsResponse } from "@/adapters";
import {
  QueryEmptyState,
  QueryErrorState,
  QueryLoadingState,
} from "@/components/shared/query-state";
import type { ResourceInventory } from "@/features/resource/resource-models";
import { domainResourceHref } from "@/shared/navigation/domains";
import { parseConcreteRouteFamilyId, useOperatorContext } from "@/shared/operator-context";
import { streamService } from "./stream-service";

type StreamHistoryMode = "resource" | "correlation" | "replay";

interface StreamResourceRow {
  area: string;
  realm: string;
  resource: string;
}

export interface StreamHistoryExplorerProps {
  error?: unknown;
  inventory?: ResourceInventory | null;
  loading?: boolean;
}

const historyModes: Array<{
  description: string;
  label: string;
  value: StreamHistoryMode;
}> = [
  {
    description: "Use existing stream resource events and watermark detail APIs.",
    label: "Resource events",
    value: "resource",
  },
  {
    description: "Search committed stream records by route-family scope and discriminator.",
    label: "Correlation trace",
    value: "correlation",
  },
  {
    description: "Read committed records from an offset for replay planning.",
    label: "Replay read",
    value: "replay",
  },
];

const streamColumns: readonly VirtualTableColumn<StreamResourceRow>[] = [
  {
    id: "realm",
    header: "Realm",
    width: "22%",
    cellComponent: ({ row }) => <span class="domain-table-cell-truncate">{row.realm}</span>,
  },
  {
    id: "area",
    header: "Area",
    width: "22%",
    cellComponent: ({ row }) => <span class="domain-table-cell-truncate">{row.area}</span>,
  },
  {
    id: "resource",
    header: "Stream",
    width: "34%",
    cellComponent: ({ row }) => <span class="domain-table-cell-truncate">{row.resource}</span>,
  },
  {
    id: "history",
    header: "History",
    width: "22%",
    cellComponent: ({ row }) => (
      <Link class="text-link" href={domainResourceHref("stream", row)}>
        Open events
      </Link>
    ),
  },
];

const recordColumns: readonly VirtualTableColumn<StreamAdminRecord>[] = [
  {
    id: "scope",
    header: "Scope",
    width: "28%",
    cellComponent: ({ row }) => (
      <span class="domain-table-cell-truncate" title={`${row.realm}/${row.area}/${row.resource}`}>
        {row.realm}/{row.area}/{row.resource}
      </span>
    ),
  },
  {
    id: "offset",
    header: "Offset",
    width: "16%",
    cellComponent: ({ row }) => (
      <span class="domain-table-cell-truncate">{row.resource_offset}</span>
    ),
  },
  {
    id: "created",
    header: "Created",
    width: "22%",
    cellComponent: ({ row }) => (
      <span class="domain-table-cell-truncate" title={formatTimestamp(row.created_at_ms)}>
        {formatTimestamp(row.created_at_ms)}
      </span>
    ),
  },
  {
    id: "body",
    header: "Body",
    width: "20%",
    cellComponent: ({ row }) => (
      <span class="domain-table-cell-truncate" title={describeBytes(row.body)}>
        {displayBytes(row.body)}
      </span>
    ),
  },
  {
    id: "metadata",
    header: "Metadata",
    width: "14%",
    cellComponent: ({ row }) => (
      <span
        class="domain-table-cell-truncate"
        title={row.metadata ? describeBytes(row.metadata) : "None"}
      >
        {row.metadata ? displayBytes(row.metadata) : "None"}
      </span>
    ),
  },
];

function flattenInventory(inventory?: ResourceInventory | null): StreamResourceRow[] {
  return (
    inventory?.realms.flatMap((realm) =>
      realm.areas.flatMap((area) =>
        area.resources.map((resource) => ({
          area: area.area,
          realm: realm.realm,
          resource,
        })),
      ),
    ) ?? []
  );
}

function includesQuery(value: string, query: string) {
  const normalized = query.trim().toLowerCase();

  return normalized.length === 0 || value.toLowerCase().includes(normalized);
}

function filterRows(
  rows: StreamResourceRow[],
  filters: {
    area: string;
    realm: string;
    resource: string;
  },
) {
  return rows.filter(
    (row) =>
      includesQuery(row.realm, filters.realm) &&
      includesQuery(row.area, filters.area) &&
      includesQuery(row.resource, filters.resource),
  );
}

function trimToUndefined(value: string) {
  const trimmed = value.trim();

  return trimmed.length > 0 ? trimmed : undefined;
}

function parseOffsetQuery(value: string) {
  const trimmed = value.trim();
  const firstNumber = trimmed.match(/\d+/)?.[0];

  return firstNumber ? Number(firstNumber) : undefined;
}

function displayBytes(value: KvByteValue) {
  return value.utf8 ?? value.base64;
}

function describeBytes(value: KvByteValue) {
  const format = value.utf8 === null ? "base64" : "UTF-8";

  return `${displayBytes(value)} (${value.len_bytes} bytes, ${format})`;
}

function formatTimestamp(timestampMs: number) {
  if (!Number.isFinite(timestampMs) || timestampMs <= 0) {
    return "Unknown";
  }

  return new Date(timestampMs).toISOString();
}

function isRecordMode(mode: StreamHistoryMode) {
  return mode === "correlation" || mode === "replay";
}

function recordQueryLabel(mode: StreamHistoryMode) {
  if (mode === "replay") return "From offset";
  if (mode === "correlation") return "Discriminator";

  return "Filter";
}

function recordQueryPlaceholder(mode: StreamHistoryMode) {
  if (mode === "replay") return "1200";
  if (mode === "correlation") return "corr-123";

  return "Optional";
}

function StreamRecordsPanel({ result }: { result: StreamRecordsResponse }) {
  return (
    <div class="stream-record-result" aria-live="polite">
      <Flex justify="between" align="center" gap="3" wrap="wrap">
        <p class="domain-muted">
          {result.records.length} committed record
          {result.records.length === 1 ? "" : "s"} in route family {result.route_family}
        </p>
        {result.has_more ? <Badge variant="warning">More available</Badge> : null}
      </Flex>

      {result.records.length === 0 ? (
        <QueryEmptyState
          title="No committed records"
          description="No committed stream records matched the selected route family and scope."
        />
      ) : (
        <VirtualTable<StreamAdminRecord>
          aria-label="Committed stream records"
          class="stream-resource-virtual-table"
          columns={recordColumns}
          getKey={(row) =>
            `${row.route_family}:${row.realm}:${row.area}:${row.resource}:${row.resource_offset}`
          }
          headerHeight={44}
          overscan={8}
          rowHeight={48}
          rows={result.records}
          style={{ height: "320px" }}
        />
      )}
    </div>
  );
}

export default function StreamHistoryExplorer({
  error,
  inventory,
  loading = false,
}: StreamHistoryExplorerProps) {
  const operatorContext = useOperatorContext();
  const [mode, setMode] = state<StreamHistoryMode>("resource");
  const [realm, setRealm] = state("");
  const [area, setArea] = state("");
  const [resource, setResource] = state("");
  const [historyQuery, setHistoryQuery] = state("");
  const [recordLoading, setRecordLoading] = state(false);
  const [recordError, setRecordError] = state<unknown>(null);
  const [recordResult, setRecordResult] = state<StreamRecordsResponse | null>(null);
  const modeValue = mode();
  const realmValue = realm();
  const areaValue = area();
  const resourceValue = resource();
  const historyQueryValue = historyQuery();
  const recordLoadingValue = recordLoading();
  const recordErrorValue = recordError();
  const recordResultValue = recordResult();
  const rows = flattenInventory(inventory);
  const filteredRows = filterRows(rows, {
    area: areaValue,
    realm: realmValue,
    resource: resourceValue,
  });
  const routeFamily = parseConcreteRouteFamilyId(operatorContext.selectedRouteFamilyId);
  const routeFamilyReady = routeFamily !== null;
  const recordMode = isRecordMode(modeValue);
  const trimmedRealm = trimToUndefined(realmValue);
  const trimmedArea = trimToUndefined(areaValue);
  const trimmedResource = trimToUndefined(resourceValue);
  const trimmedQuery = trimToUndefined(historyQueryValue);
  const canRunRecordQuery = recordMode && routeFamilyReady && !recordLoadingValue;
  const canOpenExactResource =
    !recordMode &&
    filteredRows.some(
      (row) => row.realm === realmValue && row.area === areaValue && row.resource === resourceValue,
    );
  const badgeLabel = recordMode
    ? routeFamilyReady
      ? "Existing API"
      : "Select Route Family"
    : "Existing API";
  const badgeVariant = recordMode ? (routeFamilyReady ? "success" : "warning") : "outline";

  async function runRecordQuery() {
    if (!canRunRecordQuery || routeFamily === null) {
      return;
    }

    setRecordLoading(true);
    setRecordError(null);
    setRecordResult(null);

    try {
      const scope = {
        area: trimmedArea,
        discriminator: modeValue === "correlation" ? trimmedQuery : undefined,
        fromOffset: modeValue === "replay" ? parseOffsetQuery(historyQueryValue) : undefined,
        limit: 50,
        realm: trimmedRealm,
        resource: trimmedResource,
        routeFamily,
      };

      if (scope.realm && scope.area && scope.resource) {
        setRecordResult(
          await streamService.readResourceRecords({
            area: scope.area,
            discriminator: scope.discriminator,
            fromOffset: scope.fromOffset,
            limit: scope.limit,
            realm: scope.realm,
            resource: scope.resource,
            routeFamily,
          }),
        );
      } else {
        setRecordResult(await streamService.searchRecords(scope));
      }
    } catch (caughtError) {
      setRecordError(caughtError);
    } finally {
      setRecordLoading(false);
    }
  }

  function onSubmit(event: Event) {
    event.preventDefault();
    void runRecordQuery();
  }

  return (
    <Card padding="sm" variant="default">
      <CardHeader>
        <Flex justify="between" align="start" gap="3" wrap="wrap">
          <Stack gap="1">
            <CardTitle>History workspace</CardTitle>
            <CardDescription>
              Locate stream resources by realm, area, and resource, then inspect resource-level
              events and consumer watermarks through the existing admin APIs.
            </CardDescription>
          </Stack>
          <Badge variant={badgeVariant}>{badgeLabel}</Badge>
        </Flex>
      </CardHeader>

      <CardContent>
        <Stack gap="3">
          <div class="domain-query-mode-grid" role="group" aria-label="Stream history mode">
            <For each={historyModes} by={(historyMode) => historyMode.value}>
              {(historyMode) => (
                <Button
                  type="button"
                  variant={modeValue === historyMode.value ? "primary" : "outline"}
                  onPress={() => {
                    setMode(historyMode.value);
                    setRecordError(null);
                    setRecordResult(null);
                  }}
                  aria-pressed={modeValue === historyMode.value}
                  title={historyMode.description}
                >
                  <span>{historyMode.label}</span>
                </Button>
              )}
            </For>
          </div>

          <form class="stream-history-form" onSubmit={onSubmit}>
            <div class="form-grid">
              <div class="auth-field">
                <Label for="stream-history-realm">Realm</Label>
                <Input
                  id="stream-history-realm"
                  value={realmValue}
                  onInput={(event: Event) => setRealm((event.target as HTMLInputElement).value)}
                  placeholder="billing"
                />
              </div>
              <div class="auth-field">
                <Label for="stream-history-area">Area</Label>
                <Input
                  id="stream-history-area"
                  value={areaValue}
                  onInput={(event: Event) => setArea((event.target as HTMLInputElement).value)}
                  placeholder="payments"
                />
              </div>
              <div class="auth-field">
                <Label for="stream-history-resource">Resource</Label>
                <Input
                  id="stream-history-resource"
                  value={resourceValue}
                  onInput={(event: Event) => setResource((event.target as HTMLInputElement).value)}
                  placeholder="ledger-events"
                />
              </div>
              <div class="auth-field">
                <Label for="stream-history-query">{recordQueryLabel(modeValue)}</Label>
                <Input
                  id="stream-history-query"
                  value={historyQueryValue}
                  disabled={!recordMode}
                  onInput={(event: Event) =>
                    setHistoryQuery((event.target as HTMLInputElement).value)
                  }
                  placeholder={recordQueryPlaceholder(modeValue)}
                />
              </div>
            </div>
            {recordMode ? (
              <Flex
                class="stream-query-actions"
                justify="between"
                align="center"
                gap="3"
                wrap="wrap"
              >
                <p class="domain-muted">
                  Querying {operatorContext.selectedRouteFamily.label}. Stream reads require a
                  concrete numeric Route Family.
                </p>
                <Button type="submit" disabled={!canRunRecordQuery}>
                  {recordLoadingValue ? "Running" : "Run read"}
                </Button>
              </Flex>
            ) : null}
          </form>

          {recordMode && !routeFamilyReady ? (
            <QueryEmptyState
              title="Concrete Route Family required"
              description="Choose a numeric Route Family from the global selector before reading committed stream records."
            />
          ) : null}

          {recordMode && recordLoadingValue ? (
            <QueryLoadingState description="Reading committed stream records..." />
          ) : null}
          {recordMode && recordErrorValue ? (
            <QueryErrorState
              title="Unable to read stream records"
              error={recordErrorValue}
              onRetry={() => void runRecordQuery()}
            />
          ) : null}
          {recordMode && recordResultValue && !recordLoadingValue ? (
            <StreamRecordsPanel result={recordResultValue} />
          ) : null}

          {loading ? <QueryLoadingState description="Loading stream resources..." /> : null}
          {error ? <QueryErrorState title="Unable to load stream resources" error={error} /> : null}

          {!loading && !error ? (
            filteredRows.length === 0 ? (
              <QueryEmptyState
                title="No matching streams"
                description="Adjust the realm, area, or resource filters to find visible stream resources."
              />
            ) : (
              <Stack gap="3">
                <Flex justify="between" align="center" gap="3" wrap="wrap">
                  <p class="domain-muted">
                    {filteredRows.length} matching stream{filteredRows.length === 1 ? "" : "s"}
                  </p>
                  {canOpenExactResource ? (
                    <Link
                      class="text-link"
                      href={domainResourceHref("stream", {
                        area: areaValue,
                        realm: realmValue,
                        resource: resourceValue,
                      })}
                    >
                      Open exact stream
                    </Link>
                  ) : null}
                </Flex>

                <VirtualTable<StreamResourceRow>
                  aria-label="Matching stream resources"
                  class="stream-resource-virtual-table"
                  columns={streamColumns}
                  getKey={(row) => `${row.realm}:${row.area}:${row.resource}`}
                  headerHeight={44}
                  overscan={6}
                  rowHeight={48}
                  rows={filteredRows}
                  style={{ height: "384px" }}
                />
              </Stack>
            )
          ) : null}
        </Stack>
      </CardContent>
    </Card>
  );
}
