import { state } from "@askrjs/askr";
import { For } from "@askrjs/askr/control";
import { Link } from "@askrjs/askr/router";
import { Button } from "@askrjs/themes/components";
import { Inline, Stack } from "@askrjs/themes/components";
import {
  Badge,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@askrjs/themes/components";
import { Input, Label, VirtualTable, type VirtualTableColumn } from "@askrjs/ui";
import {
  QueryEmptyState,
  QueryErrorState,
  QueryLoadingState,
} from "@/components/shared/query-state";
import type { ResourceInventory } from "@/features/resource/resource-models";
import { domainResourceHref } from "@/shared/navigation/domains";
import { parseConcreteRouteFamilyId, useOperatorContext } from "@/shared/operator-context";
import type {
  KvByteValue,
  KvCommittedPair,
  KvCommittedValueResult,
  KvKeyEncoding,
  KvPrefixScanResult,
} from "./kv-models";
import { kvService } from "./kv-service";

type KvQueryMode = "resource" | "key" | "prefix";

interface KvResourceRow {
  area: string;
  realm: string;
  resource: string;
}

type KvLookupResult =
  | {
      data: KvCommittedValueResult;
      mode: "key";
    }
  | {
      data: KvPrefixScanResult;
      mode: "prefix";
    };

export interface KvStateExplorerProps {
  error?: unknown;
  inventory?: ResourceInventory | null;
  loading?: boolean;
}

const queryModes: Array<{
  description: string;
  label: string;
  value: KvQueryMode;
}> = [
  {
    description: "Use existing realm, area, and resource inventory APIs.",
    label: "Resource",
    value: "resource",
  },
  {
    description: "Read one committed KV value by exact key.",
    label: "Key",
    value: "key",
  },
  {
    description: "Scan committed KV values under a key prefix.",
    label: "Prefix",
    value: "prefix",
  },
];

const encodingOptions: Array<{
  description: string;
  label: string;
  value: KvKeyEncoding;
}> = [
  {
    description: "Send the key text as UTF-8 bytes.",
    label: "UTF-8",
    value: "utf8",
  },
  {
    description: "Decode the key text from base64 before querying.",
    label: "Base64",
    value: "base64",
  },
];

function displayKvBytes(value: KvByteValue) {
  return value.utf8 ?? value.base64;
}

function describeKvBytes(value: KvByteValue) {
  const displayValue = displayKvBytes(value);
  const format = value.utf8 === null ? "base64" : "UTF-8";

  return `${displayValue} (${value.lenBytes} bytes, ${format})`;
}

const resourceColumns: readonly VirtualTableColumn<KvResourceRow>[] = [
  {
    id: "realm",
    header: "Realm",
    width: "24%",
    cellComponent: ({ row }) => <span class="domain-table-cell-truncate">{row.realm}</span>,
  },
  {
    id: "area",
    header: "Area",
    width: "24%",
    cellComponent: ({ row }) => <span class="domain-table-cell-truncate">{row.area}</span>,
  },
  {
    id: "resource",
    header: "Resource",
    width: "34%",
    cellComponent: ({ row }) => <span class="domain-table-cell-truncate">{row.resource}</span>,
  },
  {
    id: "action",
    header: "Action",
    width: "18%",
    cellComponent: ({ row }) => (
      <Link class="text-link" href={domainResourceHref("kv", row)}>
        Inspect
      </Link>
    ),
  },
];

const prefixColumns: readonly VirtualTableColumn<KvCommittedPair>[] = [
  {
    id: "key",
    header: "Key",
    width: "38%",
    cellComponent: ({ row }) => (
      <span class="domain-table-cell-truncate" title={describeKvBytes(row.key)}>
        {displayKvBytes(row.key)}
      </span>
    ),
  },
  {
    id: "value",
    header: "Value",
    width: "44%",
    cellComponent: ({ row }) => (
      <span class="domain-table-cell-truncate" title={describeKvBytes(row.value)}>
        {displayKvBytes(row.value)}
      </span>
    ),
  },
  {
    id: "bytes",
    header: "Bytes",
    width: "18%",
    cellComponent: ({ row }) => (
      <span class="domain-table-cell-truncate">
        {row.key.lenBytes} / {row.value.lenBytes}
      </span>
    ),
  },
];

function flattenInventory(inventory?: ResourceInventory | null): KvResourceRow[] {
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
  return query.trim().length === 0 || value.toLowerCase().includes(query.trim().toLowerCase());
}

function filterRows(
  rows: KvResourceRow[],
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

function isCommittedMode(mode: KvQueryMode) {
  return mode === "key" || mode === "prefix";
}

function KvLookupResultPanel({ result }: { result: KvLookupResult }) {
  if (result.mode === "key") {
    const value = result.data.value;

    return (
      <div class="kv-query-result" aria-live="polite">
        <div class="kv-query-result-grid">
          <span>Route Family</span>
          <strong>{result.data.routeFamily}</strong>
          <span>Key</span>
          <strong title={describeKvBytes(result.data.key)}>
            {displayKvBytes(result.data.key)}
          </strong>
          <span>Status</span>
          <Badge variant={result.data.found ? "success" : "warning"}>
            {result.data.found ? "Found" : "Missing"}
          </Badge>
          <span>Value</span>
          <strong title={value ? describeKvBytes(value) : "No committed value"}>
            {value ? displayKvBytes(value) : "No committed value"}
          </strong>
        </div>
      </div>
    );
  }

  return (
    <div class="kv-query-result" aria-live="polite">
      <Inline justify="between" align="center" gap="3" wrap="wrap">
        <p class="domain-muted">
          {result.data.items.length} committed pair
          {result.data.items.length === 1 ? "" : "s"} for prefix{" "}
          <strong>{displayKvBytes(result.data.prefix)}</strong>
        </p>
        {result.data.hasMore ? <Badge variant="warning">More available</Badge> : null}
      </Inline>
      {result.data.items.length === 0 ? (
        <QueryEmptyState
          title="No committed keys"
          description="No committed KV entries matched this prefix in the selected Route Family."
        />
      ) : (
        <VirtualTable<KvCommittedPair>
          aria-label="Committed KV prefix results"
          class="kv-resource-virtual-table"
          columns={prefixColumns}
          getKey={(row) => row.key.base64}
          headerHeight={44}
          overscan={6}
          rowHeight={48}
          rows={result.data.items}
          style={{ height: "320px" }}
        />
      )}
    </div>
  );
}

export default function KvStateExplorer({
  error,
  inventory,
  loading = false,
}: KvStateExplorerProps) {
  const operatorContext = useOperatorContext();
  const [mode, setMode] = state<KvQueryMode>("resource");
  const [keyEncoding, setKeyEncoding] = state<KvKeyEncoding>("utf8");
  const [realm, setRealm] = state("");
  const [area, setArea] = state("");
  const [resource, setResource] = state("");
  const [keyQuery, setKeyQuery] = state("");
  const [lookupLoading, setLookupLoading] = state(false);
  const [lookupError, setLookupError] = state<unknown>(null);
  const [lookupResult, setLookupResult] = state<KvLookupResult | null>(null);
  const modeValue = mode();
  const keyEncodingValue = keyEncoding();
  const realmValue = realm();
  const areaValue = area();
  const resourceValue = resource();
  const keyQueryValue = keyQuery();
  const lookupLoadingValue = lookupLoading();
  const lookupErrorValue = lookupError();
  const lookupResultValue = lookupResult();
  const rows = flattenInventory(inventory);
  const filteredRows = filterRows(rows, {
    area: areaValue,
    realm: realmValue,
    resource: resourceValue,
  });
  const committedMode = isCommittedMode(modeValue);
  const selectedRouteFamily = operatorContext.selectedRouteFamily;
  const routeFamily = parseConcreteRouteFamilyId(operatorContext.selectedRouteFamilyId);
  const routeFamilyReady = routeFamily !== null;
  const trimmedRealm = realmValue.trim();
  const trimmedArea = areaValue.trim();
  const trimmedResource = resourceValue.trim();
  const trimmedKeyQuery = keyQueryValue.trim();
  const canRunStateQuery =
    committedMode &&
    routeFamilyReady &&
    trimmedRealm.length > 0 &&
    trimmedArea.length > 0 &&
    trimmedResource.length > 0 &&
    (modeValue === "prefix" || trimmedKeyQuery.length > 0) &&
    !lookupLoadingValue;
  const canOpenExactResource =
    !committedMode &&
    filteredRows.some(
      (row) => row.realm === realmValue && row.area === areaValue && row.resource === resourceValue,
    );
  const queryBadgeLabel = committedMode
    ? routeFamilyReady
      ? "Committed API"
      : "Select Route Family"
    : "Inventory API";
  const queryBadgeVariant = committedMode ? (routeFamilyReady ? "success" : "warning") : "outline";

  async function runCommittedQuery() {
    if (!canRunStateQuery || routeFamily === null) {
      return;
    }

    setLookupLoading(true);
    setLookupError(null);
    setLookupResult(null);

    const scope = {
      area: trimmedArea,
      realm: trimmedRealm,
      resource: trimmedResource,
      routeFamily,
    };

    try {
      if (modeValue === "key") {
        setLookupResult({
          data: await kvService.getCommittedValue(scope, trimmedKeyQuery, keyEncodingValue),
          mode: "key",
        });
      } else if (modeValue === "prefix") {
        setLookupResult({
          data: await kvService.scanCommittedPrefix(scope, trimmedKeyQuery, keyEncodingValue),
          mode: "prefix",
        });
      }
    } catch (caughtError) {
      setLookupError(caughtError);
    } finally {
      setLookupLoading(false);
    }
  }

  function onSubmit(event: Event) {
    event.preventDefault();
    void runCommittedQuery();
  }

  return (
    <Card padding="sm" variant="default">
      <CardHeader>
        <Inline justify="between" align="start" gap="3" wrap="wrap">
          <Stack gap="1">
            <CardTitle>Query workspace</CardTitle>
            <CardDescription>
              Filter visible KV resources by realm, area, and resource, or query durable committed
              KV state in the selected Route Family.
            </CardDescription>
          </Stack>
          <Badge variant={queryBadgeVariant}>{queryBadgeLabel}</Badge>
        </Inline>
      </CardHeader>

      <CardContent>
        <Stack gap="3">
          <div class="domain-query-mode-grid" role="group" aria-label="KV query mode">
            <For each={queryModes} by={(queryMode) => queryMode.value}>
              {(queryMode) => (
                <Button
                  type="button"
                  variant={modeValue === queryMode.value ? "primary" : "outline"}
                  onPress={() => {
                    setMode(queryMode.value);
                    setLookupError(null);
                    setLookupResult(null);
                  }}
                  aria-pressed={modeValue === queryMode.value}
                  title={queryMode.description}
                >
                  <span>{queryMode.label}</span>
                </Button>
              )}
            </For>
          </div>

          <form class="kv-query-form" onSubmit={onSubmit}>
            <div class="form-grid">
              <div class="auth-field">
                <Label for="kv-query-realm">Realm</Label>
                <Input
                  id="kv-query-realm"
                  value={realmValue}
                  onInput={(event: Event) => setRealm((event.target as HTMLInputElement).value)}
                  placeholder="billing"
                />
              </div>
              <div class="auth-field">
                <Label for="kv-query-area">Area</Label>
                <Input
                  id="kv-query-area"
                  value={areaValue}
                  onInput={(event: Event) => setArea((event.target as HTMLInputElement).value)}
                  placeholder="payments"
                />
              </div>
              <div class="auth-field">
                <Label for="kv-query-resource">Resource</Label>
                <Input
                  id="kv-query-resource"
                  value={resourceValue}
                  onInput={(event: Event) => setResource((event.target as HTMLInputElement).value)}
                  placeholder="ledger"
                />
              </div>
              <div class="auth-field">
                <Label for="kv-query-key">{modeValue === "prefix" ? "Prefix" : "Key"}</Label>
                <Input
                  id="kv-query-key"
                  value={keyQueryValue}
                  disabled={!committedMode}
                  onInput={(event: Event) => setKeyQuery((event.target as HTMLInputElement).value)}
                  placeholder={modeValue === "prefix" ? "customer:" : "customer:123"}
                />
              </div>
              <div class="auth-field">
                <Label for="kv-key-encoding">Key encoding</Label>
                <div
                  id="kv-key-encoding"
                  class="kv-encoding-controls"
                  role="group"
                  aria-label="KV key encoding"
                >
                  <For each={encodingOptions} by={(encodingOption) => encodingOption.value}>
                    {(encodingOption) => (
                      <Button
                        type="button"
                        variant={keyEncodingValue === encodingOption.value ? "primary" : "outline"}
                        disabled={!committedMode}
                        onPress={() => {
                          setKeyEncoding(encodingOption.value);
                          setLookupError(null);
                          setLookupResult(null);
                        }}
                        aria-pressed={keyEncodingValue === encodingOption.value}
                        title={encodingOption.description}
                      >
                        <span>{encodingOption.label}</span>
                      </Button>
                    )}
                  </For>
                </div>
              </div>
            </div>
            {committedMode ? (
              <Inline class="kv-query-actions" justify="between" align="center" gap="3" wrap="wrap">
                <p class="domain-muted">
                  Querying {selectedRouteFamily.label}. Exact KV reads require a concrete numeric
                  Route Family.
                </p>
                <Button type="submit" disabled={!canRunStateQuery}>
                  {lookupLoadingValue ? "Running" : "Run query"}
                </Button>
              </Inline>
            ) : null}
          </form>

          {committedMode && !routeFamilyReady ? (
            <QueryEmptyState
              title="Concrete Route Family required"
              description="Choose a numeric Route Family from the global selector before reading committed KV state."
            />
          ) : null}

          {committedMode && lookupLoadingValue ? (
            <QueryLoadingState description="Querying committed KV state..." />
          ) : null}
          {committedMode && lookupErrorValue ? (
            <QueryErrorState
              title="Unable to query committed KV state"
              error={lookupErrorValue}
              onRetry={() => void runCommittedQuery()}
            />
          ) : null}
          {committedMode && lookupResultValue && !lookupLoadingValue ? (
            <KvLookupResultPanel result={lookupResultValue} />
          ) : null}

          {loading ? <QueryLoadingState description="Loading KV resources..." /> : null}
          {error ? <QueryErrorState title="Unable to load KV resources" error={error} /> : null}

          {!loading && !error ? (
            filteredRows.length === 0 ? (
              <QueryEmptyState
                title="No matching resources"
                description="Adjust the realm, area, or resource filters to find visible KV resources."
              />
            ) : (
              <Stack gap="3">
                <Inline justify="between" align="center" gap="3" wrap="wrap">
                  <p class="domain-muted">
                    {filteredRows.length} matching resource{filteredRows.length === 1 ? "" : "s"}
                  </p>
                  {canOpenExactResource ? (
                    <Link
                      class="text-link"
                      href={domainResourceHref("kv", {
                        area: areaValue,
                        realm: realmValue,
                        resource: resourceValue,
                      })}
                    >
                      Open exact resource
                    </Link>
                  ) : null}
                </Inline>

                <VirtualTable<KvResourceRow>
                  aria-label="Matching KV resources"
                  class="kv-resource-virtual-table"
                  columns={resourceColumns}
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
