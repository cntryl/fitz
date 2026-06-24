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
import type { LeaseSearchItem, LeaseSearchResponse } from "@/adapters";
import {
  QueryEmptyState,
  QueryErrorState,
  QueryLoadingState,
} from "@/components/shared/query-state";
import type { ResourceInventory } from "@/features/resource/resource-models";
import { domainResourceHref } from "@/shared/navigation/domains";
import { parseConcreteRouteFamilyId, useOperatorContext } from "@/shared/operator-context";
import { leaseService } from "./lease-service";

type LeaseConsoleMode = "resource" | "history" | "owner" | "contention";

interface LeaseResourceRow {
  area: string;
  realm: string;
  resource: string;
}

export interface LeaseOwnershipConsoleProps {
  error?: unknown;
  inventory?: ResourceInventory | null;
  loading?: boolean;
}

const consoleModes: Array<{
  description: string;
  label: string;
  value: LeaseConsoleMode;
}> = [
  {
    description: "Use existing lease resource detail, bounded event timeline, and compare APIs.",
    label: "Ownership",
    value: "resource",
  },
  {
    description: "Use existing resource-level ownership-change timeline evidence.",
    label: "History",
    value: "history",
  },
  {
    description: "Search current broker-local lease owners by Route Family and scope.",
    label: "Owner search",
    value: "owner",
  },
  {
    description: "Search current owners and waiters where contention is visible.",
    label: "Contention",
    value: "contention",
  },
];

const leaseColumns: readonly VirtualTableColumn<LeaseResourceRow>[] = [
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
    header: "Lease",
    width: "34%",
    cellComponent: ({ row }) => <span class="domain-table-cell-truncate">{row.resource}</span>,
  },
  {
    id: "action",
    header: "Console",
    width: "22%",
    cellComponent: ({ row }) => (
      <Link class="text-link" href={domainResourceHref("lease", row)}>
        Open lease
      </Link>
    ),
  },
];

const leaseSearchColumns: readonly VirtualTableColumn<LeaseSearchItem>[] = [
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
    id: "state",
    header: "State",
    width: "18%",
    cellComponent: ({ row }) => (
      <Badge variant={row.state === "waiting" ? "warning" : "success"}>{row.state}</Badge>
    ),
  },
  {
    id: "owner",
    header: "Owner/session",
    width: "24%",
    cellComponent: ({ row }) => (
      <span
        class="domain-table-cell-truncate"
        title={row.owner_session_id ?? row.owner_id ?? "None"}
      >
        {row.owner_session_id ?? row.owner_id ?? "None"}
      </span>
    ),
  },
  {
    id: "waiters",
    header: "Waiters",
    width: "12%",
    cellComponent: ({ row }) => <span>{row.pending_waiters}</span>,
  },
  {
    id: "token",
    header: "Token",
    width: "18%",
    cellComponent: ({ row }) => (
      <span class="domain-table-cell-truncate">{row.queued_token ?? "None"}</span>
    ),
  },
];

function flattenInventory(inventory?: ResourceInventory | null): LeaseResourceRow[] {
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
  rows: LeaseResourceRow[],
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

function isSearchMode(mode: LeaseConsoleMode) {
  return mode === "owner" || mode === "contention";
}

function modeQueryLabel(mode: LeaseConsoleMode) {
  if (mode === "owner") return "Owner/session";
  if (mode === "contention") return "Waiter/session";
  if (mode === "history") return "Window";

  return "Token/session";
}

function modeQueryPlaceholder(mode: LeaseConsoleMode) {
  if (mode === "owner") return "session-123";
  if (mode === "contention") return "waiter-123";
  if (mode === "history") return "last 1h";

  return "session-123";
}

function searchStateForMode(mode: LeaseConsoleMode) {
  if (mode === "owner") return "owned" as const;
  if (mode === "contention") return "contention" as const;

  return undefined;
}

function LeaseSearchPanel({ result }: { result: LeaseSearchResponse }) {
  return (
    <div class="lease-search-result" aria-live="polite">
      <Flex justify="between" align="center" gap="3" wrap="wrap">
        <p class="domain-muted">
          {result.items.length} lease row{result.items.length === 1 ? "" : "s"} in route family{" "}
          {result.route_family}
        </p>
      </Flex>

      {result.items.length === 0 ? (
        <QueryEmptyState
          title="No lease evidence"
          description="No current lease owners or waiters matched the selected Route Family and scope."
        />
      ) : (
        <VirtualTable<LeaseSearchItem>
          aria-label="Lease ownership search results"
          class="lease-resource-virtual-table"
          columns={leaseSearchColumns}
          getKey={(row) =>
            `${row.route_family}:${row.realm}:${row.area}:${row.resource}:${row.state}:${row.owner_session_id ?? row.owner_id ?? "none"}:${row.queued_token ?? "none"}`
          }
          headerHeight={44}
          overscan={6}
          rowHeight={48}
          rows={result.items}
          style={{ height: "320px" }}
        />
      )}
    </div>
  );
}

export default function LeaseOwnershipConsole({
  error,
  inventory,
  loading = false,
}: LeaseOwnershipConsoleProps) {
  const operatorContext = useOperatorContext();
  const [mode, setMode] = state<LeaseConsoleMode>("resource");
  const [realm, setRealm] = state("");
  const [area, setArea] = state("");
  const [resource, setResource] = state("");
  const [ownerQuery, setOwnerQuery] = state("");
  const [searchLoading, setSearchLoading] = state(false);
  const [searchError, setSearchError] = state<unknown>(null);
  const [searchResult, setSearchResult] = state<LeaseSearchResponse | null>(null);
  const modeValue = mode();
  const realmValue = realm();
  const areaValue = area();
  const resourceValue = resource();
  const ownerQueryValue = ownerQuery();
  const searchLoadingValue = searchLoading();
  const searchErrorValue = searchError();
  const searchResultValue = searchResult();
  const rows = flattenInventory(inventory);
  const filteredRows = filterRows(rows, {
    area: areaValue,
    realm: realmValue,
    resource: resourceValue,
  });
  const routeFamily = parseConcreteRouteFamilyId(operatorContext.selectedRouteFamilyId);
  const routeFamilyReady = routeFamily !== null;
  const searchMode = isSearchMode(modeValue);
  const trimmedRealm = trimToUndefined(realmValue);
  const trimmedArea = trimToUndefined(areaValue);
  const trimmedResource = trimToUndefined(resourceValue);
  const trimmedOwner = trimToUndefined(ownerQueryValue);
  const canRunSearch = searchMode && routeFamilyReady && !searchLoadingValue;
  const canOpenExactResource = filteredRows.some(
    (row) => row.realm === realmValue && row.area === areaValue && row.resource === resourceValue,
  );
  const badgeLabel = searchMode
    ? routeFamilyReady
      ? "Existing API"
      : "Select Route Family"
    : "Existing API";
  const badgeVariant = searchMode ? (routeFamilyReady ? "success" : "warning") : "outline";

  async function runLeaseSearch() {
    if (!canRunSearch || routeFamily === null) {
      return;
    }

    setSearchLoading(true);
    setSearchError(null);
    setSearchResult(null);

    try {
      setSearchResult(
        await leaseService.searchOwnership({
          area: trimmedArea,
          limit: 50,
          owner: trimmedOwner,
          realm: trimmedRealm,
          resource: trimmedResource,
          routeFamily,
          state: searchStateForMode(modeValue),
        }),
      );
    } catch (caughtError) {
      setSearchError(caughtError);
    } finally {
      setSearchLoading(false);
    }
  }

  function onSubmit(event: Event) {
    event.preventDefault();
    void runLeaseSearch();
  }

  return (
    <Card padding="sm" variant="default">
      <CardHeader>
        <Flex justify="between" align="start" gap="3" wrap="wrap">
          <Stack gap="1">
            <CardTitle>Ownership console</CardTitle>
            <CardDescription>
              Locate lease resources by realm, area, and resource, then inspect broker-local
              ownership, waiter pressure, contention, and bounded ownership-change history.
            </CardDescription>
          </Stack>
          <Badge variant={badgeVariant}>{badgeLabel}</Badge>
        </Flex>
      </CardHeader>

      <CardContent>
        <Stack gap="3">
          <div class="domain-query-mode-grid" role="group" aria-label="Lease ownership mode">
            <For each={consoleModes} by={(consoleMode) => consoleMode.value}>
              {(consoleMode) => (
                <Button
                  type="button"
                  variant={modeValue === consoleMode.value ? "primary" : "outline"}
                  onPress={() => {
                    setMode(consoleMode.value);
                    setSearchError(null);
                    setSearchResult(null);
                  }}
                  aria-pressed={modeValue === consoleMode.value}
                  title={consoleMode.description}
                >
                  <span>{consoleMode.label}</span>
                </Button>
              )}
            </For>
          </div>

          <form class="lease-console-form" onSubmit={onSubmit}>
            <div class="form-grid">
              <div class="auth-field">
                <Label for="lease-console-realm">Realm</Label>
                <Input
                  id="lease-console-realm"
                  value={realmValue}
                  onInput={(event: Event) => setRealm((event.target as HTMLInputElement).value)}
                  placeholder="billing"
                />
              </div>
              <div class="auth-field">
                <Label for="lease-console-area">Area</Label>
                <Input
                  id="lease-console-area"
                  value={areaValue}
                  onInput={(event: Event) => setArea((event.target as HTMLInputElement).value)}
                  placeholder="payments"
                />
              </div>
              <div class="auth-field">
                <Label for="lease-console-resource">Resource</Label>
                <Input
                  id="lease-console-resource"
                  value={resourceValue}
                  onInput={(event: Event) => setResource((event.target as HTMLInputElement).value)}
                  placeholder="settlement-lock"
                />
              </div>
              <div class="auth-field">
                <Label for="lease-console-owner">{modeQueryLabel(modeValue)}</Label>
                <Input
                  id="lease-console-owner"
                  value={ownerQueryValue}
                  disabled={modeValue === "resource" || modeValue === "history"}
                  onInput={(event: Event) =>
                    setOwnerQuery((event.target as HTMLInputElement).value)
                  }
                  placeholder={modeQueryPlaceholder(modeValue)}
                />
              </div>
            </div>
            {searchMode ? (
              <Flex
                class="lease-query-actions"
                justify="between"
                align="center"
                gap="3"
                wrap="wrap"
              >
                <p class="domain-muted">
                  Querying {operatorContext.selectedRouteFamily.label}. Lease ownership reads
                  require a concrete numeric Route Family.
                </p>
                <Button type="submit" disabled={!canRunSearch}>
                  {searchLoadingValue ? "Running" : "Run search"}
                </Button>
              </Flex>
            ) : null}
          </form>

          {searchMode && !routeFamilyReady ? (
            <QueryEmptyState
              title="Concrete Route Family required"
              description="Choose a numeric Route Family from the global selector before searching lease ownership."
            />
          ) : null}

          {searchMode && searchLoadingValue ? (
            <QueryLoadingState description="Searching lease ownership..." />
          ) : null}
          {searchMode && searchErrorValue ? (
            <QueryErrorState
              title="Unable to search lease ownership"
              error={searchErrorValue}
              onRetry={() => void runLeaseSearch()}
            />
          ) : null}
          {searchMode && searchResultValue && !searchLoadingValue ? (
            <LeaseSearchPanel result={searchResultValue} />
          ) : null}

          {modeValue === "history" ? (
            <Card padding="sm" variant="default">
              <CardHeader>
                <CardTitle>History is bounded and broker-local</CardTitle>
                <CardDescription>
                  Select a lease resource to inspect recent ownership-change events. Lease state is
                  ephemeral and must not be treated as durable ownership continuity.
                </CardDescription>
              </CardHeader>
            </Card>
          ) : null}

          {loading ? <QueryLoadingState description="Loading lease resources..." /> : null}
          {error ? <QueryErrorState title="Unable to load lease resources" error={error} /> : null}

          {!loading && !error ? (
            filteredRows.length === 0 ? (
              <QueryEmptyState
                title="No matching leases"
                description="Adjust the realm, area, or resource filters to find visible lease resources."
              />
            ) : (
              <Stack gap="3">
                <Flex justify="between" align="center" gap="3" wrap="wrap">
                  <p class="domain-muted">
                    {filteredRows.length} matching lease{filteredRows.length === 1 ? "" : "s"}
                  </p>
                  {canOpenExactResource ? (
                    <Link
                      class="text-link"
                      href={domainResourceHref("lease", {
                        area: areaValue,
                        realm: realmValue,
                        resource: resourceValue,
                      })}
                    >
                      Open exact lease
                    </Link>
                  ) : null}
                </Flex>

                <VirtualTable<LeaseResourceRow>
                  aria-label="Matching lease resources"
                  class="lease-resource-virtual-table"
                  columns={leaseColumns}
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
