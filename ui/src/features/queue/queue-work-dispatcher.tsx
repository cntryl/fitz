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
import type { QueueInventory } from "@/features/queue/queue-models";
import { searchService } from "@/features/search/search-service";
import type { AdminSearchResult, AdminSearchResults } from "@/features/search/search-models";
import { domainResourceHref, formatFitzRoute } from "@/shared/navigation/domains";
import { useOperatorContext } from "@/shared/operator-context";

type QueueDispatcherMode = "resource" | "message" | "worker" | "dlq";

interface QueueResourceRow {
  area: string;
  realm: string;
  resource: string;
}

export interface QueueWorkDispatcherProps {
  error?: unknown;
  inventory?: QueueInventory | null;
  loading?: boolean;
}

const dispatcherModes: Array<{
  description: string;
  label: string;
  value: QueueDispatcherMode;
}> = [
  {
    description:
      "Use existing queue resource detail, inflight, dead-letter, event, and compare APIs.",
    label: "Resources",
    value: "resource",
  },
  {
    description: "Use global admin search across queue resources, inflight entries, and DLQ rows.",
    label: "Messages",
    value: "message",
  },
  {
    description: "Use global admin search across queue inflight session ownership.",
    label: "Workers",
    value: "worker",
  },
  {
    description: "Uses resource-level dead-letter APIs after a queue resource is selected.",
    label: "DLQ",
    value: "dlq",
  },
];

const queueColumns: readonly VirtualTableColumn<QueueResourceRow>[] = [
  {
    id: "route",
    header: "Route",
    width: "76%",
    cellComponent: ({ row }) => {
      const route = formatFitzRoute("queue", row);

      return (
        <span class="domain-table-cell-truncate" title={route}>
          {route}
        </span>
      );
    },
  },
  {
    id: "action",
    header: "Dispatcher",
    width: "24%",
    cellComponent: ({ row }) => (
      <Link class="text-link" href={domainResourceHref("queue", row)}>
        Open work
      </Link>
    ),
  },
];

const searchColumns: readonly VirtualTableColumn<AdminSearchResult>[] = [
  {
    id: "title",
    header: "Result",
    width: "26%",
    cellComponent: ({ row }) => (
      <Link class="text-link" href={row.href}>
        {row.title}
      </Link>
    ),
  },
  {
    id: "kind",
    header: "Kind",
    width: "18%",
    cellComponent: ({ row }) => <Badge variant="outline">{row.kind}</Badge>,
  },
  {
    id: "route",
    header: "Route",
    width: "28%",
    cellComponent: ({ row }) => {
      const route = formatFitzRoute(row.domain, row);

      return (
        <span class="domain-table-cell-truncate" title={route}>
          {route}
        </span>
      );
    },
  },
  {
    id: "summary",
    header: "Summary",
    width: "28%",
    cellComponent: ({ row }) => (
      <span class="domain-table-cell-truncate" title={row.summary}>
        {row.summary}
      </span>
    ),
  },
];

function flattenInventory(inventory?: QueueInventory | null): QueueResourceRow[] {
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
  rows: QueueResourceRow[],
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

function isSearchMode(mode: QueueDispatcherMode) {
  return mode === "message" || mode === "worker" || mode === "dlq";
}

function modeQueryLabel(mode: QueueDispatcherMode) {
  if (mode === "worker") return "Worker/session";
  if (mode === "message" || mode === "dlq") return "Message id";

  return "Message id";
}

function modeQueryPlaceholder(mode: QueueDispatcherMode) {
  if (mode === "worker") return "session-123";
  if (mode === "dlq") return "42";

  return "message-42";
}

function modeDetailTitle(mode: QueueDispatcherMode) {
  return mode === "worker" ? "Worker search" : "Message search";
}

function modeDetailDescription(mode: QueueDispatcherMode) {
  if (mode === "worker") {
    return "Admin search indexes queue inflight session ownership across visible queue resources.";
  }

  return "Admin search indexes queue resources, inflight messages, and dead-letter rows across visible queue resources.";
}

function QueueSearchPanel({ result }: { result: AdminSearchResults }) {
  return (
    <div class="queue-search-result" aria-live="polite">
      <Inline justify="between" align="center" gap="3" wrap="wrap">
        <p class="domain-muted">
          {result.total} queue result{result.total === 1 ? "" : "s"}
          {result.routeFamily ? ` in route family ${result.routeFamily}` : ""}
        </p>
        {result.truncated ? <Badge variant="warning">Truncated</Badge> : null}
      </Inline>

      {result.results.length === 0 ? (
        <QueryEmptyState
          title="No queue evidence"
          description="No queue resources, inflight messages, or dead-letter rows matched this search."
        />
      ) : (
        <VirtualTable<AdminSearchResult>
          aria-label="Queue admin search results"
          class="queue-resource-virtual-table"
          columns={searchColumns}
          getKey={(row) => row.id}
          headerHeight={44}
          overscan={6}
          rowHeight={48}
          rows={result.results}
          style={{ height: "320px" }}
        />
      )}
    </div>
  );
}

export default function QueueWorkDispatcher({
  error,
  inventory,
  loading = false,
}: QueueWorkDispatcherProps) {
  const operatorContext = useOperatorContext();
  const [mode, setMode] = state<QueueDispatcherMode>("resource");
  const [realm, setRealm] = state("");
  const [area, setArea] = state("");
  const [resource, setResource] = state("");
  const [messageQuery, setMessageQuery] = state("");
  const [searchLoading, setSearchLoading] = state(false);
  const [searchError, setSearchError] = state<unknown>(null);
  const [searchResult, setSearchResult] = state<AdminSearchResults | null>(null);
  const modeValue = mode();
  const realmValue = realm();
  const areaValue = area();
  const resourceValue = resource();
  const messageQueryValue = messageQuery();
  const searchLoadingValue = searchLoading();
  const searchErrorValue = searchError();
  const searchResultValue = searchResult();
  const rows = flattenInventory(inventory);
  const filteredRows = filterRows(rows, {
    area: areaValue,
    realm: realmValue,
    resource: resourceValue,
  });
  const searchMode = isSearchMode(modeValue);
  const canRunSearch = searchMode && !searchLoadingValue;
  const canOpenExactResource = filteredRows.some(
    (row) => row.realm === realmValue && row.area === areaValue && row.resource === resourceValue,
  );
  const badgeLabel = searchMode ? "Queue admin search" : "Live admin data";
  const badgeVariant = searchMode ? "success" : "outline";

  async function runSearch() {
    if (!canRunSearch) {
      return;
    }

    setSearchLoading(true);
    setSearchError(null);
    setSearchResult(null);

    try {
      setSearchResult(
        await searchService.searchAdminState({
          area: areaValue.trim() || undefined,
          domain: "queue",
          limit: 50,
          query: messageQueryValue.trim(),
          realm: realmValue.trim() || undefined,
          resource: resourceValue.trim() || undefined,
          routeFamily: operatorContext.selectedRouteFamilyId,
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
    void runSearch();
  }

  return (
    <Card padding="sm" variant="default">
      <CardHeader>
        <Inline justify="between" align="start" gap="3" wrap="wrap">
          <Stack gap="1">
            <CardTitle>Work dispatcher</CardTitle>
            <CardDescription>
              Locate queue resources, then inspect messages, inflight ownership, failures, retries,
              and DLQ decisions through the resource-level Queue APIs.
            </CardDescription>
          </Stack>
          <Badge variant={badgeVariant}>{badgeLabel}</Badge>
        </Inline>
      </CardHeader>

      <CardContent>
        <Stack gap="3">
          <div class="domain-query-mode-grid" role="group" aria-label="Queue dispatcher mode">
            <For each={dispatcherModes} by={(dispatcherMode) => dispatcherMode.value}>
              {(dispatcherMode) => (
                <Button
                  type="button"
                  variant={modeValue === dispatcherMode.value ? "primary" : "outline"}
                  onPress={() => {
                    setMode(dispatcherMode.value);
                    setSearchError(null);
                    setSearchResult(null);
                  }}
                  aria-pressed={modeValue === dispatcherMode.value}
                  title={dispatcherMode.description}
                >
                  <span>{dispatcherMode.label}</span>
                </Button>
              )}
            </For>
          </div>

          <form class="queue-dispatcher-form" onSubmit={onSubmit}>
            <div class="form-grid">
              <div class="auth-field">
                <Label for="queue-dispatcher-realm">Realm</Label>
                <Input
                  id="queue-dispatcher-realm"
                  value={realmValue}
                  onInput={(event: Event) => setRealm((event.target as HTMLInputElement).value)}
                  placeholder="billing"
                />
              </div>
              <div class="auth-field">
                <Label for="queue-dispatcher-area">Area</Label>
                <Input
                  id="queue-dispatcher-area"
                  value={areaValue}
                  onInput={(event: Event) => setArea((event.target as HTMLInputElement).value)}
                  placeholder="payments"
                />
              </div>
              <div class="auth-field">
                <Label for="queue-dispatcher-resource">Resource</Label>
                <Input
                  id="queue-dispatcher-resource"
                  value={resourceValue}
                  onInput={(event: Event) => setResource((event.target as HTMLInputElement).value)}
                  placeholder="settlement-queue"
                />
              </div>
              <div class="auth-field">
                <Label for="queue-dispatcher-message">{modeQueryLabel(modeValue)}</Label>
                <Input
                  id="queue-dispatcher-message"
                  value={messageQueryValue}
                  disabled={modeValue === "resource"}
                  onInput={(event: Event) =>
                    setMessageQuery((event.target as HTMLInputElement).value)
                  }
                  placeholder={modeQueryPlaceholder(modeValue)}
                />
              </div>
            </div>
            {searchMode ? (
              <Inline
                class="queue-query-actions"
                justify="between"
                align="center"
                gap="3"
                wrap="wrap"
              >
                <p class="domain-muted">
                  Searching {operatorContext.selectedRouteFamily.label} through the admin search
                  index for queue resources, inflight work, and dead letters.
                </p>
                <Button type="submit" disabled={!canRunSearch}>
                  {searchLoadingValue ? "Running" : "Run search"}
                </Button>
              </Inline>
            ) : null}
          </form>

          {searchMode ? (
            <Card padding="sm" variant="default">
              <CardHeader>
                <CardTitle>{modeDetailTitle(modeValue)}</CardTitle>
                <CardDescription>{modeDetailDescription(modeValue)}</CardDescription>
              </CardHeader>
            </Card>
          ) : null}

          {searchMode && searchLoadingValue ? (
            <QueryLoadingState description="Searching queue evidence..." />
          ) : null}
          {searchMode && searchErrorValue ? (
            <QueryErrorState
              title="Unable to search queue evidence"
              error={searchErrorValue}
              onRetry={() => void runSearch()}
            />
          ) : null}
          {searchMode && searchResultValue && !searchLoadingValue ? (
            <QueueSearchPanel result={searchResultValue} />
          ) : null}

          {modeValue === "dlq" ? (
            <Card padding="sm" variant="default">
              <CardHeader>
                <CardTitle>DLQ actions live on the resource page</CardTitle>
                <CardDescription>
                  Select a queue resource to inspect dead-letter rows and use the existing replay or
                  purge confirmation flows.
                </CardDescription>
              </CardHeader>
            </Card>
          ) : null}

          {loading ? <QueryLoadingState description="Loading queue resources..." /> : null}
          {error ? <QueryErrorState title="Unable to load queue resources" error={error} /> : null}

          {!loading && !error ? (
            filteredRows.length === 0 ? (
              <QueryEmptyState
                title="No matching queues"
                description="Clear filters, check the selected Route Family, or broaden scope to find visible queue resources."
              />
            ) : (
              <Stack gap="3">
                <Inline justify="between" align="center" gap="3" wrap="wrap">
                  <p class="domain-muted">
                    {filteredRows.length} matching queue{filteredRows.length === 1 ? "" : "s"}
                  </p>
                  {canOpenExactResource ? (
                    <Link
                      class="text-link"
                      href={domainResourceHref("queue", {
                        area: areaValue,
                        realm: realmValue,
                        resource: resourceValue,
                      })}
                    >
                      Open exact queue
                    </Link>
                  ) : null}
                </Inline>

                <VirtualTable<QueueResourceRow>
                  aria-label="Matching queue resources"
                  class="queue-resource-virtual-table"
                  columns={queueColumns}
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
