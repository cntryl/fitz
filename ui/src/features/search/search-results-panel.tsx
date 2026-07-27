import { For, Show } from "@askrjs/askr/control";
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
import { VirtualTable, type VirtualTableColumn } from "@askrjs/ui";
import {
  QueryEmptyState,
  QueryErrorState,
  QueryLoadingState,
} from "@/components/shared/query-state";
import { formatFitzRoute, pathWithRouteFamily } from "@/shared/navigation/domains";
import type { AdminSearchResult, AdminSearchResults } from "./search-models";

export interface SearchResultsPanelProps {
  error?: unknown;
  loading?: boolean;
  onRetry: () => void;
  routeFamilyId: string;
  routeFamilyLabel: string;
  search?: AdminSearchResults | null;
}

function titleCase(value: string) {
  return value
    .split(/[-_\s]+/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function routeText(result: AdminSearchResult) {
  const hasDomainRoute = Boolean(
    result.realm || result.area || result.resource || result.operation,
  );

  if (hasDomainRoute) return formatFitzRoute(result.domain, result);
  if (result.routeFamily) return `Route Family ${result.routeFamily}`;
  return "Broker";
}

function resultHref(result: AdminSearchResult, selectedRouteFamilyId: string) {
  const target = new URL(result.href, "http://fitz.local");
  const family = result.routeFamily || selectedRouteFamilyId;

  return `${pathWithRouteFamily(target.pathname, family)}${target.search}${target.hash}`;
}

function searchColumns(
  selectedRouteFamilyId: string,
): readonly VirtualTableColumn<AdminSearchResult>[] {
  return [
    {
      id: "result",
      header: "Result",
      width: "28%",
      cellComponent: ({ row }) => (
        <Stack gap="1">
          <strong class="domain-table-cell-truncate">{row.title}</strong>
          <span class="domain-muted">
            {titleCase(row.domain)} · {titleCase(row.kind)}
          </span>
        </Stack>
      ),
    },
    {
      id: "route",
      header: "Route",
      width: "27%",
      cellComponent: ({ row }) => (
        <span class="domain-table-cell-truncate" title={routeText(row)}>
          {routeText(row) || row.routeFamily || "Broker"}
        </span>
      ),
    },
    {
      id: "summary",
      header: "Summary",
      width: "28%",
      cellComponent: ({ row }) => <span class="domain-table-cell-truncate">{row.summary}</span>,
    },
    {
      id: "health",
      header: "Health",
      width: "9%",
      cellComponent: ({ row }) => (
        <Badge variant={row.health === "failing" || row.health === "stale" ? "warning" : "outline"}>
          {row.health ? titleCase(row.health) : "Observed"}
        </Badge>
      ),
    },
    {
      id: "action",
      header: "Action",
      width: "8%",
      cellComponent: ({ row }) => (
        <Link class="text-link" href={resultHref(row, selectedRouteFamilyId)}>
          Open
        </Link>
      ),
    },
  ];
}

export default function SearchResultsPanel({
  error,
  loading = false,
  onRetry,
  routeFamilyId,
  routeFamilyLabel,
  search,
}: SearchResultsPanelProps) {
  const results = search?.results ?? [];
  const description = search
    ? `${search.total} result${search.total === 1 ? "" : "s"} for "${search.query}" in ${routeFamilyLabel}.`
    : `Searching ${routeFamilyLabel}.`;

  return (
    <Card padding="sm" variant="default">
      <CardHeader>
        <Inline justify="between" align="start" gap="3" wrap="wrap">
          <Stack gap="1">
            <CardTitle titleAs="h2">Search results</CardTitle>
            <CardDescription>{description}</CardDescription>
          </Stack>
          <Inline gap="2" align="center" wrap="wrap">
            <Show when={search?.truncated}>
              <Badge variant="warning">Truncated</Badge>
            </Show>
            <Button type="button" variant="outline" size="sm" onPress={onRetry}>
              Refresh
            </Button>
          </Inline>
        </Inline>
      </CardHeader>
      <CardContent>
        <Show when={loading && !search}>
          <QueryLoadingState description="Searching admin state..." />
        </Show>
        <Show when={!search && error}>
          <QueryErrorState title="Unable to search admin state" error={error} onRetry={onRetry} />
        </Show>
        <Show when={search && results.length === 0}>
          <QueryEmptyState
            title="No matching admin state"
            description="Try a realm, area, resource, operation, message id, session id, owner, or correlation id."
          />
        </Show>
        <Show when={results.length > 0}>
          <Stack gap="3">
            <VirtualTable<AdminSearchResult>
              aria-label="Admin search results"
              columns={searchColumns(routeFamilyId)}
              rows={results}
              getKey={(row) => row.id}
              headerHeight={44}
              overscan={6}
              rowHeight={56}
              style={{ height: "360px" }}
            />
            <div class="search-match-strip" aria-label="Matched search fields">
              <For each={results.slice(0, 8)} by={(result) => result.id}>
                {(result) => (
                  <span class="search-match-chip">
                    {result.title}
                    {result.matchedFields.length > 0 ? ` · ${result.matchedFields.join(", ")}` : ""}
                  </span>
                )}
              </For>
            </div>
          </Stack>
        </Show>
      </CardContent>
    </Card>
  );
}
