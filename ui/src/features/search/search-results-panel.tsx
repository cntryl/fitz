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
import { VirtualTable, type VirtualTableColumn } from "@askrjs/ui";
import {
  QueryEmptyState,
  QueryErrorState,
  QueryLoadingState,
} from "@/components/shared/query-state";
import { formatFitzRoute } from "@/shared/navigation/domains";
import type { AdminSearchResult, AdminSearchResults } from "./search-models";

export interface SearchResultsPanelProps {
  error?: unknown;
  loading?: boolean;
  onRetry: () => void;
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
  return formatFitzRoute(result.domain, result);
}

const searchColumns: readonly VirtualTableColumn<AdminSearchResult>[] = [
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
      <Link class="text-link" href={row.href}>
        Open
      </Link>
    ),
  },
];

export default function SearchResultsPanel({
  error,
  loading = false,
  onRetry,
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
            <CardTitle>Search results</CardTitle>
            <CardDescription>{description}</CardDescription>
          </Stack>
          <Inline gap="2" align="center" wrap="wrap">
            {search?.truncated ? <Badge variant="warning">Truncated</Badge> : null}
            <Button type="button" variant="outline" size="sm" onPress={onRetry}>
              Refresh
            </Button>
          </Inline>
        </Inline>
      </CardHeader>
      <CardContent>
        {loading && !search ? <QueryLoadingState description="Searching admin state..." /> : null}
        {!search && error ? (
          <QueryErrorState title="Unable to search admin state" error={error} onRetry={onRetry} />
        ) : null}
        {search && results.length === 0 ? (
          <QueryEmptyState
            title="No matching admin state"
            description="Try a realm, area, resource, operation, message id, session id, owner, or correlation id."
          />
        ) : null}
        {results.length > 0 ? (
          <Stack gap="3">
            <VirtualTable<AdminSearchResult>
              aria-label="Admin search results"
              columns={searchColumns}
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
        ) : null}
      </CardContent>
    </Card>
  );
}
