import { state } from "@askrjs/askr";
import { currentRoute, updateRouteQuery } from "@askrjs/askr/router";
import { Input, Label } from "@askrjs/ui";
import { Button, Inline, Stack } from "@askrjs/themes/components";
import DomainHeader from "@/components/shared/domain-header";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import { QueryErrorState, QueryLoadingState } from "@/components/shared/query-state";
import DiagnosticsConsole from "@/features/diagnostics/diagnostics-console";
import { createMetricsOverviewQuery } from "@/features/metrics/metrics-query";
import { createAdminSearchQuery } from "@/features/search/search-query";
import SearchResultsPanel from "@/features/search/search-results-panel";
import { createSystemOverviewQuery } from "@/features/system/system-query";
import { createMessagingTopologyQuery } from "@/features/topology/topology-query";
import { formatRelativeTime } from "@/shared/format";
import { useOperatorScope } from "@/shared/operator-scope";

function DiagnosticsSearchResults({
  query,
  routeFamilyId,
  routeFamilyLabel,
}: {
  query: string;
  routeFamilyId: string;
  routeFamilyLabel: string;
}) {
  const search = createAdminSearchQuery({
    limit: 100,
    query,
    routeFamily: routeFamilyId,
  });

  return (
    <SearchResultsPanel
      error={search.error}
      loading={search.loading && !search.data}
      onRetry={() => search.refresh()}
      routeFamilyId={routeFamilyId}
      routeFamilyLabel={routeFamilyLabel}
      search={search.data}
    />
  );
}

export default function DiagnosticsPage() {
  const route = currentRoute();
  const system = createSystemOverviewQuery();
  const metrics = createMetricsOverviewQuery();
  const topology = createMessagingTopologyQuery();
  const operator = useOperatorScope();
  const searchQuery = route.query.get("q");
  const searchRouteFamily =
    route.query.get("route_family") ??
    route.query.get("routeFamily") ??
    operator.selectedRouteFamilyId;
  const [searchDraft, setSearchDraft] = state(searchQuery ?? "");
  const data = system.data;
  const refreshing = system.refreshing || metrics.refreshing || topology.refreshing;
  const stale = system.stale || metrics.stale || topology.stale;
  const partial = Boolean(
    data && ((!metrics.data && metrics.error) || (!topology.data && topology.error)),
  );

  function refreshDiagnostics() {
    void system.refresh();
    void metrics.refresh();
    void topology.refresh();
  }

  function submitSearch(event: Event) {
    event.preventDefault();
    const query = searchDraft().trim();

    updateRouteQuery({ q: query || null }, { history: "push" });
  }

  function clearSearch() {
    setSearchDraft("");
    updateRouteQuery({ q: null }, { history: "push" });
  }

  return (
    <DomainPageFrame>
      <Stack gap="3">
        <DomainHeader
          eyebrow="Infrastructure internals"
          title="Diagnostics"
          description="Advanced operational views for storage health, metrics, topology, and broker-local internals."
          primaryAction={{
            busy: refreshing,
            disabled: refreshing,
            label: "Refresh diagnostics",
            onPress: refreshDiagnostics,
          }}
          status={{
            detail: data
              ? `Snapshot ${formatRelativeTime(data.fetchedAt)} for ${operator.selectedRouteFamily.label}.`
              : `Loading diagnostics for ${operator.selectedRouteFamily.label}.`,
            label: refreshing
              ? "Refreshing"
              : partial
                ? "Partial"
                : stale
                  ? "Stale"
                  : data
                    ? "Live"
                    : system.error
                      ? "Unavailable"
                      : "Loading",
            tone: refreshing
              ? "info"
              : partial || stale
                ? "warning"
                : data
                  ? "success"
                  : system.error
                    ? "danger"
                    : "info",
          }}
        />

        <section class="domain-section" aria-label="Search diagnostics">
          <div class="domain-section-header">
            <div>
              <h2>Search admin state</h2>
              <p>Find sessions and domain resources within the selected Route Family.</p>
            </div>
          </div>
          <form role="search" onSubmit={submitSearch}>
            <Inline align="end" gap="2" wrap="wrap">
              <div class="auth-field">
                <Label for="diagnostics-query">Search</Label>
                <Input
                  id="diagnostics-query"
                  aria-label="Search diagnostics"
                  name="diagnostics-query"
                  type="search"
                  value={searchDraft()}
                  onInput={(event: Event) =>
                    setSearchDraft((event.target as HTMLInputElement).value)
                  }
                />
              </div>
              <Button type="submit">Search</Button>
              {searchQuery ? (
                <Button type="button" variant="ghost" onPress={clearSearch}>
                  Clear search
                </Button>
              ) : null}
            </Inline>
          </form>
        </section>

        {searchQuery ? (
          <DiagnosticsSearchResults
            query={searchQuery}
            routeFamilyId={searchRouteFamily}
            routeFamilyLabel={
              operator.routeFamilies.find((family) => family.id === searchRouteFamily)?.label ??
              operator.selectedRouteFamily.label
            }
          />
        ) : null}

        {!data && system.loading ? (
          <QueryLoadingState description="Loading broker diagnostics..." />
        ) : null}

        {!data && system.error ? (
          <QueryErrorState
            title="Unable to load diagnostics"
            error={system.error}
            onRetry={() => system.refresh()}
          />
        ) : null}

        {data ? (
          <Stack gap="3">
            <DiagnosticsConsole
              metrics={metrics.data}
              metricsError={metrics.error}
              metricsLoading={metrics.loading && !metrics.data}
              onRetryMetrics={() => metrics.refresh()}
              onRetryTopology={() => topology.refresh()}
              operatorLabel={operator.selectedRouteFamily.label}
              system={data}
              topology={topology.data}
              topologyError={topology.error}
              topologyLoading={topology.loading && !topology.data}
            />
          </Stack>
        ) : null}
      </Stack>
    </DomainPageFrame>
  );
}
