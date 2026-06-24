import { currentRoute } from "@askrjs/askr/router";
import { Stack } from "@askrjs/themes/layouts";
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
import { useOperatorContext } from "@/shared/operator-context";

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
  const operator = useOperatorContext();
  const searchQuery = route.query.get("q");
  const searchRouteFamily =
    route.query.get("route_family") ??
    route.query.get("routeFamily") ??
    operator.selectedRouteFamilyId;
  const data = system.data;

  return (
    <DomainPageFrame>
      <Stack gap="3">
        <DomainHeader
          eyebrow="Infrastructure internals"
          title="Diagnostics"
          description="Advanced operational views for storage health, metrics, topology, and broker-local internals."
          primaryAction={{
            label: "Refresh diagnostics",
            onPress: () => system.refresh(),
          }}
          status={{
            detail: data
              ? `Snapshot ${formatRelativeTime(data.fetchedAt)} for ${operator.selectedRouteFamily.label}.`
              : `Loading diagnostics for ${operator.selectedRouteFamily.label}.`,
            label: system.refreshing
              ? "Refreshing"
              : system.stale
                ? "Stale"
                : data
                  ? "Live"
                  : "Loading",
            tone: system.refreshing ? "info" : system.stale ? "warning" : data ? "success" : "info",
          }}
        />

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
