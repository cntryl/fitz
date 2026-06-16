import { Stack } from "@askrjs/themes/layouts";
import DomainBarChart from "@/components/shared/domain-bar-chart";
import DomainHeader from "@/components/shared/domain-header";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import DomainResourceBrowser from "@/components/shared/domain-resource-browser";
import DomainRealmTable from "@/components/shared/domain-realm-table";
import {
  QueryErrorState,
  QueryLoadingState,
  QueryRefreshingState,
} from "@/components/shared/query-state";
import { createDomainSidebar } from "@/components/shared/domain-sidebar";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import { createRpcOverviewQuery } from "@/features/rpc/rpc-query";
import { createResourceInventoryQuery } from "@/features/resource/resource-query";

export default function RpcPage() {
  const overview = createRpcOverviewQuery();
  const inventory = createResourceInventoryQuery("rpc");
  const data = overview.data;
  const sidebar = createDomainSidebar({
    data,
    title: "RPC snapshot",
    description: "Live worker coverage and pending request pressure.",
    stats: (current) => [
      { label: "Workers", value: current.stats.workersRegistered },
      { label: "Requests pending", value: current.stats.requestsPending },
      {
        label: "Ops / sec",
        value: current.stats.operationsPerSecond.toFixed(2),
        note: "Live broker snapshot",
      },
    ],
  });

  return (
    <DomainPageFrame sidebar={sidebar}>
      <Stack gap="3">
        <DomainHeader
          eyebrow="Live request/response"
          title="RPC overview"
          description="Live request/response pressure, worker registrations, and realm inventory."
          primaryAction={{
            label: "Refresh RPC",
            onPress: () => overview.refresh(),
          }}
          status={{
            detail: "RPC pending state is ephemeral and only exists while workers are live.",
            label: overview.refreshing ? "Refreshing" : overview.stale ? "Stale" : "Live",
            tone: overview.refreshing ? "info" : overview.stale ? "warning" : "success",
          }}
        />

        {!data && overview.loading ? (
          <QueryLoadingState description="Loading RPC overview..." />
        ) : null}

        {!data && overview.error ? (
          <QueryErrorState error={overview.error} onRetry={() => overview.refresh()} />
        ) : null}

        {data ? (
          <Stack gap="3">
            {overview.refreshing ? (
              <QueryRefreshingState description="Refreshing RPC overview..." />
            ) : null}

            <DomainMetricTable
              title="RPC metrics"
              description="Live workers, pending requests, and request/response failure pressure."
              metrics={[
                { label: "Workers", value: data.stats.workersRegistered },
                { label: "Requests pending", value: data.stats.requestsPending },
                {
                  label: "Closed caller drops",
                  value: data.stats.responsesDroppedClosedCallerTotal,
                },
                { label: "Missing pending", value: data.stats.responsesMissingPendingTotal },
                { label: "Invalid seq responses", value: data.stats.invalidSequenceResponsesTotal },
                {
                  label: "Invalid seq fwd",
                  value: data.stats.invalidSequenceErrorsForwardedTotal,
                },
                {
                  label: "Invalid seq drops",
                  value: data.stats.invalidSequenceErrorsDroppedTotal,
                },
                {
                  label: "Ops / sec",
                  value: data.stats.operationsPerSecond.toFixed(2),
                },
              ]}
            />

            <DomainBarChart
              title="RPC signal"
              description="Worker capacity, pending request pressure, and current throughput."
              label="RPC state snapshot"
              scope="Live RPC snapshot"
              data={[
                { label: "Workers", unitLabel: "workers", value: data.stats.workersRegistered },
                { label: "Pending", unitLabel: "requests", value: data.stats.requestsPending },
                {
                  label: "Ops / sec",
                  unitLabel: "ops/sec",
                  value: data.stats.operationsPerSecond,
                },
              ]}
            />

            <DomainRealmTable
              title="RPC realms"
              realms={data.realms}
              emptyMessage="No RPC realms are currently visible."
            />

            <DomainResourceBrowser
              domain="rpc"
              inventory={inventory.data}
              loading={inventory.loading}
            />
          </Stack>
        ) : null}
      </Stack>
    </DomainPageFrame>
  );
}
