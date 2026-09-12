import { Show } from "@askrjs/askr/control";
import { currentRoute } from "@askrjs/askr/router";
import { Block } from "@askrjs/themes/components";
import DomainHeader from "@/components/shared/domain-header";
import DomainOperationTable, {
  type DomainOperationMetricColumn,
} from "@/components/shared/domain-operation-table";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import DomainSummaryStrip from "@/components/shared/domain-summary-strip";
import OperatorScopeStrip from "@/components/shared/operator-scope-strip";
import { queryFreshness, queryHeaderStatus } from "@/components/shared/query-header-status";
import {
  QueryErrorState,
  QueryLoadingState,
  QueryRefreshingState,
} from "@/components/shared/query-state";
import { createRpcResourceQuery } from "@/features/rpc/rpc-query";
import type { RpcResourceOperationRows } from "@/features/rpc/rpc-models";
import { formatCount, formatNumber } from "@/shared/format";

function decodeParam(value: string | undefined) {
  if (!value) return "";

  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

type RpcOperationRow = RpcResourceOperationRows["operations"][number];

const rpcOperationColumns: readonly DomainOperationMetricColumn<RpcOperationRow>[] = [
  {
    id: "workers",
    header: "Workers",
    width: "12%",
    cell: (row) => formatNumber(row.workers),
    sortValue: (row) => row.workers,
  },
  {
    id: "pending",
    header: "Pending",
    width: "12%",
    cell: (row) => formatNumber(row.pendingRequests),
    sortValue: (row) => row.pendingRequests,
  },
  {
    id: "handled",
    header: "Handled",
    width: "12%",
    cell: (row) => formatNumber(row.requestsHandled),
    sortValue: (row) => row.requestsHandled,
  },
  {
    // Derived as the worst worker average, matching the inventory tier's column.
    id: "slowest-latency",
    header: "Slowest avg ms",
    width: "18%",
    cell: (row) => (row.averageLatencyMs == null ? "--" : row.averageLatencyMs.toFixed(1)),
    sortValue: (row) => row.averageLatencyMs,
  },
];

export default function RpcResourcePage() {
  const route = currentRoute();
  const realm = decodeParam(route.params.realm);
  const area = decodeParam(route.params.area);
  const resource = decodeParam(route.params.resource);
  const query = createRpcResourceQuery(realm, area, resource);
  const data = query.data;
  const totalWorkers = data?.operations.reduce((sum, row) => sum + row.workers, 0) ?? 0;
  const pendingRequests = data?.operations.reduce((sum, row) => sum + row.pendingRequests, 0) ?? 0;
  const requestsHandled = data?.operations.reduce((sum, row) => sum + row.requestsHandled, 0) ?? 0;

  return (
    <DomainPageFrame>
      <Block direction="column" gap="sm">
        <DomainHeader
          eyebrow="RPC resource"
          title={resource}
          description={`Live operation evidence for ${realm} / ${area} / ${resource}.`}
          primaryAction={{
            busy: query.refreshing,
            disabled: query.refreshing,
            label: "Refresh operations",
            onPress: () => query.refresh(),
          }}
          status={queryHeaderStatus(query, {
            loading: "Loading RPC operations.",
            ready: data
              ? `${formatCount(data.operations.length, "operation")}, ${formatCount(
                  totalWorkers,
                  "live worker",
                )}, ${formatCount(pendingRequests, "pending request")}.`
              : "",
            unavailable: "RPC operation evidence is unavailable for this resource.",
          })}
        />
        <OperatorScopeStrip
          realm={realm}
          area={area}
          resource={resource}
          freshness={queryFreshness(query)}
        />
        <Show when={!data && query.loading}>
          <QueryLoadingState description="Loading RPC operations..." />
        </Show>
        <Show when={!data && query.error}>
          <QueryErrorState
            title="Unable to load RPC operations"
            error={query.error}
            onRetry={() => query.refresh()}
          />
        </Show>
        <Show when={data}>
          <Block direction="column" gap="sm">
            <Show when={query.refreshing}>
              <QueryRefreshingState description="Refreshing RPC operations..." />
            </Show>
            <DomainSummaryStrip
              id="rpc-resource-rollup"
              class="domain-inventory-summary"
              items={[
                { label: "Operations", value: formatNumber(data?.operations.length ?? 0) },
                { label: "Workers", value: formatNumber(totalWorkers) },
                { label: "Pending", value: formatNumber(pendingRequests) },
                { label: "Handled", value: formatNumber(requestsHandled) },
              ]}
            />
            <DomainOperationTable<RpcOperationRow>
              domain="rpc"
              description="Live operation evidence: workers, handled calls, latency, and in-memory pending request evidence."
              emptyDescription="No RPC operations are currently visible for this resource."
              metricColumns={rpcOperationColumns}
              rows={data?.operations ?? []}
              scope={{ area, realm, resource }}
              title="RPC operations"
            />
          </Block>
        </Show>
      </Block>
    </DomainPageFrame>
  );
}
