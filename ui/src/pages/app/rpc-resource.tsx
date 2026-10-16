import { Show } from "@askrjs/askr/control";
import { currentRoute } from "@askrjs/askr/router";
import { Block } from "@askrjs/themes/components";
import DomainHeader from "@/components/shared/domain-header";
import DomainOperationTable, {
  type DomainOperationMetricColumn,
} from "@/components/shared/domain-operation-table";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import { queryHeaderStatus } from "@/components/shared/query-header-status";
import {
  QueryErrorState,
  QueryLoadingState,
  QueryRefreshingState,
} from "@/components/shared/query-state";
import { createRpcResourceQuery } from "@/features/rpc/rpc-query";
import type { RpcResourceOperationRows } from "@/features/rpc/rpc-models";
import { formatNumber } from "@/shared/format";

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
    id: "pending",
    header: "Pending",
    width: "12%",
    cell: (row) => formatNumber(row.pendingRequests),
    sortValue: (row) => row.pendingRequests,
  },
  {
    id: "workers",
    header: "Matching registrations",
    width: "12%",
    cell: (row) => formatNumber(row.workers),
    sortValue: (row) => row.workers,
  },
  {
    id: "handled",
    priority: "secondary",
    header: "Handled by live workers",
    width: "12%",
    cell: (row) => (row.requestsHandled == null ? "--" : formatNumber(row.requestsHandled)),
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
  const pendingRequests = data?.totalPendingRequests ?? 0;

  return (
    <DomainPageFrame>
      <Block direction="column" gap="sm">
        <DomainHeader
          compact={true}
          eyebrow="RPC resource"
          title={resource}
          primaryAction={{
            busy: query.refreshing,
            disabled: query.refreshing,
            label: "Refresh operations",
            onPress: () => query.refresh(),
          }}
          status={queryHeaderStatus(
            query,
            pendingRequests > 0 ? { label: "Pending calls", tone: "info" } : {},
          )}
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
            <DomainOperationTable<RpcOperationRow>
              domain="rpc"
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
