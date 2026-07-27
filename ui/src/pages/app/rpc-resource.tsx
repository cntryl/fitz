import { Show } from "@askrjs/askr/control";
import { currentRoute, Link } from "@askrjs/askr/router";
import { VirtualTable, type VirtualTableColumn } from "@askrjs/ui";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  Stack,
} from "@askrjs/themes/components";
import DomainHeader from "@/components/shared/domain-header";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import OperatorScopeStrip from "@/components/shared/operator-scope-strip";
import { queryFreshness, queryHeaderStatus } from "@/components/shared/query-header-status";
import {
  QueryEmptyState,
  QueryErrorState,
  QueryLoadingState,
  QueryRefreshingState,
} from "@/components/shared/query-state";
import { createRpcResourceQuery } from "@/features/rpc/rpc-query";
import type { RpcResourceOperationRows } from "@/features/rpc/rpc-models";
import { formatCount, formatNumber } from "@/shared/format";
import { domainScopeHref, formatFitzRoute } from "@/shared/navigation/domains";

type RpcResourceOperationRow = RpcResourceOperationRows["operations"][number];

function decodeParam(value: string | undefined) {
  if (!value) return "";

  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

function formatLatency(value: number | null) {
  return value == null ? "--" : `${formatNumber(value)} ms`;
}

function rpcOperationColumns(
  data: RpcResourceOperationRows,
): readonly VirtualTableColumn<RpcResourceOperationRow>[] {
  return [
    {
      id: "route",
      header: "Route",
      width: "44%",
      cellComponent: ({ row }) => {
        const route = formatFitzRoute("rpc", {
          area: data.area,
          operation: row.operation,
          realm: data.realm,
          resource: data.resource,
        });

        return (
          <Link
            class="domain-link-cell"
            href={domainScopeHref("rpc", {
              area: data.area,
              operation: row.operation,
              realm: data.realm,
              resource: data.resource,
            })}
            title={route}
          >
            {route}
          </Link>
        );
      },
    },
    {
      id: "workers",
      header: "Workers",
      width: "12%",
      cellComponent: ({ row }) => <span>{formatNumber(row.workers)}</span>,
    },
    {
      id: "pending",
      header: "Pending requests",
      width: "16%",
      cellComponent: ({ row }) => <span>{formatNumber(row.pendingRequests)}</span>,
    },
    {
      id: "handled",
      header: "Requests handled",
      width: "16%",
      cellComponent: ({ row }) => <span>{formatNumber(row.requestsHandled)}</span>,
    },
    {
      id: "latency",
      header: "Latency",
      width: "12%",
      cellComponent: ({ row }) => <span>{formatLatency(row.averageLatencyMs)}</span>,
    },
  ];
}

function rpcOperationTableHeight(rowCount: number) {
  return `${Math.min(240, Math.max(144, 44 + rowCount * 48))}px`;
}

export default function RpcResourcePage() {
  const route = currentRoute();
  const realm = decodeParam(route.params.realm);
  const area = decodeParam(route.params.area);
  const resource = decodeParam(route.params.resource);
  const query = createRpcResourceQuery(realm, area, resource);
  const data = query.data;
  const totalWorkers = data?.operations.reduce((sum, row) => sum + row.workers, 0) ?? 0;
  const pendingRequests = data?.operations.reduce((sum, row) => sum + row.pendingRequests, 0) ?? 0;

  return (
    <DomainPageFrame>
      <Stack gap="3">
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
          <Stack gap="3">
            <Show when={query.refreshing}>
              <QueryRefreshingState description="Refreshing RPC operations..." />
            </Show>
            <Show
              when={data && data.operations.length === 0}
              fallback={
                data ? (
                  <Card padding="sm" variant="default">
                    <CardHeader>
                      <CardTitle titleAs="h2">RPC operations</CardTitle>
                      <CardDescription>
                        Live operation evidence: workers, handled calls, latency, and in-memory
                        pending request evidence.
                      </CardDescription>
                    </CardHeader>
                    <CardContent>
                      <VirtualTable<RpcResourceOperationRow>
                        aria-label="RPC operations"
                        class="rpc-operation-virtual-table"
                        columns={rpcOperationColumns(data)}
                        getKey={(row) => row.operation}
                        headerHeight={44}
                        overscan={4}
                        rowHeight={48}
                        rows={data.operations}
                        style={{ height: rpcOperationTableHeight(data.operations.length) }}
                      />
                    </CardContent>
                  </Card>
                ) : null
              }
            >
              <QueryEmptyState description="No RPC operations are currently visible for this resource." />
            </Show>
          </Stack>
        </Show>
      </Stack>
    </DomainPageFrame>
  );
}
