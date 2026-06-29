import { For } from "@askrjs/askr/control";
import { currentRoute, Link } from "@askrjs/askr/router";
import { Table, TableBody, TableCell, TableHead, TableHeaderCell, TableRow } from "@askrjs/ui";
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
import { createDomainSidebar } from "@/components/shared/domain-sidebar";
import {
  QueryEmptyState,
  QueryErrorState,
  QueryLoadingState,
  QueryRefreshingState,
} from "@/components/shared/query-state";
import { createRpcResourceQuery } from "@/features/rpc/rpc-query";
import type { RpcResourceOperationRows } from "@/features/rpc/rpc-models";
import { formatNumber } from "@/shared/format";
import { domainScopeHref } from "@/shared/navigation/domains";

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

function RpcOperationTableRows(props: { data: RpcResourceOperationRows }) {
  return (
    <For each={props.data.operations} by={(row) => row.operation}>
      {(row) => (
        <TableRow>
          <TableCell>
            <Link
              href={domainScopeHref("rpc", {
                area: props.data.area,
                operation: row.operation,
                realm: props.data.realm,
                resource: props.data.resource,
              })}
            >
              {row.operation}
            </Link>
          </TableCell>
          <TableCell>{formatNumber(row.workers)}</TableCell>
          <TableCell>{formatNumber(row.pendingRequests)}</TableCell>
          <TableCell>{formatNumber(row.requestsHandled)}</TableCell>
          <TableCell>{formatLatency(row.averageLatencyMs)}</TableCell>
        </TableRow>
      )}
    </For>
  );
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

  const snapshot = createDomainSidebar({
    data,
    title: `RPC operations for ${resource}`,
    description: `${realm} / ${area} / ${resource}`,
    stats: (current) => [
      { label: "Operations", value: current.operations.length },
      { label: "Workers", value: totalWorkers },
      { label: "Pending requests", value: pendingRequests },
    ],
    footer: <Link href={domainScopeHref("rpc", { area, realm })}>Back to area</Link>,
  });

  return (
    <DomainPageFrame>
      <Stack gap="3">
        <DomainHeader
          eyebrow="RPC resource"
          title={resource}
          description={`${realm} / ${area}`}
          primaryAction={{ label: "Refresh operations", onPress: () => query.refresh() }}
          status={{
            detail: data
              ? `${data.operations.length} operation(s), ${totalWorkers} live worker(s), ${pendingRequests} pending request(s).`
              : "Loading RPC operations.",
            label: query.refreshing ? "Refreshing" : query.stale ? "Stale" : "Live",
            tone: query.refreshing ? "info" : query.stale ? "warning" : "success",
          }}
        />
        {snapshot}
        {!data && query.loading ? (
          <QueryLoadingState description="Loading RPC operations..." />
        ) : null}
        {!data && query.error ? (
          <QueryErrorState
            title="Unable to load RPC operations"
            error={query.error}
            onRetry={() => query.refresh()}
          />
        ) : null}
        {data ? (
          <Stack gap="3">
            {query.refreshing ? (
              <QueryRefreshingState description="Refreshing RPC operations..." />
            ) : null}
            {data.operations.length === 0 ? (
              <QueryEmptyState description="No RPC operations are currently visible for this resource." />
            ) : (
              <Card padding="sm" variant="default">
                <CardHeader>
                  <CardTitle>RPC operations</CardTitle>
                  <CardDescription>
                    Live worker capacity and in-memory pending request evidence.
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableHeaderCell>Operation</TableHeaderCell>
                        <TableHeaderCell>Workers</TableHeaderCell>
                        <TableHeaderCell>Pending requests</TableHeaderCell>
                        <TableHeaderCell>Requests handled</TableHeaderCell>
                        <TableHeaderCell>Latency</TableHeaderCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      <RpcOperationTableRows data={data} />
                    </TableBody>
                  </Table>
                </CardContent>
              </Card>
            )}
          </Stack>
        ) : null}
      </Stack>
    </DomainPageFrame>
  );
}
