import { For } from "@askrjs/askr/control";
import { currentRoute } from "@askrjs/askr/router";
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
import DomainMetricTable from "@/components/shared/domain-metric-table";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import OperatorScopeStrip from "@/components/shared/operator-scope-strip";
import { queryFreshness, queryHeaderStatus } from "@/components/shared/query-header-status";
import {
  QueryEmptyState,
  QueryErrorState,
  QueryLoadingState,
  QueryRefreshingState,
} from "@/components/shared/query-state";
import type { RpcCallObservation } from "@/adapters";
import { createRpcOperationQuery } from "@/features/rpc/rpc-query";
import { formatCount, formatNumber } from "@/shared/format";

function decodeParam(value: string | undefined) {
  if (!value) return "";

  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

function parseLimit(value: string | null) {
  const parsed = Number(value ?? 50);
  return Number.isFinite(parsed) ? Math.max(1, Math.min(200, Math.floor(parsed))) : 50;
}

function formatLatency(value: number | null | undefined) {
  return value == null ? "--" : `${formatNumber(value)} ms`;
}

function formatObservationState(value: string) {
  return value
    .split("_")
    .filter(Boolean)
    .map((part) => `${part[0]?.toUpperCase() ?? ""}${part.slice(1)}`)
    .join(" ");
}

function RpcCallEvidenceRows(props: { rows: RpcCallObservation[] }) {
  return (
    <For
      each={props.rows}
      by={(row, index) => row.correlation_id ?? `${row.worker_session_id}:${index}`}
    >
      {(row) => (
        <TableRow>
          <TableCell>{formatObservationState(row.state)}</TableCell>
          <TableCell>{row.worker_session_id ?? "--"}</TableCell>
          <TableCell>{row.correlation_id ?? "--"}</TableCell>
          <TableCell>{formatNumber(row.requests_handled ?? 0)}</TableCell>
          <TableCell>{formatLatency(row.average_latency_ms)}</TableCell>
        </TableRow>
      )}
    </For>
  );
}

export default function RpcOperationPage() {
  const route = currentRoute();
  const realm = decodeParam(route.params.realm);
  const area = decodeParam(route.params.area);
  const resource = decodeParam(route.params.resource);
  const operation = decodeParam(route.params.operation);
  const limit = parseLimit(route.query.get("limit"));
  const query = createRpcOperationQuery({ area, limit, operation, realm, resource });
  const data = query.data;
  const detail = data?.detail;
  const rows = data?.calls.observations ?? [];

  return (
    <DomainPageFrame>
      <Stack gap="3">
        <DomainHeader
          eyebrow="RPC operation"
          title={operation}
          description={`${realm} / ${area} / ${resource}`}
          primaryAction={{
            busy: query.refreshing,
            disabled: query.refreshing,
            label: "Refresh operation",
            onPress: () => query.refresh(),
          }}
          status={queryHeaderStatus(query, {
            loading: "Loading RPC operation.",
            ready: detail
              ? `${formatCount(detail.workers_registered, "worker")}, ${formatCount(
                  detail.requests_pending,
                  "pending request",
                )}. Pending RPC state is live in-memory state only.`
              : "",
            unavailable: "RPC operation evidence is unavailable.",
          })}
        />
        <OperatorScopeStrip
          realm={realm}
          area={area}
          resource={resource}
          operation={operation}
          freshness={queryFreshness(query)}
        />
        {!data && query.loading ? (
          <QueryLoadingState description="Loading RPC operation..." />
        ) : null}
        {!data && query.error ? (
          <QueryErrorState
            title="Unable to load RPC operation"
            error={query.error}
            onRetry={() => query.refresh()}
          />
        ) : null}
        {data && detail ? (
          <Stack gap="3">
            {query.refreshing ? (
              <QueryRefreshingState description="Refreshing RPC operation..." />
            ) : null}
            <DomainMetricTable
              title="RPC operation metrics"
              description="Live worker capacity and pending requests. Latency buckets are current observations; the API does not report a reset window for handled-call counters."
              metrics={[
                { label: "Workers", value: detail.workers_registered },
                { label: "Pending requests", value: detail.requests_pending },
                {
                  label: "Slowest average latency",
                  value: formatLatency(detail.slowest_worker_average_latency_ms),
                },
                { label: "Latency <5ms", value: detail.worker_latency_buckets.under_5ms },
                { label: "Latency <25ms", value: detail.worker_latency_buckets.under_25ms },
                { label: "Latency <100ms", value: detail.worker_latency_buckets.under_100ms },
                { label: "Latency 100ms+", value: detail.worker_latency_buckets.over_100ms },
              ]}
            />
            <Card padding="sm" variant="default">
              <CardHeader>
                <CardTitle titleAs="h2">Live call evidence</CardTitle>
                <CardDescription>
                  Broker-local worker registrations, pending calls, and correlation rows.
                </CardDescription>
              </CardHeader>
              <CardContent>
                {rows.length === 0 ? (
                  <QueryEmptyState description="No live RPC call evidence is currently visible." />
                ) : (
                  <div class="domain-table-wrap rpc-operation-table-wrap">
                    <Table>
                      <TableHead>
                        <TableRow>
                          <TableHeaderCell>State</TableHeaderCell>
                          <TableHeaderCell>Worker</TableHeaderCell>
                          <TableHeaderCell>Correlation</TableHeaderCell>
                          <TableHeaderCell>Observed handled total</TableHeaderCell>
                          <TableHeaderCell>Latency</TableHeaderCell>
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        <RpcCallEvidenceRows rows={rows} />
                      </TableBody>
                    </Table>
                  </div>
                )}
              </CardContent>
            </Card>
          </Stack>
        ) : null}
      </Stack>
    </DomainPageFrame>
  );
}
