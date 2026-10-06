import { Show } from "@askrjs/askr/control";
import { currentRoute } from "@askrjs/askr/router";
import { Badge, Block } from "@askrjs/themes/components";
import { Table, TableBody, TableCell, TableHead, TableHeaderCell, TableRow } from "@askrjs/ui";
import DomainDataSection from "@/components/shared/domain-data-section";
import DomainHeader from "@/components/shared/domain-header";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import DomainFacts from "@/components/shared/domain-facts";
import { queryHeaderStatus } from "@/components/shared/query-header-status";
import {
  QueryCompactEmptyState,
  QueryErrorState,
  QueryLoadingState,
  QueryRefreshingState,
} from "@/components/shared/query-state";
import type { RpcCallObservation } from "@/adapters";
import { createRpcOperationQuery } from "@/features/rpc/rpc-query";
import { formatNumber, formatTimestamp } from "@/shared/format";

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
  return Number.isFinite(parsed) ? Math.max(1, Math.min(100, Math.floor(parsed))) : 50;
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

function RpcCallEvidenceList(props: { rows: RpcCallObservation[] }) {
  return (
    <div class="domain-table-wrap">
      <Table>
        <TableHead>
          <TableRow>
            <TableHeaderCell>Correlation</TableHeaderCell>
            <TableHeaderCell>State</TableHeaderCell>
            <TableHeaderCell>Age</TableHeaderCell>
            <TableHeaderCell>Observed at</TableHeaderCell>
            <TableHeaderCell>Worker</TableHeaderCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {props.rows.map((row) => (
            <TableRow>
              <TableCell>{row.correlation_id ?? "--"}</TableCell>
              <TableCell>{formatObservationState(row.state)}</TableCell>
              <TableCell>
                {row.age_seconds === null ? "--" : `${formatNumber(row.age_seconds)}s`}
              </TableCell>
              <TableCell>
                {row.submitted_at
                  ? `Submitted ${formatTimestamp(row.submitted_at)}`
                  : row.registered_at
                    ? `Registered ${formatTimestamp(row.registered_at)}`
                    : "--"}
              </TableCell>
              <TableCell>{row.worker_session_id ?? "--"}</TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
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
      <Block direction="column" gap="sm">
        <DomainHeader
          compact={true}
          eyebrow="RPC operation"
          title={operation}
          description={`${realm} / ${area} / ${resource}`}
          primaryAction={{
            busy: query.refreshing,
            disabled: query.refreshing,
            label: "Refresh operation",
            onPress: () => query.refresh(),
          }}
          status={queryHeaderStatus(
            query,
            detail?.requests_pending ? { label: "Pending calls", tone: "info" } : {},
          )}
        />
        <Show when={!data && query.loading}>
          <QueryLoadingState description="Loading RPC operation..." />
        </Show>
        <Show when={!data && query.error}>
          <QueryErrorState
            title="Unable to load RPC operation"
            error={query.error}
            onRetry={() => query.refresh()}
          />
        </Show>
        <Show when={detail}>
          {(detail) => (
            <Block direction="column" gap="sm">
              <Show when={query.refreshing}>
                <QueryRefreshingState description="Refreshing RPC operation..." />
              </Show>
              <DomainFacts
                id="rpc-operation-facts"
                title="Current route"
                items={[
                  { label: "Pending calls", value: formatNumber(detail.requests_pending) },
                  {
                    label: "Matching worker registrations",
                    value: formatNumber(detail.workers_registered),
                  },
                  {
                    label: "Slowest worker average",
                    value:
                      detail.workers_registered === 0
                        ? "--"
                        : formatLatency(detail.slowest_worker_average_latency_ms),
                  },
                ]}
              />
              <details class="domain-evidence-disclosure">
                <summary>Worker averages and handled counts</summary>
                <DomainFacts
                  id="rpc-worker-average-distribution"
                  title="Workers by average latency"
                  items={[
                    { label: "Under 5 ms", value: detail.worker_latency_buckets.under_5ms },
                    { label: "Under 25 ms", value: detail.worker_latency_buckets.under_25ms },
                    { label: "Under 100 ms", value: detail.worker_latency_buckets.under_100ms },
                    { label: "100 ms and over", value: detail.worker_latency_buckets.over_100ms },
                    {
                      label: "Handled by exact live workers",
                      value: detail.requests_handled_by_live_workers ?? "--",
                      title: "Wildcard registrations prevent exact operation attribution.",
                    },
                  ]}
                />
              </details>
              <DomainDataSection
                id="rpc-live-call-evidence"
                title="Live call evidence"
                actions={
                  data && rows.length >= data.calls.limit ? (
                    <Badge variant="warning">Observation sample reached {data.calls.limit}</Badge>
                  ) : undefined
                }
              >
                <Show when={rows.length === 0} fallback={<RpcCallEvidenceList rows={rows} />}>
                  <QueryCompactEmptyState
                    title="No live calls"
                    description="No live RPC call evidence is currently visible."
                  />
                </Show>
              </DomainDataSection>
            </Block>
          )}
        </Show>
      </Block>
    </DomainPageFrame>
  );
}
