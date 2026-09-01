import { For, Show } from "@askrjs/askr/control";
import { currentRoute } from "@askrjs/askr/router";
import {
  Badge,
  Block,
  Item,
  ItemActions,
  ItemContent,
  ItemDescription,
  ItemGroup,
  ItemTitle,
  Text,
} from "@askrjs/themes/components";
import DomainDataSection from "@/components/shared/domain-data-section";
import DomainHeader from "@/components/shared/domain-header";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import DomainSummaryStrip from "@/components/shared/domain-summary-strip";
import OperatorScopeStrip from "@/components/shared/operator-scope-strip";
import { queryFreshness, queryHeaderStatus } from "@/components/shared/query-header-status";
import {
  QueryCompactEmptyState,
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

function RpcCallEvidenceList(props: { rows: RpcCallObservation[] }) {
  return (
    <ItemGroup as="ul" aria-label="Live call evidence" class="domain-divided-list rpc-call-list">
      <For
        each={props.rows}
        by={(row, index) => row.correlation_id ?? `${row.worker_session_id}:${index}`}
      >
        {(row) => (
          <Item as="li">
            <ItemContent>
              <ItemTitle>
                <Text as="strong" font="mono" weight="semibold" wrap="anywhere">
                  {row.correlation_id ?? row.worker_session_id ?? "--"}
                </Text>
              </ItemTitle>
              <ItemDescription>
                <Block direction="row" gap="md" wrap>
                  <Text as="span" font="mono" size="sm" tone="muted" wrap="anywhere">
                    Worker: {row.worker_session_id ?? "--"}
                  </Text>
                  <Text as="span" font="mono" numeric="tabular" size="sm" tone="muted">
                    Observed handled total: {formatNumber(row.requests_handled ?? 0)}
                  </Text>
                  <Text as="span" font="mono" numeric="tabular" size="sm" tone="muted">
                    Latency: {formatLatency(row.average_latency_ms)}
                  </Text>
                </Block>
              </ItemDescription>
            </ItemContent>
            <ItemActions>
              <Badge aria-label={`State: ${formatObservationState(row.state)}`} variant="outline">
                {formatObservationState(row.state)}
              </Badge>
            </ItemActions>
          </Item>
        )}
      </For>
    </ItemGroup>
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
              <DomainSummaryStrip
                title="RPC operation metrics"
                description="Live worker capacity and pending requests. Latency buckets are current observations; the API does not report a reset window for handled-call counters."
                items={[
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
              <DomainDataSection
                id="rpc-live-call-evidence"
                title="Live call evidence"
                description="Broker-local worker registrations, pending calls, and correlation rows."
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
