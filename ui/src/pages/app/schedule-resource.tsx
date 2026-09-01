import { For, Show } from "@askrjs/askr/control";
import { currentRoute, Link } from "@askrjs/askr/router";
import {
  Badge,
  Block,
  Button,
  Item,
  ItemActions,
  ItemContent,
  ItemDescription,
  ItemFooter,
  ItemGroup,
  ItemTitle,
} from "@askrjs/themes/components";
import DomainDataSection from "@/components/shared/domain-data-section";
import DomainHeader from "@/components/shared/domain-header";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import OperatorScopeStrip from "@/components/shared/operator-scope-strip";
import { queryFreshness, queryHeaderStatus } from "@/components/shared/query-header-status";
import {
  QueryCompactEmptyState,
  QueryErrorState,
  QueryLoadingState,
  QueryRefreshingState,
} from "@/components/shared/query-state";
import { createScheduleResourceQuery } from "@/features/schedule/schedule-query";
import type { ScheduleResourceView } from "@/features/schedule/schedule-models";
import {
  decodeScheduleParam,
  formatScheduleTimestamp,
  scheduleTimingMetric,
} from "@/features/schedule/schedule-format";
import { formatCount, formatNumber } from "@/shared/format";
import { domainResourceHref, domainScopeHref, formatFitzRoute } from "@/shared/navigation/domains";

const RESOURCE_SCHEDULE_LIMIT = 50;

interface ScheduleOperationRow {
  cron: string | null;
  deliveryMode: string | null;
  executionsTotal: number;
  lastRun: string | null;
  nextRun: string | null;
  operation: string;
  pendingHandoffs: number;
  status: string | null;
}

function formatObservationStatus(value?: string | null) {
  if (!value) return "Unknown";

  const label = value.replace(/_/g, " ").trim();
  return label.length > 0 ? `${label.charAt(0).toUpperCase()}${label.slice(1)}` : "Unknown";
}

function formatDeliveryMode(value?: string | null) {
  return value ? `${formatObservationStatus(value)} delivery` : "Delivery mode unknown";
}

function parseOffset(value: string | null) {
  const parsed = Number(value ?? 0);
  return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : 0;
}

function schedulePageHref(
  scope: { area: string; realm: string; resource: string },
  offset: number,
) {
  const href = domainResourceHref("schedule", scope);
  return offset > 0 ? `${href}?offset=${offset}` : href;
}

export function scheduleOperationRows(data: ScheduleResourceView): ScheduleOperationRow[] {
  return data.executionObservations.observations.map((observation) => ({
    cron: observation.cron,
    deliveryMode: observation.delivery_mode,
    executionsTotal: observation.executions_total,
    lastRun: observation.last_run,
    nextRun: observation.next_run,
    operation: observation.operation,
    pendingHandoffs: observation.pending_handoffs,
    status: observation.status,
  }));
}

function ScheduleOperationRows(props: {
  area: string;
  realm: string;
  resource: string;
  rows: ScheduleOperationRow[];
}) {
  return (
    <Show
      when={props.rows.length > 0}
      fallback={
        <QueryCompactEmptyState
          title="No individual schedules"
          description="No individual schedules are currently visible for this resource."
        />
      }
    >
      <ItemGroup
        as="ul"
        aria-label="Individual schedules"
        class="domain-divided-list schedule-operation-list"
      >
        <For each={props.rows} by={(row) => row.operation}>
          {(row) => {
            const route = formatFitzRoute("schedule", {
              area: props.area,
              operation: row.operation,
              realm: props.realm,
              resource: props.resource,
            });

            return (
              <Item as="li">
                <ItemContent>
                  <ItemTitle>
                    <Link
                      class="domain-link-cell schedule-operation-link"
                      href={domainScopeHref("schedule", {
                        area: props.area,
                        operation: row.operation,
                        realm: props.realm,
                        resource: props.resource,
                      })}
                      title={route}
                    >
                      {route}
                    </Link>
                  </ItemTitle>
                  <ItemDescription>
                    {formatDeliveryMode(row.deliveryMode)} · {formatObservationStatus(row.status)}
                  </ItemDescription>
                  <ItemFooter class="schedule-evidence-metadata">
                    <dl>
                      <div>
                        <dt>Cron</dt>
                        <dd>{row.cron ?? "unset"}</dd>
                      </div>
                      <div>
                        <dt>Next run</dt>
                        <dd>{formatScheduleTimestamp(row.nextRun)}</dd>
                      </div>
                      <div>
                        <dt>Last handoff</dt>
                        <dd>{formatScheduleTimestamp(row.lastRun)}</dd>
                      </div>
                      <div>
                        <dt>Pending handoffs</dt>
                        <dd>{formatNumber(row.pendingHandoffs)}</dd>
                      </div>
                      <div>
                        <dt>Handoff count</dt>
                        <dd>{formatNumber(row.executionsTotal)}</dd>
                      </div>
                    </dl>
                  </ItemFooter>
                </ItemContent>
                <ItemActions>
                  <Badge variant={row.pendingHandoffs > 0 ? "warning" : "success"}>
                    {formatCount(row.pendingHandoffs, "pending handoff")}
                  </Badge>
                </ItemActions>
              </Item>
            );
          }}
        </For>
      </ItemGroup>
    </Show>
  );
}

export default function ScheduleResourcePage() {
  const route = currentRoute();
  const ref = {
    area: decodeScheduleParam(route.params.area),
    realm: decodeScheduleParam(route.params.realm),
    resource: decodeScheduleParam(route.params.resource),
  };
  const offset = parseOffset(route.query.get("offset"));
  const query = createScheduleResourceQuery({
    ...ref,
    limit: RESOURCE_SCHEDULE_LIMIT,
    offset,
  });
  const data = query.data;
  const scopeLabel = `${ref.realm} / ${ref.area} / ${ref.resource}`;
  const rows = data ? scheduleOperationRows(data) : [];
  const pendingHandoffs = rows.reduce((sum, row) => sum + row.pendingHandoffs, 0);
  const timingMetric = data ? scheduleTimingMetric(data.detail.next_run) : null;

  return (
    <DomainPageFrame>
      <Block direction="column" gap="sm">
        <DomainHeader
          eyebrow="Schedule resource"
          title={ref.resource}
          description={`Individual schedules registered for ${scopeLabel}.`}
          primaryAction={{
            busy: query.refreshing,
            disabled: query.refreshing,
            label: "Refresh schedule",
            onPress: () => query.refresh(),
          }}
          status={queryHeaderStatus(
            query,
            {
              loading: "Loading schedules for this resource.",
              ready: data
                ? `${formatCount(rows.length, "visible individual schedule")}, ${formatCount(
                    pendingHandoffs,
                    "visible pending handoff",
                  )}.`
                : "",
              unavailable: "Schedule evidence is unavailable for this resource.",
            },
            data?.detail.enabled === false
              ? { label: "Disabled", tone: "warning" }
              : { label: "Enabled", tone: "info" },
          )}
        />
        <OperatorScopeStrip
          realm={ref.realm}
          area={ref.area}
          resource={ref.resource}
          freshness={queryFreshness(query)}
        />

        <Show when={!data && query.loading}>
          <QueryLoadingState description="Loading schedules..." />
        </Show>

        <Show when={!data && query.error}>
          <QueryErrorState
            title="Unable to load schedule resource"
            error={query.error}
            onRetry={() => query.refresh()}
          />
        </Show>

        <Show when={data}>
          {(current) => (
            <Block direction="column" gap="sm">
              <Show when={query.refreshing}>
                <QueryRefreshingState description="Refreshing schedules..." />
              </Show>

              <DomainMetricTable
                title="Schedule timing"
                description="Persisted resource-level timing intent and broker-observed, non-authoritative handoff counters."
                metrics={[
                  { label: "Enabled", value: current.detail.enabled ? "yes" : "no" },
                  { label: "Cron", value: current.detail.cron ?? "varies by schedule" },
                  timingMetric ?? { label: "Next run", value: "No next run scheduled" },
                  {
                    label: "Broker observation counter",
                    value: current.detail.executions_total,
                    caption: "Non-authoritative; not downstream execution history",
                  },
                  { label: "Visible schedules", value: rows.length },
                  { label: "Visible pending handoffs", value: pendingHandoffs },
                ]}
              />

              <DomainDataSection
                id="schedule-operations"
                title="Individual schedules"
                description={`Showing ${formatCount(rows.length, "schedule")} from offset ${formatNumber(offset)}. Use the page controls to inspect the complete operation inventory.`}
                actions={
                  <Badge variant={pendingHandoffs > 0 ? "warning" : "success"}>
                    {formatCount(pendingHandoffs, "pending handoff")}
                  </Badge>
                }
              >
                <ScheduleOperationRows {...ref} rows={rows} />
              </DomainDataSection>

              <Block as="nav" aria-label="Schedule pages" direction="row" gap="xs" wrap={true}>
                <Show when={offset > 0}>
                  <Link class="page-action-link" href={schedulePageHref(ref, 0)}>
                    First page
                  </Link>
                  <Link
                    class="page-action-link"
                    href={schedulePageHref(ref, Math.max(0, offset - RESOURCE_SCHEDULE_LIMIT))}
                  >
                    Previous page
                  </Link>
                </Show>
                <Show when={current.executionObservations.has_more}>
                  <Button asChild>
                    <Link href={schedulePageHref(ref, offset + RESOURCE_SCHEDULE_LIMIT)}>
                      Next page
                    </Link>
                  </Button>
                </Show>
              </Block>

              <DomainMetricTable
                title="Diagnostics"
                description="Live broker diagnostics for this resource scope."
                metrics={[
                  { label: "Severity", value: current.detail.diagnostics.severity },
                  { label: "Trend", value: current.detail.diagnostics.trend },
                  { label: "Current stage", value: current.detail.diagnostics.current_stage },
                  { label: "Failure count", value: current.detail.diagnostics.failure_count },
                  { label: "Waiters", value: current.detail.diagnostics.waiter_count },
                  { label: "Contention", value: current.detail.diagnostics.contention_count },
                ]}
              />
            </Block>
          )}
        </Show>
      </Block>
    </DomainPageFrame>
  );
}
