import { For, Show } from "@askrjs/askr/control";
import { currentRoute, Link } from "@askrjs/askr/router";
import {
  Badge,
  Block,
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
  formatCount,
  formatDurationSeconds,
  formatNumber,
  formatRelativeTime,
  formatTimestamp,
} from "@/shared/format";
import { domainScopeHref, formatFitzRoute } from "@/shared/navigation/domains";

const RESOURCE_SCHEDULE_LIMIT = 100;

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

function decodeParam(value: string | undefined) {
  if (!value) return "";

  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

function formatMaybeTimestamp(value?: string | null) {
  return value ? formatTimestamp(value) : "--";
}

function formatObservationStatus(value?: string | null) {
  if (!value) return "Unknown";

  const label = value.replace(/_/g, " ").trim();
  return label.length > 0 ? `${label.charAt(0).toUpperCase()}${label.slice(1)}` : "Unknown";
}

function formatDeliveryMode(value?: string | null) {
  return value ? `${formatObservationStatus(value)} delivery` : "Delivery mode unknown";
}

export function formatScheduleTiming(value?: string | null, reference = Date.now()) {
  if (!value) return "No next run scheduled";

  const timestamp = new Date(value).getTime();
  if (Number.isNaN(timestamp)) return value;

  const relative = formatRelativeTime(value, reference);
  return timestamp >= reference ? `Next run ${relative}` : `Scheduled run was ${relative}`;
}

function scheduleTimingMetric(value?: string | null) {
  if (!value) {
    return {
      label: "Next run",
      value: "No next run scheduled",
    };
  }

  const timestamp = new Date(value).getTime();
  if (Number.isNaN(timestamp)) {
    return {
      label: "Scheduled value",
      value,
    };
  }

  return {
    caption: formatScheduleTiming(value),
    label: timestamp >= Date.now() ? "Next run" : "Scheduled run",
    value: formatTimestamp(value),
  };
}

function oldestPendingAge(data: ScheduleResourceView) {
  const oldest = data.missedHandoffs.observations.reduce<number | null>(
    (max, row) => (max == null ? row.age_seconds : Math.max(max, row.age_seconds)),
    null,
  );

  return oldest == null ? "--" : formatDurationSeconds(oldest);
}

function scheduleOperationRows(data: ScheduleResourceView): ScheduleOperationRow[] {
  const rows = new Map<string, ScheduleOperationRow>();
  for (const observation of data.executionObservations.observations) {
    rows.set(observation.operation, {
      cron: observation.cron,
      deliveryMode: observation.delivery_mode,
      executionsTotal: observation.executions_total,
      lastRun: observation.last_run,
      nextRun: observation.next_run,
      operation: observation.operation,
      pendingHandoffs: 0,
      status: observation.status,
    });
  }

  for (const missed of data.missedHandoffs.observations) {
    const current = rows.get(missed.operation);
    if (current) {
      current.pendingHandoffs += 1;
    } else {
      rows.set(missed.operation, {
        cron: null,
        deliveryMode: missed.delivery_mode,
        executionsTotal: 0,
        lastRun: null,
        nextRun: null,
        operation: missed.operation,
        pendingHandoffs: 1,
        status: null,
      });
    }
  }

  return [...rows.values()];
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
                  <ItemDescription>{formatDeliveryMode(row.deliveryMode)}</ItemDescription>
                  <ItemFooter class="schedule-evidence-metadata">
                    <dl>
                      <div>
                        <dt>Cron</dt>
                        <dd>{row.cron ?? "unset"}</dd>
                      </div>
                      <div>
                        <dt>Next run</dt>
                        <dd>{formatMaybeTimestamp(row.nextRun)}</dd>
                      </div>
                      <div>
                        <dt>Last handoff</dt>
                        <dd>{formatMaybeTimestamp(row.lastRun)}</dd>
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
                  <Badge variant="outline">{formatObservationStatus(row.status)}</Badge>
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
    area: decodeParam(route.params.area),
    realm: decodeParam(route.params.realm),
    resource: decodeParam(route.params.resource),
  };
  const query = createScheduleResourceQuery({ ...ref, limit: RESOURCE_SCHEDULE_LIMIT });
  const data = query.data;
  const scopeLabel = `${ref.realm} / ${ref.area} / ${ref.resource}`;
  const rows = data ? scheduleOperationRows(data) : [];
  const pendingHandoffs = rows.reduce((sum, row) => sum + row.pendingHandoffs, 0);
  const isTruncated =
    (data?.executionObservations.observations.length ?? 0) >= RESOURCE_SCHEDULE_LIMIT ||
    (data?.missedHandoffs.observations.length ?? 0) >= RESOURCE_SCHEDULE_LIMIT;
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
          status={queryHeaderStatus(query, {
            loading: "Loading schedules for this resource.",
            ready: data
              ? `${formatCount(rows.length, "visible individual schedule")}, ${formatCount(
                  pendingHandoffs,
                  "visible pending handoff",
                )}.`
              : "",
            unavailable: "Schedule evidence is unavailable for this resource.",
          })}
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
                  { label: "Oldest pending claim age", value: oldestPendingAge(current) },
                ]}
              />

              <DomainDataSection
                id="schedule-operations"
                title="Individual schedules"
                description={
                  isTruncated
                    ? `One or more observation lists reached the ${RESOURCE_SCHEDULE_LIMIT}-entry API cap. Schedule rows and pending-handoff counts may be incomplete.`
                    : "Each row is one visible schedule operation. Select a schedule to inspect its timing and handoff evidence."
                }
                actions={<Badge variant="outline">{formatCount(rows.length, "visible row")}</Badge>}
              >
                <ScheduleOperationRows {...ref} rows={rows} />
              </DomainDataSection>

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
