import { For, Show } from "@askrjs/askr/control";
import { currentRoute } from "@askrjs/askr/router";
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
  Text,
} from "@askrjs/themes/components";
import type { ScheduleExecutionObservation, ScheduleMissedObservation } from "@/adapters";
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
import { formatFitzRoute } from "@/shared/navigation/domains";

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

function formatObservationStatus(value: string) {
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

function handoffEvidenceLabel(data: ScheduleResourceView) {
  const observations = data.executionObservations.observations.length;
  const pending = data.missedHandoffs.observations.length;

  if (observations > 0) {
    return `${formatCount(observations, "recent acknowledged handoff observation")}`;
  }

  if (pending > 0) {
    return "Pending handoff claim visible";
  }

  return "No recent handoff observations";
}

function oldestPendingAge(data: ScheduleResourceView) {
  const oldest = data.missedHandoffs.observations.reduce<number | null>(
    (max, row) => (max == null ? row.age_seconds : Math.max(max, row.age_seconds)),
    null,
  );

  return oldest == null ? "--" : formatDurationSeconds(oldest);
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

function ExecutionRows(props: { rows: ScheduleExecutionObservation[] }) {
  return (
    <Show
      when={props.rows.length > 0}
      fallback={
        <QueryCompactEmptyState
          title="No acknowledged handoffs"
          description="No schedule-owned handoff observations matched this resource."
        />
      }
    >
      <ItemGroup
        as="ul"
        class="domain-divided-list schedule-evidence-list schedule-execution-list"
        aria-label="Acknowledged handoff observations"
      >
        <For each={props.rows} by={(row) => `${row.route_family}:${row.operation}:${row.next_run}`}>
          {(row) => (
            <Item as="li" class="schedule-evidence-item" size="sm">
              <ItemContent>
                <ItemTitle>
                  <Text as="strong" font="mono" weight="semibold" wrap="anywhere">
                    {formatFitzRoute("schedule", row)}
                  </Text>
                </ItemTitle>
                <ItemDescription>
                  Route Family {formatNumber(row.route_family)} ·{" "}
                  {formatDeliveryMode(row.delivery_mode)}
                </ItemDescription>
                <ItemFooter class="schedule-evidence-metadata">
                  <dl>
                    <div>
                      <dt>Scheduled time</dt>
                      <dd>
                        <time dateTime={row.next_run}>{formatMaybeTimestamp(row.next_run)}</time>
                      </dd>
                    </div>
                    <div>
                      <dt>Last handoff</dt>
                      <dd>
                        {row.last_run ? (
                          <time dateTime={row.last_run}>{formatMaybeTimestamp(row.last_run)}</time>
                        ) : (
                          "--"
                        )}
                      </dd>
                    </div>
                    <div>
                      <dt>Handoff count</dt>
                      <dd>{formatNumber(row.executions_total)}</dd>
                    </div>
                  </dl>
                </ItemFooter>
              </ItemContent>
              <ItemActions>
                <Badge variant="outline">{formatObservationStatus(row.status)}</Badge>
              </ItemActions>
            </Item>
          )}
        </For>
      </ItemGroup>
    </Show>
  );
}

function MissedRows(props: { rows: ScheduleMissedObservation[] }) {
  return (
    <Show
      when={props.rows.length > 0}
      fallback={
        <QueryCompactEmptyState
          title="No pending handoffs"
          description="No pending or missed schedule handoff claims matched this resource."
        />
      }
    >
      <ItemGroup
        as="ul"
        class="domain-divided-list schedule-evidence-list schedule-pending-list"
        aria-label="Pending and missed handoffs"
      >
        <For each={props.rows} by={(row) => `${row.route_family}:${row.operation}:${row.fire_ms}`}>
          {(row) => (
            <Item as="li" class="schedule-evidence-item" size="sm">
              <ItemContent>
                <ItemTitle>
                  <Text as="strong" font="mono" weight="semibold" wrap="anywhere">
                    {formatFitzRoute("schedule", row)}
                  </Text>
                </ItemTitle>
                <ItemDescription>
                  Route Family {formatNumber(row.route_family)} ·{" "}
                  {formatDeliveryMode(row.delivery_mode)}
                </ItemDescription>
                <ItemFooter class="schedule-evidence-metadata">
                  <dl>
                    <div>
                      <dt>Fire at</dt>
                      <dd>
                        <time dateTime={row.fire_at}>{formatTimestamp(row.fire_at)}</time>
                      </dd>
                    </div>
                    <div>
                      <dt>Claimed at</dt>
                      <dd>
                        <time dateTime={row.claimed_at}>{formatTimestamp(row.claimed_at)}</time>
                      </dd>
                    </div>
                    <div>
                      <dt>Age</dt>
                      <dd>{formatDurationSeconds(row.age_seconds)}</dd>
                    </div>
                  </dl>
                </ItemFooter>
              </ItemContent>
              <ItemActions>
                <Badge variant="warning">{formatObservationStatus(row.status)}</Badge>
              </ItemActions>
            </Item>
          )}
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
  const query = createScheduleResourceQuery({ ...ref, limit: 20 });
  const data = query.data;
  const scopeLabel = `${ref.realm} / ${ref.area} / ${ref.resource}`;
  const timingMetric = data ? scheduleTimingMetric(data.detail.next_run) : null;
  return (
    <DomainPageFrame>
      <Block direction="column" gap="sm">
        <DomainHeader
          eyebrow="Schedule resource"
          title={ref.resource}
          description={`Durable timing intent and schedule-owned handoff evidence for ${scopeLabel}.`}
          primaryAction={{
            busy: query.refreshing,
            disabled: query.refreshing,
            label: "Refresh schedule",
            onPress: () => query.refresh(),
          }}
          status={queryHeaderStatus(
            query,
            {
              loading: "Loading schedule resource.",
              ready: data
                ? `${handoffEvidenceLabel(data)}. ${formatScheduleTiming(data.detail.next_run)}.`
                : "",
              unavailable:
                "Schedule timing and handoff evidence are unavailable for this resource.",
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
          <QueryLoadingState description="Loading schedule resource..." />
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
                <QueryRefreshingState description="Refreshing schedule resource..." />
              </Show>

              <DomainMetricTable
                title="Schedule timing"
                description="Persisted timing intent and broker-observed, non-authoritative handoff counters."
                metrics={[
                  { label: "Enabled", value: current.detail.enabled ? "yes" : "no" },
                  { label: "Cron", value: current.detail.cron ?? "unset" },
                  timingMetric ?? { label: "Next run", value: "No next run scheduled" },
                  {
                    label: "Broker observation counter",
                    value: current.detail.executions_total,
                    caption: "Non-authoritative; not downstream execution history",
                  },
                  { label: "Handoff evidence", value: handoffEvidenceLabel(current) },
                  { label: "Pending handoffs", value: current.missedHandoffs.observations.length },
                  { label: "Oldest pending claim age", value: oldestPendingAge(current) },
                ]}
              />

              <DomainDataSection
                id="schedule-acknowledged-handoffs"
                title="Acknowledged handoff observations"
                description="Schedule-owned handoff evidence, not durable downstream execution history."
                actions={
                  <Badge variant="outline">
                    {formatCount(current.executionObservations.observations.length, "observation")}
                  </Badge>
                }
              >
                <ExecutionRows rows={current.executionObservations.observations} />
              </DomainDataSection>

              <DomainDataSection
                id="schedule-pending-handoffs"
                title="Pending and missed handoffs"
                description="Persisted pending schedule fire claims that have not been acknowledged."
                actions={
                  <Badge
                    variant={current.missedHandoffs.observations.length > 0 ? "warning" : "success"}
                  >
                    {formatCount(current.missedHandoffs.observations.length, "claim")}
                  </Badge>
                }
              >
                <MissedRows rows={current.missedHandoffs.observations} />
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
