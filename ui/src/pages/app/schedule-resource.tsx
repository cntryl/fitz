import { For, Show } from "@askrjs/askr/control";
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
import type { ScheduleExecutionObservation, ScheduleMissedObservation } from "@/adapters";
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
import { createScheduleResourceQuery } from "@/features/schedule/schedule-query";
import type { ScheduleResourceView } from "@/features/schedule/schedule-models";
import {
  formatDurationSeconds,
  formatCount,
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

function ScheduleCard(props: { children: unknown; description?: string; title: string }) {
  return (
    <Card padding="sm" variant="default">
      <CardHeader>
        <CardTitle titleAs="h2">{props.title}</CardTitle>
        {props.description ? <CardDescription>{props.description}</CardDescription> : null}
      </CardHeader>
      <CardContent>{props.children}</CardContent>
    </Card>
  );
}

function ExecutionRows(props: { rows: ScheduleExecutionObservation[] }) {
  return (
    <Show
      when={props.rows.length > 0}
      fallback={
        <QueryEmptyState description="No schedule-owned handoff observations matched this resource." />
      }
    >
      <div>
        <p class="domain-scroll-hint">Scroll the table horizontally on narrow screens.</p>
        <div class="domain-table-wrap schedule-observation-table-wrap">
          <Table>
            <TableHead>
              <TableRow>
                <TableHeaderCell>Route</TableHeaderCell>
                <TableHeaderCell>Status</TableHeaderCell>
                <TableHeaderCell>Mode</TableHeaderCell>
                <TableHeaderCell>Scheduled time</TableHeaderCell>
                <TableHeaderCell>Last handoff</TableHeaderCell>
                <TableHeaderCell>Handoff count</TableHeaderCell>
              </TableRow>
            </TableHead>
            <TableBody>
              <For each={props.rows} by={(row) => `${row.operation}:${row.next_run}`}>
                {(row) => (
                  <TableRow>
                    <TableCell>
                      <span
                        class="domain-table-cell-truncate"
                        title={formatFitzRoute("schedule", row)}
                      >
                        {formatFitzRoute("schedule", row)}
                      </span>
                    </TableCell>
                    <TableCell>{row.status}</TableCell>
                    <TableCell>{row.delivery_mode}</TableCell>
                    <TableCell>{formatMaybeTimestamp(row.next_run)}</TableCell>
                    <TableCell>{formatMaybeTimestamp(row.last_run)}</TableCell>
                    <TableCell>{formatNumber(row.executions_total)}</TableCell>
                  </TableRow>
                )}
              </For>
            </TableBody>
          </Table>
        </div>
      </div>
    </Show>
  );
}

function MissedRows(props: { rows: ScheduleMissedObservation[] }) {
  return (
    <Show
      when={props.rows.length > 0}
      fallback={
        <QueryEmptyState description="No pending or missed schedule handoff claims matched this resource." />
      }
    >
      <div>
        <p class="domain-scroll-hint">Scroll the table horizontally on narrow screens.</p>
        <div class="domain-table-wrap">
          <Table>
            <TableHead>
              <TableRow>
                <TableHeaderCell>Route</TableHeaderCell>
                <TableHeaderCell>Status</TableHeaderCell>
                <TableHeaderCell>Mode</TableHeaderCell>
                <TableHeaderCell>Fire at</TableHeaderCell>
                <TableHeaderCell>Claimed at</TableHeaderCell>
                <TableHeaderCell>Age</TableHeaderCell>
              </TableRow>
            </TableHead>
            <TableBody>
              <For each={props.rows} by={(row) => `${row.operation}:${row.fire_ms}`}>
                {(row) => (
                  <TableRow>
                    <TableCell>
                      <span
                        class="domain-table-cell-truncate"
                        title={formatFitzRoute("schedule", row)}
                      >
                        {formatFitzRoute("schedule", row)}
                      </span>
                    </TableCell>
                    <TableCell>{row.status}</TableCell>
                    <TableCell>{row.delivery_mode}</TableCell>
                    <TableCell>{formatTimestamp(row.fire_at)}</TableCell>
                    <TableCell>{formatTimestamp(row.claimed_at)}</TableCell>
                    <TableCell>{formatDurationSeconds(row.age_seconds)}</TableCell>
                  </TableRow>
                )}
              </For>
            </TableBody>
          </Table>
        </div>
      </div>
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
      <Stack gap="3">
        <DomainHeader
          eyebrow="Schedule resource"
          title="Schedule resource inspection"
          description={`Durable timing intent and schedule-owned handoff evidence for ${scopeLabel}.`}
          primaryAction={{
            busy: query.refreshing,
            disabled: query.refreshing,
            label: "Refresh resource",
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
            <Stack gap="3">
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

              <ScheduleCard
                title="Acknowledged handoff observations"
                description="Schedule-owned handoff evidence, not durable downstream execution history."
              >
                <ExecutionRows rows={current.executionObservations.observations} />
              </ScheduleCard>

              <ScheduleCard
                title="Pending and missed handoffs"
                description="Persisted pending schedule fire claims that have not been acknowledged."
              >
                <MissedRows rows={current.missedHandoffs.observations} />
              </ScheduleCard>

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
            </Stack>
          )}
        </Show>
      </Stack>
    </DomainPageFrame>
  );
}
