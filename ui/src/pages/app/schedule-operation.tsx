import { Show } from "@askrjs/askr/control";
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
import type { ScheduleMissedObservation } from "@/adapters";
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
import { createScheduleOperationQuery } from "@/features/schedule/schedule-query";
import { formatDurationSeconds, formatRelativeTime, formatTimestamp } from "@/shared/format";

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

const RESOURCE_SCHEDULE_LIMIT = 100;

function MissedRows(props: { rows: ScheduleMissedObservation[] }) {
  return (
    <Show
      when={props.rows.length > 0}
      fallback={
        <QueryEmptyState description="No pending or missed handoff claims matched this schedule." />
      }
    >
      <div>
        <p class="domain-scroll-hint">Scroll the table horizontally on narrow screens.</p>
        <div class="domain-table-wrap">
          <Table>
            <TableHead>
              <TableRow>
                <TableHeaderCell>Mode</TableHeaderCell>
                <TableHeaderCell>Fire at</TableHeaderCell>
                <TableHeaderCell>Claimed at</TableHeaderCell>
                <TableHeaderCell>Age</TableHeaderCell>
                <TableHeaderCell>Status</TableHeaderCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {props.rows.map((row) => (
                <TableRow>
                  <TableCell>{row.delivery_mode}</TableCell>
                  <TableCell>{formatTimestamp(row.fire_at)}</TableCell>
                  <TableCell>{formatTimestamp(row.claimed_at)}</TableCell>
                  <TableCell>{formatDurationSeconds(row.age_seconds)}</TableCell>
                  <TableCell>{row.status}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      </div>
    </Show>
  );
}

export default function ScheduleOperationPage() {
  const route = currentRoute();
  const ref = {
    area: decodeParam(route.params.area),
    realm: decodeParam(route.params.realm),
    resource: decodeParam(route.params.resource),
  };
  const operation = decodeParam(route.params.operation);
  const query = createScheduleOperationQuery({
    ...ref,
    limit: RESOURCE_SCHEDULE_LIMIT,
    operation,
  });
  const data = query.data;
  const scopeLabel = `${ref.realm} / ${ref.area} / ${ref.resource}`;
  const scheduleRow = data?.executionObservations.observations[0];
  const missedRows = data?.missedHandoffs.observations ?? [];
  const missedTruncated = missedRows.length >= RESOURCE_SCHEDULE_LIMIT;
  const timingMetric = scheduleTimingMetric(scheduleRow?.next_run);

  return (
    <DomainPageFrame>
      <Stack gap="3">
        <DomainHeader
          eyebrow="Schedule"
          title={operation}
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
              loading: "Loading schedule.",
              ready: scheduleRow
                ? `${scheduleRow.status}. ${formatScheduleTiming(scheduleRow.next_run)}.`
                : "No acknowledged handoff observations are visible for this schedule.",
              unavailable: "Schedule timing and handoff evidence are unavailable.",
            },
            scheduleRow
              ? { label: scheduleRow.status, tone: "info" }
              : { label: "No observations", tone: "warning" },
          )}
        />
        <OperatorScopeStrip
          realm={ref.realm}
          area={ref.area}
          resource={ref.resource}
          operation={operation}
          freshness={queryFreshness(query)}
        />

        <Show when={!data && query.loading}>
          <QueryLoadingState description="Loading schedule..." />
        </Show>

        <Show when={!data && query.error}>
          <QueryErrorState
            title="Unable to load schedule"
            error={query.error}
            onRetry={() => query.refresh()}
          />
        </Show>

        <Show when={data}>
          <Stack gap="3">
            <Show when={query.refreshing}>
              <QueryRefreshingState description="Refreshing schedule..." />
            </Show>

            <DomainMetricTable
              title="Schedule timing"
              description="Persisted timing intent and broker-observed, non-authoritative handoff counters for this individual schedule."
              metrics={[
                { label: "Cron", value: scheduleRow?.cron ?? "unset" },
                timingMetric,
                { label: "Delivery mode", value: scheduleRow?.delivery_mode ?? "--" },
                {
                  label: "Broker observation counter",
                  value: scheduleRow?.executions_total ?? 0,
                  caption: "Non-authoritative; not downstream execution history",
                },
                { label: "Last handoff", value: formatMaybeTimestamp(scheduleRow?.last_run) },
                {
                  label: "Pending handoffs",
                  value: missedTruncated ? `${missedRows.length}+` : missedRows.length,
                  caption: missedTruncated ? "Observation list reached the API cap" : undefined,
                },
              ]}
            />

            <Card padding="sm" variant="default">
              <CardHeader>
                <CardTitle titleAs="h2">Pending and missed handoffs</CardTitle>
                <CardDescription>
                  Persisted pending schedule fire claims for this schedule that have not been
                  acknowledged.
                  <Show when={missedTruncated}>
                    {" "}
                    The observation list reached the {RESOURCE_SCHEDULE_LIMIT}-entry API cap and may
                    be incomplete.
                  </Show>
                </CardDescription>
              </CardHeader>
              <CardContent>
                <MissedRows rows={missedRows} />
              </CardContent>
            </Card>

            <DomainMetricTable
              title="Resource diagnostics"
              description="Live broker diagnostics for the resource this schedule belongs to."
              metrics={[
                { label: "Severity", value: data?.detail.diagnostics.severity ?? "--" },
                { label: "Trend", value: data?.detail.diagnostics.trend ?? "--" },
                { label: "Current stage", value: data?.detail.diagnostics.current_stage ?? "--" },
                { label: "Failure count", value: data?.detail.diagnostics.failure_count ?? 0 },
                { label: "Waiters", value: data?.detail.diagnostics.waiter_count ?? 0 },
                { label: "Contention", value: data?.detail.diagnostics.contention_count ?? 0 },
              ]}
            />
          </Stack>
        </Show>
      </Stack>
    </DomainPageFrame>
  );
}
