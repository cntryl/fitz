import { For, Show } from "@askrjs/askr/control";
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
import type { ScheduleExecutionObservation, ScheduleMissedObservation } from "@/adapters";
import DomainHeader from "@/components/shared/domain-header";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import { createDomainSidebar } from "@/components/shared/domain-sidebar";
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
  formatNumber,
  formatRelativeTime,
  formatTimestamp,
} from "@/shared/format";
import { domainScopeHref } from "@/shared/navigation/domains";

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

function formatNextRun(value?: string | null) {
  return value ? formatRelativeTime(value) : "No next run visible";
}

function listenerLabel(data: ScheduleResourceView) {
  const observations = data.executionObservations.observations.length;
  const pending = data.missedHandoffs.observations.length;

  if (observations > 0) {
    return `${formatNumber(observations)} recent handoff observation(s)`;
  }

  if (pending > 0) {
    return "Pending handoff claim visible";
  }

  return "No live listeners visible";
}

function oldestPendingAge(data: ScheduleResourceView) {
  const oldest = data.missedHandoffs.observations.reduce<number | null>(
    (max, row) => (max == null ? row.age_seconds : Math.max(max, row.age_seconds)),
    null,
  );

  return oldest == null ? "--" : formatDurationSeconds(oldest);
}

function diagnosticSummary(data: ScheduleResourceView) {
  const diagnostics = data.detail.diagnostics;
  const parts = [
    `severity ${diagnostics.severity}`,
    `trend ${diagnostics.trend}`,
    diagnostics.likely_bottleneck ? `bottleneck ${diagnostics.likely_bottleneck}` : null,
  ].filter((part): part is string => part !== null);

  return parts.join(", ");
}

function ScheduleCard(props: { children: unknown; description?: string; title: string }) {
  return (
    <Card padding="sm" variant="default">
      <CardHeader>
        <CardTitle>{props.title}</CardTitle>
        {props.description ? <CardDescription>{props.description}</CardDescription> : null}
      </CardHeader>
      <CardContent>{props.children}</CardContent>
    </Card>
  );
}

function ExecutionRows(props: { rows: ScheduleExecutionObservation[] }) {
  if (props.rows.length === 0) {
    return (
      <QueryEmptyState description="No schedule-owned handoff observations matched this resource." />
    );
  }

  return (
    <div class="domain-table-wrap">
      <Table>
        <TableHead>
          <TableRow>
            <TableHeaderCell>Operation</TableHeaderCell>
            <TableHeaderCell>Status</TableHeaderCell>
            <TableHeaderCell>Next run</TableHeaderCell>
            <TableHeaderCell>Last run</TableHeaderCell>
            <TableHeaderCell>Executions</TableHeaderCell>
          </TableRow>
        </TableHead>
        <TableBody>
          <For each={props.rows} by={(row) => `${row.operation}:${row.next_run}`}>
            {(row) => (
              <TableRow>
                <TableCell>{row.operation}</TableCell>
                <TableCell>{row.status}</TableCell>
                <TableCell>{formatMaybeTimestamp(row.next_run)}</TableCell>
                <TableCell>{formatMaybeTimestamp(row.last_run)}</TableCell>
                <TableCell>{formatNumber(row.executions_total)}</TableCell>
              </TableRow>
            )}
          </For>
        </TableBody>
      </Table>
    </div>
  );
}

function MissedRows(props: { rows: ScheduleMissedObservation[] }) {
  if (props.rows.length === 0) {
    return (
      <QueryEmptyState description="No pending or missed schedule handoff claims matched this resource." />
    );
  }

  return (
    <div class="domain-table-wrap">
      <Table>
        <TableHead>
          <TableRow>
            <TableHeaderCell>Operation</TableHeaderCell>
            <TableHeaderCell>Status</TableHeaderCell>
            <TableHeaderCell>Fire at</TableHeaderCell>
            <TableHeaderCell>Claimed at</TableHeaderCell>
            <TableHeaderCell>Age</TableHeaderCell>
          </TableRow>
        </TableHead>
        <TableBody>
          <For each={props.rows} by={(row) => `${row.operation}:${row.fire_ms}`}>
            {(row) => (
              <TableRow>
                <TableCell>{row.operation}</TableCell>
                <TableCell>{row.status}</TableCell>
                <TableCell>{formatTimestamp(row.fire_at)}</TableCell>
                <TableCell>{formatTimestamp(row.claimed_at)}</TableCell>
                <TableCell>{formatDurationSeconds(row.age_seconds)}</TableCell>
              </TableRow>
            )}
          </For>
        </TableBody>
      </Table>
    </div>
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
  const sidebar = createDomainSidebar({
    data,
    title: "Schedule resource scope",
    description: scopeLabel,
    stats: (current: ScheduleResourceView) => [
      { label: "Realm", value: current.detail.realm },
      { label: "Area", value: current.detail.area },
      { label: "Resource", value: current.detail.resource },
      { label: "Enabled", value: current.detail.enabled ? "yes" : "no" },
      {
        label: "Next run",
        value: current.detail.next_run ? formatRelativeTime(current.detail.next_run) : "--",
      },
      { label: "Listeners", value: listenerLabel(current) },
    ],
  });

  return (
    <DomainPageFrame sidebar={sidebar}>
      <Stack gap="3">
        <DomainHeader
          eyebrow="Schedule resource"
          title="Schedule resource inspection"
          description={`Durable timing intent and live handoff observations for ${scopeLabel}.`}
          primaryAction={{ label: "Refresh resource", onPress: () => query.refresh() }}
          status={{
            detail: data
              ? `${listenerLabel(data)}. Next run: ${formatNextRun(data.detail.next_run)}.`
              : "Loading schedule resource.",
            label: query.refreshing
              ? "Refreshing"
              : query.stale
                ? "Stale"
                : data?.detail.enabled === false
                  ? "Disabled"
                  : "Live",
            tone: query.refreshing
              ? "info"
              : query.stale
                ? "warning"
                : data?.detail.enabled === false
                  ? "warning"
                  : "success",
          }}
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
                description="Persisted timing intent and broker-observed, non-authoritative execution counters."
                metrics={[
                  { label: "Enabled", value: current.detail.enabled ? "yes" : "no" },
                  { label: "Cron", value: current.detail.cron ?? "unset" },
                  {
                    label: "Next run",
                    value: current.detail.next_run
                      ? formatTimestamp(current.detail.next_run)
                      : "No next run visible",
                    caption: formatNextRun(current.detail.next_run),
                  },
                  {
                    label: "Executions total",
                    value: current.detail.executions_total,
                    caption: "Broker-observed, non-authoritative counter",
                  },
                  { label: "Is anyone listening?", value: listenerLabel(current) },
                  { label: "Pending handoffs", value: current.missedHandoffs.observations.length },
                  { label: "Oldest pending claim age", value: oldestPendingAge(current) },
                  { label: "Diagnostics", value: diagnosticSummary(current) },
                ]}
              />

              <ScheduleCard
                title="Execution observations"
                description="Schedule-owned handoff observations, not durable downstream execution history."
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

              <Link
                class="text-link"
                href={domainScopeHref("schedule", { area: ref.area, realm: ref.realm })}
              >
                Back to schedule area
              </Link>
            </Stack>
          )}
        </Show>
      </Stack>
    </DomainPageFrame>
  );
}
