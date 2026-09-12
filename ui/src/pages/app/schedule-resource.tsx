import { Show } from "@askrjs/askr/control";
import { currentRoute, Link } from "@askrjs/askr/router";
import { Block, Button } from "@askrjs/themes/components";
import type { ScheduleExecutionObservation } from "@/adapters";
import DomainHeader from "@/components/shared/domain-header";
import DomainOperationTable, {
  type DomainOperationMetricColumn,
} from "@/components/shared/domain-operation-table";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import DomainSummaryStrip from "@/components/shared/domain-summary-strip";
import OperatorScopeStrip from "@/components/shared/operator-scope-strip";
import { queryFreshness, queryHeaderStatus } from "@/components/shared/query-header-status";
import {
  QueryErrorState,
  QueryLoadingState,
  QueryRefreshingState,
} from "@/components/shared/query-state";
import { createScheduleResourceQuery } from "@/features/schedule/schedule-query";
import {
  decodeScheduleParam,
  formatScheduleTimestamp,
  scheduleTimingMetric,
} from "@/features/schedule/schedule-format";
import { formatCount, formatNumber, formatRelativeTime } from "@/shared/format";
import { domainResourceHref } from "@/shared/navigation/domains";

const RESOURCE_SCHEDULE_LIMIT = 50;

/**
 * Timestamps read as relative so schedules stay comparable at a glance; the exact
 * value stays available on hover. Delivery mode is single-schedule detail and lives
 * on the operation tier.
 */
const scheduleOperationColumns: readonly DomainOperationMetricColumn<ScheduleExecutionObservation>[] =
  [
    {
      id: "status",
      header: "Status",
      width: "12%",
      cell: (row) => row.status,
    },
    {
      id: "cron",
      header: "Cron",
      width: "14%",
      cell: (row) => row.cron || "unset",
    },
    {
      id: "next-run",
      header: "Next run",
      width: "16%",
      cell: (row) => formatRelativeTime(row.next_run),
      sortValue: (row) => Date.parse(row.next_run),
      title: (row) => formatScheduleTimestamp(row.next_run),
    },
    {
      id: "last-handoff",
      header: "Last handoff",
      width: "16%",
      cell: (row) => (row.last_run ? formatRelativeTime(row.last_run) : "--"),
      sortValue: (row) => (row.last_run ? Date.parse(row.last_run) : null),
      title: (row) => formatScheduleTimestamp(row.last_run),
    },
    {
      id: "pending-handoffs",
      header: "Pending",
      width: "12%",
      cell: (row) => formatNumber(row.pending_handoffs),
      sortValue: (row) => row.pending_handoffs,
      title: () => "Pending handoff claims awaiting acknowledgement",
    },
  ];

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
  const rows = data?.executionObservations.observations ?? [];
  const detail = data?.detail;
  const pendingHandoffs = rows.reduce((sum, row) => sum + row.pending_handoffs, 0);

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
              loading: "Loading schedules for this resource.",
              ready: data
                ? `${formatCount(rows.length, "observed schedule")}, ${formatCount(
                    pendingHandoffs,
                    "pending handoff",
                  )}.`
                : "",
              unavailable: "Schedule operations are unavailable for this resource.",
            },
            { label: "Operations", tone: "info" },
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

              <Show when={detail}>
                {(resourceDetail) => (
                  <DomainSummaryStrip
                    id="schedule-resource-detail"
                    title="Durable timing intent"
                    description="Persisted definition state for this resource. The broker observation counter is non-authoritative and is not downstream execution history."
                    items={[
                      { label: "Enabled", value: resourceDetail.enabled ? "Yes" : "No" },
                      { label: "Cron", value: resourceDetail.cron ?? "unset" },
                      scheduleTimingMetric(resourceDetail.next_run),
                      {
                        label: "Broker observation counter",
                        value: resourceDetail.executions_total,
                        caption: "Non-authoritative; not downstream execution history",
                      },
                      { label: "Pending handoffs", value: pendingHandoffs },
                    ]}
                  />
                )}
              </Show>

              <DomainOperationTable<ScheduleExecutionObservation>
                domain="schedule"
                description={`Schedule-owned observations from persisted timing intent and acknowledged handoffs. Showing ${formatCount(rows.length, "schedule")} from offset ${formatNumber(offset)}; a schedule with no observation does not appear.`}
                emptyDescription="No schedule observations are currently visible for this resource."
                metricColumns={scheduleOperationColumns}
                rows={rows}
                scope={ref}
                title="Individual schedules"
              />

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
            </Block>
          )}
        </Show>
      </Block>
    </DomainPageFrame>
  );
}
