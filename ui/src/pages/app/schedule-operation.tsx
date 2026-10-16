import { state } from "@askrjs/askr";
import { Show } from "@askrjs/askr/control";
import { currentRoute } from "@askrjs/askr/router";
import { Table, TableBody, TableCell, TableHead, TableHeaderCell, TableRow } from "@askrjs/ui";
import { Badge, Block } from "@askrjs/themes/components";
import type { ScheduleMissedObservation, ScheduleRunNowResponse } from "@/adapters";
import DomainHeader from "@/components/shared/domain-header";
import DomainDataSection from "@/components/shared/domain-data-section";
import DomainFacts from "@/components/shared/domain-facts";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import { queryHeaderStatus } from "@/components/shared/query-header-status";
import {
  QueryEmptyState,
  QueryErrorState,
  QueryLoadingState,
  QueryRefreshingState,
} from "@/components/shared/query-state";
import {
  decodeScheduleParam,
  formatScheduleTimestamp,
  scheduleTimingMetric,
} from "@/features/schedule/schedule-format";
import { createScheduleOperationQuery } from "@/features/schedule/schedule-query";
import { scheduleService } from "@/features/schedule/schedule-service";
import ScheduleRunNowDialog from "@/features/schedule/schedule-run-now-dialog";
import ScheduleRunNowResult from "@/features/schedule/schedule-run-now-result";
import TitledCell from "@/components/shared/titled-cell";
import { currentRouteFamilySegment } from "@/shared/navigation/domains";
import { formatDurationSeconds, formatTimestamp } from "@/shared/format";

const RESOURCE_SCHEDULE_LIMIT = 100;

function MissedRows(props: { rows: ScheduleMissedObservation[] }) {
  return (
    <Show
      when={props.rows.length > 0}
      fallback={
        <QueryEmptyState description="No pending or missed handoff claims matched this schedule." />
      }
    >
      <div id="schedule-missed-handoffs" class="domain-table-wrap">
        <Table>
          <TableHead>
            <TableRow>
              <TableHeaderCell>Fire at</TableHeaderCell>
              <TableHeaderCell>Age</TableHeaderCell>
              <TableHeaderCell>Status</TableHeaderCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {props.rows.map((row) => (
              <TableRow>
                <TableCell>
                  <TitledCell
                    subtitle={`Claimed ${formatTimestamp(row.claimed_at)} · ${row.delivery_mode}`}
                  >
                    {formatTimestamp(row.fire_at)}
                  </TitledCell>
                </TableCell>
                <TableCell>{formatDurationSeconds(row.age_seconds)}</TableCell>
                <TableCell>{row.status}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>
    </Show>
  );
}

export default function ScheduleOperationPage() {
  const route = currentRoute();
  const ref = {
    area: decodeScheduleParam(route.params.area),
    realm: decodeScheduleParam(route.params.realm),
    resource: decodeScheduleParam(route.params.resource),
  };
  const operation = decodeScheduleParam(route.params.operation);
  const query = createScheduleOperationQuery({
    ...ref,
    limit: RESOURCE_SCHEDULE_LIMIT,
    operation,
  });
  const data = query.data;
  const scheduleRow = data?.executionObservations.observations[0];
  const missedRows = data?.missedHandoffs.observations ?? [];
  const missedTruncated = missedRows.length >= RESOURCE_SCHEDULE_LIMIT;
  const timingMetric = scheduleTimingMetric(scheduleRow?.next_run);
  const [runNowOpen, setRunNowOpen] = state(false);
  const [runNowPending, setRunNowPending] = state(false);
  const [runNowError, setRunNowError] = state<unknown>(null);
  const [runNowResult, setRunNowResult] = state<ScheduleRunNowResponse | null>(null);

  async function runNow() {
    setRunNowPending(true);
    setRunNowError(null);
    try {
      const result = await scheduleService.runScheduleNow({
        ...ref,
        operation,
        routeFamily: currentRouteFamilySegment(),
      });
      setRunNowResult(result);
      setRunNowOpen(false);
    } catch (error) {
      setRunNowError(error);
    } finally {
      setRunNowPending(false);
    }
  }

  return (
    <DomainPageFrame>
      <Block direction="column" gap="sm">
        <DomainHeader
          compact={true}
          eyebrow="Schedule"
          title={operation}
          primaryAction={{
            busy: runNowPending(),
            disabled: runNowPending() || !scheduleRow,
            label: runNowPending() ? "Running…" : "Run now",
            onPress: () => setRunNowOpen(true),
          }}
          secondaryAction={{
            busy: query.refreshing,
            disabled: query.refreshing || runNowPending(),
            label: "Refresh schedule",
            onPress: () => query.refresh(),
          }}
          status={queryHeaderStatus(
            query,
            scheduleRow
              ? { label: scheduleRow.status, tone: "info" }
              : { label: "No observations", tone: "warning" },
          )}
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
          <Block direction="column" gap="sm">
            <Show when={query.refreshing}>
              <QueryRefreshingState description="Refreshing schedule..." />
            </Show>

            <DomainFacts
              id="schedule-operation-timing"
              title="Schedule timing"
              items={[
                { label: "Cron", value: scheduleRow?.cron ?? "unset" },
                {
                  label: timingMetric.label,
                  value: timingMetric.value,
                  title: timingMetric.caption,
                },
                { label: "Delivery mode", value: scheduleRow?.delivery_mode ?? "--" },
                { label: "Last handoff", value: formatScheduleTimestamp(scheduleRow?.last_run) },
                {
                  label: "Pending handoffs",
                  value: missedTruncated ? `${missedRows.length}+` : missedRows.length,
                  title: missedTruncated ? "Observation list reached the API cap" : undefined,
                },
              ]}
            />

            <Block direction="row" gap="sm">
              <ScheduleRunNowResult result={runNowResult()} />
            </Block>

            <DomainDataSection
              id="schedule-missed-handoffs-section"
              title="Pending and missed handoffs"
              actions={
                missedTruncated ? (
                  <Badge variant="warning">
                    Observation sample reached {RESOURCE_SCHEDULE_LIMIT}
                  </Badge>
                ) : undefined
              }
            >
              <MissedRows rows={missedRows} />
            </DomainDataSection>
          </Block>
        </Show>
        <ScheduleRunNowDialog
          open={runNowOpen()}
          actionPending={runNowPending()}
          actionError={runNowError()}
          routeLabel={`schedule://${ref.realm}/${ref.area}/${ref.resource}/${operation}`}
          onOpenChange={(open) => {
            if (!runNowPending()) setRunNowOpen(open);
          }}
          onRunAction={runNow}
        />
      </Block>
    </DomainPageFrame>
  );
}
