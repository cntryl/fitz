import DomainHeader from "@/components/shared/domain-header";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import DomainResourceBrowser from "@/components/shared/domain-resource-browser";
import DomainWorkflowPanel from "@/components/shared/domain-workflow-panel";
import DomainRealmTable from "@/components/shared/domain-realm-table";
import { Stack } from "@askrjs/themes/layouts";
import {
  QueryErrorState,
  QueryLoadingState,
  QueryRefreshingState,
} from "@/components/shared/query-state";
import { createDomainSidebar } from "@/components/shared/domain-sidebar";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import ScheduleTimePlanner from "@/features/schedule/schedule-time-planner";
import { createScheduleOverviewQuery } from "@/features/schedule/schedule-query";
import { createResourceInventoryQuery } from "@/features/resource/resource-query";

interface ScheduleHealth {
  detail: string;
  label: "Live" | "Pressure" | "Attention";
  tone: "success" | "warning" | "danger";
}

function summarizeScheduleHealth(stats: {
  ackFailuresTotal: number;
  cancelPersistenceFailuresTotal: number;
  createPersistenceFailuresTotal: number;
  notifyFailuresTotal: number;
  overdueNormalizationsTotal: number;
  pendingFireClaims: number;
  schedulesActive: number;
  subscriptionsActive: number;
  upsertPersistenceFailuresTotal: number;
}): ScheduleHealth {
  const persistenceFailures =
    stats.createPersistenceFailuresTotal +
    stats.upsertPersistenceFailuresTotal +
    stats.cancelPersistenceFailuresTotal;

  const reliabilityFailures =
    stats.ackFailuresTotal + stats.notifyFailuresTotal + stats.overdueNormalizationsTotal;

  if (stats.pendingFireClaims > 4 || reliabilityFailures > 0 || persistenceFailures > 0) {
    return {
      detail: `Timing intent is durable, but ${stats.pendingFireClaims} pending fire claim(s) and ${
        reliabilityFailures + persistenceFailures
      } delivery/retry failures indicate handoff attention is needed.`,
      label: "Attention",
      tone: "danger",
    };
  }

  if (stats.pendingFireClaims > 0) {
    return {
      detail: `${stats.schedulesActive} active schedules are visible, with ${stats.pendingFireClaims} claim(s) still waiting for broker handoff.`,
      label: "Pressure",
      tone: "warning",
    };
  }

  return {
    detail: `${stats.schedulesActive} active schedules and ${stats.subscriptionsActive} active handoff subscriptions are currently visible.`,
    label: "Live",
    tone: "success",
  };
}

function metricWithAttention(value: number, label: string) {
  return {
    label,
    value,
    ...(value > 0 ? { caption: "attention" } : undefined),
  };
}

function persistenceFailureCount(stats: {
  cancelPersistenceFailuresTotal: number;
  createPersistenceFailuresTotal: number;
  upsertPersistenceFailuresTotal: number;
}) {
  return (
    stats.createPersistenceFailuresTotal +
    stats.upsertPersistenceFailuresTotal +
    stats.cancelPersistenceFailuresTotal
  );
}

export default function SchedulePage() {
  const overview = createScheduleOverviewQuery();
  const inventory = createResourceInventoryQuery("schedule");
  const data = overview.data;
  const health = summarizeScheduleHealth(
    data?.stats
      ? {
          ackFailuresTotal: data.stats.ackFailuresTotal,
          cancelPersistenceFailuresTotal: data.stats.cancelPersistenceFailuresTotal,
          createPersistenceFailuresTotal: data.stats.createPersistenceFailuresTotal,
          notifyFailuresTotal: data.stats.notifyFailuresTotal,
          overdueNormalizationsTotal: data.stats.overdueNormalizationsTotal,
          pendingFireClaims: data.stats.pendingFireClaims,
          schedulesActive: data.stats.schedulesActive,
          subscriptionsActive: data.stats.subscriptionsActive,
          upsertPersistenceFailuresTotal: data.stats.upsertPersistenceFailuresTotal,
        }
      : {
          ackFailuresTotal: 0,
          cancelPersistenceFailuresTotal: 0,
          createPersistenceFailuresTotal: 0,
          notifyFailuresTotal: 0,
          overdueNormalizationsTotal: 0,
          pendingFireClaims: 0,
          schedulesActive: 0,
          subscriptionsActive: 0,
          upsertPersistenceFailuresTotal: 0,
        },
  );
  const snapshot = createDomainSidebar({
    data,
    title: "Schedule snapshot",
    description: "Durable timing intent with live handoff context.",
    stats: (current) => [
      { label: "Visible schedule realms", value: current.realms.length },
      { label: "Active schedules", value: current.stats.schedulesActive },
      {
        label: "Pending fire claims",
        value: current.stats.pendingFireClaims,
        note: "Live handoff backlog",
      },
      { label: "Active subscriptions", value: current.stats.subscriptionsActive },
      {
        label: "Persistence failures",
        value: persistenceFailureCount(current.stats),
      },
    ],
  });

  return (
    <DomainPageFrame>
      <Stack gap="3">
        <DomainHeader
          eyebrow="Timing intent"
          title="Schedule overview"
          description="Durable schedule definitions and live handoff status."
          primaryAction={{
            label: "Refresh schedule",
            onPress: () => overview.refresh(),
          }}
          status={{
            detail: `${health.detail} Timing intent is independent of downstream completion guarantees.`,
            label: overview.refreshing ? "Refreshing" : overview.stale ? "Stale" : health.label,
            tone: overview.refreshing ? "info" : overview.stale ? "warning" : health.tone,
          }}
        />

        {snapshot}

        {!data && overview.loading ? (
          <QueryLoadingState description="Loading schedule overview snapshot..." />
        ) : null}

        {!data && overview.error ? (
          <QueryErrorState
            title="Schedule overview loading failure"
            error={overview.error}
            onRetry={() => overview.refresh()}
          />
        ) : null}

        {data ? (
          <Stack gap="3">
            {overview.refreshing ? (
              <QueryRefreshingState description="Refreshing schedule overview..." />
            ) : null}

            <ScheduleTimePlanner
              error={inventory.error}
              inventory={inventory.data}
              loading={inventory.loading}
            />

            <DomainMetricTable
              title="Schedule metrics"
              description="Active schedules, pending handoffs, and failure signals."
              metrics={[
                { label: "Active schedules", value: data.stats.schedulesActive },
                { label: "Pending fire claims", value: data.stats.pendingFireClaims },
                {
                  label: "Executions / min",
                  value: data.stats.executionsPerMinute.toFixed(2),
                },
                metricWithAttention(data.stats.ackFailuresTotal, "Ack failures"),
                metricWithAttention(data.stats.notifyFailuresTotal, "Notify failures"),
                {
                  label: "Active subscriptions",
                  value: data.stats.subscriptionsActive,
                },
                metricWithAttention(
                  data.stats.createPersistenceFailuresTotal,
                  "Create persistence failures",
                ),
                metricWithAttention(
                  data.stats.upsertPersistenceFailuresTotal,
                  "Upsert persistence failures",
                ),
                metricWithAttention(
                  data.stats.cancelPersistenceFailuresTotal,
                  "Cancel persistence failures",
                ),
              ]}
            />

            <DomainRealmTable
              title="Schedule realms"
              realms={data.realms}
              emptyMessage="No schedule realms are currently visible."
            />

            <DomainResourceBrowser
              domain="schedule"
              error={inventory.error}
              inventory={inventory.data}
              loading={inventory.loading}
            />

            <DomainWorkflowPanel
              archetype="Schedule Time Planner"
              workflows={[
                "Timeline review",
                "Execution review",
                "Failure investigation",
                "Configuration review",
              ]}
              questions={["What should happen?", "What happened?", "What will happen next?"]}
              diagnostics={["Persistence failures", "Overdue normalization", "Timing internals"]}
            />
          </Stack>
        ) : null}
      </Stack>
    </DomainPageFrame>
  );
}
