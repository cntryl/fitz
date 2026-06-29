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
import DomainHeader from "@/components/shared/domain-header";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import { createDomainSidebar } from "@/components/shared/domain-sidebar";
import DomainWorkflowPanel from "@/components/shared/domain-workflow-panel";
import {
  QueryEmptyState,
  QueryErrorState,
  QueryLoadingState,
  QueryRefreshingState,
} from "@/components/shared/query-state";
import {
  createScheduleAreaQuery,
  createScheduleOverviewQuery,
  createScheduleRealmQuery,
} from "@/features/schedule/schedule-query";
import type {
  ScheduleAreaInventory,
  ScheduleOverview,
  ScheduleRealmInventory,
} from "@/features/schedule/schedule-models";
import { formatDurationSeconds, formatNumber } from "@/shared/format";
import { domainHref, domainResourceHref, domainScopeHref } from "@/shared/navigation/domains";

interface ScheduleHealth {
  detail: string;
  label: "Live" | "Pressure" | "Attention";
  tone: "success" | "warning" | "danger";
}

function decodeParam(value: string | undefined) {
  if (!value) return undefined;

  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

function summarizeScheduleHealth(stats: ScheduleOverview["stats"]): ScheduleHealth {
  const persistenceFailures =
    stats.createPersistenceFailuresTotal +
    stats.upsertPersistenceFailuresTotal +
    stats.cancelPersistenceFailuresTotal;
  const handoffFailures =
    stats.ackFailuresTotal + stats.notifyFailuresTotal + stats.overdueNormalizationsTotal;

  if (persistenceFailures > 0 || handoffFailures > 0) {
    return {
      detail: `${formatNumber(stats.schedulesActive)} active schedules are visible. Persistence and handoff failure counters need attention.`,
      label: "Attention",
      tone: "danger",
    };
  }

  if (stats.pendingFireClaims > 0) {
    return {
      detail: `${formatNumber(stats.pendingFireClaims)} pending fire claim(s) are waiting for live handoff.`,
      label: "Pressure",
      tone: "warning",
    };
  }

  return {
    detail:
      stats.subscriptionsActive > 0
        ? `${formatNumber(stats.subscriptionsActive)} active live subscription(s) are visible for handoff.`
        : "No live handoff subscriptions are visible.",
    label: "Live",
    tone: "success",
  };
}

function persistenceFailureCount(stats: ScheduleOverview["stats"]) {
  return (
    stats.createPersistenceFailuresTotal +
    stats.upsertPersistenceFailuresTotal +
    stats.cancelPersistenceFailuresTotal
  );
}

function failureCount(stats: ScheduleOverview["stats"]) {
  return (
    persistenceFailureCount(stats) +
    stats.ackFailuresTotal +
    stats.notifyFailuresTotal +
    stats.overdueNormalizationsTotal
  );
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

function ScheduleRealmRows(props: { realms: ScheduleOverview["realms"] }) {
  if (props.realms.length === 0) {
    return <QueryEmptyState description="No schedule realms are currently visible." />;
  }

  return (
    <div class="domain-table-wrap">
      <Table>
        <TableHead>
          <TableRow>
            <TableHeaderCell>Realm</TableHeaderCell>
            <TableHeaderCell>Next step</TableHeaderCell>
          </TableRow>
        </TableHead>
        <TableBody>
          <For each={props.realms} by={(realm) => realm.realm}>
            {(realm) => (
              <TableRow>
                <TableCell>
                  <Link
                    class="domain-link-cell"
                    href={domainScopeHref("schedule", { realm: realm.realm })}
                  >
                    {realm.realm}
                  </Link>
                </TableCell>
                <TableCell>Inspect areas and schedule counts</TableCell>
              </TableRow>
            )}
          </For>
        </TableBody>
      </Table>
    </div>
  );
}

function ScheduleAreaRows(props: { data: ScheduleRealmInventory }) {
  if (props.data.areas.length === 0) {
    return <QueryEmptyState description="No schedule areas are currently visible." />;
  }

  return (
    <div class="domain-table-wrap">
      <Table>
        <TableHead>
          <TableRow>
            <TableHeaderCell>Area</TableHeaderCell>
            <TableHeaderCell>Schedules</TableHeaderCell>
          </TableRow>
        </TableHead>
        <TableBody>
          <For each={props.data.areas} by={(area) => area.area}>
            {(area) => (
              <TableRow>
                <TableCell>
                  <Link
                    class="domain-link-cell"
                    href={domainScopeHref("schedule", {
                      area: area.area,
                      realm: props.data.realm,
                    })}
                  >
                    {area.area}
                  </Link>
                </TableCell>
                <TableCell>{formatNumber(area.resources.length)}</TableCell>
              </TableRow>
            )}
          </For>
        </TableBody>
      </Table>
    </div>
  );
}

function ScheduleResourceRows(props: { data: ScheduleAreaInventory }) {
  if (props.data.resources.length === 0) {
    return <QueryEmptyState description="No schedule resources are currently visible." />;
  }

  return (
    <div class="domain-table-wrap">
      <Table>
        <TableHead>
          <TableRow>
            <TableHeaderCell>Schedule</TableHeaderCell>
            <TableHeaderCell>Scope</TableHeaderCell>
          </TableRow>
        </TableHead>
        <TableBody>
          <For each={props.data.resources} by={(resource) => resource}>
            {(resource) => (
              <TableRow>
                <TableCell>
                  <Link
                    class="domain-link-cell"
                    href={domainResourceHref("schedule", {
                      area: props.data.area,
                      realm: props.data.realm,
                      resource,
                    })}
                  >
                    {resource}
                  </Link>
                </TableCell>
                <TableCell>
                  {props.data.realm} / {props.data.area}
                </TableCell>
              </TableRow>
            )}
          </For>
        </TableBody>
      </Table>
    </div>
  );
}

function ScheduleOverviewPage() {
  const overview = createScheduleOverviewQuery();
  const data = overview.data;
  const emptyStats: ScheduleOverview["stats"] = {
    ackFailuresTotal: 0,
    cancelPersistenceFailuresTotal: 0,
    createPersistenceFailuresTotal: 0,
    executionsPerMinute: 0,
    notifyFailuresTotal: 0,
    overdueNormalizationsTotal: 0,
    pendingFireClaims: 0,
    schedulesActive: 0,
    subscriptionsActive: 0,
    upsertPersistenceFailuresTotal: 0,
  };
  const health = summarizeScheduleHealth(data?.stats ?? emptyStats);
  const snapshot = createDomainSidebar({
    data,
    title: "Schedule snapshot",
    description: "Durable timing intent and live handoff visibility.",
    stats: (current: ScheduleOverview) => [
      { label: "Visible realms", value: current.realms.length },
      { label: "Active schedules", value: current.stats.schedulesActive },
      {
        label: "Active subscriptions",
        value: current.stats.subscriptionsActive,
        note: "Live listener visibility",
      },
      {
        label: "Pending fire claims",
        value: current.stats.pendingFireClaims,
        note: "Persisted timing claims awaiting live handoff",
      },
      { label: "Failure signals", value: failureCount(current.stats) },
    ],
  });

  return (
    <DomainPageFrame>
      <Stack gap="3">
        <DomainHeader
          eyebrow="Timing intent"
          title="Schedule overview"
          description="Durable schedule definitions with live listener and next-run posture."
          primaryAction={{
            label: "Refresh schedule",
            onPress: () => overview.refresh(),
          }}
          status={{
            detail: `${health.detail} Schedule does not imply durable downstream delivery.`,
            label: overview.refreshing ? "Refreshing" : overview.stale ? "Stale" : health.label,
            tone: overview.refreshing ? "info" : overview.stale ? "warning" : health.tone,
          }}
        />

        {snapshot}

        <Show when={!data && overview.loading}>
          <QueryLoadingState description="Loading schedule overview snapshot..." />
        </Show>

        <Show when={!data && overview.error}>
          <QueryErrorState
            title="Schedule overview loading failure"
            error={overview.error}
            onRetry={() => overview.refresh()}
          />
        </Show>

        <Show when={data}>
          {(current) => (
            <Stack gap="3">
              <Show when={overview.refreshing}>
                <QueryRefreshingState description="Refreshing schedule overview..." />
              </Show>

              <DomainMetricTable
                title="Schedule posture"
                description="Domain-level answers for listener visibility, pending claims, and failure signals."
                metrics={[
                  {
                    label: "Is anyone listening?",
                    value:
                      current.stats.subscriptionsActive > 0
                        ? `${formatNumber(current.stats.subscriptionsActive)} live subscription(s)`
                        : "No live listeners visible",
                    caption: "Live subscription visibility only",
                  },
                  {
                    label: "When is the next run?",
                    value: "Open a schedule resource",
                    caption: "Next run is resource-specific",
                  },
                  { label: "Active schedules", value: current.stats.schedulesActive },
                  { label: "Pending fire claims", value: current.stats.pendingFireClaims },
                  {
                    label: "Oldest pending claim age",
                    value:
                      current.stats.pendingFireClaims > 0
                        ? "Open a schedule resource"
                        : formatDurationSeconds(0),
                    caption: "Available on missed handoff rows when present",
                  },
                  {
                    label: "Persistence failures",
                    value: persistenceFailureCount(current.stats),
                  },
                  {
                    label: "Handoff failures",
                    value:
                      current.stats.ackFailuresTotal +
                      current.stats.notifyFailuresTotal +
                      current.stats.overdueNormalizationsTotal,
                  },
                ]}
              />

              <ScheduleCard
                title="Schedule realms"
                description={`${formatNumber(current.realms.length)} visible realm(s).`}
              >
                <ScheduleRealmRows realms={current.realms} />
              </ScheduleCard>

              <DomainWorkflowPanel
                archetype="Schedule Timing"
                workflows={["Drill down", "Inspect next run", "Review handoff evidence"]}
                questions={["Is anyone listening?", "When is the next run?"]}
                diagnostics={["Persistence failures", "Pending claims", "Live subscriptions"]}
              />
            </Stack>
          )}
        </Show>
      </Stack>
    </DomainPageFrame>
  );
}

function ScheduleRealmPage(props: { realm: string }) {
  const query = createScheduleRealmQuery(props.realm);
  const data = query.data;

  return (
    <DomainPageFrame>
      <Stack gap="3">
        <DomainHeader
          eyebrow="Schedule realm"
          title={props.realm}
          description={`Schedule areas and resource counts for ${props.realm}.`}
          primaryAction={{ label: "Refresh realm", onPress: () => query.refresh() }}
          status={{
            detail: data
              ? `${formatNumber(data.areas.length)} area(s), ${formatNumber(data.resourceCount)} schedule resource(s).`
              : "Loading schedule realm.",
            label: query.refreshing ? "Refreshing" : query.stale ? "Stale" : "Live",
            tone: query.refreshing ? "info" : query.stale ? "warning" : "success",
          }}
        />

        <Show when={!data && query.loading}>
          <QueryLoadingState description="Loading schedule realm..." />
        </Show>

        <Show when={!data && query.error}>
          <QueryErrorState
            title="Unable to load schedule realm"
            error={query.error}
            onRetry={() => query.refresh()}
          />
        </Show>

        <Show when={data}>
          {(current) => (
            <Stack gap="3">
              <Show when={query.refreshing}>
                <QueryRefreshingState description="Refreshing schedule realm..." />
              </Show>

              <ScheduleCard
                title="Schedule areas"
                description={`${formatNumber(current.resourceCount)} schedule resource(s).`}
              >
                <ScheduleAreaRows data={current} />
              </ScheduleCard>

              <Link class="text-link" href={domainHref("schedule")}>
                Back to Schedule overview
              </Link>
            </Stack>
          )}
        </Show>
      </Stack>
    </DomainPageFrame>
  );
}

function ScheduleAreaPage(props: { area: string; realm: string }) {
  const query = createScheduleAreaQuery(props.realm, props.area);
  const data = query.data;

  return (
    <DomainPageFrame>
      <Stack gap="3">
        <DomainHeader
          eyebrow="Schedule area"
          title={props.area}
          description={`Schedule resources in ${props.realm}/${props.area}.`}
          primaryAction={{ label: "Refresh area", onPress: () => query.refresh() }}
          status={{
            detail: data
              ? `${formatNumber(data.resourceCount)} schedule resource(s).`
              : "Loading schedule area.",
            label: query.refreshing ? "Refreshing" : query.stale ? "Stale" : "Live",
            tone: query.refreshing ? "info" : query.stale ? "warning" : "success",
          }}
        />

        <Show when={!data && query.loading}>
          <QueryLoadingState description="Loading schedule area..." />
        </Show>

        <Show when={!data && query.error}>
          <QueryErrorState
            title="Unable to load schedule area"
            error={query.error}
            onRetry={() => query.refresh()}
          />
        </Show>

        <Show when={data}>
          {(current) => (
            <Stack gap="3">
              <Show when={query.refreshing}>
                <QueryRefreshingState description="Refreshing schedule area..." />
              </Show>

              <ScheduleCard
                title="Schedule resources"
                description="Open a resource for listener visibility, next run, and handoff evidence."
              >
                <ScheduleResourceRows data={current} />
              </ScheduleCard>

              <Link class="text-link" href={domainScopeHref("schedule", { realm: props.realm })}>
                Back to schedule realm
              </Link>
            </Stack>
          )}
        </Show>
      </Stack>
    </DomainPageFrame>
  );
}

export default function SchedulePage() {
  const route = currentRoute();
  const realm = decodeParam(route.params.realm);
  const area = decodeParam(route.params.area);

  if (realm && area) return <ScheduleAreaPage area={area} realm={realm} />;
  if (realm) return <ScheduleRealmPage realm={realm} />;

  return <ScheduleOverviewPage />;
}
