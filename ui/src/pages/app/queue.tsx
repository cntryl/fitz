import { For } from "@askrjs/askr/control";
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
  createQueueAreaQuery,
  createQueueOverviewQuery,
  createQueueRealmQuery,
} from "@/features/queue/queue-query";
import type {
  QueueAreaDetail,
  QueueAreaSummary,
  QueueOperationalSummary,
  QueueOverview,
  QueueRealmDetail,
  QueueRealmSummary,
  QueueResourceSummary,
  QueueStatus,
} from "@/features/queue/queue-models";
import { formatDurationSeconds, formatNumber } from "@/shared/format";
import { domainResourceHref, domainScopeHref } from "@/shared/navigation/domains";

type QueueTone = "info" | "success" | "warning" | "danger";

function statusLabel(status: QueueStatus) {
  switch (status) {
    case "falling_behind":
      return "Falling behind";
    case "backlogged":
      return "Backlogged";
    case "draining":
      return "Draining";
    case "idle":
      return "Idle";
  }
}

function statusTone(status: QueueStatus): QueueTone {
  switch (status) {
    case "falling_behind":
      return "danger";
    case "backlogged":
      return "warning";
    case "draining":
      return "info";
    case "idle":
      return "success";
  }
}

function rate(value: number) {
  return value.toFixed(2);
}

function describeQueue(summary: QueueOperationalSummary) {
  if (summary.status === "falling_behind") {
    return `Backlog exists and enqueues are outpacing completes. Oldest backlog is ${formatDurationSeconds(
      summary.oldestBacklogAgeSeconds,
    )}.`;
  }

  if (summary.status === "backlogged") {
    return `${formatNumber(summary.messagesTotal)} messages are visible across ready, delayed, inflight, and dead-letter states.`;
  }

  if (summary.status === "draining") {
    return `${formatNumber(summary.messagesInflight)} messages are currently in flight.`;
  }

  return "No visible queue backlog at this level.";
}

function describeQueueStats(stats: QueueOverview["stats"]) {
  const visible =
    stats.messagesReady + stats.messagesDelayed + stats.inflightActive + stats.messagesDeadLettered;

  if (stats.messagesDeadLettered > 0) {
    return `${formatNumber(stats.messagesDeadLettered)} dead-lettered message(s) need explicit operator action.`;
  }

  if (stats.messagesReady > 0 || stats.messagesDelayed > 0) {
    return `${formatNumber(visible)} message(s) are visible across ready, delayed, inflight, and dead-letter states. Oldest backlog is ${formatDurationSeconds(
      stats.oldestBacklogAgeSeconds,
    )}.`;
  }

  if (stats.inflightActive > 0) {
    return `${formatNumber(stats.inflightActive)} message(s) are currently in flight.`;
  }

  return "No visible queue backlog at this level.";
}

function metricsFor(summary: QueueOperationalSummary) {
  return [
    { label: "Total messages", value: summary.messagesTotal },
    { label: "Ready", value: summary.messagesReady },
    { label: "In flight", value: summary.messagesInflight },
    { label: "Subscriptions", value: summary.subscriptionsActive },
    { label: "In / sec", value: rate(summary.inRatePerSecond) },
    { label: "Out / sec", value: rate(summary.outRatePerSecond) },
    {
      label: "Oldest backlog",
      value: formatDurationSeconds(summary.oldestBacklogAgeSeconds),
    },
    { label: "Status", value: statusLabel(summary.status) },
  ];
}

function QueueCard(props: { children: unknown; description?: string; title: string }) {
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

function RealmRows(props: { realms: QueueRealmSummary[] }) {
  if (props.realms.length === 0) {
    return <QueryEmptyState description="No visible queues at the current level." />;
  }

  return (
    <div class="domain-table-wrap">
      <Table>
        <TableHead>
          <TableRow>
            <TableHeaderCell>Realm</TableHeaderCell>
            <TableHeaderCell>Queues</TableHeaderCell>
            <TableHeaderCell>Total</TableHeaderCell>
            <TableHeaderCell>In flight</TableHeaderCell>
            <TableHeaderCell>Subscriptions</TableHeaderCell>
            <TableHeaderCell>Status</TableHeaderCell>
          </TableRow>
        </TableHead>
        <TableBody>
          <For each={props.realms} by={(realm) => realm.realm}>
            {(realm) => (
              <TableRow>
                <TableCell>
                  <Link href={domainScopeHref("queue", { realm: realm.realm })}>{realm.realm}</Link>
                </TableCell>
                <TableCell>{formatNumber(realm.queueCount)}</TableCell>
                <TableCell>{formatNumber(realm.messagesTotal)}</TableCell>
                <TableCell>{formatNumber(realm.messagesInflight)}</TableCell>
                <TableCell>{formatNumber(realm.subscriptionsActive)}</TableCell>
                <TableCell>{statusLabel(realm.status)}</TableCell>
              </TableRow>
            )}
          </For>
        </TableBody>
      </Table>
    </div>
  );
}

function AreaRows(props: { areas: QueueAreaSummary[] }) {
  if (props.areas.length === 0) {
    return <QueryEmptyState description="No visible queues at the current level." />;
  }

  return (
    <div class="domain-table-wrap">
      <Table>
        <TableHead>
          <TableRow>
            <TableHeaderCell>Area</TableHeaderCell>
            <TableHeaderCell>Queues</TableHeaderCell>
            <TableHeaderCell>Total</TableHeaderCell>
            <TableHeaderCell>Ready</TableHeaderCell>
            <TableHeaderCell>In flight</TableHeaderCell>
            <TableHeaderCell>Status</TableHeaderCell>
          </TableRow>
        </TableHead>
        <TableBody>
          <For each={props.areas} by={(area) => `${area.realm}/${area.area}`}>
            {(area) => (
              <TableRow>
                <TableCell>
                  <Link href={domainScopeHref("queue", { area: area.area, realm: area.realm })}>
                    {area.area}
                  </Link>
                </TableCell>
                <TableCell>{formatNumber(area.queueCount)}</TableCell>
                <TableCell>{formatNumber(area.messagesTotal)}</TableCell>
                <TableCell>{formatNumber(area.messagesReady)}</TableCell>
                <TableCell>{formatNumber(area.messagesInflight)}</TableCell>
                <TableCell>{statusLabel(area.status)}</TableCell>
              </TableRow>
            )}
          </For>
        </TableBody>
      </Table>
    </div>
  );
}

function QueueRows(props: { queues: QueueResourceSummary[] }) {
  if (props.queues.length === 0) {
    return <QueryEmptyState description="No visible queues at the current level." />;
  }

  return (
    <div class="domain-table-wrap">
      <Table>
        <TableHead>
          <TableRow>
            <TableHeaderCell>Queue</TableHeaderCell>
            <TableHeaderCell>Total</TableHeaderCell>
            <TableHeaderCell>Ready</TableHeaderCell>
            <TableHeaderCell>In flight</TableHeaderCell>
            <TableHeaderCell>Subscriptions</TableHeaderCell>
            <TableHeaderCell>In / sec</TableHeaderCell>
            <TableHeaderCell>Out / sec</TableHeaderCell>
            <TableHeaderCell>Oldest</TableHeaderCell>
            <TableHeaderCell>Status</TableHeaderCell>
          </TableRow>
        </TableHead>
        <TableBody>
          <For each={props.queues} by={(queue) => `${queue.realm}/${queue.area}/${queue.resource}`}>
            {(queue) => (
              <TableRow>
                <TableCell>
                  <Link href={domainResourceHref("queue", queue)}>{queue.resource}</Link>
                </TableCell>
                <TableCell>{formatNumber(queue.messagesTotal)}</TableCell>
                <TableCell>{formatNumber(queue.messagesReady)}</TableCell>
                <TableCell>{formatNumber(queue.messagesInflight)}</TableCell>
                <TableCell>{formatNumber(queue.subscriptionsActive)}</TableCell>
                <TableCell>{rate(queue.inRatePerSecond)}</TableCell>
                <TableCell>{rate(queue.outRatePerSecond)}</TableCell>
                <TableCell>{formatDurationSeconds(queue.oldestBacklogAgeSeconds)}</TableCell>
                <TableCell>{statusLabel(queue.status)}</TableCell>
              </TableRow>
            )}
          </For>
        </TableBody>
      </Table>
    </div>
  );
}

function QueueOverviewPage() {
  const overview = createQueueOverviewQuery();
  const data = overview.data;

  const snapshot = createDomainSidebar({
    data,
    title: "Scope summary",
    description: "Queue realms",
    stats: (current: QueueOverview) => [
      { label: "Visible realms", value: current.realms.length },
      { label: "Ready", value: current.stats.messagesReady },
      { label: "Delayed", value: current.stats.messagesDelayed },
      { label: "In flight", value: current.stats.inflightActive },
      { label: "Dead-lettered", value: current.stats.messagesDeadLettered },
      {
        label: "Oldest backlog",
        value: formatDurationSeconds(current.stats.oldestBacklogAgeSeconds),
      },
    ],
  });

  return (
    <DomainPageFrame>
      <Stack gap="3">
        <DomainHeader
          eyebrow="Durable work"
          title="Queue overview"
          description="Durable work backlog, reservations, delayed work, and dead-letter pressure by scope."
          primaryAction={{ label: "Refresh queue", onPress: () => overview.refresh() }}
          status={{
            detail: data
              ? `${formatNumber(data.realms.length)} visible realm(s). ${describeQueueStats(data.stats)}`
              : "Loading queue realms.",
            label: overview.refreshing ? "Refreshing" : overview.stale ? "Stale" : "Live",
            tone: overview.refreshing ? "info" : overview.stale ? "warning" : "success",
          }}
        />

        {snapshot}

        {!data && overview.loading ? (
          <QueryLoadingState description="Loading queue overview..." />
        ) : null}
        {!data && overview.error ? (
          <QueryErrorState
            title="Unable to load Queue overview"
            error={overview.error}
            onRetry={() => overview.refresh()}
          />
        ) : null}
        {data ? (
          <Stack gap="3">
            {overview.refreshing ? (
              <QueryRefreshingState description="Refreshing queue overview..." />
            ) : null}
            <DomainMetricTable
              title="Queue totals"
              description="Durable work posture across ready, delayed, inflight, and dead-letter states."
              metrics={[
                { label: "Ready", value: data.stats.messagesReady },
                { label: "Delayed", value: data.stats.messagesDelayed },
                { label: "In flight", value: data.stats.inflightActive },
                { label: "Dead-lettered", value: data.stats.messagesDeadLettered },
                { label: "Pending", value: data.stats.messagesPending },
                {
                  label: "Oldest backlog",
                  value: formatDurationSeconds(data.stats.oldestBacklogAgeSeconds),
                },
              ]}
            />
            <QueueCard title="Queue realms">
              <RealmRows realms={data.realms} />
            </QueueCard>
            <DomainWorkflowPanel
              archetype="Queue operations"
              workflows={[
                "Review realm and area rollups before inspecting an individual queue.",
                "Use ready, in-flight, and dead-letter counts to separate backlog from worker drain.",
                "Inspect the queue detail page when a single queue owns the pressure.",
              ]}
              questions={[
                "Is backlog growing faster than completes?",
                "Is work delayed, ready, in flight, or dead-lettered?",
                "Is the oldest backlog age inside the operating window?",
              ]}
              diagnostics={[
                "Falling behind requires backlog plus enqueue rate above complete rate.",
                "Backlogged means visible ready, delayed, or dead-lettered work exists.",
                "Draining means work is currently in flight with no visible backlog.",
              ]}
            />
          </Stack>
        ) : null}
      </Stack>
    </DomainPageFrame>
  );
}

function QueueRealmPage(props: { realm: string }) {
  const realmQuery = createQueueRealmQuery(props.realm);
  const data = realmQuery.data;
  const snapshot = createDomainSidebar({
    data,
    title: "Scope summary",
    description: props.realm,
    stats: (current: QueueRealmDetail) => metricsFor(current),
  });

  return (
    <DomainPageFrame>
      <Stack gap="3">
        <DomainHeader
          eyebrow="Queue realm"
          title={props.realm}
          description="Areas and queues currently visible in this realm."
          primaryAction={{ label: "Refresh realm", onPress: () => realmQuery.refresh() }}
          status={{
            detail: data ? describeQueue(data) : "Loading queue realm.",
            label: realmQuery.refreshing
              ? "Refreshing"
              : realmQuery.stale
                ? "Stale"
                : data
                  ? statusLabel(data.status)
                  : "Loading",
            tone: realmQuery.refreshing
              ? "info"
              : realmQuery.stale
                ? "warning"
                : data
                  ? statusTone(data.status)
                  : "info",
          }}
        />

        {snapshot}

        {!data && realmQuery.loading ? (
          <QueryLoadingState description="Loading queue realm..." />
        ) : null}
        {!data && realmQuery.error ? (
          <QueryErrorState error={realmQuery.error} onRetry={() => realmQuery.refresh()} />
        ) : null}

        {data ? (
          <Stack gap="3">
            {realmQuery.refreshing ? (
              <QueryRefreshingState description="Refreshing queue realm..." />
            ) : null}
            <DomainMetricTable
              title="Realm totals"
              description="Durable work posture across this realm."
              metrics={metricsFor(data)}
            />
            <QueueCard title="Areas">
              <AreaRows areas={data.areas} />
            </QueueCard>
            <QueueCard title="Queues">
              <QueueRows queues={data.queues} />
            </QueueCard>
          </Stack>
        ) : null}
      </Stack>
    </DomainPageFrame>
  );
}

function QueueAreaPage(props: { area: string; realm: string }) {
  const areaQuery = createQueueAreaQuery(props.realm, props.area);
  const data = areaQuery.data;
  const snapshot = createDomainSidebar({
    data,
    title: "Scope summary",
    description: `${props.realm}/${props.area}`,
    stats: (current: QueueAreaDetail) => metricsFor(current),
    footer: <Link href={domainScopeHref("queue", { realm: props.realm })}>Back to realm</Link>,
  });

  return (
    <DomainPageFrame>
      <Stack gap="3">
        <DomainHeader
          eyebrow="Queue area"
          title={props.area}
          description={`${props.realm}/${props.area}`}
          primaryAction={{ label: "Refresh area", onPress: () => areaQuery.refresh() }}
          status={{
            detail: data ? describeQueue(data) : "Loading queue area.",
            label: areaQuery.refreshing
              ? "Refreshing"
              : areaQuery.stale
                ? "Stale"
                : data
                  ? statusLabel(data.status)
                  : "Loading",
            tone: areaQuery.refreshing
              ? "info"
              : areaQuery.stale
                ? "warning"
                : data
                  ? statusTone(data.status)
                  : "info",
          }}
        />

        {snapshot}

        {!data && areaQuery.loading ? (
          <QueryLoadingState description="Loading queue area..." />
        ) : null}
        {!data && areaQuery.error ? (
          <QueryErrorState error={areaQuery.error} onRetry={() => areaQuery.refresh()} />
        ) : null}

        {data ? (
          <Stack gap="3">
            {areaQuery.refreshing ? (
              <QueryRefreshingState description="Refreshing queue area..." />
            ) : null}
            <DomainMetricTable
              title="Area totals"
              description="Durable work posture across this area."
              metrics={metricsFor(data)}
            />
            <QueueCard title="Queues">
              <QueueRows queues={data.queues} />
            </QueueCard>
          </Stack>
        ) : null}
      </Stack>
    </DomainPageFrame>
  );
}

export default function QueuePage() {
  const { area, realm } = currentRoute().params;

  if (realm && area) {
    return <QueueAreaPage area={area} realm={realm} />;
  }

  if (realm) {
    return <QueueRealmPage realm={realm} />;
  }

  return <QueueOverviewPage />;
}
