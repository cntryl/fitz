import { For } from "@askrjs/askr/control";
import { currentRoute, Link } from "@askrjs/askr/router";
import {
  QueryEmptyState,
  QueryErrorState,
  QueryLoadingState,
  QueryRefreshingState,
} from "@/components/shared/query-state";
import { Table, TableBody, TableCell, TableHead, TableHeaderCell, TableRow } from "@askrjs/ui";
import {
  Inline,
  Stack,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@askrjs/themes/components";
import DomainHeader from "@/components/shared/domain-header";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import { createDomainSidebar } from "@/components/shared/domain-sidebar";
import DomainWorkflowPanel from "@/components/shared/domain-workflow-panel";
import {
  createLeaseAreaQuery,
  createLeaseOverviewQuery,
  createLeaseRealmQuery,
} from "@/features/lease/lease-query";
import type {
  LeaseAreaResourceRows,
  LeaseOverview,
  LeaseRealmInventory,
} from "@/features/lease/lease-models";
import { formatDurationSeconds, formatNumber } from "@/shared/format";
import { domainResourceHref, domainScopeHref } from "@/shared/navigation/domains";

function decodeParam(value: string | undefined) {
  if (!value) return undefined;

  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

function riskSignal(stats: {
  acquireTimeoutsTotal: number;
  forcedReleasesTotal: number;
  invalidTokenRejectsTotal: number;
  oldestLeaseAgeSeconds: number;
  leasesActive: number;
  waiterDepth: number;
}) {
  const pressureSignals =
    stats.acquireTimeoutsTotal + stats.forcedReleasesTotal + stats.invalidTokenRejectsTotal;
  const pressureCount = pressureSignals + stats.waiterDepth;
  const riskBits = [
    stats.acquireTimeoutsTotal > 0 ? `${stats.acquireTimeoutsTotal} acquire timeout(s)` : null,
    stats.forcedReleasesTotal > 0 ? `${stats.forcedReleasesTotal} forced release(s)` : null,
    stats.invalidTokenRejectsTotal > 0 ? `${stats.invalidTokenRejectsTotal} token reject(s)` : null,
  ].filter(Boolean) as string[];
  const detailBase = `${stats.leasesActive} active leases, ${stats.waiterDepth} waiters, ${formatDurationSeconds(stats.oldestLeaseAgeSeconds)} oldest lease age.`;

  if (pressureCount > 6) {
    return {
      details: `${detailBase} Attention is warranted due ${riskBits.join(", ")}.`,
      label: "Attention" as const,
      tone: "danger" as const,
    };
  }

  if (pressureSignals > 0 || stats.waiterDepth > 0) {
    return {
      details: `${detailBase} ${stats.waiterDepth ? `Waiters visible (${stats.waiterDepth}). ` : ""}${
        riskBits.length > 0 ? `Current risk signals: ${riskBits.join(", ")}.` : ""
      }`,
      label: "Pressure" as const,
      tone: "warning" as const,
    };
  }

  return {
    details: "No immediate lease contention risk is visible.",
    label: "Live" as const,
    tone: "success" as const,
  };
}

function LeaseRealmTableRows(props: { realms: LeaseOverview["realms"] }) {
  return (
    <For each={props.realms} by={(realm) => realm.realm}>
      {(realm) => (
        <TableRow>
          <TableCell>
            <Link href={domainScopeHref("lease", { realm: realm.realm })}>{realm.realm}</Link>
          </TableCell>
          <TableCell>{formatNumber(0)}</TableCell>
        </TableRow>
      )}
    </For>
  );
}

function LeaseAreaTableRows(props: { areas: LeaseRealmInventory["areas"]; realm: string }) {
  return (
    <For each={props.areas} by={(area) => `${props.realm}:${area.area}`}>
      {(area) => (
        <TableRow>
          <TableCell>
            <Link href={domainScopeHref("lease", { area: area.area, realm: props.realm })}>
              {area.area}
            </Link>
          </TableCell>
          <TableCell>{formatNumber(area.resources.length)}</TableCell>
        </TableRow>
      )}
    </For>
  );
}

function LeaseResourceTableRows(props: { data: LeaseAreaResourceRows }) {
  return (
    <For
      each={props.data.resources}
      by={(resource) => `${props.data.realm}:${props.data.area}:${resource}`}
    >
      {(resource) => (
        <TableRow>
          <TableCell>
            <Link
              href={domainResourceHref("lease", {
                area: props.data.area,
                realm: props.data.realm,
                resource,
              })}
            >
              {resource}
            </Link>
          </TableCell>
          <TableCell>active</TableCell>
          <TableCell>
            <Inline align="center" gap="2">
              <span>{props.data.realm}</span>
              <span>/</span>
              <span>{props.data.area}</span>
            </Inline>
          </TableCell>
        </TableRow>
      )}
    </For>
  );
}

function LeaseOverviewPage() {
  const overview = createLeaseOverviewQuery();
  const data = overview.data;

  const snapshot = createDomainSidebar({
    data,
    title: "Lease scope",
    description: "Lease realms for this Route Family.",
    stats: (current) => [
      { label: "Visible realms", value: current.realms.length },
      { label: "Leases", value: current.stats.leasesActive },
      { label: "Waiters", value: current.stats.waiterDepth },
      {
        label: "Ownership pressure",
        value:
          current.stats.acquireTimeoutsTotal +
          current.stats.forcedReleasesTotal +
          current.stats.invalidTokenRejectsTotal,
      },
    ],
  });

  const status =
    data &&
    riskSignal({
      acquireTimeoutsTotal: data.stats.acquireTimeoutsTotal,
      forcedReleasesTotal: data.stats.forcedReleasesTotal,
      invalidTokenRejectsTotal: data.stats.invalidTokenRejectsTotal,
      oldestLeaseAgeSeconds: data.stats.oldestLeaseAgeSeconds,
      leasesActive: data.stats.leasesActive,
      waiterDepth: data.stats.waiterDepth,
    });

  return (
    <DomainPageFrame>
      <Stack gap="3">
        <DomainHeader
          eyebrow="Ownership coordination"
          title="Lease overview"
          description="Ephemeral ownership, TTL pressure, waiters, and contention by scope."
          primaryAction={{ label: "Refresh lease overview", onPress: () => overview.refresh() }}
          status={{
            detail: data
              ? `${data.realms.length} visible realm(s). ${status?.details ?? "Loading lease overview."}`
              : "Loading lease overview.",
            label: overview.refreshing
              ? "Refreshing"
              : overview.stale
                ? "Stale"
                : (status?.label ?? "Live"),
            tone: overview.refreshing
              ? "info"
              : overview.stale
                ? "warning"
                : (status?.tone ?? "success"),
          }}
        />

        {snapshot}

        {!data && overview.loading ? (
          <QueryLoadingState description="Loading lease overview snapshot..." />
        ) : null}
        {!data && overview.error ? (
          <QueryErrorState
            title="Lease overview loading failure"
            error={overview.error}
            onRetry={() => overview.refresh()}
          />
        ) : null}

        {data ? (
          <Stack gap="3">
            {overview.refreshing ? (
              <QueryRefreshingState description="Refreshing lease overview..." />
            ) : null}

            <DomainMetricTable
              title="Lease metrics"
              description="Broker-local owners, waiter pressure, and contention signals for current scope."
              metrics={[
                { label: "Active leases", value: data.stats.leasesActive },
                { label: "Waiters", value: data.stats.waiterDepth },
                {
                  label: "Oldest lease age",
                  value: formatDurationSeconds(data.stats.oldestLeaseAgeSeconds),
                },
                {
                  label: "Ownership pressure",
                  value:
                    data.stats.acquireTimeoutsTotal +
                    data.stats.forcedReleasesTotal +
                    data.stats.invalidTokenRejectsTotal,
                },
                { label: "Acquire timeouts", value: data.stats.acquireTimeoutsTotal },
                { label: "Forced releases", value: data.stats.forcedReleasesTotal },
                { label: "Token rejects", value: data.stats.invalidTokenRejectsTotal },
                { label: "Ops / sec", value: data.stats.operationsPerSecond.toFixed(2) },
              ]}
            />

            <Card padding="sm" variant="default">
              <CardHeader>
                <CardTitle>Lease realms</CardTitle>
                <CardDescription>{data.realms.length} realm(s)</CardDescription>
              </CardHeader>
              <CardContent>
                {data.realms.length === 0 ? (
                  <QueryEmptyState description="No lease realms are currently visible." />
                ) : (
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableHeaderCell>Realm</TableHeaderCell>
                        <TableHeaderCell>Waiters</TableHeaderCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      <LeaseRealmTableRows realms={data.realms} />
                    </TableBody>
                  </Table>
                )}
              </CardContent>
            </Card>

            <DomainWorkflowPanel
              archetype="Lease"
              diagnostics={[
                "Waiting workers indicate potential lease contention and handoff delays.",
                "High waiter depth can indicate missed lock releases or uneven ownership traffic.",
                "Lease ownership is ephemeral; restart or disconnect requires explicit reacquire.",
              ]}
              questions={[
                "Are there hotspots in a single realm or area?",
                "Are owners failing to release before expiration?",
                "Do waiters rise after deployment or traffic spikes?",
              ]}
              workflows={[
                "Drill into an impacted realm, then area, then resource pages.",
                "Use lease ownership rows to identify owners, queued tokens, and waiter pressure.",
                "Validate owning sessions against active traffic and workload changes.",
              ]}
            />
          </Stack>
        ) : null}
      </Stack>
    </DomainPageFrame>
  );
}

function LeaseRealmPage(props: { realm: string }) {
  const realmQuery = createLeaseRealmQuery(props.realm);
  const data = realmQuery.data;

  const snapshot = createDomainSidebar({
    data,
    title: `Lease realm ${props.realm}`,
    description: props.realm,
    stats: (current) => [
      { label: "Areas", value: current.areas.length },
      {
        label: "Resources",
        value: current.areas.reduce((sum, area) => sum + area.resources.length, 0),
      },
    ],
    footer: <Link href={domainScopeHref("lease")}>Back to overview</Link>,
  });

  return (
    <DomainPageFrame>
      <Stack gap="3">
        <DomainHeader
          eyebrow="Lease realm"
          title={props.realm}
          description={`Areas for ${props.realm}.`}
          primaryAction={{ label: "Refresh realm", onPress: () => realmQuery.refresh() }}
          status={{
            detail: data ? `${data.areas.length} visible area(s).` : "Loading lease realm.",
            label: realmQuery.refreshing ? "Refreshing" : realmQuery.stale ? "Stale" : "Live",
            tone: realmQuery.refreshing ? "info" : realmQuery.stale ? "warning" : "success",
          }}
        />

        {snapshot}

        {!data && realmQuery.loading ? (
          <QueryLoadingState description="Loading lease realm details." />
        ) : null}
        {!data && realmQuery.error ? (
          <QueryErrorState
            title="Unable to load lease realm"
            error={realmQuery.error}
            onRetry={() => realmQuery.refresh()}
          />
        ) : null}

        {data ? (
          <Stack gap="3">
            {realmQuery.refreshing ? (
              <QueryRefreshingState description="Refreshing realm." />
            ) : null}
            <Card padding="sm" variant="default">
              <CardHeader>
                <CardTitle>Lease areas</CardTitle>
                <CardDescription>{data.areas.length} area(s)</CardDescription>
              </CardHeader>
              <CardContent>
                {data.areas.length === 0 ? (
                  <QueryEmptyState description="No visible lease areas at the current level." />
                ) : (
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableHeaderCell>Area</TableHeaderCell>
                        <TableHeaderCell>Resources</TableHeaderCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      <LeaseAreaTableRows areas={data.areas} realm={props.realm} />
                    </TableBody>
                  </Table>
                )}
              </CardContent>
            </Card>
          </Stack>
        ) : null}
      </Stack>
    </DomainPageFrame>
  );
}

function LeaseAreaPage(props: { realm: string; area: string }) {
  const areaQuery = createLeaseAreaQuery(props.realm, props.area);
  const data = areaQuery.data;

  const snapshot = createDomainSidebar({
    data,
    title: `Lease area ${props.area}`,
    description: `${props.realm} / ${props.area}`,
    stats: (current) => [{ label: "Resources", value: current.resources.length }],
    footer: (
      <Inline gap="2" wrap="wrap">
        <Link href={domainScopeHref("lease", { realm: props.realm })}>Back to realm</Link>
        <Link href={domainScopeHref("lease")}>Back to overview</Link>
      </Inline>
    ),
  });

  return (
    <DomainPageFrame>
      <Stack gap="3">
        <DomainHeader
          eyebrow="Lease area"
          title={props.area}
          description={`${props.realm}/${props.area}`}
          primaryAction={{ label: "Refresh area", onPress: () => areaQuery.refresh() }}
          status={{
            detail: data ? `${data.resources.length} resource(s).` : "Loading lease area.",
            label: areaQuery.refreshing ? "Refreshing" : areaQuery.stale ? "Stale" : "Live",
            tone: areaQuery.refreshing ? "info" : areaQuery.stale ? "warning" : "success",
          }}
        />

        {snapshot}

        {!data && areaQuery.loading ? (
          <QueryLoadingState description="Loading lease area resources." />
        ) : null}
        {!data && areaQuery.error ? (
          <QueryErrorState
            title="Unable to load lease area"
            error={areaQuery.error}
            onRetry={() => areaQuery.refresh()}
          />
        ) : null}

        {data ? (
          <Stack gap="3">
            {areaQuery.refreshing ? <QueryRefreshingState description="Refreshing area." /> : null}

            <Card padding="sm" variant="default">
              <CardHeader>
                <CardTitle>Lease resources</CardTitle>
                <CardDescription>{data.resources.length} resource(s)</CardDescription>
              </CardHeader>
              <CardContent>
                {data.resources.length === 0 ? (
                  <QueryEmptyState description="No visible lease resources at the current level." />
                ) : (
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableHeaderCell>Resource</TableHeaderCell>
                        <TableHeaderCell>Status</TableHeaderCell>
                        <TableHeaderCell>Scope</TableHeaderCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      <LeaseResourceTableRows data={data} />
                    </TableBody>
                  </Table>
                )}
              </CardContent>
            </Card>
          </Stack>
        ) : null}
      </Stack>
    </DomainPageFrame>
  );
}

export default function LeasePage() {
  const route = currentRoute();
  const realm = decodeParam(route.params.realm);
  const area = decodeParam(route.params.area);

  if (realm && area) {
    return <LeaseAreaPage realm={realm} area={area} />;
  }

  if (realm) {
    return <LeaseRealmPage realm={realm} />;
  }

  return <LeaseOverviewPage />;
}
