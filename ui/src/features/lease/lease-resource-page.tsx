import { currentRoute } from "@askrjs/askr/router";
import { state } from "@askrjs/askr";
import { Link } from "@askrjs/askr/router";
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
import DomainPageFrame from "@/components/shared/domain-page-frame";
import OperatorScopeStrip from "@/components/shared/operator-scope-strip";
import { createDomainSidebar } from "@/components/shared/domain-sidebar";
import {
  QueryEmptyState,
  QueryErrorState,
  QueryLoadingState,
  QueryRefreshingState,
} from "@/components/shared/query-state";
import { formatDurationSeconds, formatNumber } from "@/shared/format";
import { createLeaseResourceRowsQuery } from "@/features/lease/lease-query";
import { domainScopeHref } from "@/shared/navigation/domains";
import { deriveLeaseRemainingLifetime } from "@/features/lease/lease-mappers";
import type { LeaseOwnershipSearchRow } from "@/features/lease/lease-models";

let leaseClockSetNow: ((now: number) => void) | null = null;
let leaseClockHandle: ReturnType<Window["setInterval"]> | null = null;

function ensureLeaseClockRunning(setClockNow: (now: number) => void) {
  if (typeof window === "undefined") {
    return;
  }

  leaseClockSetNow = setClockNow;

  if (leaseClockHandle !== null) {
    return;
  }

  leaseClockHandle = window.setInterval(() => {
    leaseClockSetNow?.(Date.now());
  }, 1000);
}

function decodeParam(value: string | undefined) {
  if (!value) {
    return undefined;
  }

  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

function parseLimit(value: string | null) {
  if (!value) {
    return 50;
  }

  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return 50;
  }

  return Math.min(Math.floor(parsed), 250);
}

function formatRemaining(expiresAt: string | null, now: number) {
  const lifetime = deriveLeaseRemainingLifetime(expiresAt, now);

  return lifetime.status === "missing" ? "--" : lifetime.label;
}

function formatOwner(row: LeaseOwnershipSearchRow) {
  return row.ownerSessionId ?? row.ownerId ?? "--";
}

function LeaseResourceRowsTable(props: { rows: LeaseOwnershipSearchRow[]; now: number }) {
  const totalWaiters = props.rows.reduce((sum, row) => sum + row.pendingWaiters, 0);

  return (
    <Card padding="sm" variant="default">
      <CardHeader>
        <CardTitle>Lease ownership rows</CardTitle>
        <CardDescription>
          Owner/session, queued token, waiters, age, remaining TTL, and expiry for this scope.
          {` ${props.rows.length} row${props.rows.length === 1 ? "" : "s"}, ${totalWaiters} waiter${
            totalWaiters === 1 ? "" : "s"
          }.`}
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div class="domain-table-wrap">
          <Table>
            <TableHead>
              <TableRow>
                <TableHeaderCell>Owner / session</TableHeaderCell>
                <TableHeaderCell>State</TableHeaderCell>
                <TableHeaderCell>Queued token</TableHeaderCell>
                <TableHeaderCell>Waiters</TableHeaderCell>
                <TableHeaderCell>Age</TableHeaderCell>
                <TableHeaderCell>Remaining TTL</TableHeaderCell>
                <TableHeaderCell>Expiry</TableHeaderCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {props.rows.map((row) => (
                <TableRow
                  key={`${row.ownerSessionId}-${row.ownerId ?? "none"}-${row.queuedToken ?? "none"}-${row.area}-${row.realm}-${row.resource}-${row.state}`}
                >
                  <TableCell>{formatOwner(row)}</TableCell>
                  <TableCell>{row.state}</TableCell>
                  <TableCell>{row.queuedToken ?? "--"}</TableCell>
                  <TableCell>{formatNumber(row.pendingWaiters)}</TableCell>
                  <TableCell>
                    {row.ageSeconds === null ? "--" : formatDurationSeconds(row.ageSeconds)}
                  </TableCell>
                  <TableCell>{formatRemaining(row.expiresAt, props.now)}</TableCell>
                  <TableCell>{row.expiresAt ?? "--"}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      </CardContent>
    </Card>
  );
}

export default function LeaseResourcePage() {
  const [leaseClockNow, setLeaseClockNow] = state(Date.now());

  ensureLeaseClockRunning(setLeaseClockNow);

  const route = currentRoute();
  const realm = decodeParam(route.params.realm);
  const area = decodeParam(route.params.area);
  const resource = decodeParam(route.params.resource);

  const limit = parseLimit(route.query.get("limit"));
  const rowsQuery =
    realm && area && resource
      ? createLeaseResourceRowsQuery({
          area,
          limit,
          realm,
          resource,
        })
      : null;

  const rowsData = rowsQuery?.data;
  const rows = rowsData?.items ?? [];
  const waiters = rows.reduce((sum, row) => sum + row.pendingWaiters, 0);

  const snapshot = createDomainSidebar({
    data: rowsData,
    title: "Lease ownership rows",
    description: realm && area && resource ? [realm, area, resource].join(" / ") : "Lease resource",
    stats: (current) => [
      { label: "Rows", value: current.items.length },
      { label: "Waiters", value: waiters },
      { label: "Route Family", value: current.routeFamily },
    ],
    footer: <Link href={domainScopeHref("lease", { area, realm })}>Back to lease area</Link>,
  });

  if (!realm || !area || !resource || !rowsQuery) {
    return null;
  }

  return (
    <DomainPageFrame sidebar={snapshot}>
      <Stack gap="3">
        <DomainHeader
          eyebrow="Lease ownership"
          title={resource}
          description={`Ephemeral owner/session rows with TTL and waiter pressure for ${realm} / ${area} / ${resource}.`}
          primaryAction={{ label: "Refresh ownership rows", onPress: () => rowsQuery.refresh() }}
          status={{
            detail: rowsData
              ? `${formatNumber(rows.length)} row${rows.length === 1 ? "" : "s"} visible in scope. ${waiters} waiter${waiters === 1 ? "" : "s"} visible. Ephemeral, not crash-safe continuity.`
              : "Loading lease ownership rows.",
            label: rowsQuery.refreshing ? "Refreshing" : rowsQuery.stale ? "Stale" : "Live",
            tone: rowsQuery.refreshing ? "info" : rowsQuery.stale ? "warning" : "success",
          }}
        />
        <OperatorScopeStrip
          realm={realm}
          area={area}
          resource={resource}
          freshness={
            rowsQuery.refreshing
              ? "Refreshing"
              : rowsQuery.stale
                ? "Stale"
                : rowsData
                  ? "Live"
                  : rowsQuery.loading
                    ? "Loading"
                    : undefined
          }
        />
        {!rowsData && rowsQuery.loading ? (
          <QueryLoadingState description="Loading lease ownership rows..." />
        ) : null}
        {rowsQuery.error ? (
          <QueryErrorState
            title="Unable to load lease ownership rows"
            error={rowsQuery.error}
            onRetry={() => rowsQuery.refresh()}
          />
        ) : null}

        {rowsData ? <LeaseResourceRowsTable rows={rows} now={leaseClockNow()} /> : null}

        {rowsData && rows.length === 0 ? (
          <QueryEmptyState description="No visible lease ownership rows at the current level." />
        ) : null}

        {rowsQuery.refreshing ? (
          <QueryRefreshingState description="Refreshing lease ownership rows..." />
        ) : null}

        <Link class="text-link" href={domainScopeHref("lease", { area, realm })}>
          Back to area
        </Link>
      </Stack>
    </DomainPageFrame>
  );
}
