import { currentRoute } from "@askrjs/askr/router";
import { state } from "@askrjs/askr";
import { For, Show } from "@askrjs/askr/control";
import { task } from "@askrjs/askr/resources";
import { Badge, Block } from "@askrjs/themes/components";
import { Table, TableBody, TableCell, TableHead, TableHeaderCell, TableRow } from "@askrjs/ui";
import DomainDataSection from "@/components/shared/domain-data-section";
import DomainHeader from "@/components/shared/domain-header";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import { queryHeaderStatus } from "@/components/shared/query-header-status";
import {
  QueryEmptyState,
  QueryErrorState,
  QueryLoadingState,
  QueryRefreshingState,
} from "@/components/shared/query-state";
import { formatDurationSeconds, formatNumber, formatTimestamp } from "@/shared/format";
import { createLeaseResourceRowsQuery } from "@/features/lease/lease-query";
import { deriveLeaseRemainingLifetime } from "@/features/lease/lease-mappers";
import type {
  LeaseOwnershipRowState,
  LeaseOwnershipSearchRow,
} from "@/features/lease/lease-models";

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

function formatState(state: LeaseOwnershipRowState) {
  switch (state) {
    case "owned":
      return "Owned";
    case "owned_with_waiters":
      return "Owned with waiters";
    case "waiting":
      return "Waiting";
  }
}

function LeaseOwnershipTable(props: {
  limit: number;
  rows: LeaseOwnershipSearchRow[];
  now: () => number;
}) {
  return (
    <DomainDataSection
      id="lease-ownership-rows"
      title="Owners and waiters"
      actions={
        props.rows.length >= props.limit ? (
          <Badge variant="warning">Observation sample reached {props.limit}</Badge>
        ) : undefined
      }
    >
      <div class="domain-table-wrap">
        <Table>
          <TableHead>
            <TableRow>
              <TableHeaderCell>Owner / waiting session</TableHeaderCell>
              <TableHeaderCell>State</TableHeaderCell>
              <TableHeaderCell>Fencing / queued token</TableHeaderCell>
              <TableHeaderCell>Waiters</TableHeaderCell>
              <TableHeaderCell>Age</TableHeaderCell>
              <TableHeaderCell>Remaining TTL</TableHeaderCell>
              <TableHeaderCell>Expiry</TableHeaderCell>
            </TableRow>
          </TableHead>
          <TableBody>
            <For
              each={props.rows}
              by={(row) =>
                `${row.ownerSessionId}-${row.ownerId ?? "none"}-${row.queuedToken ?? "none"}-${row.area}-${row.realm}-${row.resource}-${row.state}`
              }
            >
              {(row) => (
                <TableRow>
                  <TableCell>{formatOwner(row)}</TableCell>
                  <TableCell>{formatState(row.state)}</TableCell>
                  <TableCell>
                    {row.state === "waiting" ? "Queued token" : "Fencing token"}{" "}
                    {row.queuedToken ?? "--"}
                  </TableCell>
                  <TableCell>
                    {row.state === "waiting" ? "--" : formatNumber(row.pendingWaiters)}
                  </TableCell>
                  <TableCell>
                    {row.ageSeconds === null ? "--" : formatDurationSeconds(row.ageSeconds)}
                  </TableCell>
                  <TableCell class="lease-remaining-ttl" data-field="remaining-ttl">
                    {() => formatRemaining(row.expiresAt, props.now())}
                  </TableCell>
                  <TableCell>{row.expiresAt ? formatTimestamp(row.expiresAt) : "--"}</TableCell>
                </TableRow>
              )}
            </For>
          </TableBody>
        </Table>
      </div>
    </DomainDataSection>
  );
}

export default function LeaseResourcePage() {
  const [leaseClockNow, setLeaseClockNow] = state(Date.now());
  leaseClockNow();

  task(() => {
    if (typeof window === "undefined") {
      return;
    }

    const handle = window.setInterval(() => setLeaseClockNow(Date.now()), 1000);
    return () => window.clearInterval(handle);
  });

  const route = currentRoute();
  const realm = decodeParam(route.params.realm);
  const area = decodeParam(route.params.area);
  const resource = decodeParam(route.params.resource);

  const limit = parseLimit(route.query.get("limit"));
  const hasScope = Boolean(realm && area && resource);
  const rowsQuery = createLeaseResourceRowsQuery({
    area: area ?? "",
    limit,
    realm: realm ?? "",
    resource: resource ?? "",
  });

  const rowsData = rowsQuery?.data;
  const rows = rowsData?.items ?? [];
  const waiters = rows.reduce((sum, row) => sum + row.pendingWaiters, 0);

  return (
    <Show when={hasScope}>
      <DomainPageFrame>
        <Block direction="column" gap="sm">
          <DomainHeader
            compact={true}
            eyebrow="Lease ownership"
            title={resource ?? ""}
            primaryAction={{
              busy: rowsQuery.refreshing,
              disabled: rowsQuery.refreshing,
              label: "Refresh ownership rows",
              onPress: () => rowsQuery.refresh(),
            }}
            status={queryHeaderStatus(
              rowsQuery,
              waiters > 0 ? { label: "Waiters present", tone: "warning" } : {},
            )}
          />
          <Show when={!rowsData && rowsQuery.loading}>
            <QueryLoadingState description="Loading lease ownership rows..." />
          </Show>
          <Show when={rowsQuery.error}>
            <QueryErrorState
              title="Unable to load lease ownership rows"
              error={rowsQuery.error}
              onRetry={() => rowsQuery.refresh()}
            />
          </Show>

          <Show when={rowsData && rows.length > 0}>
            <LeaseOwnershipTable rows={rows} limit={rowsData?.limit ?? limit} now={leaseClockNow} />
          </Show>

          <Show when={rowsData && rows.length === 0}>
            <QueryEmptyState description="No visible lease ownership rows at the current level." />
          </Show>

          <Show when={rowsQuery.refreshing}>
            <QueryRefreshingState description="Refreshing lease ownership rows..." />
          </Show>
        </Block>
      </DomainPageFrame>
    </Show>
  );
}
