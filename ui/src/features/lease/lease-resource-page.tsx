import { currentRoute } from "@askrjs/askr/router";
import { state } from "@askrjs/askr";
import { For, Show } from "@askrjs/askr/control";
import { task } from "@askrjs/askr/resources";
import { Block, Card, CardContent, CardTitle } from "@askrjs/themes/components";
import DomainDataSection from "@/components/shared/domain-data-section";
import DomainHeader from "@/components/shared/domain-header";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import OperatorScopeStrip from "@/components/shared/operator-scope-strip";
import { queryFreshness, queryHeaderStatus } from "@/components/shared/query-header-status";
import {
  QueryEmptyState,
  QueryErrorState,
  QueryLoadingState,
  QueryRefreshingState,
} from "@/components/shared/query-state";
import { formatDurationSeconds, formatNumber, formatTimestamp } from "@/shared/format";
import { createLeaseResourceRowsQuery } from "@/features/lease/lease-query";
import { deriveLeaseRemainingLifetime } from "@/features/lease/lease-mappers";
import type { LeaseOwnershipSearchRow } from "@/features/lease/lease-models";

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

function LeaseOwnershipCards(props: { rows: LeaseOwnershipSearchRow[]; now: () => number }) {
  const totalWaiters = props.rows.reduce((sum, row) => sum + row.pendingWaiters, 0);

  return (
    <DomainDataSection
      id="lease-ownership-rows"
      title="Ownership details"
      description={`${props.rows.length} ownership observation${props.rows.length === 1 ? "" : "s"} and ${totalWaiters} queued waiter${totalWaiters === 1 ? "" : "s"} in this scope.`}
    >
      <Block className="lease-ownership-list" direction="column" gap="sm">
        <For
          each={props.rows}
          by={(row) =>
            `${row.ownerSessionId}-${row.ownerId ?? "none"}-${row.queuedToken ?? "none"}-${row.area}-${row.realm}-${row.resource}-${row.state}`
          }
        >
          {(row) => (
            <Card class="lease-ownership-card" padding="sm" variant="default">
              <CardContent>
                <Block direction="column" gap="sm">
                  <CardTitle titleAs="h3">{formatOwner(row)}</CardTitle>
                  <dl class="lease-ownership-details">
                    <div>
                      <dt>State</dt>
                      <dd>{row.state}</dd>
                    </div>
                    <div>
                      <dt>Queued token</dt>
                      <dd>{row.queuedToken ?? "--"}</dd>
                    </div>
                    <div>
                      <dt>Waiters</dt>
                      <dd>{formatNumber(row.pendingWaiters)}</dd>
                    </div>
                    <div>
                      <dt>Age</dt>
                      <dd>
                        {row.ageSeconds === null ? "--" : formatDurationSeconds(row.ageSeconds)}
                      </dd>
                    </div>
                    <div>
                      <dt>Remaining TTL</dt>
                      <dd class="lease-remaining-ttl" data-field="remaining-ttl">
                        {() => formatRemaining(row.expiresAt, props.now())}
                      </dd>
                    </div>
                    <div>
                      <dt>Expiry</dt>
                      <dd>{row.expiresAt ? formatTimestamp(row.expiresAt) : "--"}</dd>
                    </div>
                  </dl>
                </Block>
              </CardContent>
            </Card>
          )}
        </For>
      </Block>
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
  const rowsQuery = createLeaseResourceRowsQuery(
    {
      area: area ?? "",
      limit,
      realm: realm ?? "",
      resource: resource ?? "",
    },
    { skipInitialFetch: !hasScope },
  );

  const rowsData = rowsQuery?.data;
  const rows = rowsData?.items ?? [];
  const waiters = rows.reduce((sum, row) => sum + row.pendingWaiters, 0);

  return (
    <Show when={hasScope}>
      <DomainPageFrame>
        <Block direction="column" gap="sm">
          <DomainHeader
            eyebrow="Lease ownership"
            title={resource ?? ""}
            description={`Ephemeral owner/session rows with TTL and waiter pressure for ${realm} / ${area} / ${resource}.`}
            primaryAction={{
              busy: rowsQuery.refreshing,
              disabled: rowsQuery.refreshing,
              label: "Refresh ownership rows",
              onPress: () => rowsQuery.refresh(),
            }}
            status={queryHeaderStatus(rowsQuery, {
              loading: "Loading lease ownership rows.",
              ready: rowsData
                ? `${formatNumber(rows.length)} row${rows.length === 1 ? "" : "s"} visible in scope. ${waiters} waiter${waiters === 1 ? "" : "s"} visible. Ephemeral, not crash-safe continuity.`
                : "",
              unavailable: "Lease ownership evidence is unavailable for this resource.",
            })}
          />
          <OperatorScopeStrip
            realm={realm}
            area={area}
            resource={resource}
            freshness={queryFreshness(rowsQuery)}
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
            <LeaseOwnershipCards rows={rows} now={leaseClockNow} />
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
