import { For } from "@askrjs/askr/control";
import { Link, currentRoute } from "@askrjs/askr/router";
import { Table, TableBody, TableCell, TableHead, TableHeaderCell, TableRow } from "@askrjs/ui";
import { Stack } from "@askrjs/themes/components";
import DomainHeader from "@/components/shared/domain-header";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import { createDomainSidebar } from "@/components/shared/domain-sidebar";
import {
  QueryEmptyState,
  QueryErrorState,
  QueryLoadingState,
  QueryRefreshingState,
} from "@/components/shared/query-state";
import { formatNumber } from "@/shared/format";
import { createNoticeOperationRowsQuery } from "@/features/notice/notice-query";
import type { NoticeDeliveryRows } from "@/features/notice/notice-models";
import { domainScopeHref } from "@/shared/navigation/domains";

function decodeParam(value: string | undefined) {
  if (!value) return undefined;

  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

function parseLimit(value: string | null) {
  if (!value) return 50;

  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return 50;

  return Math.max(1, Math.min(200, Math.floor(parsed)));
}

function NoticeDeliveryTableRows(props: { rows: NoticeDeliveryRows["observations"] }) {
  return (
    <For
      each={props.rows}
      by={(observation) =>
        `${observation.sessionId ?? "session"}:${observation.subscriptionId ?? "none"}`
      }
    >
      {(observation) => (
        <TableRow>
          <TableCell>{observation.status}</TableCell>
          <TableCell>{observation.sessionId ?? "--"}</TableCell>
          <TableCell>{formatNumber(observation.notificationsReceived)}</TableCell>
          <TableCell>{formatNumber(observation.publishesPerMinute)}</TableCell>
          <TableCell>{formatNumber(observation.publishesTotal)}</TableCell>
        </TableRow>
      )}
    </For>
  );
}

export default function NoticeOperationPage(props: {
  realm?: string;
  area?: string;
  resource?: string;
  operation?: string;
}) {
  const route = currentRoute();
  const realm = props.realm ?? decodeParam(route.params.realm) ?? "";
  const area = props.area ?? decodeParam(route.params.area) ?? "";
  const resource = props.resource ?? decodeParam(route.params.resource) ?? "";
  const query = decodeParam(route.params.operation) ?? props.operation ?? "";
  const limit = parseLimit(route.query.get("limit"));

  const rowsQuery = createNoticeOperationRowsQuery({
    area,
    limit,
    operation: query,
    realm,
    resource,
  });

  const data = rowsQuery.data;
  const deliveries = data?.observations ?? [];
  const activeSubscribers = new Set(
    deliveries.map((row) => `${row.subscriptionId ?? "session"}:${row.sessionId ?? "session"}`),
  ).size;
  const rollingMessages = deliveries.reduce((sum, row) => sum + row.publishesPerMinute, 0);

  const snapshot = createDomainSidebar({
    data,
    title: `Notice operation ${query}`,
    description: `${realm} / ${area} / ${resource} / ${query}`,
    stats: (current) => [
      {
        label: "Active subscribers",
        value: activeSubscribers,
      },
      {
        label: "Rolling messages / min",
        value: current.observations.length === 0 ? 0 : rollingMessages,
      },
      {
        label: "Latency",
        value: "--/N/A",
        note: "Latency unavailable via current API",
      },
    ],
    footer: (
      <Link
        href={domainScopeHref("notice", {
          area,
          realm,
          resource,
        })}
      >
        Back to resource
      </Link>
    ),
  });

  return (
    <DomainPageFrame>
      <Stack gap="3">
        <DomainHeader
          eyebrow="Notice operation"
          title={query}
          description={`${realm} / ${area} / ${resource} / ${query}`}
          primaryAction={{
            label: "Refresh operation deliveries",
            onPress: () => rowsQuery.refresh(),
          }}
          status={{
            detail: data
              ? `${formatNumber(data.observations.length)} row${data.observations.length === 1 ? "" : "s"} currently visible. ${activeSubscribers} active subscriber${activeSubscribers === 1 ? "" : "s"}.`
              : "Loading notice deliveries for this operation.",
            label: rowsQuery.refreshing ? "Refreshing" : rowsQuery.stale ? "Stale" : "Live",
            tone: rowsQuery.refreshing ? "info" : rowsQuery.stale ? "warning" : "success",
          }}
        />

        {snapshot}

        {!data && rowsQuery.loading ? (
          <QueryLoadingState description="Loading notice operation deliveries..." />
        ) : null}
        {!data && rowsQuery.error ? (
          <QueryErrorState
            title="Unable to load notice operation deliveries"
            error={rowsQuery.error}
            onRetry={() => rowsQuery.refresh()}
          />
        ) : null}

        {data ? (
          <Stack gap="3">
            {rowsQuery.refreshing ? (
              <QueryRefreshingState description="Refreshing notice operation deliveries..." />
            ) : null}

            {data.observations.length === 0 ? (
              <QueryEmptyState description="No matching notice deliveries are currently visible." />
            ) : (
              <Table>
                <TableHead>
                  <TableRow>
                    <TableHeaderCell>Status</TableHeaderCell>
                    <TableHeaderCell>Session</TableHeaderCell>
                    <TableHeaderCell>Notifications received</TableHeaderCell>
                    <TableHeaderCell>Publishes / min</TableHeaderCell>
                    <TableHeaderCell>Publishes total</TableHeaderCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  <NoticeDeliveryTableRows rows={data.observations} />
                </TableBody>
              </Table>
            )}
          </Stack>
        ) : null}
      </Stack>
    </DomainPageFrame>
  );
}
