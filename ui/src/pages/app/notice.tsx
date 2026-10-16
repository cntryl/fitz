import { Show } from "@askrjs/askr/control";
import { currentRoute } from "@askrjs/askr/router";
import { Badge, Block } from "@askrjs/themes/components";
import DomainHeader from "@/components/shared/domain-header";
import DomainInventoryPage from "@/components/shared/domain-inventory-page";
import DomainOperationTable, {
  type DomainOperationMetricColumn,
} from "@/components/shared/domain-operation-table";
import type { DomainResourceMetricColumn } from "@/components/shared/domain-resource-inventory-table";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import { queryHeaderStatus } from "@/components/shared/query-header-status";
import {
  QueryErrorState,
  QueryLoadingState,
  QueryRefreshingState,
} from "@/components/shared/query-state";
import { createNoticeResourceRowsQuery } from "@/features/notice/notice-query";
import type { NoticeResourceOperationRow } from "@/features/notice/notice-models";
import { createResourceInventoryQuery } from "@/features/resource/resource-query";
import { formatNumber } from "@/shared/format";
import NoticeOperationPage from "./notice-operation";

const noticeMetricColumns: readonly DomainResourceMetricColumn[] = [
  {
    id: "subscriptions",
    header: "Observed subscribers",
    width: "16%",
    cell: (row) =>
      row.subscriptionsActive === undefined ? "--" : formatNumber(row.subscriptionsActive),
    sortValue: (row) => row.subscriptionsActive,
  },
  {
    id: "publishes",
    header: "Publishes/min",
    width: "16%",
    cell: (row) =>
      row.publishesPerMinute === undefined ? "--" : row.publishesPerMinute.toFixed(2),
    sortValue: (row) => row.publishesPerMinute,
  },
  {
    id: "delivered",
    priority: "secondary",
    header: "Delivered",
    width: "14%",
    cell: (row) =>
      row.notificationsReceived === undefined ? "--" : formatNumber(row.notificationsReceived),
    sortValue: (row) => row.notificationsReceived,
  },
];

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

function NoticeLandingPage() {
  const inventory = createResourceInventoryQuery("notice");
  return (
    <DomainInventoryPage
      domain="notice"
      eyebrow="Live awareness"
      title="Notice inventory"
      refreshLabel="Refresh notice"
      inventory={inventory}
      loadingDescription="Loading notice inventory..."
      errorTitle="Unable to load notice inventory"
      refreshingDescription="Refreshing notice inventory..."
      emptyDescription="No notice resources are currently visible. Check the selected Route Family or broaden scope."
      tableTitle="Resource inventory"
      metricColumns={noticeMetricColumns}
      status={inventory.data ? { label: "Delivery health unavailable", tone: "info" } : undefined}
    />
  );
}

const noticeOperationColumns: readonly DomainOperationMetricColumn<NoticeResourceOperationRow>[] = [
  {
    id: "subscriptions",
    header: "Observed subscribers",
    width: "16%",
    cell: (row) => formatNumber(row.activeSubscribers),
    sortValue: (row) => row.activeSubscribers,
  },
  {
    id: "publishes",
    header: "Publishes/min",
    width: "16%",
    cell: (row) => row.rollingMessageCount.toFixed(2),
    sortValue: (row) => row.rollingMessageCount,
  },
];

function NoticeResourcePage(props: { realm: string; area: string; resource: string }) {
  const route = currentRoute();
  const limit = parseLimit(route.query.get("limit"));
  const rowsQuery = createNoticeResourceRowsQuery({
    area: props.area,
    limit,
    realm: props.realm,
    resource: props.resource,
  });
  const data = rowsQuery.data;

  return (
    <DomainPageFrame>
      <Block direction="column" gap="sm">
        <DomainHeader
          compact={true}
          eyebrow="Notice resource"
          title={props.resource}
          description={`${props.realm} / ${props.area} / ${props.resource}`}
          primaryAction={{
            busy: rowsQuery.refreshing,
            disabled: rowsQuery.refreshing,
            label: "Refresh operations",
            onPress: () => rowsQuery.refresh(),
          }}
          status={queryHeaderStatus(rowsQuery)}
        />
        <Show when={!data && rowsQuery.loading}>
          <QueryLoadingState description="Loading notice operation rows..." />
        </Show>
        <Show when={!data && rowsQuery.error}>
          <QueryErrorState
            title="Unable to load notice operation rows"
            error={rowsQuery.error}
            onRetry={() => rowsQuery.refresh()}
          />
        </Show>

        <Show when={data}>
          <Block direction="column" gap="sm">
            <Show when={rowsQuery.refreshing}>
              <QueryRefreshingState description="Refreshing observed subscriptions..." />
            </Show>

            <DomainOperationTable<NoticeResourceOperationRow>
              domain="notice"
              emptyDescription="No matching notice operations are currently visible."
              metricColumns={noticeOperationColumns}
              rows={data?.operations ?? []}
              scope={{ area: props.area, realm: props.realm, resource: props.resource }}
              title="Observed subscription patterns"
            />
            <Show when={data && data.observationsReturned >= data.limit}>
              <Badge role="status" variant="warning">
                Observation sample reached {data?.limit}
              </Badge>
            </Show>
          </Block>
        </Show>
      </Block>
    </DomainPageFrame>
  );
}

export default function NoticePage() {
  const route = currentRoute();
  const realm = decodeParam(route.params.realm);
  const area = decodeParam(route.params.area);
  const resource = decodeParam(route.params.resource);
  const operation = decodeParam(route.params.operation);

  if (realm && area && resource && operation) {
    return (
      <NoticeOperationPage area={area} operation={operation} realm={realm} resource={resource} />
    );
  }

  if (realm && area && resource) {
    return <NoticeResourcePage area={area} realm={realm} resource={resource} />;
  }

  return <NoticeLandingPage />;
}
