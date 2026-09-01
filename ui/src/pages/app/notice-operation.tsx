import { For, Show } from "@askrjs/askr/control";
import { currentRoute } from "@askrjs/askr/router";
import {
  Badge,
  Block,
  Item,
  ItemActions,
  ItemContent,
  ItemDescription,
  ItemGroup,
  ItemTitle,
  Text,
} from "@askrjs/themes/components";
import DomainDataSection from "@/components/shared/domain-data-section";
import DomainHeader from "@/components/shared/domain-header";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import OperatorScopeStrip from "@/components/shared/operator-scope-strip";
import { queryFreshness, queryHeaderStatus } from "@/components/shared/query-header-status";
import {
  QueryCompactEmptyState,
  QueryErrorState,
  QueryLoadingState,
  QueryRefreshingState,
} from "@/components/shared/query-state";
import { formatNumber } from "@/shared/format";
import { createNoticeOperationRowsQuery } from "@/features/notice/notice-query";
import type { NoticeDeliveryRow, NoticeDeliveryRows } from "@/features/notice/notice-models";

function countActiveSubscribers(deliveries: NoticeDeliveryRow[]) {
  return new Set(
    deliveries.map((row) => `${row.subscriptionId ?? "session"}:${row.sessionId ?? "session"}`),
  ).size;
}

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

function NoticeDeliveryList(props: { rows: NoticeDeliveryRows["observations"] }) {
  return (
    <ItemGroup
      as="ul"
      aria-label="Delivery evidence"
      class="domain-divided-list notice-delivery-list"
    >
      <For
        each={props.rows}
        by={(observation) =>
          `${observation.sessionId ?? "session"}:${observation.subscriptionId ?? "none"}`
        }
      >
        {(observation) => (
          <Item as="li">
            <ItemContent>
              <ItemTitle>
                <Text as="strong" font="mono" weight="semibold" wrap="anywhere">
                  {observation.sessionId ?? "--"}
                </Text>
              </ItemTitle>
              <ItemDescription>
                <Block direction="row" gap="md" wrap>
                  {observation.subscriptionId == null ? null : (
                    <Text as="span" font="mono" numeric="tabular" size="sm" tone="muted">
                      Subscription: {formatNumber(observation.subscriptionId)}
                    </Text>
                  )}
                  <Text as="span" font="mono" numeric="tabular" size="sm" tone="muted">
                    Notifications observed: {formatNumber(observation.notificationsReceived)}
                  </Text>
                  <Text as="span" font="mono" numeric="tabular" size="sm" tone="muted">
                    Current publishes / min: {formatNumber(observation.publishesPerMinute)}
                  </Text>
                  <Text as="span" font="mono" numeric="tabular" size="sm" tone="muted">
                    Observed publish total: {formatNumber(observation.publishesTotal)}
                  </Text>
                </Block>
              </ItemDescription>
            </ItemContent>
            <ItemActions>
              <Badge aria-label={`Status: ${observation.status}`} variant="outline">
                {observation.status}
              </Badge>
            </ItemActions>
          </Item>
        )}
      </For>
    </ItemGroup>
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
  const activeSubscribers = countActiveSubscribers(deliveries);
  return (
    <DomainPageFrame>
      <Block direction="column" gap="sm">
        <DomainHeader
          eyebrow="Notice operation"
          title={query}
          description={`${realm} / ${area} / ${resource} / ${query}`}
          primaryAction={{
            busy: rowsQuery.refreshing,
            disabled: rowsQuery.refreshing,
            label: "Refresh operation deliveries",
            onPress: () => rowsQuery.refresh(),
          }}
          status={queryHeaderStatus(rowsQuery, {
            loading: "Loading notice deliveries for this operation.",
            ready: data
              ? `${formatNumber(data.observations.length)} live subscription row${data.observations.length === 1 ? "" : "s"} for this operation route. ${activeSubscribers} active subscriber${activeSubscribers === 1 ? "" : "s"}.`
              : "",
            unavailable: "Notice delivery evidence is unavailable for this operation.",
          })}
        />
        <OperatorScopeStrip
          realm={realm}
          area={area}
          resource={resource}
          operation={query}
          freshness={queryFreshness(rowsQuery)}
        />
        <Show when={!data && rowsQuery.loading}>
          <QueryLoadingState description="Loading notice operation deliveries..." />
        </Show>
        <Show when={!data && rowsQuery.error}>
          <QueryErrorState
            title="Unable to load notice operation deliveries"
            error={rowsQuery.error}
            onRetry={() => rowsQuery.refresh()}
          />
        </Show>

        <Show when={data}>
          {(data) => (
            <Block direction="column" gap="sm">
              <Show when={rowsQuery.refreshing}>
                <QueryRefreshingState description="Refreshing notice operation deliveries..." />
              </Show>

              <DomainDataSection
                id="notice-delivery-evidence"
                title="Delivery evidence"
                description="Live subscription observations for this operation route; not delivery history. The API does not report a reset scope for total counters."
              >
                <Show
                  when={data.observations.length === 0}
                  fallback={<NoticeDeliveryList rows={data.observations} />}
                >
                  <QueryCompactEmptyState
                    title="No delivery evidence"
                    description="No matching notice deliveries are currently visible."
                  />
                </Show>
              </DomainDataSection>
            </Block>
          )}
        </Show>
      </Block>
    </DomainPageFrame>
  );
}
