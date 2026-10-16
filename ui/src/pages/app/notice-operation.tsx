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
import DomainFacts from "@/components/shared/domain-facts";
import DomainHeader from "@/components/shared/domain-header";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import { queryHeaderStatus } from "@/components/shared/query-header-status";
import {
  QueryCompactEmptyState,
  QueryErrorState,
  QueryLoadingState,
  QueryRefreshingState,
} from "@/components/shared/query-state";
import { formatNumber } from "@/shared/format";
import { createNoticeOperationRowsQuery } from "@/features/notice/notice-query";
import type { NoticeDeliveryRow, NoticeDeliveryRows } from "@/features/notice/notice-models";

function countObservedSubscribers(deliveries: NoticeDeliveryRow[]) {
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
  const observedSubscribers = countObservedSubscribers(deliveries);
  const publishesPerMinute = deliveries.reduce(
    (maximum, row) => Math.max(maximum, row.publishesPerMinute),
    0,
  );
  return (
    <DomainPageFrame>
      <Block direction="column" gap="sm">
        <DomainHeader
          compact={true}
          eyebrow="Notice subscription pattern"
          title={query}
          description={`${realm} / ${area} / ${resource} / ${query}`}
          primaryAction={{
            busy: rowsQuery.refreshing,
            disabled: rowsQuery.refreshing,
            label: "Refresh operation deliveries",
            onPress: () => rowsQuery.refresh(),
          }}
          status={queryHeaderStatus(rowsQuery)}
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

              <DomainFacts
                id="notice-operation-facts"
                title="Current route"
                items={[
                  { label: "Subscribers observed", value: formatNumber(observedSubscribers) },
                  {
                    label: "Publishes/min",
                    value: deliveries.length === 0 ? "--" : formatNumber(publishesPerMinute),
                  },
                  {
                    label: "Delivery rows",
                    value: `${deliveries.length}${data.observationsReturned >= data.limit ? ` (limit ${data.limit})` : ""}`,
                    title: "Bounded live observations, not delivery history.",
                  },
                ]}
              />

              <DomainDataSection
                id="notice-delivery-evidence"
                title="Delivery evidence"
                actions={
                  data.observationsReturned >= data.limit ? (
                    <Badge variant="warning">Observation sample reached {data.limit}</Badge>
                  ) : undefined
                }
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
