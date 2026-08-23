import { For, Show } from "@askrjs/askr/control";
import { currentRoute, Link } from "@askrjs/askr/router";
import { Block, Item, ItemContent, ItemGroup, ItemTitle } from "@askrjs/themes/components";
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
import { createRpcResourceQuery } from "@/features/rpc/rpc-query";
import type { RpcResourceOperationRows } from "@/features/rpc/rpc-models";
import { formatCount, formatNumber } from "@/shared/format";
import { domainScopeHref, formatFitzRoute } from "@/shared/navigation/domains";

function decodeParam(value: string | undefined) {
  if (!value) return "";

  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

function formatLatency(value: number | null) {
  return value == null ? "--" : `${formatNumber(value)} ms`;
}

function RpcOperationList(props: { data: RpcResourceOperationRows }) {
  return (
    <ItemGroup as="ul" aria-label="RPC operations" class="domain-divided-list rpc-operation-list">
      <For each={props.data.operations} by={(row) => row.operation}>
        {(row) => {
          const route = formatFitzRoute("rpc", {
            area: props.data.area,
            operation: row.operation,
            realm: props.data.realm,
            resource: props.data.resource,
          });

          return (
            <Item as="li">
              <ItemContent>
                <ItemTitle>
                  <Link
                    class="domain-link-cell rpc-operation-link"
                    href={domainScopeHref("rpc", {
                      area: props.data.area,
                      operation: row.operation,
                      realm: props.data.realm,
                      resource: props.data.resource,
                    })}
                    title={route}
                  >
                    {route}
                  </Link>
                </ItemTitle>
                <dl class="domain-operation-metrics">
                  <div>
                    <dt>Workers</dt>
                    <dd>{formatNumber(row.workers)}</dd>
                  </div>
                  <div>
                    <dt>Pending requests</dt>
                    <dd>{formatNumber(row.pendingRequests)}</dd>
                  </div>
                  <div>
                    <dt>Requests handled</dt>
                    <dd>{formatNumber(row.requestsHandled)}</dd>
                  </div>
                  <div>
                    <dt>Average latency</dt>
                    <dd>{formatLatency(row.averageLatencyMs)}</dd>
                  </div>
                </dl>
              </ItemContent>
            </Item>
          );
        }}
      </For>
    </ItemGroup>
  );
}

export default function RpcResourcePage() {
  const route = currentRoute();
  const realm = decodeParam(route.params.realm);
  const area = decodeParam(route.params.area);
  const resource = decodeParam(route.params.resource);
  const query = createRpcResourceQuery(realm, area, resource);
  const data = query.data;
  const totalWorkers = data?.operations.reduce((sum, row) => sum + row.workers, 0) ?? 0;
  const pendingRequests = data?.operations.reduce((sum, row) => sum + row.pendingRequests, 0) ?? 0;

  return (
    <DomainPageFrame>
      <Block direction="column" gap="sm">
        <DomainHeader
          eyebrow="RPC resource"
          title={resource}
          description={`Live operation evidence for ${realm} / ${area} / ${resource}.`}
          primaryAction={{
            busy: query.refreshing,
            disabled: query.refreshing,
            label: "Refresh operations",
            onPress: () => query.refresh(),
          }}
          status={queryHeaderStatus(query, {
            loading: "Loading RPC operations.",
            ready: data
              ? `${formatCount(data.operations.length, "operation")}, ${formatCount(
                  totalWorkers,
                  "live worker",
                )}, ${formatCount(pendingRequests, "pending request")}.`
              : "",
            unavailable: "RPC operation evidence is unavailable for this resource.",
          })}
        />
        <OperatorScopeStrip
          realm={realm}
          area={area}
          resource={resource}
          freshness={queryFreshness(query)}
        />
        <Show when={!data && query.loading}>
          <QueryLoadingState description="Loading RPC operations..." />
        </Show>
        <Show when={!data && query.error}>
          <QueryErrorState
            title="Unable to load RPC operations"
            error={query.error}
            onRetry={() => query.refresh()}
          />
        </Show>
        <Show when={data}>
          <Block direction="column" gap="sm">
            <Show when={query.refreshing}>
              <QueryRefreshingState description="Refreshing RPC operations..." />
            </Show>
            <DomainDataSection
              id="rpc-resource-operations"
              title="RPC operations"
              description="Live operation evidence: workers, handled calls, latency, and in-memory pending request evidence."
            >
              <Show
                when={data && data.operations.length === 0}
                fallback={data ? <RpcOperationList data={data} /> : null}
              >
                <QueryCompactEmptyState
                  title="No operations"
                  description="No RPC operations are currently visible for this resource."
                />
              </Show>
            </DomainDataSection>
          </Block>
        </Show>
      </Block>
    </DomainPageFrame>
  );
}
