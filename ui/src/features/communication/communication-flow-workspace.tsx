import { state } from "@askrjs/askr";
import { For } from "@askrjs/askr/control";
import { Link } from "@askrjs/askr/router";
import { Button } from "@askrjs/themes/controls";
import { Flex, Stack } from "@askrjs/themes/layouts";
import {
  Badge,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@askrjs/themes/surfaces";
import { Input, Label, VirtualTable, type VirtualTableColumn } from "@askrjs/ui";
import type {
  NoticeDeliveryObservation,
  NoticeDeliveryObservationList,
  RpcCallObservation,
  RpcCallObservationList,
} from "@/adapters";
import {
  QueryEmptyState,
  QueryErrorState,
  QueryLoadingState,
} from "@/components/shared/query-state";
import type { ResourceInventory } from "@/features/resource/resource-models";
import { noticeService } from "@/features/notice/notice-service";
import { rpcService } from "@/features/rpc/rpc-service";
import { domainResourceHref } from "@/shared/navigation/domains";
import { parseConcreteRouteFamilyId, useOperatorContext } from "@/shared/operator-context";

type CommunicationDomain = "notice" | "rpc";
type CommunicationMode = "flow" | "participants" | "failures" | "search";

interface CommunicationResourceRow {
  area: string;
  realm: string;
  resource: string;
}

interface CommunicationModeOption {
  description: string;
  label: string;
  value: CommunicationMode;
}

interface FlowStage {
  caption: string;
  label: string;
  tone?: "danger" | "warning";
  value: string | number;
}

export interface NoticeCommunicationStats {
  deliveryDropsTotal: number;
  maxRouteSubscribers: number;
  publishesPerSecond: number;
  routesActive: number;
  subscriptionsActive: number;
  wildcardLimitRejectsTotal: number;
}

export interface RpcCommunicationStats {
  failureTotal: number;
  operationsPerSecond: number;
  pendingRoutesActive: number;
  requestsPending: number;
  requestTimeoutsTotal: number;
  responsesDroppedClosedCallerTotal: number;
  responsesMissingPendingTotal: number;
  workersRegistered: number;
}

export interface CommunicationFlowWorkspaceProps {
  domain: CommunicationDomain;
  error?: unknown;
  inventory?: ResourceInventory | null;
  loading?: boolean;
  stats: NoticeCommunicationStats | RpcCommunicationStats;
}

function modeOptionsFor(domain: CommunicationDomain): CommunicationModeOption[] {
  const participantDescription =
    domain === "notice"
      ? "Use existing resource-level Notice subscription evidence after a route is selected."
      : "Use existing resource-level RPC operation, worker, and pending request evidence after a route is selected.";
  const searchLabel = domain === "notice" ? "Delivery search" : "Call search";
  const searchDescription =
    domain === "notice"
      ? "Search broker-local Notice delivery-counter evidence by Route Family and scope."
      : "Search broker-local RPC worker and pending-call evidence by Route Family and scope.";

  return [
    {
      description: "Use existing live route, participant, rate, and failure counters.",
      label: "Flow graph",
      value: "flow",
    },
    {
      description: participantDescription,
      label: "Participants",
      value: "participants",
    },
    {
      description: "Use existing overview counters and resource-level bounded event timelines.",
      label: "Failures",
      value: "failures",
    },
    {
      description: searchDescription,
      label: searchLabel,
      value: "search",
    },
  ];
}

function isNoticeStats(
  stats: NoticeCommunicationStats | RpcCommunicationStats,
): stats is NoticeCommunicationStats {
  return "subscriptionsActive" in stats;
}

function formatRate(value: number) {
  return `${value.toFixed(2)} / sec`;
}

function failureSignalCount(stats: NoticeCommunicationStats | RpcCommunicationStats) {
  if (isNoticeStats(stats)) {
    return stats.deliveryDropsTotal + stats.wildcardLimitRejectsTotal;
  }

  return (
    stats.failureTotal +
    stats.requestTimeoutsTotal +
    stats.responsesDroppedClosedCallerTotal +
    stats.responsesMissingPendingTotal
  );
}

function flowStagesFor(
  domain: CommunicationDomain,
  stats: NoticeCommunicationStats | RpcCommunicationStats,
): FlowStage[] {
  if (isNoticeStats(stats)) {
    const failures = failureSignalCount(stats);

    return [
      {
        caption: "Live publish pressure entering Notice fanout.",
        label: "Publishers",
        value: formatRate(stats.publishesPerSecond),
      },
      {
        caption: "Active in-memory notice routes visible to admin.",
        label: "Routes",
        value: stats.routesActive,
      },
      {
        caption: "Session-scoped subscribers; removed on disconnect or restart.",
        label: "Subscribers",
        value: stats.subscriptionsActive,
      },
      {
        caption: "Delivery drops plus wildcard limit rejects.",
        label: "Failure signals",
        tone: failures > 0 ? "danger" : undefined,
        value: failures,
      },
    ];
  }

  const failures = failureSignalCount(stats);

  return [
    {
      caption: "Live request/response throughput across RPC.",
      label: "Calls",
      value: formatRate(stats.operationsPerSecond),
    },
    {
      caption: "Requests waiting in broker-local pending state.",
      label: "Pending",
      tone: stats.requestsPending > stats.workersRegistered ? "warning" : undefined,
      value: stats.requestsPending,
    },
    {
      caption: "Live workers currently registered to handle calls.",
      label: "Workers",
      value: stats.workersRegistered,
    },
    {
      caption: "Failures, timeouts, closed callers, and missing pending responses.",
      label: "Failure signals",
      tone: failures > 0 ? "danger" : undefined,
      value: failures,
    },
  ];
}

function flattenInventory(inventory?: ResourceInventory | null): CommunicationResourceRow[] {
  return (
    inventory?.realms.flatMap((realm) =>
      realm.areas.flatMap((area) =>
        area.resources.map((resource) => ({
          area: area.area,
          realm: realm.realm,
          resource,
        })),
      ),
    ) ?? []
  );
}

function includesQuery(value: string, query: string) {
  const normalized = query.trim().toLowerCase();

  return normalized.length === 0 || value.toLowerCase().includes(normalized);
}

function filterRows(
  rows: CommunicationResourceRow[],
  filters: {
    area: string;
    realm: string;
    resource: string;
  },
) {
  return rows.filter(
    (row) =>
      includesQuery(row.realm, filters.realm) &&
      includesQuery(row.area, filters.area) &&
      includesQuery(row.resource, filters.resource),
  );
}

function trimToUndefined(value: string) {
  const trimmed = value.trim();

  return trimmed.length > 0 ? trimmed : undefined;
}

function queryLabel(domain: CommunicationDomain, mode: CommunicationMode) {
  if (domain === "notice") {
    if (mode === "participants") return "Session/pattern";
    if (mode === "failures") return "Drop or reject";
    if (mode === "search") return "Delivery";

    return "Pattern";
  }

  if (mode === "participants") return "Operation/worker";
  if (mode === "failures") return "Failure/correlation";
  if (mode === "search") return "Call/correlation";

  return "Operation";
}

function queryPlaceholder(domain: CommunicationDomain, mode: CommunicationMode) {
  if (domain === "notice") {
    if (mode === "participants") return "session-123 or billing.*";
    if (mode === "failures") return "delivery drop";
    if (mode === "search") return "delivery-123";

    return "billing.*";
  }

  if (mode === "participants") return "SettlePayment or session-123";
  if (mode === "failures") return "timeout or correlation id";
  if (mode === "search") return "corr-123";

  return "SettlePayment";
}

function resourceNoun(domain: CommunicationDomain, count: number) {
  const singular = domain === "notice" ? "notice route" : "RPC route";

  return count === 1 ? singular : `${singular}s`;
}

function actionLabel(domain: CommunicationDomain) {
  return domain === "notice" ? "Open subscriptions" : "Open operations";
}

function exactActionLabel(domain: CommunicationDomain) {
  return domain === "notice" ? "Open exact notice route" : "Open exact RPC route";
}

function modeDetailTitle(domain: CommunicationDomain, mode: CommunicationMode) {
  if (mode === "participants") {
    return domain === "notice" ? "Subscription participants" : "Operation participants";
  }

  if (mode === "failures") return "Failure trace evidence";

  return domain === "notice" ? "Delivery search" : "Call search";
}

function modeDetailDescription(domain: CommunicationDomain, mode: CommunicationMode) {
  if (mode === "participants") {
    return domain === "notice"
      ? "Select a notice route to inspect active in-memory subscriptions, patterns, sessions, and delivered notification counters exposed by the current resource API."
      : "Select an RPC route to inspect operations, workers for the first operation, and broker-local pending requests exposed by the current resource API.";
  }

  if (mode === "failures") {
    return domain === "notice"
      ? "Notice failure tracing starts with delivery drop and wildcard reject counters, then narrows through resource-level bounded event timelines."
      : "RPC failure tracing starts with pending, timeout, missing-pending, and closed-caller counters, then narrows through resource-level bounded event timelines.";
  }

  return domain === "notice"
    ? "Search broker-local subscription delivery counters and route publish counters by Route Family, realm, area, resource, session, or route text."
    : "Search broker-local RPC worker registrations and pending calls by Route Family, realm, area, resource, operation, session, or correlation id.";
}

const noticeObservationColumns: readonly VirtualTableColumn<NoticeDeliveryObservation>[] = [
  {
    id: "route",
    header: "Route",
    width: "34%",
    cellComponent: ({ row }) => (
      <span class="domain-table-cell-truncate" title={row.route}>
        {row.route}
      </span>
    ),
  },
  {
    id: "status",
    header: "Status",
    width: "18%",
    cellComponent: ({ row }) => <Badge variant="outline">{row.status}</Badge>,
  },
  {
    id: "session",
    header: "Session",
    width: "18%",
    cellComponent: ({ row }) => (
      <span class="domain-table-cell-truncate" title={row.session_id ?? "Route"}>
        {row.session_id ?? "Route"}
      </span>
    ),
  },
  {
    id: "received",
    header: "Received",
    width: "15%",
    cellComponent: ({ row }) => <span>{row.notifications_received}</span>,
  },
  {
    id: "publishes",
    header: "Publishes",
    width: "15%",
    cellComponent: ({ row }) => <span>{row.publishes_total}</span>,
  },
];

const rpcObservationColumns: readonly VirtualTableColumn<RpcCallObservation>[] = [
  {
    id: "route",
    header: "Route",
    width: "30%",
    cellComponent: ({ row }) => (
      <span class="domain-table-cell-truncate" title={row.route}>
        {row.route}
      </span>
    ),
  },
  {
    id: "state",
    header: "State",
    width: "16%",
    cellComponent: ({ row }) => (
      <Badge variant={row.state === "pending" ? "warning" : "success"}>{row.state}</Badge>
    ),
  },
  {
    id: "operation",
    header: "Operation",
    width: "18%",
    cellComponent: ({ row }) => (
      <span class="domain-table-cell-truncate" title={row.operation ?? "Unknown"}>
        {row.operation ?? "Unknown"}
      </span>
    ),
  },
  {
    id: "correlation",
    header: "Correlation",
    width: "20%",
    cellComponent: ({ row }) => (
      <span class="domain-table-cell-truncate" title={row.correlation_id ?? "None"}>
        {row.correlation_id ?? "None"}
      </span>
    ),
  },
  {
    id: "worker",
    header: "Worker",
    width: "16%",
    cellComponent: ({ row }) => (
      <span class="domain-table-cell-truncate" title={row.worker_session_id ?? "None"}>
        {row.worker_session_id ?? "None"}
      </span>
    ),
  },
];

function NoticeObservationPanel({ result }: { result: NoticeDeliveryObservationList }) {
  return (
    <div class="communication-search-result" aria-live="polite">
      <Flex justify="between" align="center" gap="3" wrap="wrap">
        <p class="domain-muted">
          {result.observations.length} notice observation
          {result.observations.length === 1 ? "" : "s"} in route family {result.route_family}
        </p>
      </Flex>

      {result.observations.length === 0 ? (
        <QueryEmptyState
          title="No notice delivery evidence"
          description="No broker-local Notice observations matched the selected Route Family and scope."
        />
      ) : (
        <VirtualTable<NoticeDeliveryObservation>
          aria-label="Notice delivery observations"
          class="communication-resource-virtual-table"
          columns={noticeObservationColumns}
          getKey={(row) =>
            `${row.route_family}:${row.route}:${row.status}:${row.session_id ?? "route"}:${row.subscription_id ?? "none"}`
          }
          headerHeight={44}
          overscan={6}
          rowHeight={48}
          rows={result.observations}
          style={{ height: "320px" }}
        />
      )}
    </div>
  );
}

function RpcObservationPanel({ result }: { result: RpcCallObservationList }) {
  return (
    <div class="communication-search-result" aria-live="polite">
      <Flex justify="between" align="center" gap="3" wrap="wrap">
        <p class="domain-muted">
          {result.observations.length} RPC observation
          {result.observations.length === 1 ? "" : "s"} in route family {result.route_family}
        </p>
      </Flex>

      {result.observations.length === 0 ? (
        <QueryEmptyState
          title="No RPC call evidence"
          description="No broker-local RPC observations matched the selected Route Family and scope."
        />
      ) : (
        <VirtualTable<RpcCallObservation>
          aria-label="RPC call observations"
          class="communication-resource-virtual-table"
          columns={rpcObservationColumns}
          getKey={(row) =>
            `${row.route_family}:${row.route}:${row.state}:${row.correlation_id ?? "none"}:${row.worker_session_id ?? "none"}`
          }
          headerHeight={44}
          overscan={6}
          rowHeight={48}
          rows={result.observations}
          style={{ height: "320px" }}
        />
      )}
    </div>
  );
}

export default function CommunicationFlowWorkspace({
  domain,
  error,
  inventory,
  loading = false,
  stats,
}: CommunicationFlowWorkspaceProps) {
  const operatorContext = useOperatorContext();
  const [mode, setMode] = state<CommunicationMode>("flow");
  const [realm, setRealm] = state("");
  const [area, setArea] = state("");
  const [resource, setResource] = state("");
  const [query, setQuery] = state("");
  const [searchLoading, setSearchLoading] = state(false);
  const [searchError, setSearchError] = state<unknown>(null);
  const [noticeResult, setNoticeResult] = state<NoticeDeliveryObservationList | null>(null);
  const [rpcResult, setRpcResult] = state<RpcCallObservationList | null>(null);
  const modeValue = mode();
  const realmValue = realm();
  const areaValue = area();
  const resourceValue = resource();
  const queryValue = query();
  const searchLoadingValue = searchLoading();
  const searchErrorValue = searchError();
  const noticeResultValue = noticeResult();
  const rpcResultValue = rpcResult();
  const rows = flattenInventory(inventory);
  const filteredRows = filterRows(rows, {
    area: areaValue,
    realm: realmValue,
    resource: resourceValue,
  });
  const flowStages = flowStagesFor(domain, stats);
  const modeOptions = modeOptionsFor(domain);
  const routeFamily = parseConcreteRouteFamilyId(operatorContext.selectedRouteFamilyId);
  const routeFamilyReady = routeFamily !== null;
  const searchMode = modeValue === "search";
  const trimmedRealm = trimToUndefined(realmValue);
  const trimmedArea = trimToUndefined(areaValue);
  const trimmedResource = trimToUndefined(resourceValue);
  const trimmedQuery = trimToUndefined(queryValue);
  const canRunSearch = searchMode && routeFamilyReady && !searchLoadingValue;
  const canOpenExactResource = filteredRows.some(
    (row) =>
      row.realm === realmValue && row.area === areaValue && row.resource === resourceValue,
  );
  const badgeLabel = searchMode
    ? routeFamilyReady
      ? "Existing API"
      : "Select Route Family"
    : "Existing API";
  const badgeVariant = searchMode ? (routeFamilyReady ? "success" : "warning") : "success";
  const columns: readonly VirtualTableColumn<CommunicationResourceRow>[] = [
    {
      id: "realm",
      header: "Realm",
      width: "21%",
      cellComponent: ({ row }) => (
        <span class="domain-table-cell-truncate">{row.realm}</span>
      ),
    },
    {
      id: "area",
      header: "Area",
      width: "21%",
      cellComponent: ({ row }) => (
        <span class="domain-table-cell-truncate">{row.area}</span>
      ),
    },
    {
      id: "resource",
      header: domain === "notice" ? "Notice route" : "RPC route",
      width: "34%",
      cellComponent: ({ row }) => (
        <span class="domain-table-cell-truncate">{row.resource}</span>
      ),
    },
    {
      id: "action",
      header: "Inspect",
      width: "24%",
      cellComponent: ({ row }) => (
        <Link class="text-link" href={domainResourceHref(domain, row)}>
          {actionLabel(domain)}
        </Link>
      ),
    },
  ];

  async function runSearch() {
    if (!canRunSearch || routeFamily === null) {
      return;
    }

    setSearchLoading(true);
    setSearchError(null);
    setNoticeResult(null);
    setRpcResult(null);

    try {
      if (domain === "notice") {
        setNoticeResult(
          await noticeService.searchDeliveries({
            area: trimmedArea,
            limit: 50,
            query: trimmedQuery,
            realm: trimmedRealm,
            resource: trimmedResource,
            routeFamily,
          }),
        );
      } else {
        setRpcResult(
          await rpcService.searchCalls({
            area: trimmedArea,
            correlationId: trimmedQuery,
            limit: 50,
            query: trimmedQuery,
            realm: trimmedRealm,
            resource: trimmedResource,
            routeFamily,
          }),
        );
      }
    } catch (caughtError) {
      setSearchError(caughtError);
    } finally {
      setSearchLoading(false);
    }
  }

  function onSubmit(event: Event) {
    event.preventDefault();
    void runSearch();
  }

  return (
    <Card padding="sm" variant="default">
      <CardHeader>
        <Flex justify="between" align="start" gap="3" wrap="wrap">
          <Stack gap="1">
            <CardTitle>Communication flow</CardTitle>
            <CardDescription>
              Follow live communication from ingress through route, participant, failure, and
              performance signals without treating ephemeral state as durable history.
            </CardDescription>
          </Stack>
          <Badge variant={badgeVariant}>{badgeLabel}</Badge>
        </Flex>
      </CardHeader>

      <CardContent>
        <Stack gap="3">
          <div class="communication-flow-grid" aria-label={`${domain} flow graph`}>
            <For each={flowStages} by={(stage) => stage.label}>
              {(stage) => (
                <div class="communication-flow-card" data-tone={stage.tone ?? "default"}>
                  <span class="domain-header-kicker">{stage.label}</span>
                  <strong class="communication-flow-value">{stage.value}</strong>
                  <span class="domain-muted">{stage.caption}</span>
                </div>
              )}
            </For>
          </div>

          <div class="domain-query-mode-grid" role="group" aria-label={`${domain} flow mode`}>
            <For each={modeOptions} by={(modeOption) => modeOption.value}>
              {(modeOption) => (
                <Button
                  type="button"
                  variant={modeValue === modeOption.value ? "primary" : "outline"}
                  onPress={() => {
                    setMode(modeOption.value);
                    setSearchError(null);
                    setNoticeResult(null);
                    setRpcResult(null);
                  }}
                  aria-pressed={modeValue === modeOption.value}
                  title={modeOption.description}
                >
                  <span>{modeOption.label}</span>
                </Button>
              )}
            </For>
          </div>

          <form class="communication-flow-form" onSubmit={onSubmit}>
            <div class="form-grid">
              <div class="auth-field">
                <Label for={`${domain}-flow-realm`}>Realm</Label>
                <Input
                  id={`${domain}-flow-realm`}
                  value={realmValue}
                  onInput={(event: Event) => setRealm((event.target as HTMLInputElement).value)}
                  placeholder="billing"
                />
              </div>
              <div class="auth-field">
                <Label for={`${domain}-flow-area`}>Area</Label>
                <Input
                  id={`${domain}-flow-area`}
                  value={areaValue}
                  onInput={(event: Event) => setArea((event.target as HTMLInputElement).value)}
                  placeholder="payments"
                />
              </div>
              <div class="auth-field">
                <Label for={`${domain}-flow-resource`}>Resource</Label>
                <Input
                  id={`${domain}-flow-resource`}
                  value={resourceValue}
                  onInput={(event: Event) =>
                    setResource((event.target as HTMLInputElement).value)
                  }
                  placeholder={domain === "notice" ? "invoice-events" : "settlement-api"}
                />
              </div>
              <div class="auth-field">
                <Label for={`${domain}-flow-query`}>{queryLabel(domain, modeValue)}</Label>
                <Input
                  id={`${domain}-flow-query`}
                  value={queryValue}
                  disabled={modeValue === "flow"}
                  onInput={(event: Event) => setQuery((event.target as HTMLInputElement).value)}
                  placeholder={queryPlaceholder(domain, modeValue)}
                />
              </div>
            </div>
            {searchMode ? (
              <Flex class="communication-query-actions" justify="between" align="center" gap="3" wrap="wrap">
                <p class="domain-muted">
                  Querying {operatorContext.selectedRouteFamily.label}. Communication observation
                  reads require a concrete numeric Route Family.
                </p>
                <Button type="submit" disabled={!canRunSearch}>
                  {searchLoadingValue ? "Running" : "Run search"}
                </Button>
              </Flex>
            ) : null}
          </form>

          {modeValue !== "flow" ? (
            <Card padding="sm" variant="default">
              <CardHeader>
                <CardTitle>{modeDetailTitle(domain, modeValue)}</CardTitle>
                <CardDescription>{modeDetailDescription(domain, modeValue)}</CardDescription>
              </CardHeader>
            </Card>
          ) : null}

          {searchMode && !routeFamilyReady ? (
            <QueryEmptyState
              title="Concrete Route Family required"
              description="Choose a numeric Route Family from the global selector before searching communication evidence."
            />
          ) : null}

          {searchMode && searchLoadingValue ? (
            <QueryLoadingState description={`Searching ${domain.toUpperCase()} evidence...`} />
          ) : null}
          {searchMode && searchErrorValue ? (
            <QueryErrorState
              title={`Unable to search ${domain.toUpperCase()} evidence`}
              error={searchErrorValue}
              onRetry={() => void runSearch()}
            />
          ) : null}
          {domain === "notice" && searchMode && noticeResultValue && !searchLoadingValue ? (
            <NoticeObservationPanel result={noticeResultValue} />
          ) : null}
          {domain === "rpc" && searchMode && rpcResultValue && !searchLoadingValue ? (
            <RpcObservationPanel result={rpcResultValue} />
          ) : null}

          {loading ? (
            <QueryLoadingState description={`Loading ${domain.toUpperCase()} flow resources...`} />
          ) : null}
          {error ? (
            <QueryErrorState
              title={`Unable to load ${domain.toUpperCase()} flow resources`}
              error={error}
            />
          ) : null}

          {!loading && !error ? (
            filteredRows.length === 0 ? (
              <QueryEmptyState
                title={`No matching ${domain === "notice" ? "notice routes" : "RPC routes"}`}
                description="Adjust the realm, area, or resource filters to find visible communication resources."
              />
            ) : (
              <Stack gap="3">
                <Flex justify="between" align="center" gap="3" wrap="wrap">
                  <p class="domain-muted">
                    {filteredRows.length} matching {resourceNoun(domain, filteredRows.length)}
                  </p>
                  {canOpenExactResource ? (
                    <Link
                      class="text-link"
                      href={domainResourceHref(domain, {
                        area: areaValue,
                        realm: realmValue,
                        resource: resourceValue,
                      })}
                    >
                      {exactActionLabel(domain)}
                    </Link>
                  ) : null}
                </Flex>

                <VirtualTable<CommunicationResourceRow>
                  aria-label={`${domain} communication resources`}
                  class="communication-resource-virtual-table"
                  columns={columns}
                  getKey={(row) => `${row.realm}:${row.area}:${row.resource}`}
                  headerHeight={44}
                  overscan={6}
                  rowHeight={48}
                  rows={filteredRows}
                  style={{ height: "384px" }}
                />
              </Stack>
            )
          ) : null}
        </Stack>
      </CardContent>
    </Card>
  );
}
