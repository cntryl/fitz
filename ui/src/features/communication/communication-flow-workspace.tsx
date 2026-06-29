import { state } from "@askrjs/askr";
import { For } from "@askrjs/askr/control";
import { Link } from "@askrjs/askr/router";
import {
  Badge,
  Button,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  Inline,
  Label,
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectLabel,
  SelectPortal,
  SelectTrigger,
  SelectValue,
  Stack,
} from "@askrjs/themes/components";
import { VirtualTable, type VirtualTableColumn } from "@askrjs/ui";
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
import { domainResourceHref, formatFitzRoute } from "@/shared/navigation/domains";
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

function uniqueSorted(values: string[]) {
  return Array.from(new Set(values)).sort((first, second) => first.localeCompare(second));
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
    ? "Search broker-local subscription delivery counters and route publish counters by selected Route Family and route scope."
    : "Search broker-local RPC worker registrations and pending calls by selected Route Family and route scope.";
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
    width: "40%",
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
    id: "correlation",
    header: "Correlation",
    width: "24%",
    cellComponent: ({ row }) => (
      <span class="domain-table-cell-truncate" title={row.correlation_id ?? "None"}>
        {row.correlation_id ?? "None"}
      </span>
    ),
  },
  {
    id: "worker",
    header: "Worker",
    width: "20%",
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
      <Inline justify="between" align="center" gap="3" wrap="wrap">
        <p class="domain-muted">
          {result.observations.length} notice observation
          {result.observations.length === 1 ? "" : "s"} in route family {result.route_family}
        </p>
      </Inline>

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
      <Inline justify="between" align="center" gap="3" wrap="wrap">
        <p class="domain-muted">
          {result.observations.length} RPC observation
          {result.observations.length === 1 ? "" : "s"} in route family {result.route_family}
        </p>
      </Inline>

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
  const [searchLoading, setSearchLoading] = state(false);
  const [searchError, setSearchError] = state<unknown>(null);
  const [noticeResult, setNoticeResult] = state<NoticeDeliveryObservationList | null>(null);
  const [rpcResult, setRpcResult] = state<RpcCallObservationList | null>(null);
  const modeValue = mode();
  const realmValue = realm();
  const areaValue = area();
  const resourceValue = resource();
  const searchLoadingValue = searchLoading();
  const searchErrorValue = searchError();
  const noticeResultValue = noticeResult();
  const rpcResultValue = rpcResult();
  const rows = flattenInventory(inventory);
  const selectedRealmRows = realmValue ? rows.filter((row) => row.realm === realmValue) : rows;
  const selectedAreaRows = areaValue
    ? selectedRealmRows.filter((row) => row.area === areaValue)
    : selectedRealmRows;
  const realmOptions = uniqueSorted(rows.map((row) => row.realm));
  const areaOptions = uniqueSorted(selectedRealmRows.map((row) => row.area));
  const resourceOptions = uniqueSorted(selectedAreaRows.map((row) => row.resource));
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
  const canRunSearch = searchMode && routeFamilyReady && !searchLoadingValue;
  const canOpenExactResource = filteredRows.some(
    (row) => row.realm === realmValue && row.area === areaValue && row.resource === resourceValue,
  );
  const badgeLabel = searchMode
    ? routeFamilyReady
      ? "Existing API"
      : "Select Route Family"
    : "Existing API";
  const badgeVariant = searchMode ? (routeFamilyReady ? "success" : "warning") : "success";
  const columns: readonly VirtualTableColumn<CommunicationResourceRow>[] = [
    {
      id: "route",
      header: "Route",
      width: "76%",
      cellComponent: ({ row }) => {
        const route = formatFitzRoute(domain, row);

        return (
          <span class="domain-table-cell-truncate" title={route}>
            {route}
          </span>
        );
      },
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

  function resetSearchResults() {
    setSearchError(null);
    setNoticeResult(null);
    setRpcResult(null);
  }

  function selectRealm(nextRealm: string) {
    setRealm(nextRealm);
    setArea("");
    setResource("");
    resetSearchResults();
  }

  function selectArea(nextArea: string) {
    setArea(nextArea);
    setResource("");
    resetSearchResults();
  }

  function selectResource(nextResource: string) {
    setResource(nextResource);
    resetSearchResults();
  }

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
            realm: trimmedRealm,
            resource: trimmedResource,
            routeFamily,
          }),
        );
      } else {
        setRpcResult(
          await rpcService.searchCalls({
            area: trimmedArea,
            limit: 50,
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
        <Inline justify="between" align="start" gap="3" wrap="wrap">
          <Stack gap="1">
            <CardTitle>Communication flow</CardTitle>
            <CardDescription>
              Follow live communication from ingress through route, participant, failure, and
              performance signals without treating ephemeral state as durable history.
            </CardDescription>
          </Stack>
          <Badge variant={badgeVariant}>{badgeLabel}</Badge>
        </Inline>
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
                    resetSearchResults();
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
                <Select
                  value={realmValue}
                  onValueChange={selectRealm}
                  disabled={realmOptions.length === 0}
                >
                  <SelectTrigger id={`${domain}-flow-realm`}>
                    <SelectValue placeholder="All realms" />
                  </SelectTrigger>
                  <SelectPortal>
                    <SelectContent align="start" sideOffset={6}>
                      <SelectGroup>
                        <SelectLabel>Realm scope</SelectLabel>
                        <SelectItem value="">All realms</SelectItem>
                        <For each={realmOptions} by={(option) => option}>
                          {(option) => <SelectItem value={option}>{option}</SelectItem>}
                        </For>
                      </SelectGroup>
                    </SelectContent>
                  </SelectPortal>
                </Select>
              </div>
              <div class="auth-field">
                <Label for={`${domain}-flow-area`}>Area</Label>
                <Select
                  value={areaValue}
                  onValueChange={selectArea}
                  disabled={areaOptions.length === 0}
                >
                  <SelectTrigger id={`${domain}-flow-area`}>
                    <SelectValue placeholder="All areas" />
                  </SelectTrigger>
                  <SelectPortal>
                    <SelectContent align="start" sideOffset={6}>
                      <SelectGroup>
                        <SelectLabel>Area scope</SelectLabel>
                        <SelectItem value="">All areas</SelectItem>
                        <For each={areaOptions} by={(option) => option}>
                          {(option) => <SelectItem value={option}>{option}</SelectItem>}
                        </For>
                      </SelectGroup>
                    </SelectContent>
                  </SelectPortal>
                </Select>
              </div>
              <div class="auth-field">
                <Label for={`${domain}-flow-resource`}>
                  {domain === "notice" ? "Notice route" : "RPC route"}
                </Label>
                <Select
                  value={resourceValue}
                  onValueChange={selectResource}
                  disabled={resourceOptions.length === 0}
                >
                  <SelectTrigger id={`${domain}-flow-resource`}>
                    <SelectValue
                      placeholder={domain === "notice" ? "All notice routes" : "All RPC routes"}
                    />
                  </SelectTrigger>
                  <SelectPortal>
                    <SelectContent align="start" sideOffset={6}>
                      <SelectGroup>
                        <SelectLabel>
                          {domain === "notice" ? "Notice route scope" : "RPC route scope"}
                        </SelectLabel>
                        <SelectItem value="">
                          {domain === "notice" ? "All notice routes" : "All RPC routes"}
                        </SelectItem>
                        <For each={resourceOptions} by={(option) => option}>
                          {(option) => <SelectItem value={option}>{option}</SelectItem>}
                        </For>
                      </SelectGroup>
                    </SelectContent>
                  </SelectPortal>
                </Select>
              </div>
            </div>
            {searchMode ? (
              <Inline
                class="communication-query-actions"
                justify="between"
                align="center"
                gap="3"
                wrap="wrap"
              >
                <p class="domain-muted">
                  Querying {operatorContext.selectedRouteFamily.label} with the selected route
                  scope. Leave selectors on All to broaden the evidence read.
                </p>
                <Button type="submit" disabled={!canRunSearch}>
                  {searchLoadingValue ? "Running" : "Run search"}
                </Button>
              </Inline>
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
                description="Adjust the realm, area, or route selectors to find visible communication resources."
              />
            ) : (
              <Stack gap="3">
                <Inline justify="between" align="center" gap="3" wrap="wrap">
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
                </Inline>

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
