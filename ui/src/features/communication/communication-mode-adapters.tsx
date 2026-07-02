import { Badge, Inline } from "@askrjs/themes/components";
import { VirtualTable, type VirtualTableColumn } from "@askrjs/ui";
import type {
  NoticeDeliveryObservation,
  NoticeDeliveryObservationList,
  RpcCallObservation,
  RpcCallObservationList,
} from "@/adapters";
import { QueryEmptyState } from "@/components/shared/query-state";
import { noticeService } from "@/features/notice/notice-service";
import { rpcService } from "@/features/rpc/rpc-service";

export type CommunicationDomain = "notice" | "rpc";
export type CommunicationMode = "flow" | "participants" | "failures" | "search";
export type CommunicationSearchResult = NoticeDeliveryObservationList | RpcCallObservationList;

export interface CommunicationModeOption {
  description: string;
  label: string;
  value: CommunicationMode;
}

export interface FlowStage {
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

export interface CommunicationSearchParams {
  area?: string;
  limit: number;
  realm?: string;
  resource?: string;
  routeFamily: number;
}

export interface CommunicationModeAdapter {
  actionLabel: string;
  allResourcesLabel: string;
  domain: CommunicationDomain;
  emptyResourceTitle: string;
  exactActionLabel: string;
  flowStages(stats: NoticeCommunicationStats | RpcCommunicationStats): FlowStage[];
  liveDataLabel: string;
  loadErrorTitle: string;
  modeDetailDescription(mode: CommunicationMode): string;
  modeDetailTitle(mode: CommunicationMode): string;
  modeOptions: CommunicationModeOption[];
  renderSearchResult(result: CommunicationSearchResult): JSX.Element | null;
  resourceLabel: string;
  resourceNoun(count: number): string;
  resourceScopeLabel: string;
  routeFamilyRequiredLabel: string;
  search(params: CommunicationSearchParams): Promise<CommunicationSearchResult>;
  searchErrorTitle: string;
  searchReadyLabel: string;
}

function formatRate(value: number) {
  return `${value.toFixed(2)} / sec`;
}

function noticeFailures(stats: NoticeCommunicationStats) {
  return stats.deliveryDropsTotal + stats.wildcardLimitRejectsTotal;
}

function rpcFailures(stats: RpcCommunicationStats) {
  return (
    stats.failureTotal +
    stats.requestTimeoutsTotal +
    stats.responsesDroppedClosedCallerTotal +
    stats.responsesMissingPendingTotal
  );
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
          description="No broker-local Notice observations matched the selected Route Family and scope. Clear filters or broaden scope before searching again."
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
          description="No broker-local RPC observations matched the selected Route Family and scope. Clear filters or broaden scope before searching again."
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

const noticeAdapter: CommunicationModeAdapter = {
  actionLabel: "Open subscriptions",
  allResourcesLabel: "All notice routes",
  domain: "notice",
  emptyResourceTitle: "No matching notice routes",
  exactActionLabel: "Open exact notice route",
  flowStages(stats) {
    const noticeStats = stats as NoticeCommunicationStats;
    const failures = noticeFailures(noticeStats);

    return [
      {
        caption: "Live publish pressure entering Notice fanout.",
        label: "Publishers",
        value: formatRate(noticeStats.publishesPerSecond),
      },
      {
        caption: "Active in-memory notice routes visible to admin.",
        label: "Routes",
        value: noticeStats.routesActive,
      },
      {
        caption: "Session-scoped subscribers; removed on disconnect or restart.",
        label: "Subscribers",
        value: noticeStats.subscriptionsActive,
      },
      {
        caption: "Delivery drops plus wildcard limit rejects.",
        label: "Failure signals",
        tone: failures > 0 ? "danger" : undefined,
        value: failures,
      },
    ];
  },
  liveDataLabel: "Live admin data",
  loadErrorTitle: "Unable to load NOTICE flow resources",
  modeDetailDescription(mode) {
    if (mode === "participants") {
      return "Select a notice route to inspect active in-memory subscriptions, patterns, sessions, and delivered notification counters exposed by the current resource API.";
    }

    if (mode === "failures") {
      return "Notice failure tracing starts with delivery drop and wildcard reject counters, then narrows through resource-level bounded event timelines.";
    }

    return "Search broker-local subscription delivery counters and route publish counters by selected Route Family and route scope.";
  },
  modeDetailTitle(mode) {
    if (mode === "participants") return "Subscription participants";
    if (mode === "failures") return "Failure trace evidence";
    return "Delivery search";
  },
  modeOptions: [
    {
      description: "Use live Notice route, participant, rate, and failure counters.",
      label: "Flow graph",
      value: "flow",
    },
    {
      description:
        "Use live resource-level Notice subscription evidence after a route is selected.",
      label: "Participants",
      value: "participants",
    },
    {
      description: "Use Notice overview counters and resource-level bounded event timelines.",
      label: "Failures",
      value: "failures",
    },
    {
      description:
        "Search broker-local Notice delivery-counter evidence by Route Family and scope.",
      label: "Delivery search",
      value: "search",
    },
  ],
  renderSearchResult(result) {
    return <NoticeObservationPanel result={result as NoticeDeliveryObservationList} />;
  },
  resourceLabel: "Notice route",
  resourceNoun(count) {
    return count === 1 ? "notice route" : "notice routes";
  },
  resourceScopeLabel: "Notice route scope",
  routeFamilyRequiredLabel: "Route Family required",
  search(params) {
    return noticeService.searchDeliveries(params);
  },
  searchErrorTitle: "Unable to search NOTICE evidence",
  searchReadyLabel: "Notice delivery evidence",
};

const rpcAdapter: CommunicationModeAdapter = {
  actionLabel: "Open operations",
  allResourcesLabel: "All RPC routes",
  domain: "rpc",
  emptyResourceTitle: "No matching RPC routes",
  exactActionLabel: "Open exact RPC route",
  flowStages(stats) {
    const rpcStats = stats as RpcCommunicationStats;
    const failures = rpcFailures(rpcStats);

    return [
      {
        caption: "Live request/response throughput across RPC.",
        label: "Calls",
        value: formatRate(rpcStats.operationsPerSecond),
      },
      {
        caption: "Requests waiting in broker-local pending state.",
        label: "Pending",
        tone: rpcStats.requestsPending > rpcStats.workersRegistered ? "warning" : undefined,
        value: rpcStats.requestsPending,
      },
      {
        caption: "Live workers currently registered to handle calls.",
        label: "Workers",
        value: rpcStats.workersRegistered,
      },
      {
        caption: "Failures, timeouts, closed callers, and missing pending responses.",
        label: "Failure signals",
        tone: failures > 0 ? "danger" : undefined,
        value: failures,
      },
    ];
  },
  liveDataLabel: "Live admin data",
  loadErrorTitle: "Unable to load RPC flow resources",
  modeDetailDescription(mode) {
    if (mode === "participants") {
      return "Select an RPC route to inspect operations, workers for the first operation, and broker-local pending requests exposed by the current resource API.";
    }

    if (mode === "failures") {
      return "RPC failure tracing starts with pending, timeout, missing-pending, and closed-caller counters, then narrows through resource-level bounded event timelines.";
    }

    return "Search broker-local RPC worker registrations and pending calls by selected Route Family and route scope.";
  },
  modeDetailTitle(mode) {
    if (mode === "participants") return "Operation participants";
    if (mode === "failures") return "Failure trace evidence";
    return "Call search";
  },
  modeOptions: [
    {
      description: "Use live RPC route, participant, rate, and failure counters.",
      label: "Flow graph",
      value: "flow",
    },
    {
      description:
        "Use live resource-level RPC operation, worker, and pending request evidence after a route is selected.",
      label: "Participants",
      value: "participants",
    },
    {
      description: "Use RPC overview counters and resource-level bounded event timelines.",
      label: "Failures",
      value: "failures",
    },
    {
      description:
        "Search broker-local RPC worker and pending-call evidence by Route Family and scope.",
      label: "Call search",
      value: "search",
    },
  ],
  renderSearchResult(result) {
    return <RpcObservationPanel result={result as RpcCallObservationList} />;
  },
  resourceLabel: "RPC route",
  resourceNoun(count) {
    return count === 1 ? "RPC route" : "RPC routes";
  },
  resourceScopeLabel: "RPC route scope",
  routeFamilyRequiredLabel: "Route Family required",
  search(params) {
    return rpcService.searchCalls(params);
  },
  searchErrorTitle: "Unable to search RPC evidence",
  searchReadyLabel: "RPC call evidence",
};

export const communicationModeAdapters = {
  notice: noticeAdapter,
  rpc: rpcAdapter,
} satisfies Record<CommunicationDomain, CommunicationModeAdapter>;
