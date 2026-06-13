import type { MessagingTopologyOverview, TopologyDomain, TopologyLane, TopologyState, TrendDirection } from "./topology-models";
import { formatNumber } from "@/shared/format";
import { isTopologyDomain, topologyScopeHref } from "./topology-mappers";

export interface BehaviorRow {
  lane: TopologyLane;
  primary: string;
  secondary: string;
}

export interface BehaviorGroup {
  description: string;
  rows: BehaviorRow[];
  title: string;
}

export function humanizeSeconds(seconds: number) {
  if (seconds < 60) return `${seconds}s`;

  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m`;

  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h`;

  return `${Math.floor(hours / 24)}d`;
}

export function formatTopologyRate(value: number) {
  return value.toFixed(2);
}

export function incidentSeverity(topology: MessagingTopologyOverview) {
  return topology.diagnostics.incident_summary?.severity ?? "informational";
}

export function incidentTitle(topology: MessagingTopologyOverview) {
  return topology.diagnostics.incident_summary?.title ?? "No incident detected";
}

export function incidentDescription(topology: MessagingTopologyOverview) {
  return topology.diagnostics.incident_summary?.explanation ?? "No active pressure detected.";
}

export function badgeVariant(value: string) {
  if (value === "critical" || value === "high" || value === "blocked" || value === "error") {
    return "danger";
  }
  if (value === "medium" || value === "low" || value === "pressure" || value === "warning") {
    return "warning";
  }
  if (value === "informational" || value === "flowing") return "success";
  return "info";
}

export function stateLabel(state: TopologyState) {
  if (state === "blocked") return "Blocked";
  if (state === "pressure") return "Pressure";
  if (state === "flowing") return "Flowing";
  return "Quiet";
}

export function trendLabel(direction: TrendDirection) {
  if (direction === "rising") return "Rising";
  if (direction === "falling") return "Falling";
  return "Stable";
}

function counterValue(lane: TopologyLane, key: string) {
  return lane.counters.find((counter) => counter.key === key)?.value ?? 0;
}

function laneById(topology: MessagingTopologyOverview, id: TopologyDomain) {
  return topology.lanes.find((lane) => lane.id === id)!;
}

export function topologyBehaviorGroups(topology: MessagingTopologyOverview): BehaviorGroup[] {
  const queue = laneById(topology, "queue");
  const schedule = laneById(topology, "schedule");
  const rpc = laneById(topology, "rpc");
  const notice = laneById(topology, "notice");
  const lease = laneById(topology, "lease");
  const kv = laneById(topology, "kv");
  const stream = laneById(topology, "stream");

  return [
    {
      title: "Work backlog",
      description: "Durable timing and queued work pressure.",
      rows: [
        {
          lane: queue,
          primary: `Ready ${formatNumber(counterValue(queue, "ready"))}`,
          secondary: `Inflight ${formatNumber(counterValue(queue, "inflight"))} / dead letters ${formatNumber(
            counterValue(queue, "dead_letters"),
          )}`,
        },
        {
          lane: schedule,
          primary: `Schedules ${formatNumber(counterValue(schedule, "schedules"))}`,
          secondary: `Claims ${formatNumber(
            counterValue(schedule, "pending_claims"),
          )} / subscriptions ${formatNumber(counterValue(schedule, "subscriptions"))}`,
        },
      ],
    },
    {
      title: "Live paths",
      description: "Current-process delivery, ownership, and request/response activity.",
      rows: [
        {
          lane: rpc,
          primary: `Pending ${formatNumber(counterValue(rpc, "pending"))}`,
          secondary: `Workers ${formatNumber(counterValue(rpc, "workers"))} / ops per sec ${formatTopologyRate(
            rpc.activityPerSecond,
          )}`,
        },
        {
          lane: notice,
          primary: `Subscriptions ${formatNumber(counterValue(notice, "subscriptions"))}`,
          secondary: `Publishes per sec ${formatTopologyRate(
            notice.activityPerSecond,
          )} / drops ${formatNumber(counterValue(notice, "delivery_drops"))}`,
        },
        {
          lane: lease,
          primary: `Active ${formatNumber(counterValue(lease, "leases"))}`,
          secondary: `Waiters ${formatNumber(counterValue(lease, "waiters"))} / oldest ${humanizeSeconds(
            counterValue(lease, "oldest_lease_age_seconds"),
          )}`,
        },
      ],
    },
    {
      title: "Durable state/history",
      description: "Committed state and history surfaces, with live activity counters.",
      rows: [
        {
          lane: kv,
          primary: `Keys ${formatNumber(counterValue(kv, "keys"))}`,
          secondary: `Transactions ${formatNumber(
            counterValue(kv, "transactions"),
          )} / ops per sec ${formatTopologyRate(kv.activityPerSecond)}`,
        },
        {
          lane: stream,
          primary: `Events ${formatNumber(counterValue(stream, "events"))}`,
          secondary: `Streams ${formatNumber(
            counterValue(stream, "streams"),
          )} / subscriptions ${formatNumber(counterValue(stream, "subscriptions"))}`,
        },
      ],
    },
  ];
}

export function scopeText(scope: { domain?: string; realm?: string | null; area?: string | null; resource?: string | null }) {
  return [scope.domain, scope.realm, scope.area, scope.resource].filter(Boolean).join(" / ");
}

export function hotspotHref(hotspot: MessagingTopologyOverview["diagnostics"]["hotspots"][number]) {
  if (!isTopologyDomain(hotspot.domain)) {
    return null;
  }

  return topologyScopeHref(hotspot.domain, {
    area: hotspot.area ?? undefined,
    realm: hotspot.realm ?? undefined,
    resource: hotspot.resource ?? undefined,
  });
}

export function consumerTotal(topology: MessagingTopologyOverview) {
  return topology.lanes.reduce((sum, lane) => sum + lane.consumers + lane.observers, 0);
}
