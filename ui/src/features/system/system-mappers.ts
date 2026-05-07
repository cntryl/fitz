import type { GlobalStats, HealthStatus } from "@/adapters";
import type { SystemOverview } from "./system-models";

function normalizeMetricsValue(value: unknown) {
  if (typeof value === "string") {
    return value;
  }

  if (value == null) {
    return "";
  }

  if (typeof value === "number" || typeof value === "boolean" || typeof value === "bigint") {
    return `${value}`;
  }

  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return "";
  }
}

export function mapSystemOverview(
  stats: GlobalStats,
  health: HealthStatus,
  metricsValue: unknown,
): SystemOverview {
  const raw = normalizeMetricsValue(metricsValue);
  const lines = raw
    .split(/\r?\n/)
    .map((line) => line.trimEnd())
    .filter((line) => line.length > 0);

  return {
    broker: {
      connections: stats.broker.connections,
      messagesPerSecond: stats.broker.messages_per_second,
      realms: stats.broker.realms,
      sessions: stats.broker.sessions,
      uptimeSeconds: stats.broker.uptime_seconds,
    },
    domains: {
      kv: {
        keysTotal: stats.domains.kv.keys_total,
        operationsPerSecond: stats.domains.kv.operations_per_second,
        transactionsActive: stats.domains.kv.transactions_active,
      },
      lease: {
        leasesActive: stats.domains.lease.leases_active,
        operationsPerSecond: stats.domains.lease.operations_per_second,
      },
      notice: {
        publishesPerSecond: stats.domains.notice.publishes_per_second,
        subscriptionsActive: stats.domains.notice.subscriptions_active,
      },
      queue: {
        inflightActive: stats.domains.queue.inflight_active,
        messagesDeadLettered: stats.domains.queue.messages_dead_lettered,
        messagesDelayed: stats.domains.queue.messages_delayed,
        messagesPending: stats.domains.queue.messages_pending,
        messagesReady: stats.domains.queue.messages_ready,
        operationsPerSecond: stats.domains.queue.operations_per_second,
      },
      rpc: {
        operationsPerSecond: stats.domains.rpc.operations_per_second,
        requestsPending: stats.domains.rpc.requests_pending,
        workersRegistered: stats.domains.rpc.workers_registered,
      },
      schedule: {
        ackFailuresTotal: stats.domains.schedule.ack_failures_total,
        executionsPerMinute: stats.domains.schedule.executions_per_minute,
        notifyFailuresTotal: stats.domains.schedule.notify_failures_total,
        overdueNormalizationsTotal: stats.domains.schedule.overdue_normalizations_total,
        pendingFireClaims: stats.domains.schedule.pending_fire_claims,
        schedulesActive: stats.domains.schedule.schedules_active,
        subscriptionsActive: stats.domains.schedule.subscriptions_active,
      },
      stream: {
        eventsTotal: stats.domains.stream.events_total,
        operationsPerSecond: stats.domains.stream.operations_per_second,
        streamsActive: stats.domains.stream.streams_active,
        subscriptionsActive: stats.domains.stream.subscriptions_active,
      },
    },
    healthStatus: health.status,
    metrics: {
      raw,
      lines: lines.slice(0, 8),
      lineCount: lines.length,
    },
  };
}
