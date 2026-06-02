import type { GlobalStats } from "@/adapters";
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

export function mapSystemOverview(stats: GlobalStats, metricsValue: unknown): SystemOverview {
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
    diagnostics: stats.diagnostics,
    domains: {
      kv: {
        commitsFailedTotal: stats.domains.kv.commits_failed_total,
        invalidTransactionRejectsTotal: stats.domains.kv.invalid_transaction_rejects_total,
        keysTotal: stats.domains.kv.keys_total,
        operationsPerSecond: stats.domains.kv.operations_per_second,
        rollbacksTotal: stats.domains.kv.rollbacks_total,
        transactionsActive: stats.domains.kv.transactions_active,
      },
      lease: {
        acquireTimeoutsTotal: stats.domains.lease.acquire_timeouts_total,
        failureTotal: stats.domains.lease.failure_total,
        forcedReleasesTotal: stats.domains.lease.forced_releases_total,
        invalidTokenRejectsTotal: stats.domains.lease.invalid_token_rejects_total,
        leasesActive: stats.domains.lease.leases_active,
        oldestLeaseAgeSeconds: stats.domains.lease.oldest_lease_age_seconds,
        operationsPerSecond: stats.domains.lease.operations_per_second,
        requestsTotal: stats.domains.lease.requests_total,
        successTotal: stats.domains.lease.success_total,
        waiterDepth: stats.domains.lease.waiter_depth,
      },
      notice: {
        deliveryDropsTotal: stats.domains.notice.delivery_drops_total,
        failureTotal: stats.domains.notice.failure_total,
        publishesPerSecond: stats.domains.notice.publishes_per_second,
        requestsTotal: stats.domains.notice.requests_total,
        successTotal: stats.domains.notice.success_total,
        subscriptionsActive: stats.domains.notice.subscriptions_active,
        unsubscribesTotal: stats.domains.notice.unsubscribes_total,
        wildcardLimitRejectsTotal: stats.domains.notice.wildcard_limit_rejects_total,
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
        acksRejectedWrongWorkerTotal: stats.domains.rpc.acks_rejected_wrong_worker_total,
        backpressureRejectsTotal: stats.domains.rpc.backpressure_rejects_total,
        duplicateCorrelationRejectsTotal: stats.domains.rpc.duplicate_correlation_rejects_total,
        failureTotal: stats.domains.rpc.failure_total,
        invalidSequenceErrorsDroppedTotal: stats.domains.rpc.invalid_sequence_errors_dropped_total,
        invalidSequenceErrorsForwardedTotal:
          stats.domains.rpc.invalid_sequence_errors_forwarded_total,
        invalidSequenceResponsesTotal: stats.domains.rpc.invalid_sequence_responses_total,
        operationsPerSecond: stats.domains.rpc.operations_per_second,
        requestTimeoutsTotal: stats.domains.rpc.request_timeouts_total,
        requestsPending: stats.domains.rpc.requests_pending,
        requestsTotal: stats.domains.rpc.requests_total,
        responsesDroppedClosedCallerTotal: stats.domains.rpc.responses_dropped_closed_caller_total,
        responsesMissingPendingTotal: stats.domains.rpc.responses_missing_pending_total,
        successTotal: stats.domains.rpc.success_total,
        wrongWorkerRejectsTotal: stats.domains.rpc.wrong_worker_rejects_total,
        workersRegistered: stats.domains.rpc.workers_registered,
      },
      schedule: {
        ackFailuresTotal: stats.domains.schedule.ack_failures_total,
        cancelPersistenceFailuresTotal: stats.domains.schedule.cancel_persistence_failures_total,
        createPersistenceFailuresTotal: stats.domains.schedule.create_persistence_failures_total,
        executionsPerMinute: stats.domains.schedule.executions_per_minute,
        notifyFailuresTotal: stats.domains.schedule.notify_failures_total,
        overdueNormalizationsTotal: stats.domains.schedule.overdue_normalizations_total,
        pendingFireClaims: stats.domains.schedule.pending_fire_claims,
        schedulesActive: stats.domains.schedule.schedules_active,
        subscriptionsActive: stats.domains.schedule.subscriptions_active,
        upsertPersistenceFailuresTotal: stats.domains.schedule.upsert_persistence_failures_total,
      },
      stream: {
        appendConflictsTotal: stats.domains.stream.append_conflicts_total,
        failureTotal: stats.domains.stream.failure_total,
        eventsTotal: stats.domains.stream.events_total,
        notifyDropsTotal: stats.domains.stream.notify_drops_total,
        operationsPerSecond: stats.domains.stream.operations_per_second,
        requestsTotal: stats.domains.stream.requests_total,
        successTotal: stats.domains.stream.success_total,
        streamsActive: stats.domains.stream.streams_active,
        subscriptionsActive: stats.domains.stream.subscriptions_active,
      },
    },
    metrics: {
      raw,
      lines: lines.slice(0, 8),
      lineCount: lines.length,
    },
  };
}
