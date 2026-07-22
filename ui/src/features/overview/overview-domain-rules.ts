import type { DiagnosticSeverity } from "@/adapters";
import type { SystemDomainStatsSummary } from "@/features/system/system-models";
import { formatCount, formatNumber } from "@/shared/format";
import { domainSegments, type DomainSegment } from "@/shared/navigation/domains";

export interface OverviewDomainIssueDescriptor {
  description: string;
  id: string;
  severity: DiagnosticSeverity;
  title: string;
}

export interface OverviewDomainIssue extends OverviewDomainIssueDescriptor {
  domain: DomainSegment;
}

export interface OverviewDomainRule {
  domain: DomainSegment;
  issues(domains: SystemDomainStatsSummary): OverviewDomainIssueDescriptor[];
  signal(domains: SystemDomainStatsSummary): string;
}

const queueRule: OverviewDomainRule = {
  domain: "queue",
  issues(domains) {
    const queueDeadLetters = domains.queue.messagesDeadLettered;

    return queueDeadLetters > 0
      ? [
          {
            description: `${formatCount(queueDeadLetters, "message")} ${
              queueDeadLetters === 1 ? "is" : "are"
            } in dead-letter state.`,
            id: "queue-dead-letters",
            severity: "high",
            title: "Queue dead letters",
          },
        ]
      : [];
  },
  signal(domains) {
    return `${formatNumber(domains.queue.messagesReady)} ready / ${formatNumber(
      domains.queue.inflightActive,
    )} inflight`;
  },
};

// Process-lifetime counters stay visible in domain and Metrics detail, but a non-zero
// historical total does not describe a currently active incident. Current gauges and
// broker-generated incident, hotspot, and lane diagnostics own Overview issue status.

const rpcRule: OverviewDomainRule = {
  domain: "rpc",
  issues() {
    return [];
  },
  signal(domains) {
    return `${formatNumber(domains.rpc.requestsPending)} pending / ${formatNumber(
      domains.rpc.workersRegistered,
    )} workers`;
  },
};

const scheduleRule: OverviewDomainRule = {
  domain: "schedule",
  issues(domains) {
    if (domains.schedule.pendingFireClaims > 0) {
      return [
        {
          description: `${formatCount(
            domains.schedule.pendingFireClaims,
            "pending fire claim",
          )} ${domains.schedule.pendingFireClaims === 1 ? "needs" : "need"} handoff confirmation.`,
          id: "schedule-pending-claims",
          severity: "medium",
          title: "Schedule pending claims",
        },
      ];
    }

    return [];
  },
  signal(domains) {
    return `${formatNumber(domains.schedule.schedulesActive)} schedules / ${formatNumber(
      domains.schedule.pendingFireClaims,
    )} claims`;
  },
};

const leaseRule: OverviewDomainRule = {
  domain: "lease",
  issues(domains) {
    return domains.lease.waiterDepth > 0
      ? [
          {
            description: `${formatCount(domains.lease.waiterDepth, "lease waiter")} ${
              domains.lease.waiterDepth === 1 ? "is" : "are"
            } currently queued.`,
            id: "lease-pressure",
            severity: "medium",
            title: "Lease contention",
          },
        ]
      : [];
  },
  signal(domains) {
    return `${formatNumber(domains.lease.leasesActive)} leases / ${formatNumber(
      domains.lease.waiterDepth,
    )} waiters`;
  },
};

const noticeRule: OverviewDomainRule = {
  domain: "notice",
  issues() {
    return [];
  },
  signal(domains) {
    return `${formatNumber(domains.notice.subscriptionsActive)} subscriptions`;
  },
};

const streamRule: OverviewDomainRule = {
  domain: "stream",
  issues() {
    return [];
  },
  signal(domains) {
    return `${formatNumber(domains.stream.streamsActive)} streams / ${formatNumber(
      domains.stream.eventsTotal,
    )} events`;
  },
};

const kvRule: OverviewDomainRule = {
  domain: "kv",
  issues() {
    return [];
  },
  signal(domains) {
    return `${formatNumber(domains.kv.keysTotal)} keys / ${formatNumber(
      domains.kv.transactionsActive,
    )} transactions`;
  },
};

export const overviewDomainRules = {
  kv: kvRule,
  lease: leaseRule,
  notice: noticeRule,
  queue: queueRule,
  rpc: rpcRule,
  schedule: scheduleRule,
  stream: streamRule,
} satisfies Record<DomainSegment, OverviewDomainRule>;

export function overviewDomainIssueDescriptors(
  domains: SystemDomainStatsSummary,
): OverviewDomainIssue[] {
  return domainSegments.flatMap((domain) =>
    overviewDomainRules[domain].issues(domains).map((issue) => ({ ...issue, domain })),
  );
}

export function overviewDomainSignal(
  domain: DomainSegment,
  domains: SystemDomainStatsSummary | undefined,
) {
  return domains ? overviewDomainRules[domain].signal(domains) : null;
}
