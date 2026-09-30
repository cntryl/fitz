import type { DiagnosticSeverity } from "@/adapters";
import type { SystemDomainStatsSummary } from "@/features/system/system-models";
import { formatCount } from "@/shared/format";
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
};

// Process-lifetime counters stay visible in domain and Metrics detail, but a non-zero
// historical total does not describe a currently active incident. Current gauges and
// broker-generated incident, hotspot, and lane diagnostics own Overview issue status.

const rpcRule: OverviewDomainRule = {
  domain: "rpc",
  issues() {
    return [];
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
};

const noticeRule: OverviewDomainRule = {
  domain: "notice",
  issues() {
    return [];
  },
};

const streamRule: OverviewDomainRule = {
  domain: "stream",
  issues() {
    return [];
  },
};

const kvRule: OverviewDomainRule = {
  domain: "kv",
  issues() {
    return [];
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
