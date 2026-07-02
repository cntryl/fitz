import type { DiagnosticSeverity } from "@/adapters";
import type { SystemDomainStatsSummary } from "@/features/system/system-models";
import { formatNumber } from "@/shared/format";
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
            description: `${formatNumber(queueDeadLetters)} message(s) are in dead-letter state.`,
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

const rpcRule: OverviewDomainRule = {
  domain: "rpc",
  issues(domains) {
    const rpcFailures =
      domains.rpc.failureTotal +
      domains.rpc.requestTimeoutsTotal +
      domains.rpc.responsesMissingPendingTotal +
      domains.rpc.responsesDroppedClosedCallerTotal;

    return rpcFailures > 0
      ? [
          {
            description: `${formatNumber(
              rpcFailures,
            )} timeout, failure, or late-response signal(s) are active.`,
            id: "rpc-failures",
            severity: "high",
            title: "RPC failures",
          },
        ]
      : [];
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
    const scheduleFailures =
      domains.schedule.ackFailuresTotal +
      domains.schedule.notifyFailuresTotal +
      domains.schedule.createPersistenceFailuresTotal +
      domains.schedule.upsertPersistenceFailuresTotal +
      domains.schedule.cancelPersistenceFailuresTotal;

    if (scheduleFailures > 0) {
      return [
        {
          description: `${formatNumber(
            scheduleFailures,
          )} schedule persistence or delivery failure(s) are visible.`,
          id: "schedule-failures",
          severity: "high",
          title: "Schedule failures",
        },
      ];
    }

    if (domains.schedule.pendingFireClaims > 0) {
      return [
        {
          description: `${formatNumber(
            domains.schedule.pendingFireClaims,
          )} pending fire claim(s) need handoff confirmation.`,
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
    const leasePressure =
      domains.lease.failureTotal + domains.lease.acquireTimeoutsTotal + domains.lease.waiterDepth;

    return leasePressure > 0
      ? [
          {
            description: `${formatNumber(
              leasePressure,
            )} lease failure, timeout, or waiter signal(s) are active.`,
            id: "lease-pressure",
            severity:
              domains.lease.failureTotal > 0 || domains.lease.acquireTimeoutsTotal > 0
                ? "high"
                : "medium",
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
  issues(domains) {
    const noticePressure =
      domains.notice.failureTotal +
      domains.notice.deliveryDropsTotal +
      domains.notice.wildcardLimitRejectsTotal;

    return noticePressure > 0
      ? [
          {
            description: `${formatNumber(
              noticePressure,
            )} notice drop, failure, or wildcard reject signal(s) are active.`,
            id: "notice-pressure",
            severity: "medium",
            title: "Notice delivery pressure",
          },
        ]
      : [];
  },
  signal(domains) {
    return `${formatNumber(domains.notice.subscriptionsActive)} subscriptions`;
  },
};

const streamRule: OverviewDomainRule = {
  domain: "stream",
  issues(domains) {
    const streamPressure =
      domains.stream.failureTotal +
      domains.stream.appendConflictsTotal +
      domains.stream.notifyDropsTotal;

    return streamPressure > 0
      ? [
          {
            description: `${formatNumber(
              streamPressure,
            )} stream failure, append conflict, or notify-drop signal(s) are active.`,
            id: "stream-pressure",
            severity: "medium",
            title: "Stream pressure",
          },
        ]
      : [];
  },
  signal(domains) {
    return `${formatNumber(domains.stream.streamsActive)} streams / ${formatNumber(
      domains.stream.eventsTotal,
    )} events`;
  },
};

const kvRule: OverviewDomainRule = {
  domain: "kv",
  issues(domains) {
    const kvPressure = domains.kv.commitsFailedTotal + domains.kv.invalidTransactionRejectsTotal;

    return kvPressure > 0
      ? [
          {
            description: `${formatNumber(
              kvPressure,
            )} failed commit or invalid transaction reject signal(s) are active.`,
            id: "kv-pressure",
            severity: "medium",
            title: "KV write pressure",
          },
        ]
      : [];
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
