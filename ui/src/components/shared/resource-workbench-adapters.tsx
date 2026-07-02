import {
  Button,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@askrjs/themes/components";
import type {
  ResourceDetail,
  ResourceMetric,
  ResourceTimelineEvent,
} from "@/features/resource/resource-models";

export type ResourceArchetypeConfig = {
  actionLabel: string;
  actionTitle: string;
  diagnosticsDescription: string;
  evidenceTitle: string;
  failureTitle: string;
  primaryDescription: string;
  primaryTitle: string;
  timelineTitle: string;
  title: string;
};

export interface ResourceWorkbenchAdapter {
  copy: ResourceArchetypeConfig;
  domain: ResourceDetail["domain"];
  isFailureEvent: (event: ResourceTimelineEvent) => boolean;
  isFailureMetric: (metric: ResourceMetric) => boolean;
  renderOperationsPanel?: (detail: ResourceDetail) => JSX.Element | null;
}

function includesAny(value: string, words: readonly string[]) {
  const normalized = value.toLowerCase();

  return words.some((word) => normalized.includes(word));
}

function failureClassifier(words: readonly string[]) {
  return {
    isFailureMetric: (metric: ResourceMetric) => includesAny(metric.label, words),
    isFailureEvent: (event: ResourceTimelineEvent) =>
      includesAny(`${event.kind} ${event.summary}`, words),
  };
}

const genericFailureWords = [
  "fail",
  "reject",
  "timeout",
  "drop",
  "dead",
  "conflict",
  "invalid",
  "rollback",
  "blocked",
] as const;

function streamOperationsPanel() {
  return (
    <Card padding="sm" variant="default">
      <CardHeader>
        <CardTitle>Replay controls</CardTitle>
        <CardDescription>Replay remains tied to explicit Stream API support.</CardDescription>
      </CardHeader>
      <CardContent>
        <Button variant="outline" disabled>
          Replay event range
        </Button>
      </CardContent>
    </Card>
  );
}

function kvOperationsPanel() {
  return (
    <Card padding="sm" variant="default">
      <CardHeader>
        <CardTitle>State lookup</CardTitle>
        <CardDescription>
          Key lookup and prefix search require a dedicated KV admin contract.
        </CardDescription>
      </CardHeader>
      <CardContent>
        <Button variant="outline" disabled>
          Query keys
        </Button>
      </CardContent>
    </Card>
  );
}

const kvResourceWorkbenchAdapter: ResourceWorkbenchAdapter = {
  ...failureClassifier(genericFailureWords),
  domain: "kv",
  copy: {
    actionLabel: "State Explorer",
    actionTitle: "Query workspace",
    diagnosticsDescription: "Transaction pressure, current values, and raw resource payload.",
    evidenceTitle: "Results and details",
    failureTitle: "State anomalies",
    primaryDescription: "Resource-level current authoritative state from the existing admin API.",
    primaryTitle: "State query",
    timelineTitle: "State timeline",
    title: "KV State Explorer",
  },
  renderOperationsPanel: kvOperationsPanel,
};

const leaseResourceWorkbenchAdapter: ResourceWorkbenchAdapter = {
  ...failureClassifier(genericFailureWords),
  domain: "lease",
  copy: {
    actionLabel: "Ownership Console",
    actionTitle: "Ownership",
    diagnosticsDescription: "Broker-local lease health and contention evidence.",
    evidenceTitle: "Contention",
    failureTitle: "Ownership conflicts",
    primaryDescription: "Ephemeral owner, waiter, and lease coordination signals for this scope.",
    primaryTitle: "Current ownership",
    timelineTitle: "Ownership history",
    title: "Lease Ownership Console",
  },
};

const noticeResourceWorkbenchAdapter: ResourceWorkbenchAdapter = {
  ...failureClassifier(genericFailureWords),
  domain: "notice",
  copy: {
    actionLabel: "Communication Flow",
    actionTitle: "Flow graph",
    diagnosticsDescription:
      "Live fanout pressure, participants, failures, and raw broker evidence.",
    evidenceTitle: "Participants",
    failureTitle: "Delivery failures",
    primaryDescription:
      "Live Notice route, subscription, and delivery signals for connected participants.",
    primaryTitle: "Notice flow",
    timelineTitle: "Delivery trace",
    title: "Notice Communication Flow",
  },
};

const rpcResourceWorkbenchAdapter: ResourceWorkbenchAdapter = {
  ...failureClassifier(genericFailureWords),
  domain: "rpc",
  copy: {
    actionLabel: "Communication Flow",
    actionTitle: "Flow graph",
    diagnosticsDescription:
      "Live request/response participants, failures, and pending-call evidence.",
    evidenceTitle: "Participants",
    failureTitle: "Call failures",
    primaryDescription:
      "Live RPC operations, workers, and pending request signals for this resource.",
    primaryTitle: "RPC flow",
    timelineTitle: "Call trace",
    title: "RPC Communication Flow",
  },
};

const scheduleResourceWorkbenchAdapter: ResourceWorkbenchAdapter = {
  ...failureClassifier(genericFailureWords),
  domain: "schedule",
  copy: {
    actionLabel: "Time Planner",
    actionTitle: "Timeline",
    diagnosticsDescription: "Durable timing intent, execution pressure, and handoff diagnostics.",
    evidenceTitle: "Executions",
    failureTitle: "Missed or failed execution",
    primaryDescription:
      "Future timing intent and recent execution evidence for this schedule resource.",
    primaryTitle: "Execution plan",
    timelineTitle: "Execution timeline",
    title: "Schedule Time Planner",
  },
};

const streamResourceWorkbenchAdapter: ResourceWorkbenchAdapter = {
  ...failureClassifier(genericFailureWords),
  domain: "stream",
  copy: {
    actionLabel: "History Explorer",
    actionTitle: "Event explorer",
    diagnosticsDescription:
      "Durable stream indicators, consumers, replay context, and raw payload.",
    evidenceTitle: "Consumers",
    failureTitle: "Replay risks",
    primaryDescription: "Durable history indicators and recent stream events for this scope.",
    primaryTitle: "Event history",
    timelineTitle: "Event timeline",
    title: "Stream History Explorer",
  },
  renderOperationsPanel: streamOperationsPanel,
};

export const resourceWorkbenchAdapters = {
  kv: kvResourceWorkbenchAdapter,
  lease: leaseResourceWorkbenchAdapter,
  notice: noticeResourceWorkbenchAdapter,
  rpc: rpcResourceWorkbenchAdapter,
  schedule: scheduleResourceWorkbenchAdapter,
  stream: streamResourceWorkbenchAdapter,
} satisfies Record<ResourceDetail["domain"], ResourceWorkbenchAdapter>;

export function getResourceWorkbenchAdapter(
  domain: ResourceDetail["domain"],
): ResourceWorkbenchAdapter {
  return resourceWorkbenchAdapters[domain];
}
