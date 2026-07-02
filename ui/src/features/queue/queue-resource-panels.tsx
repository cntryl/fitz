import { For } from "@askrjs/askr/control";
import { VirtualTable, type VirtualTableColumn } from "@askrjs/ui";
import {
  Badge,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  Inline,
  Stack,
} from "@askrjs/themes/components";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import { QueryEmptyState } from "@/components/shared/query-state";
import QueueDeadLetterTable from "@/components/shared/queue-dead-letter-table";
import QueueInflightTable from "@/components/shared/queue-inflight-table";
import type { DeadLetterMessage } from "@/features/queue/queue-models";
import type {
  QueueInflightMessage,
  QueueResourceDetail,
  QueueResourceTimeline,
  QueueResourceTimelineEvent,
} from "@/features/queue/queue-resource-models";
import {
  formatRate,
  formatStatus,
  formatTimelineContext,
  formatTimelineKind,
  humanizeSeconds,
} from "./queue-resource-presenters";

export function QueueResourceCurrentValuesPanel({ detail }: { detail: QueueResourceDetail }) {
  return (
    <DomainMetricTable
      title="Current values"
      description="Point-in-time durable backlog and live reservation counters for this scope."
      metrics={[
        { label: "Ready", value: detail.messagesReady },
        {
          label: "Delayed",
          value: detail.messagesDelayed,
          caption: detail.messagesDelayed > 0 ? "Delayed messages visible" : undefined,
        },
        { label: "Inflight", value: detail.messagesInflight },
        { label: "Subscriptions", value: detail.subscriptionsActive },
        {
          label: "Dead letters",
          value: detail.messagesDeadLettered,
          caption: detail.messagesDeadLettered > 0 ? "Needs action before work resumes" : undefined,
        },
        { label: "Total messages", value: detail.messagesTotal },
        { label: "In / sec", value: formatRate(detail.inRatePerSecond) },
        { label: "Out / sec", value: formatRate(detail.outRatePerSecond) },
        { label: "Status", value: formatStatus(detail.status) },
        {
          label: "Oldest backlog",
          value: humanizeSeconds(detail.oldestBacklogAgeSeconds),
          caption: "Ready and delayed messages",
        },
        {
          label: "Oldest age",
          value: humanizeSeconds(detail.oldestMessageAgeSeconds),
          caption: "Live snapshot",
        },
      ]}
    />
  );
}

export function QueueResourceInflightPanel({ messages }: { messages: QueueInflightMessage[] }) {
  return (
    <Card variant="raised">
      <CardHeader>
        <Inline justify="between" gap="3" align="start" wrap="wrap">
          <Stack gap="1">
            <CardTitle>Inflight</CardTitle>
            <CardDescription>Live reservations currently owned by queue sessions.</CardDescription>
          </Stack>
          <Badge variant="info">{messages.length} entries</Badge>
        </Inline>
      </CardHeader>

      <CardContent>
        {messages.length === 0 ? (
          <QueryEmptyState description="No inflight messages are visible for this resource." />
        ) : (
          <QueueInflightTable messages={messages} />
        )}
      </CardContent>
    </Card>
  );
}

export function QueueResourceDeadLettersPanel({
  messages,
  onPurge,
  onReplay,
  pendingAction,
  pendingMessageId,
}: {
  messages: DeadLetterMessage[];
  onPurge: (message: DeadLetterMessage) => void;
  onReplay: (message: DeadLetterMessage) => void;
  pendingAction: "replay" | "purge" | null;
  pendingMessageId: number | null;
}) {
  return (
    <Card variant="raised">
      <CardHeader>
        <Inline justify="between" gap="3" align="start" wrap="wrap">
          <Stack gap="1">
            <CardTitle>Dead letters</CardTitle>
            <CardDescription>
              Durable messages awaiting explicit replay or purge decisions.
            </CardDescription>
          </Stack>
          <Badge variant={messages.length > 0 ? "warning" : "success"}>
            {messages.length} messages
          </Badge>
        </Inline>
      </CardHeader>

      <CardContent>
        {messages.length === 0 ? (
          <QueryEmptyState description="No dead-letter messages are visible for this resource. No replay or purge action is needed." />
        ) : (
          <QueueDeadLetterTable
            messages={messages}
            onReplay={onReplay}
            onPurge={onPurge}
            pendingAction={pendingAction}
            pendingMessageId={pendingMessageId}
          />
        )}
      </CardContent>
    </Card>
  );
}

const timelineColumns: readonly VirtualTableColumn<QueueResourceTimelineEvent>[] = [
  {
    id: "kind",
    header: "Kind",
    width: "14%",
    cellComponent: ({ row }) => (
      <span class="domain-table-cell-truncate" title={formatTimelineKind(row.kind)}>
        {formatTimelineKind(row.kind)}
      </span>
    ),
  },
  {
    id: "summary",
    header: "Summary",
    width: "30%",
    cellComponent: ({ row }) => (
      <span class="domain-table-cell-truncate" title={row.summary}>
        {row.summary}
      </span>
    ),
  },
  {
    id: "observed",
    header: "Observed",
    width: "18%",
    cellComponent: ({ row }) => (
      <span class="domain-table-cell-truncate" title={row.observedAt}>
        {row.observedAt}
      </span>
    ),
  },
  {
    id: "age",
    header: "Age",
    width: "12%",
    cellComponent: ({ row }) => (
      <span>{row.ageSeconds == null ? "Unknown" : humanizeSeconds(row.ageSeconds)}</span>
    ),
  },
  {
    id: "context",
    header: "Context",
    width: "26%",
    cellComponent: ({ row }) => {
      const timelineContext = formatTimelineContext(row);

      return (
        <div class="queue-timeline-context">
          {timelineContext.length > 0 ? (
            <For each={timelineContext.slice(0, 2)} by={(line) => line}>
              {(line) => <span title={line}>{line}</span>}
            </For>
          ) : (
            <span>Context unavailable</span>
          )}
        </div>
      );
    },
  },
];

export function QueueResourceTimelinePanel({ timeline }: { timeline: QueueResourceTimeline }) {
  return (
    <Card variant="raised">
      <CardHeader>
        <Inline justify="between" gap="3" align="start" wrap="wrap">
          <Stack gap="1">
            <CardTitle>Timeline</CardTitle>
            <CardDescription>
              {timeline.derived
                ? "Derived transition evidence built from surrounding queue state."
                : "Broker-observed queue transitions for this resource."}
            </CardDescription>
          </Stack>
          <Badge variant={timeline.derived ? "info" : "success"}>
            {timeline.derived ? "Derived" : "Live"}
          </Badge>
        </Inline>
      </CardHeader>

      <CardContent>
        {timeline.events.length === 0 ? (
          <QueryEmptyState
            title={timeline.derived ? "Derived timeline" : "Live timeline"}
            description="No recent transitions are visible for this resource. Use current metrics for context."
          />
        ) : (
          <VirtualTable<QueueResourceTimelineEvent>
            aria-label="Queue resource timeline"
            class="queue-resource-virtual-table"
            columns={timelineColumns}
            getKey={(event) => `${event.observedAt}:${event.summary}`}
            headerHeight={44}
            overscan={8}
            rowHeight={56}
            rows={timeline.events}
            style={{
              height: `${Math.min(456, Math.max(156, 44 + timeline.events.length * 56))}px`,
            }}
          />
        )}
      </CardContent>
    </Card>
  );
}
