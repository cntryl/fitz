import { For, Show } from "@askrjs/askr/control";
import {
  Badge,
  Block,
  Item,
  ItemActions,
  ItemContent,
  ItemFooter,
  ItemGroup,
  ItemTitle,
  Text,
} from "@askrjs/themes/components";
import DomainDataSection from "@/components/shared/domain-data-section";
import DomainFacts from "@/components/shared/domain-facts";
import { QueryCompactEmptyState } from "@/components/shared/query-state";
import QueueDeadLetterTable from "@/components/shared/queue-dead-letter-table";
import QueueInflightTable from "@/components/shared/queue-inflight-table";
import type { DeadLetterMessage } from "@/features/queue/queue-models";
import type {
  QueueInflightMessage,
  QueueResourceDetail,
  QueueResourceTimeline,
  QueueResourceTimelineEvent,
} from "@/features/queue/queue-resource-models";
import { formatNumber, formatTimestamp } from "@/shared/format";
import {
  formatRate,
  formatTimelineContext,
  formatTimelineKind,
  humanizeSeconds,
} from "./queue-resource-presenters";

export function QueueResourceCurrentValuesPanel({ detail }: { detail: QueueResourceDetail }) {
  return (
    <DomainFacts
      id="queue-current-values"
      title="Queue state"
      items={[
        { label: "Ready", value: formatNumber(detail.messagesReady) },
        {
          label: "Oldest ready",
          value: detail.messagesReady > 0 ? humanizeSeconds(detail.oldestMessageAgeSeconds) : "--",
        },
        { label: "Delayed", value: formatNumber(detail.messagesDelayed) },
        { label: "In flight", value: formatNumber(detail.messagesInflight) },
        { label: "Dead-lettered", value: formatNumber(detail.messagesDeadLettered) },
        { label: "Enqueued/s (60s)", value: formatRate(detail.inRatePerSecond) },
        { label: "Completed/s (60s)", value: formatRate(detail.outRatePerSecond) },
        {
          label: "Live subscriptions",
          value: formatNumber(detail.subscriptionsActive),
          title:
            "Live route registrations; these do not prove durable consumer identity or capacity.",
        },
      ]}
    />
  );
}

export function QueueResourceInflightPanel({ messages }: { messages: QueueInflightMessage[] }) {
  return (
    <DomainDataSection
      id="queue-inflight"
      title="Inflight"
      actions={
        <Badge variant="info">
          {messages.length} {messages.length === 1 ? "entry" : "entries"}
        </Badge>
      }
    >
      {messages.length === 0 ? (
        <QueryCompactEmptyState
          title="No inflight messages"
          description="No inflight messages are visible for this resource."
        />
      ) : (
        <Block direction="column" gap="xs">
          <QueueInflightTable messages={messages} />
        </Block>
      )}
    </DomainDataSection>
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
    <DomainDataSection
      id="queue-dead-letters"
      title="Dead letters"
      actions={
        <Badge variant={messages.length > 0 ? "warning" : "success"}>
          {messages.length} {messages.length === 1 ? "message" : "messages"} returned
        </Badge>
      }
    >
      {messages.length === 0 ? (
        <QueryCompactEmptyState
          title="No dead letters"
          description="No dead-letter messages are visible for this resource. No replay or purge action is needed."
        />
      ) : (
        <Block direction="column" gap="xs">
          <QueueDeadLetterTable
            messages={messages}
            onReplay={onReplay}
            onPurge={onPurge}
            pendingAction={pendingAction}
            pendingMessageId={pendingMessageId}
          />
        </Block>
      )}
    </DomainDataSection>
  );
}

function timelineKindVariant(kind: QueueResourceTimelineEvent["kind"]) {
  if (kind === "failure") return "danger" as const;
  if (kind === "retry") return "warning" as const;
  return "outline" as const;
}

function QueueTimelineItem({ event }: { event: QueueResourceTimelineEvent }) {
  const context = formatTimelineContext(event);

  return (
    <Item as="li" class="queue-timeline-item" size="sm">
      <ItemContent>
        <ItemTitle>
          <Block direction="row" align="center" gap="sm" wrap={true}>
            <Badge variant={timelineKindVariant(event.kind)}>
              {formatTimelineKind(event.kind)}
            </Badge>
            <Text as="strong" weight="semibold" wrap="anywhere">
              {event.summary}
            </Text>
          </Block>
        </ItemTitle>
        <ItemFooter class="queue-timeline-metadata">
          <For each={context} by={(line) => line}>
            {(line) => (
              <Text as="span" tone="muted" size="sm" wrap="anywhere">
                {line}
              </Text>
            )}
          </For>
        </ItemFooter>
      </ItemContent>
      <ItemActions class="queue-timeline-time">
        <Block
          direction={{ base: "row", sm: "column" }}
          justify={{ base: "between", sm: "start" }}
          gap="xs"
          width="full"
        >
          <Text as="span" tone="muted" size="sm">
            <time dateTime={event.observedAt}>{formatTimestamp(event.observedAt)}</time>
          </Text>
          <Text as="span" tone="muted" size="sm">
            {event.ageSeconds == null ? "Age unknown" : `${humanizeSeconds(event.ageSeconds)} ago`}
          </Text>
        </Block>
      </ItemActions>
    </Item>
  );
}

export function QueueResourceTimelinePanel({ timeline }: { timeline: QueueResourceTimeline }) {
  return (
    <DomainDataSection
      id="queue-timeline"
      title={timeline.derived ? "Derived transitions" : "Observed transitions"}
      actions={
        <Badge variant={timeline.derived ? "info" : "success"}>
          {timeline.derived ? "Derived" : "Observed"}
        </Badge>
      }
    >
      <Show
        when={timeline.events.length > 0}
        fallback={
          <QueryCompactEmptyState
            title="No transitions"
            description="No transition evidence is available for this resource."
          />
        }
      >
        <ItemGroup
          as="ul"
          class="domain-divided-list queue-timeline-list"
          aria-label="Queue resource timeline"
        >
          <For
            each={timeline.events}
            by={(event) =>
              `${event.observedAt}:${event.kind}:${event.messageId ?? "none"}:${event.summary}`
            }
          >
            {(event) => <QueueTimelineItem event={event} />}
          </For>
        </ItemGroup>
      </Show>
    </DomainDataSection>
  );
}
