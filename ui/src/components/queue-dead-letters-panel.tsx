import { Button } from "@askrjs/ui";
import { ActivityIcon, GaugeIcon } from "@askrjs/lucide";
import {
  createQueueDeadLettersQuery,
  type DeadLetterFilters,
  type QueueResourceRef,
} from "@/features/queue/queue-query";

export interface QueueDeadLettersPanelProps {
  resourceRef: QueueResourceRef;
  filters?: DeadLetterFilters;
}

function formatTimestamp(value: string) {
  const date = new Date(value);

  if (Number.isNaN(date.getTime())) {
    return value;
  }

  return date.toLocaleString();
}

function formatErrorValue(value: unknown) {
  if (typeof value === "string") {
    return value;
  }

  if (typeof value === "number" || typeof value === "boolean" || typeof value === "bigint") {
    return `${value}`;
  }

  if (value == null) {
    return "Unknown error";
  }

  try {
    return JSON.stringify(value);
  } catch {
    return "Unknown error";
  }
}

// Component boundary: consume query state only; no generated DTOs or FetchResponse.
export default function QueueDeadLettersPanel({
  resourceRef,
  filters = {},
}: QueueDeadLettersPanelProps) {
  const deadLetters = createQueueDeadLettersQuery(resourceRef, filters);
  const messages = deadLetters.data ?? [];

  return (
    <section class="queue-panel">
      <div>
        <div class="queue-panel-header">
          <div>
            <span class="status-badge">Queue</span>
            <h2>Dead letters</h2>
            <p>
              {resourceRef.realm} / {resourceRef.area} / {resourceRef.resource}
            </p>
          </div>
          <Button class="secondary-action" onPress={() => deadLetters.refresh()}>
            <ActivityIcon size={16} />
            Refresh
          </Button>
        </div>

        {deadLetters.loading ? (
          <p class="queue-panel-state">Loading dead-letter messages...</p>
        ) : null}

        {deadLetters.error ? (
          <p class="queue-panel-error">
            {deadLetters.error instanceof Error
              ? deadLetters.error.message
              : formatErrorValue(deadLetters.error)}
          </p>
        ) : null}

        {!deadLetters.loading && !deadLetters.error && messages.length === 0 ? (
          <div class="queue-empty-state">
            <GaugeIcon size={18} />
            <span>No dead-letter messages are visible for this resource.</span>
          </div>
        ) : null}

        {messages.length > 0 ? (
          <div class="queue-message-list">
            {messages.map((message) => (
              <article class="queue-message-card" data-message-id={message.messageId}>
                <div>
                  <strong>Message {message.messageId}</strong>
                  <span>Family {message.family}</span>
                </div>
                <p>{message.reason}</p>
                <dl>
                  <div>
                    <dt>Attempts</dt>
                    <dd>{message.attempts}</dd>
                  </div>
                  <div>
                    <dt>Dead-lettered</dt>
                    <dd>{formatTimestamp(message.deadLetteredAt)}</dd>
                  </div>
                </dl>
              </article>
            ))}
          </div>
        ) : null}
      </div>
    </section>
  );
}
