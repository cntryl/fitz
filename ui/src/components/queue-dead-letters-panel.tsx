import {
  createQueueDeadLettersQuery,
  type DeadLetterFilters,
  type QueueResourceRef,
} from "@/features/queue/queue-query";
import { EmptyState, Spinner } from "@askrjs/themes/feedback";
import { Section } from "@askrjs/themes/layouts";
import DomainHeader from "@/components/shared/domain-header";
import QueueDeadLetterTable from "@/components/shared/queue-dead-letter-table";
import type { DeadLetterMessage } from "@/features/queue/queue-models";
import { formatUnknownError } from "@/shared/errors/format";

export interface QueueDeadLettersPanelProps {
  resourceRef: QueueResourceRef;
  filters?: DeadLetterFilters;
  onPurge?: (message: DeadLetterMessage) => void | Promise<void>;
  onReplay?: (message: DeadLetterMessage) => void | Promise<void>;
  pendingAction?: "replay" | "purge" | null;
  pendingMessageId?: number | null;
}

// Component boundary: consume query state only; no generated DTOs or FetchResponse.
export default function QueueDeadLettersPanel({
  resourceRef,
  filters = {},
  onPurge,
  onReplay,
  pendingAction = null,
  pendingMessageId = null,
}: QueueDeadLettersPanelProps) {
  const deadLetters = createQueueDeadLettersQuery(resourceRef, filters);
  const messages = deadLetters.data ?? [];

  return (
    <Section size="3">
      <DomainHeader
        title="Dead letters"
        description={`${resourceRef.realm} / ${resourceRef.area} / ${resourceRef.resource}`}
        onRefresh={() => deadLetters.refresh()}
      />

      {deadLetters.loading ? (
        <EmptyState
          class="domain-state"
          icon={<Spinner label="Loading" />}
          description="Loading dead-letter messages..."
        />
      ) : null}

      {deadLetters.error ? (
        <EmptyState
          class="domain-state"
          title="Error"
          description={formatUnknownError(deadLetters.error)}
        />
      ) : null}

      {!deadLetters.loading && !deadLetters.error && messages.length === 0 ? (
        <EmptyState
          class="domain-state"
          description="No dead-letter messages are visible for this resource."
        />
      ) : null}

      {messages.length > 0 ? (
        <QueueDeadLetterTable
          messages={messages}
          onPurge={onPurge}
          onReplay={onReplay}
          pendingAction={pendingAction}
          pendingMessageId={pendingMessageId}
        />
      ) : null}
    </Section>
  );
}
