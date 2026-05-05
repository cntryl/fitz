import {
  createQueueDeadLettersQuery,
  type DeadLetterFilters,
  type QueueResourceRef,
} from "@/features/queue/queue-query";
import DomainHeader from "@/components/shared/domain-header";
import DomainState from "@/components/shared/domain-state";
import QueueDeadLetterTable from "@/components/shared/queue-dead-letter-table";

export interface QueueDeadLettersPanelProps {
  resourceRef: QueueResourceRef;
  filters?: DeadLetterFilters;
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
      <DomainHeader
        domain="Queue"
        title="Dead letters"
        description={`${resourceRef.realm} / ${resourceRef.area} / ${resourceRef.resource}`}
        onRefresh={() => deadLetters.refresh()}
      />

      {deadLetters.loading ? (
        <DomainState kind="loading" message="Loading dead-letter messages..." />
      ) : null}

      {deadLetters.error ? (
        <DomainState
          kind="error"
          message="Dead-letter messages could not be loaded."
          error={deadLetters.error}
        />
      ) : null}

      {!deadLetters.loading && !deadLetters.error && messages.length === 0 ? (
        <DomainState
          kind="empty"
          message="No dead-letter messages are visible for this resource."
        />
      ) : null}

      {messages.length > 0 ? <QueueDeadLetterTable messages={messages} /> : null}
    </section>
  );
}
