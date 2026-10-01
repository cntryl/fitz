import { state } from "@askrjs/askr";
import { Show } from "@askrjs/askr/control";
import { RefreshCwIcon } from "@askrjs/lucide";
import { Block, Button } from "@askrjs/themes/components";
import {
  QueryErrorState,
  QueryLoadingState,
  QueryRefreshingState,
} from "@/components/shared/query-state";
import {
  createPurgeQueueDeadLetterMutation,
  createReplayQueueDeadLetterMutation,
} from "@/features/queue/queue-actions";
import QueueDeadLetterDialog from "@/features/queue/queue-dead-letter-dialog";
import type { DeadLetterMessage } from "@/features/queue/queue-models";
import { createQueueDeadLettersQuery } from "@/features/queue/queue-query";
import type { QueueResourceRef } from "@/features/queue/queue-resource-models";
import {
  QueueResourceDeadLettersPanel,
  QueueResourceInflightPanel,
} from "@/features/queue/queue-resource-panels";
import { createQueueResourceInflightQuery } from "@/features/queue/queue-resource-query";

export interface QueueResourceMessagesProps {
  resourceRef: QueueResourceRef;
  scopeLabel: string;
}

/**
 * Dead-letter and inflight message lists. These are data rows, so the page renders
 * this component only after the operator asks; creating it is what starts the reads.
 */
export default function QueueResourceMessages({
  resourceRef,
  scopeLabel,
}: QueueResourceMessagesProps) {
  const inflightQuery = createQueueResourceInflightQuery(resourceRef);
  const deadLettersQuery = createQueueDeadLettersQuery(resourceRef);
  const replayMutation = createReplayQueueDeadLetterMutation(resourceRef);
  const purgeMutation = createPurgeQueueDeadLetterMutation(resourceRef);
  const [actionMessageId, setActionMessageId] = state<number | null>(null);
  const [actionKind, setActionKind] = state<"replay" | "purge" | null>(null);
  const [confirmMessage, setConfirmMessage] = state<DeadLetterMessage | null>(null);
  const [confirmKind, setConfirmKind] = state<"replay" | "purge" | null>(null);
  const actionError = replayMutation.error ?? purgeMutation.error;
  const actionPending = actionKind() !== null;
  const refreshing = inflightQuery.refreshing || deadLettersQuery.refreshing;

  function openDeadLetterConfirmation(kind: "replay" | "purge", message: DeadLetterMessage) {
    setConfirmKind(kind);
    setConfirmMessage(message);
  }

  async function runDeadLetterAction(kind: "replay" | "purge", message: DeadLetterMessage) {
    replayMutation.reset();
    purgeMutation.reset();
    setActionMessageId(message.messageId);
    setActionKind(kind);

    try {
      if (kind === "replay") {
        await replayMutation.execute(message);
      } else {
        await purgeMutation.execute(message);
      }
      setConfirmKind(null);
      setConfirmMessage(null);
    } catch {
      return;
    } finally {
      setActionKind(null);
      setActionMessageId(null);
    }
  }

  return (
    <Block direction="column" gap="sm">
      <Block direction="row" justify="end">
        <Button
          variant="outline"
          aria-busy={refreshing ? "true" : undefined}
          disabled={refreshing}
          onPress={() =>
            void Promise.allSettled([inflightQuery.refresh(), deadLettersQuery.refresh()])
          }
        >
          <RefreshCwIcon size={16} />
          Refresh messages
        </Button>
      </Block>

      <Block id="queue-dead-letters-state" direction="column" gap="sm">
        <Show when={deadLettersQuery.refreshing && deadLettersQuery.data}>
          <QueryRefreshingState description="Refreshing queue dead letters..." />
        </Show>
        <Show when={deadLettersQuery.loading && !deadLettersQuery.data}>
          <QueryLoadingState description="Loading queue dead letters..." />
        </Show>
        <Show when={deadLettersQuery.error}>
          <QueryErrorState
            title="Unable to load dead letters"
            error={deadLettersQuery.error}
            onRetry={() => deadLettersQuery.refresh()}
          />
        </Show>
        <Show when={actionError}>
          <QueryErrorState error={actionError} />
        </Show>
        <Show when={deadLettersQuery.data}>
          {(messages) => (
            <QueueResourceDeadLettersPanel
              messages={messages}
              onReplay={(message) => openDeadLetterConfirmation("replay", message)}
              onPurge={(message) => openDeadLetterConfirmation("purge", message)}
              pendingAction={actionKind()}
              pendingMessageId={actionMessageId()}
            />
          )}
        </Show>
      </Block>

      <Block id="queue-inflight-state" direction="column" gap="sm">
        <Show when={inflightQuery.refreshing && inflightQuery.data}>
          <QueryRefreshingState description="Refreshing queue inflight entries..." />
        </Show>
        <Show when={inflightQuery.loading && !inflightQuery.data}>
          <QueryLoadingState description="Loading queue inflight entries..." />
        </Show>
        <Show when={inflightQuery.error}>
          <QueryErrorState
            title="Unable to load inflight entries"
            error={inflightQuery.error}
            onRetry={() => inflightQuery.refresh()}
          />
        </Show>
        <Show when={inflightQuery.data}>
          {(messages) => <QueueResourceInflightPanel messages={messages} />}
        </Show>
      </Block>

      <QueueDeadLetterDialog
        actionError={actionError}
        actionPending={actionPending}
        confirmationKind={confirmKind()}
        confirmationMessage={confirmMessage()}
        onOpenChange={(open) => {
          if (!open && !actionPending) {
            setConfirmKind(null);
            setConfirmMessage(null);
          }
        }}
        onRunAction={(kind, message) => void runDeadLetterAction(kind, message)}
        scopeLabel={scopeLabel}
      />
    </Block>
  );
}
