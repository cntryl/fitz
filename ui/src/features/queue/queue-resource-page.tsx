import { state } from "@askrjs/askr";
import { Show } from "@askrjs/askr/control";
import { currentRoute } from "@askrjs/askr/router";
import { Stack } from "@askrjs/themes/components";
import DomainHeader from "@/components/shared/domain-header";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import OperatorScopeStrip from "@/components/shared/operator-scope-strip";
import {
  QueryErrorState,
  QueryLoadingState,
  QueryRefreshingState,
} from "@/components/shared/query-state";
import {
  createPurgeQueueDeadLetterMutation,
  createReplayQueueDeadLetterMutation,
} from "@/features/queue/queue-actions";
import type { DeadLetterMessage } from "@/features/queue/queue-models";
import { createQueueResourceQuery } from "@/features/queue/queue-resource-query";
import type { QueueResourceRef } from "@/features/queue/queue-resource-models";
import QueueDeadLetterDialog from "@/features/queue/queue-dead-letter-dialog";
import {
  QueueResourceCurrentValuesPanel,
  QueueResourceDeadLettersPanel,
  QueueResourceInflightPanel,
  QueueResourceTimelinePanel,
} from "@/features/queue/queue-resource-panels";
import { describeQueueState, formatQueueScope } from "@/features/queue/queue-resource-presenters";

export default function QueueResourcePage() {
  const { realm, area, resource } = currentRoute().params;
  const resourceRef: QueueResourceRef = { realm, area, resource };
  const resourceQuery = createQueueResourceQuery(resourceRef);
  const scopeLabel = formatQueueScope(resourceRef);

  const replayMutation = createReplayQueueDeadLetterMutation(resourceRef);
  const purgeMutation = createPurgeQueueDeadLetterMutation(resourceRef);
  const [actionMessageId, setActionMessageId] = state<number | null>(null);
  const [actionKind, setActionKind] = state<"replay" | "purge" | null>(null);
  const [confirmMessage, setConfirmMessage] = state<DeadLetterMessage | null>(null);
  const [confirmKind, setConfirmKind] = state<"replay" | "purge" | null>(null);
  const data = resourceQuery.data;
  const resourceQueryError = resourceQuery.error as Error | null;
  const actionError = replayMutation.error ?? purgeMutation.error;
  const confirmationMessage = confirmMessage();
  const confirmationKind = confirmKind();
  const actionPending = actionKind() !== null;
  const stateSummary = data ? describeQueueState(data.detail) : null;

  const headerStatus = {
    detail: stateSummary?.detail ?? "Inspecting queue state.",
    label: resourceQuery.refreshing
      ? "Refreshing"
      : resourceQuery.stale
        ? "Stale"
        : (stateSummary?.label ?? (data ? "Live" : "Loading")),
    tone: resourceQuery.refreshing
      ? "info"
      : resourceQuery.stale
        ? "warning"
        : (stateSummary?.tone ?? (data ? "success" : "info")),
  } as const;

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
    <DomainPageFrame>
      <Stack gap="3">
        <DomainHeader
          eyebrow="Queue resource"
          title={data ? "Queue resource inspection" : "Queue resource inspection"}
          description={
            data
              ? `Current durable backlog, live reservations, dead-letter actions, and broker-observed transitions for ${scopeLabel}.`
              : `${scopeLabel}`
          }
          primaryAction={{
            busy: resourceQuery.refreshing,
            disabled: resourceQuery.refreshing,
            label: "Refresh resource",
            onPress: () => resourceQuery.refresh(),
          }}
          status={headerStatus}
        />
        <OperatorScopeStrip
          realm={resourceRef.realm}
          area={resourceRef.area}
          resource={resourceRef.resource}
          freshness={
            resourceQuery.refreshing
              ? "Refreshing"
              : resourceQuery.stale
                ? "Stale"
                : data
                  ? "Live"
                  : resourceQuery.loading
                    ? "Loading"
                    : resourceQueryError
                      ? "Unavailable"
                      : undefined
          }
        />

        <Show when={resourceQuery.refreshing && data}>
          <QueryRefreshingState description="Refreshing queue resource..." />
        </Show>

        <Show when={resourceQuery.loading && !data}>
          <QueryLoadingState description="Loading queue resource..." />
        </Show>

        <Show when={resourceQueryError && !data}>
          <QueryErrorState error={resourceQueryError} onRetry={() => resourceQuery.refresh()} />
        </Show>

        <Show when={resourceQueryError && data}>
          <QueryErrorState error={resourceQueryError} onRetry={() => resourceQuery.refresh()} />
        </Show>

        <Show when={actionError}>
          <QueryErrorState error={actionError} />
        </Show>

        <Show when={data}>
          {(data) => (
            <Stack gap="3">
              <QueueResourceCurrentValuesPanel detail={data.detail} />
              <QueueResourceDeadLettersPanel
                messages={data.deadLetters}
                onReplay={(message) => openDeadLetterConfirmation("replay", message)}
                onPurge={(message) => openDeadLetterConfirmation("purge", message)}
                pendingAction={actionKind()}
                pendingMessageId={actionMessageId()}
              />
              <QueueResourceInflightPanel messages={data.inflight} />
              <QueueResourceTimelinePanel timeline={data.timeline} />

              <QueueDeadLetterDialog
                actionError={actionError}
                actionPending={actionPending}
                confirmationKind={confirmationKind}
                confirmationMessage={confirmationMessage}
                onOpenChange={(open) => {
                  if (!open && !actionPending) {
                    setConfirmKind(null);
                    setConfirmMessage(null);
                  }
                }}
                onRunAction={(kind, message) => void runDeadLetterAction(kind, message)}
                scopeLabel={scopeLabel}
              />
            </Stack>
          )}
        </Show>
      </Stack>
    </DomainPageFrame>
  );
}
