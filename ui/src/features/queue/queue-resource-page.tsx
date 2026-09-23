import { state } from "@askrjs/askr";
import { Block } from "@askrjs/themes/components";
import { Show } from "@askrjs/askr/control";
import { currentRoute } from "@askrjs/askr/router";
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
import { createQueueDeadLettersQuery } from "@/features/queue/queue-query";
import {
  createQueueResourceInflightQuery,
  createQueueResourceQuery,
  createQueueResourceTimelineQuery,
} from "@/features/queue/queue-resource-query";
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
  const inflightQuery = createQueueResourceInflightQuery(resourceRef);
  const deadLettersQuery = createQueueDeadLettersQuery(resourceRef);
  const timelineQuery = createQueueResourceTimelineQuery(resourceRef);
  const scopeLabel = formatQueueScope(resourceRef);

  const replayMutation = createReplayQueueDeadLetterMutation(resourceRef);
  const purgeMutation = createPurgeQueueDeadLetterMutation(resourceRef);
  const [actionMessageId, setActionMessageId] = state<number | null>(null);
  const [actionKind, setActionKind] = state<"replay" | "purge" | null>(null);
  const [confirmMessage, setConfirmMessage] = state<DeadLetterMessage | null>(null);
  const [confirmKind, setConfirmKind] = state<"replay" | "purge" | null>(null);
  const detail = resourceQuery.data;
  const refreshing =
    resourceQuery.refreshing ||
    inflightQuery.refreshing ||
    deadLettersQuery.refreshing ||
    timelineQuery.refreshing;
  const partialError = Boolean(
    resourceQuery.error || inflightQuery.error || deadLettersQuery.error || timelineQuery.error,
  );
  const actionError = replayMutation.error ?? purgeMutation.error;
  const confirmationMessage = confirmMessage();
  const confirmationKind = confirmKind();
  const actionPending = actionKind() !== null;
  const stateSummary = detail ? describeQueueState(detail) : null;

  const headerStatus = {
    detail: partialError
      ? `${stateSummary?.detail ?? "Queue detail unavailable."} Some panels could not load; available data and actions remain usable.`
      : (stateSummary?.detail ?? "Inspecting queue state."),
    label: refreshing
      ? "Refreshing"
      : partialError
        ? "Partial"
        : resourceQuery.stale
          ? "Stale"
          : (stateSummary?.label ?? (detail ? "Live" : "Loading")),
    tone: refreshing
      ? "info"
      : partialError || resourceQuery.stale
        ? "warning"
        : (stateSummary?.tone ?? (detail ? "success" : "info")),
  } as const;

  function refreshAll() {
    void Promise.allSettled([
      resourceQuery.refresh(),
      inflightQuery.refresh(),
      deadLettersQuery.refresh(),
      timelineQuery.refresh(),
    ]);
  }

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
      <Block direction="column" gap="sm">
        <DomainHeader
          eyebrow="Queue resource"
          title={resourceRef.resource}
          description={
            detail
              ? `Current durable backlog, live reservations, dead-letter actions, and broker-observed transitions for ${scopeLabel}.`
              : `${scopeLabel}`
          }
          primaryAction={{
            busy: refreshing,
            disabled: refreshing,
            label: "Refresh queue",
            onPress: refreshAll,
          }}
          status={headerStatus}
        />
        <OperatorScopeStrip
          realm={resourceRef.realm}
          area={resourceRef.area}
          resource={resourceRef.resource}
          freshness={
            refreshing
              ? "Refreshing"
              : partialError
                ? "Partial"
                : resourceQuery.stale
                  ? "Stale"
                  : detail
                    ? "Live"
                    : resourceQuery.loading
                      ? "Loading"
                      : undefined
          }
        />

        <Block id="queue-detail-state" direction="column" gap="sm">
          <Show when={resourceQuery.refreshing && detail}>
            <QueryRefreshingState description="Refreshing queue resource detail..." />
          </Show>
          <Show when={resourceQuery.loading && !detail}>
            <QueryLoadingState description="Loading queue resource detail..." />
          </Show>
          <Show when={resourceQuery.error}>
            <QueryErrorState
              title="Unable to load current values"
              error={resourceQuery.error}
              onRetry={() => resourceQuery.refresh()}
            />
          </Show>
          <Show when={detail}>{(value) => <QueueResourceCurrentValuesPanel detail={value} />}</Show>
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

        <Block id="queue-timeline-state" direction="column" gap="sm">
          <Show when={timelineQuery.refreshing && timelineQuery.data}>
            <QueryRefreshingState description="Refreshing queue timeline..." />
          </Show>
          <Show when={timelineQuery.loading && !timelineQuery.data}>
            <QueryLoadingState description="Loading queue timeline..." />
          </Show>
          <Show when={timelineQuery.error}>
            <QueryErrorState
              title="Unable to load timeline"
              error={timelineQuery.error}
              onRetry={() => timelineQuery.refresh()}
            />
          </Show>
          <Show when={timelineQuery.data}>
            {(timeline) => <QueueResourceTimelinePanel timeline={timeline} />}
          </Show>
        </Block>

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
      </Block>
    </DomainPageFrame>
  );
}
