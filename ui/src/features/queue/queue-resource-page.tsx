import { Block } from "@askrjs/themes/components";
import { Show } from "@askrjs/askr/control";
import { currentRoute } from "@askrjs/askr/router";
import DomainHeader from "@/components/shared/domain-header";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import {
  QueryErrorState,
  QueryLoadingState,
  QueryRefreshingState,
} from "@/components/shared/query-state";
import RowsRequestPrompt from "@/components/shared/rows-request-prompt";
import {
  createQueueResourceQuery,
  createQueueResourceTimelineQuery,
} from "@/features/queue/queue-resource-query";
import type { QueueResourceRef } from "@/features/queue/queue-resource-models";
import QueueResourceMessages from "@/features/queue/queue-resource-messages";
import {
  QueueResourceCurrentValuesPanel,
  QueueResourceTimelinePanel,
} from "@/features/queue/queue-resource-panels";
import { describeQueueState, formatQueueScope } from "@/features/queue/queue-resource-presenters";
import { domainResourceHref } from "@/shared/navigation/domains";
import { hasRowsRequest, rowsRequestQuery } from "@/shared/navigation/rows-request";

export default function QueueResourcePage() {
  const route = currentRoute();
  const { realm, area, resource } = route.params;
  const resourceRef: QueueResourceRef = { realm, area, resource };
  // Message lists are data rows; they load only when the operator asks.
  const messagesRequested = hasRowsRequest(route.query);
  const resourceQuery = createQueueResourceQuery(resourceRef);
  const timelineQuery = createQueueResourceTimelineQuery(resourceRef);
  const activeQueries = [resourceQuery, timelineQuery];
  const scopeLabel = formatQueueScope(resourceRef);

  const detail = resourceQuery.data;
  const refreshing = activeQueries.some((query) => query.refreshing);
  const partialError = activeQueries.some((query) => Boolean(query.error));
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
    void Promise.allSettled(activeQueries.map((query) => query.refresh()));
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

        <Show when={!messagesRequested}>
          <RowsRequestPrompt
            description="Dead-letter and inflight messages load only when requested."
            href={`${domainResourceHref("queue", resourceRef)}?${rowsRequestQuery().toString()}`}
            label="Load messages"
          />
        </Show>

        <Show when={messagesRequested}>
          <QueueResourceMessages resourceRef={resourceRef} scopeLabel={scopeLabel} />
        </Show>

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
      </Block>
    </DomainPageFrame>
  );
}
