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
import {
  hasRowsRequest,
  revealSectionHref,
  ROWS_REQUEST_PARAM,
} from "@/shared/navigation/rows-request";

function QueueResourceTimelineView({ resourceRef }: { resourceRef: QueueResourceRef }) {
  const timelineQuery = createQueueResourceTimelineQuery(resourceRef);

  return (
    <Block direction="column" gap="sm">
      <Show when={timelineQuery.refreshing && timelineQuery.data}>
        <QueryRefreshingState description="Refreshing queue transitions..." />
      </Show>
      <Show when={timelineQuery.loading && !timelineQuery.data}>
        <QueryLoadingState description="Loading queue transitions..." />
      </Show>
      <Show when={timelineQuery.error}>
        <QueryErrorState
          title="Unable to load transitions"
          error={timelineQuery.error}
          onRetry={() => timelineQuery.refresh()}
        />
      </Show>
      <Show when={timelineQuery.data}>
        {(timeline) => <QueueResourceTimelinePanel timeline={timeline} />}
      </Show>
    </Block>
  );
}

export default function QueueResourcePage() {
  const route = currentRoute();
  const { realm, area, resource } = route.params;
  const resourceRef: QueueResourceRef = { realm, area, resource };
  // Message lists are data rows; they load only when the operator asks.
  const messagesRequested = hasRowsRequest(route.query);
  const timelineRequested = route.query.get("timeline") === "1";
  const resourceQuery = createQueueResourceQuery(resourceRef);
  const scopeLabel = formatQueueScope(resourceRef);
  const resourcePath = domainResourceHref("queue", resourceRef);

  const detail = resourceQuery.data;
  const refreshing = resourceQuery.refreshing;
  const partialError = Boolean(resourceQuery.error);
  const stateSummary = detail ? describeQueueState(detail) : null;

  const headerStatus = {
    label: stateSummary?.label ?? (detail ? undefined : "Loading"),
    tone: stateSummary?.tone ?? "info",
    freshness: refreshing
      ? { label: "Refreshing", tone: "info" as const }
      : partialError || resourceQuery.stale
        ? { label: partialError ? "Partial" : "Stale", tone: "warning" as const }
        : undefined,
  } as const;

  function refreshAll() {
    void resourceQuery.refresh();
  }

  return (
    <DomainPageFrame>
      <Block direction="column" gap="sm">
        <DomainHeader
          compact={true}
          eyebrow="Queue resource"
          title={resourceRef.resource}
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
            <QueryRefreshingState description="Refreshing queue state..." />
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
            description="Load message rows to inspect live reservations or review dead-letter details."
            href={revealSectionHref(resourcePath, route.query, ROWS_REQUEST_PARAM)}
            label="Inspect messages"
          />
        </Show>

        <Show when={messagesRequested}>
          <QueueResourceMessages resourceRef={resourceRef} scopeLabel={scopeLabel} />
        </Show>

        <Show when={timelineRequested}>
          <QueueResourceTimelineView resourceRef={resourceRef} />
        </Show>
        <Show when={!timelineRequested}>
          <RowsRequestPrompt
            description="Load transition evidence for this queue."
            href={revealSectionHref(resourcePath, route.query, "timeline")}
            label="Inspect transitions"
          />
        </Show>
      </Block>
    </DomainPageFrame>
  );
}
