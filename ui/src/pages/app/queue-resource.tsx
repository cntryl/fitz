import { state } from "@askrjs/askr";
import { currentRoute, Link } from "@askrjs/askr/router";
import { For } from "@askrjs/askr";
import { Button } from "@askrjs/ui";
import {
  Badge,
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  EmptyState,
  SidebarLayout,
  Spinner,
} from "@askrjs/themes/components";
import { AlertTriangleIcon, GaugeIcon } from "@askrjs/lucide";
import DomainHeader from "@/components/shared/domain-header";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import { createDomainSidebar } from "@/components/shared/domain-sidebar";
import QueueDeadLetterTable from "@/components/shared/queue-dead-letter-table";
import QueueInflightTable from "@/components/shared/queue-inflight-table";
import {
  createPurgeQueueDeadLetterMutation,
  createReplayQueueDeadLetterMutation,
} from "@/features/queue/queue-actions";
import type { DeadLetterMessage } from "@/features/queue/queue-models";
import { createQueueResourceQuery } from "@/features/queue/queue-resource-query";
import { formatUnknownError } from "@/shared/errors/format";

function humanizeSeconds(seconds: number) {
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m`;
  const hours = Math.floor(minutes / 60);
  return `${hours}h`;
}

function formatTimelineKind(kind: string) {
  switch (kind) {
    case "failure":
      return "Failure";
    case "retry":
      return "Retry";
    case "ownership_change":
      return "Ownership change";
    case "state_flip":
      return "State flip";
    case "registration":
      return "Registration";
    case "transition":
      return "Transition";
    default:
      return "Observation";
  }
}

export default function QueueResourcePage() {
  const { realm, area, resource } = currentRoute().params;
  const resourceQuery = createQueueResourceQuery({ realm, area, resource });
  const replayMutation = createReplayQueueDeadLetterMutation({ realm, area, resource });
  const purgeMutation = createPurgeQueueDeadLetterMutation({ realm, area, resource });
  const actionError = state("");
  const actionMessageId = state<number | null>(null);
  const actionKind = state<"replay" | "purge" | null>(null);
  const data = resourceQuery.data;

  const sidebar = createDomainSidebar({
    data,
    title: "Resource snapshot",
    description: "Current queue actor state for this resource.",
    stats: (current) => [
      { label: "Realm", value: current.detail.realm },
      { label: "Area", value: current.detail.area },
      { label: "Resource", value: current.detail.resource },
      { label: "Ready", value: current.detail.messagesReady },
      { label: "Inflight", value: current.detail.messagesInflight },
      { label: "Dead-lettered", value: current.detail.messagesDeadLettered },
      { label: "Delayed", value: current.detail.messagesDelayed },
      {
        label: "Oldest age",
        value: humanizeSeconds(current.detail.oldestMessageAgeSeconds),
        note: "Point-in-time broker snapshot",
      },
    ],
    footer: (
      <div class="admin-sidebar-actions">
        <Link href="/queue" class="admin-sidebar-link">
          Back to Queue
        </Link>
        <Button class="secondary-action" onPress={() => resourceQuery.refresh()}>
          Refresh
        </Button>
      </div>
    ),
  });

  async function runDeadLetterAction(kind: "replay" | "purge", message: DeadLetterMessage) {
    const verb = kind === "replay" ? "replay" : "purge";
    const confirmMessage = `Are you sure you want to ${verb} dead-letter message ${message.messageId} in ${realm} / ${area} / ${resource}?`;

    if (typeof window !== "undefined" && !window.confirm(confirmMessage)) {
      return;
    }

    actionError.set("");
    actionMessageId.set(message.messageId);
    actionKind.set(kind);

    try {
      if (kind === "replay") {
        await replayMutation.execute(message);
      } else {
        await purgeMutation.execute(message);
      }
    } catch (error) {
      actionError.set(
        error instanceof Error ? error.message : "Unable to update dead-letter message",
      );
    } finally {
      actionKind.set(null);
      actionMessageId.set(null);
    }
  }

  return (
    <SidebarLayout
      sidebar={sidebar}
      sidebarPosition="end"
      sidebarWidth="18rem"
      gap="1.5rem"
      collapseBelow="md"
    >
      <section class="domain-page">
        <DomainHeader
          domain="Queue"
          title="Resource drill-down"
          description={`${realm} / ${area} / ${resource}`}
          onRefresh={() => resourceQuery.refresh()}
        />

        {resourceQuery.loading ? (
          <EmptyState
            class="domain-state"
            icon={<Spinner label="Loading" />}
            description="Loading queue resource..."
          />
        ) : null}

        {resourceQuery.error ? (
          <EmptyState
            class="domain-state"
            icon={<AlertTriangleIcon size={18} />}
            description={formatUnknownError(resourceQuery.error)}
          />
        ) : null}

        {actionError() ? (
          <EmptyState
            class="domain-state"
            icon={<AlertTriangleIcon size={18} />}
            description={formatUnknownError(actionError())}
          />
        ) : null}

        {data && !resourceQuery.loading && !resourceQuery.error ? (
          <>
            <Card class="domain-resource-card" variant="raised">
              <CardHeader>
                <Badge>Queue Resource</Badge>
                <CardTitle>{data.detail.resource}</CardTitle>
              </CardHeader>
              <CardContent>
                <p>
                  Live in-memory view of the broker actor for this queue resource. Message counts
                  are point-in-time and reflect the current broker process.
                </p>
              </CardContent>
            </Card>

            <DomainMetricTable
              title="Resource metrics"
              metrics={[
                { label: "Total messages", value: data.detail.messagesTotal },
                { label: "Ready", value: data.detail.messagesReady },
                { label: "Inflight", value: data.detail.messagesInflight },
                { label: "Dead-lettered", value: data.detail.messagesDeadLettered },
                { label: "Delayed", value: data.detail.messagesDelayed },
                {
                  label: "Oldest age",
                  value: humanizeSeconds(data.detail.oldestMessageAgeSeconds),
                },
              ]}
            />

            <section class="domain-section">
              <div class="domain-section-header">
                <div>
                  <p class="eyebrow">Inflight</p>
                  <h2>{data.inflight.length} entries</h2>
                </div>
              </div>

              {data.inflight.length === 0 ? (
                <EmptyState
                  class="domain-state"
                  icon={<GaugeIcon size={18} />}
                  description="No inflight messages are visible for this resource."
                />
              ) : (
                <QueueInflightTable messages={data.inflight} />
              )}
            </section>

            <section class="domain-section">
              <div class="domain-section-header">
                <div>
                  <p class="eyebrow">Dead letters</p>
                  <h2>{data.deadLetters.length} messages</h2>
                </div>
              </div>

              {data.deadLetters.length === 0 ? (
                <EmptyState
                  class="domain-state"
                  icon={<GaugeIcon size={18} />}
                  description="No dead-letter messages are visible for this resource."
                />
              ) : (
                <QueueDeadLetterTable
                  messages={data.deadLetters}
                  onReplay={(message) => runDeadLetterAction("replay", message)}
                  onPurge={(message) => runDeadLetterAction("purge", message)}
                  pendingAction={actionKind()}
                  pendingMessageId={actionMessageId()}
                />
              )}
            </section>

            <section class="domain-section">
              <div class="domain-section-header">
                <div>
                  <p class="eyebrow">Timeline</p>
                  <h2>Recent transitions</h2>
                  <p>Bounded live events from the broker view for this resource.</p>
                </div>
                <Badge>{data.timeline.derived ? "Derived" : "Live"}</Badge>
              </div>

              {data.timeline.events.length === 0 ? (
                <EmptyState
                  class="domain-state"
                  icon={<GaugeIcon size={18} />}
                  description="No recent queue transitions are visible for this resource."
                />
              ) : (
                <div class="domain-stack">
                  <Card class="domain-resource-card" variant="raised">
                    <CardHeader>
                      <Badge>Live timeline</Badge>
                      <CardTitle>{data.timeline.limit} event window</CardTitle>
                    </CardHeader>
                    <CardContent>
                      <p>
                        Recent state changes, retries, failures, and ownership flips from the
                        current broker snapshot.
                      </p>
                    </CardContent>
                  </Card>

                  <div class="domain-stack">
                    <For each={data.timeline.events} by={(event) => `${event.observedAt}:${event.summary}`}>
                      {(event) => (
                        <Card class="domain-resource-card" variant="raised">
                          <CardHeader>
                            <div class="domain-inline-tags">
                              <Badge>{formatTimelineKind(event.kind)}</Badge>
                              {event.ageSeconds != null ? (
                                <Badge>{humanizeSeconds(event.ageSeconds)}</Badge>
                              ) : null}
                            </div>
                            <CardTitle>{event.summary}</CardTitle>
                          </CardHeader>
                          <CardContent>
                            <p>{event.observedAt}</p>
                            <p>
                              {event.operation ? `Operation: ${event.operation}` : "Operation: unknown"}
                              {event.messageId != null ? ` | Message ${event.messageId}` : ""}
                            </p>
                            <p>
                              {event.ownerSession ? `Owner: ${event.ownerSession}` : "Owner: unknown"}
                              {event.workerSession ? ` | Worker: ${event.workerSession}` : ""}
                            </p>
                            {event.correlationId ? <p>Correlation: {event.correlationId}</p> : null}
                          </CardContent>
                        </Card>
                      )}
                    </For>
                  </div>
                </div>
              )}
            </section>
          </>
        ) : null}
      </section>
    </SidebarLayout>
  );
}
