import { state } from "@askrjs/askr";
import { currentRoute, Link, navigate } from "@askrjs/askr/router";
import { For } from "@askrjs/askr/control";
import { Input, Label } from "@askrjs/ui";
import { Button } from "@askrjs/themes/controls";
import {
  Badge,
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@askrjs/themes/surfaces";
import DomainHeader from "@/components/shared/domain-header";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import { createDomainSidebar } from "@/components/shared/domain-sidebar";
import { QueryEmptyState, QueryErrorState, QueryLoadingState } from "@/components/shared/query-state";
import SidebarLayout from "@/components/shared/sidebar-layout";
import QueueDeadLetterTable from "@/components/shared/queue-dead-letter-table";
import QueueInflightTable from "@/components/shared/queue-inflight-table";
import {
  createPurgeQueueDeadLetterMutation,
  createReplayQueueDeadLetterMutation,
} from "@/features/queue/queue-actions";
import type { DeadLetterMessage } from "@/features/queue/queue-models";
import {
  createQueueResourceComparisonQuery,
  createQueueResourceQuery,
} from "@/features/queue/queue-resource-query";

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

function formatComparisonValue(value: number | null | undefined) {
  if (value == null) {
    return "n/a";
  }

  if (value === 0) {
    return "0";
  }

  return value > 0 ? `+${value}` : `${value}`;
}

function parseOptionalNumber(value: string) {
  const trimmed = value.trim();

  if (!trimmed) {
    return null;
  }

  const parsed = Number(trimmed);
  return Number.isFinite(parsed) ? parsed : null;
}

function currentCompareScope() {
  if (typeof window === "undefined") {
    return {
      area: "",
      family: "",
      realm: "",
      resource: "",
    };
  }

  const query = currentRoute().query;

  return {
    area: query.get("againstArea") ?? "",
    family: query.get("againstFamily") ?? "",
    realm: query.get("againstRealm") ?? "",
    resource: query.get("againstResource") ?? "",
  };
}

export default function QueueResourcePage() {
  const { realm, area, resource } = currentRoute().params;
  const resourceQuery = createQueueResourceQuery({ realm, area, resource });
  const compareScope = currentCompareScope();
  const compareRealmInput = state(compareScope.realm);
  const compareAreaInput = state(compareScope.area);
  const compareResourceInput = state(compareScope.resource);
  const compareFamilyInput = state(compareScope.family);
  const compareTarget =
    compareRealmInput().trim() && compareAreaInput().trim() && compareResourceInput().trim()
      ? {
          area: compareAreaInput().trim(),
          family: parseOptionalNumber(compareFamilyInput()),
          realm: compareRealmInput().trim(),
          resource: compareResourceInput().trim(),
        }
      : null;
  const comparisonQuery = compareTarget
    ? createQueueResourceComparisonQuery({ realm, area, resource }, compareTarget)
    : null;
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

  function onCompareSubmit(event: Event) {
    event.preventDefault();

    if (typeof window === "undefined") {
      return;
    }

    const nextQuery = new URLSearchParams();
    const nextRealm = compareRealmInput().trim();
    const nextArea = compareAreaInput().trim();
    const nextResource = compareResourceInput().trim();
    const nextFamily = compareFamilyInput().trim();

    if (nextRealm) {
      nextQuery.set("againstRealm", nextRealm);
    }

    if (nextArea) {
      nextQuery.set("againstArea", nextArea);
    }

    if (nextResource) {
      nextQuery.set("againstResource", nextResource);
    }

    if (nextFamily) {
      nextQuery.set("againstFamily", nextFamily);
    }

    const search = nextQuery.toString();
    navigate(`/queue/${realm}/${area}/${resource}${search ? `?${search}` : ""}`);
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
          <QueryLoadingState description="Loading queue resource..." />
        ) : null}

        {resourceQuery.error ? (
          <QueryErrorState error={resourceQuery.error} />
        ) : null}

        {actionError() ? (
          <QueryErrorState error={actionError()} />
        ) : null}

        {data && !resourceQuery.loading && !resourceQuery.error ? (
          <div class="domain-stack">
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
                <QueryEmptyState description="No inflight messages are visible for this resource." />
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
                <QueryEmptyState description="No dead-letter messages are visible for this resource." />
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
                <QueryEmptyState description="No recent queue transitions are visible for this resource." />
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

            <section class="domain-section">
              <div class="domain-section-header">
                <div>
                  <p class="eyebrow">Compare</p>
                  <h2>Before / after snapshot</h2>
                  <p>Compare this resource against another queue scope.</p>
                </div>
              </div>

              <form class="domain-stack" onSubmit={onCompareSubmit}>
                <div class="auth-field">
                  <Label for="compare-realm">Against realm</Label>
                  <Input
                    id="compare-realm"
                    value={compareRealmInput()}
                    onInput={(event: Event) => compareRealmInput.set((event.target as HTMLInputElement).value)}
                    placeholder="acme"
                  />
                </div>

                <div class="auth-field">
                  <Label for="compare-area">Against area</Label>
                  <Input
                    id="compare-area"
                    value={compareAreaInput()}
                    onInput={(event: Event) => compareAreaInput.set((event.target as HTMLInputElement).value)}
                    placeholder="payments"
                  />
                </div>

                <div class="auth-field">
                  <Label for="compare-resource">Against resource</Label>
                  <Input
                    id="compare-resource"
                    value={compareResourceInput()}
                    onInput={(event: Event) => compareResourceInput.set((event.target as HTMLInputElement).value)}
                    placeholder="inbox"
                  />
                </div>

                <div class="auth-field">
                  <Label for="compare-family">Against family</Label>
                  <Input
                    id="compare-family"
                    value={compareFamilyInput()}
                    onInput={(event: Event) => compareFamilyInput.set((event.target as HTMLInputElement).value)}
                    placeholder="Optional family"
                  />
                </div>

                <div class="session-filter-actions">
                  <Button type="submit" class="primary-action">
                    Compare
                  </Button>
                  <Button
                    type="button"
                    class="secondary-action"
                    onPress={() => {
                      compareRealmInput.set("");
                      compareAreaInput.set("");
                      compareResourceInput.set("");
                      compareFamilyInput.set("");
                      navigate(`/queue/${realm}/${area}/${resource}`);
                    }}
                  >
                    Clear
                  </Button>
                </div>
              </form>

              {comparisonQuery ? (
                comparisonQuery.loading ? (
                  <QueryLoadingState description="Loading queue resource comparison..." />
                ) : comparisonQuery.error ? (
                  <QueryErrorState error={comparisonQuery.error} />
                ) : comparisonQuery.data ? (
                  <div class="domain-stack">
                    <Card class="domain-resource-card" variant="raised">
                      <CardHeader>
                        <Badge>{comparisonQuery.data.derived ? "Derived" : "Live"}</Badge>
                        <CardTitle>{comparisonQuery.data.summary}</CardTitle>
                      </CardHeader>
                      <CardContent>
                        <p>{comparisonQuery.data.comparisonMode}</p>
                      </CardContent>
                    </Card>

                    <div class="domain-grid">
                      <DomainMetricTable
                        title="Current snapshot"
                        metrics={[
                          { label: "Backlog", value: comparisonQuery.data.left.metrics.backlog ?? "n/a" },
                          { label: "Inflight", value: comparisonQuery.data.left.metrics.inflight ?? "n/a" },
                          { label: "Ready", value: comparisonQuery.data.left.metrics.ready ?? "n/a" },
                          { label: "Dead letters", value: comparisonQuery.data.left.metrics.deadLetters ?? "n/a" },
                          { label: "Waiters", value: comparisonQuery.data.left.metrics.waiters ?? "n/a" },
                          {
                            label: "Age",
                            value: comparisonQuery.data.left.metrics.ageSeconds == null ? "n/a" : humanizeSeconds(comparisonQuery.data.left.metrics.ageSeconds),
                          },
                        ]}
                      />

                      <DomainMetricTable
                        title="Comparison target"
                        metrics={[
                          { label: "Backlog", value: comparisonQuery.data.right.metrics.backlog ?? "n/a" },
                          { label: "Inflight", value: comparisonQuery.data.right.metrics.inflight ?? "n/a" },
                          { label: "Ready", value: comparisonQuery.data.right.metrics.ready ?? "n/a" },
                          { label: "Dead letters", value: comparisonQuery.data.right.metrics.deadLetters ?? "n/a" },
                          { label: "Waiters", value: comparisonQuery.data.right.metrics.waiters ?? "n/a" },
                          {
                            label: "Age",
                            value: comparisonQuery.data.right.metrics.ageSeconds == null ? "n/a" : humanizeSeconds(comparisonQuery.data.right.metrics.ageSeconds),
                          },
                        ]}
                      />
                    </div>

                    <DomainMetricTable
                      title="Delta"
                      metrics={[
                        { label: "Backlog delta", value: formatComparisonValue(comparisonQuery.data.delta.backlog) },
                        { label: "Inflight delta", value: formatComparisonValue(comparisonQuery.data.delta.inflight) },
                        { label: "Ready delta", value: formatComparisonValue(comparisonQuery.data.delta.ready) },
                        { label: "Dead-letter delta", value: formatComparisonValue(comparisonQuery.data.delta.deadLetters) },
                        { label: "Waiter delta", value: formatComparisonValue(comparisonQuery.data.delta.waiters) },
                        {
                          label: "Recent transitions delta",
                          value: formatComparisonValue(comparisonQuery.data.delta.recentTransitionCount),
                        },
                      ]}
                    />
                  </div>
                ) : null
              ) : (
                <QueryEmptyState description="Enter another queue scope to compare snapshots." />
              )}
            </section>
          </div>
        ) : null}
      </section>
    </SidebarLayout>
  );
}
