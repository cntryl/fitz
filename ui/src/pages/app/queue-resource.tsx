import { state } from "@askrjs/askr";
import { currentRoute, Link, navigate } from "@askrjs/askr/router";
import { For, Show } from "@askrjs/askr/control";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogOverlay,
  AlertDialogPortal,
  AlertDialogTitle,
  Input,
  Label,
} from "@askrjs/ui";
import { Button } from "@askrjs/themes/controls";
import { Block, Inline, Section, Stack } from "@askrjs/themes/layouts";
import { Badge, Card, CardContent, CardHeader, CardTitle } from "@askrjs/themes/surfaces";
import DomainHeader from "@/components/shared/domain-header";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import { createDomainSidebar } from "@/components/shared/domain-sidebar";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import {
  QueryEmptyState,
  QueryErrorState,
  QueryLoadingState,
  QueryRefreshingState,
} from "@/components/shared/query-state";
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
import type {
  QueueResourceOverview,
  QueueResourceTimelineEvent,
} from "@/features/queue/queue-resource-models";

interface QueueComparisonTarget {
  area: string;
  family: number | null;
  realm: string;
  resource: string;
}

function trimmedOrNull(value: string) {
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

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

interface QueueResourceComparisonResultsProps {
  compareTarget: QueueComparisonTarget;
  resourceRef: {
    area: string;
    realm: string;
    resource: string;
  };
}

function QueueResourceComparisonResults({
  compareTarget,
  resourceRef,
}: QueueResourceComparisonResultsProps) {
  const comparisonQuery = createQueueResourceComparisonQuery(resourceRef, compareTarget);
  const comparison = comparisonQuery.data;

  return (
    <>
      {comparisonQuery.loading && !comparison ? (
        <QueryLoadingState description="Loading queue resource comparison..." />
      ) : null}

      {comparisonQuery.error && !comparison ? (
        <QueryErrorState error={comparisonQuery.error} />
      ) : null}

      {comparison ? (
        <Stack gap="3">
          {comparisonQuery.refreshing ? (
            <QueryRefreshingState description="Refreshing queue resource comparison..." />
          ) : null}

          <Card class="domain-resource-card" variant="raised">
            <CardHeader>
              <Badge>{comparison.derived ? "Derived" : "Live"}</Badge>
              <CardTitle>{comparison.summary}</CardTitle>
            </CardHeader>
            <CardContent>
              <p>{comparison.comparisonMode}</p>
            </CardContent>
          </Card>

          <Block gap="3" size="sm">
            <DomainMetricTable
              title="Current snapshot"
              metrics={[
                { label: "Backlog", value: comparison.left.metrics.backlog ?? "n/a" },
                { label: "Inflight", value: comparison.left.metrics.inflight ?? "n/a" },
                { label: "Ready", value: comparison.left.metrics.ready ?? "n/a" },
                { label: "Dead letters", value: comparison.left.metrics.deadLetters ?? "n/a" },
                { label: "Waiters", value: comparison.left.metrics.waiters ?? "n/a" },
                {
                  label: "Age",
                  value:
                    comparison.left.metrics.ageSeconds == null
                      ? "n/a"
                      : humanizeSeconds(comparison.left.metrics.ageSeconds),
                },
              ]}
            />

            <DomainMetricTable
              title="Comparison target"
              metrics={[
                { label: "Backlog", value: comparison.right.metrics.backlog ?? "n/a" },
                { label: "Inflight", value: comparison.right.metrics.inflight ?? "n/a" },
                { label: "Ready", value: comparison.right.metrics.ready ?? "n/a" },
                { label: "Dead letters", value: comparison.right.metrics.deadLetters ?? "n/a" },
                { label: "Waiters", value: comparison.right.metrics.waiters ?? "n/a" },
                {
                  label: "Age",
                  value:
                    comparison.right.metrics.ageSeconds == null
                      ? "n/a"
                      : humanizeSeconds(comparison.right.metrics.ageSeconds),
                },
              ]}
            />
          </Block>

          <DomainMetricTable
            title="Delta"
            metrics={[
              { label: "Backlog delta", value: formatComparisonValue(comparison.delta.backlog) },
              { label: "Inflight delta", value: formatComparisonValue(comparison.delta.inflight) },
              { label: "Ready delta", value: formatComparisonValue(comparison.delta.ready) },
              {
                label: "Dead-letter delta",
                value: formatComparisonValue(comparison.delta.deadLetters),
              },
              { label: "Waiter delta", value: formatComparisonValue(comparison.delta.waiters) },
              {
                label: "Recent transitions delta",
                value: formatComparisonValue(comparison.delta.recentTransitionCount),
              },
            ]}
          />
        </Stack>
      ) : null}
    </>
  );
}

export default function QueueResourcePage() {
  const { realm, area, resource } = currentRoute().params;
  const resourceRef = { realm, area, resource };
  const resourceQuery = createQueueResourceQuery(resourceRef);
  const compareScope = currentCompareScope();
  const [compareRealmInput, setCompareRealmInput] = state(compareScope.realm);
  const [compareAreaInput, setCompareAreaInput] = state(compareScope.area);
  const [compareResourceInput, setCompareResourceInput] = state(compareScope.resource);
  const [compareFamilyInput, setCompareFamilyInput] = state(compareScope.family);
  const compareRealmValue = compareRealmInput();
  const compareAreaValue = compareAreaInput();
  const compareResourceValue = compareResourceInput();
  const compareFamilyValue = compareFamilyInput();
  const compareRealmTrimmed = trimmedOrNull(compareRealmValue);
  const compareAreaTrimmed = trimmedOrNull(compareAreaValue);
  const compareResourceTrimmed = trimmedOrNull(compareResourceValue);
  const compareFamilyTrimmed = trimmedOrNull(compareFamilyValue);
  const compareTarget =
    compareRealmTrimmed && compareAreaTrimmed && compareResourceTrimmed
      ? {
          area: compareAreaTrimmed,
          family: compareFamilyTrimmed ? parseOptionalNumber(compareFamilyTrimmed) : null,
          realm: compareRealmTrimmed,
          resource: compareResourceTrimmed,
        }
      : null;
  const replayMutation = createReplayQueueDeadLetterMutation(resourceRef);
  const purgeMutation = createPurgeQueueDeadLetterMutation(resourceRef);
  const [actionMessageId, setActionMessageId] = state<number | null>(null);
  const [actionKind, setActionKind] = state<"replay" | "purge" | null>(null);
  const [confirmMessage, setConfirmMessage] = state<DeadLetterMessage | null>(null);
  const [confirmKind, setConfirmKind] = state<"replay" | "purge" | null>(null);
  const data = resourceQuery.data;
  const actionError = replayMutation.error ?? purgeMutation.error;
  const confirmationMessage = confirmMessage();
  const confirmationKind = confirmKind();
  const actionPending = actionKind() !== null;

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
      <Stack gap="3">
        <Link href="/queue">Back to Queue</Link>
        <Button onPress={() => resourceQuery.refresh()}>Refresh</Button>
      </Stack>
    ),
  });

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
    } catch {
      return;
    } finally {
      setActionKind(null);
      setActionMessageId(null);
      setConfirmKind(null);
      setConfirmMessage(null);
    }
  }

  function onCompareSubmit(event: Event) {
    event.preventDefault();

    if (typeof window === "undefined") {
      return;
    }

    const nextQuery = new URLSearchParams();
    const nextRealm = trimmedOrNull(compareRealmInput());
    const nextArea = trimmedOrNull(compareAreaInput());
    const nextResource = trimmedOrNull(compareResourceInput());
    const nextFamily = trimmedOrNull(compareFamilyInput());

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
    <DomainPageFrame sidebar={sidebar}>
      <Stack gap="3">
        <DomainHeader
          domain="Queue"
          title="Resource drill-down"
          description={`${realm} / ${area} / ${resource}`}
          onRefresh={() => resourceQuery.refresh()}
        />

        <Show when={resourceQuery.loading && !data}>
          <QueryLoadingState description="Loading queue resource..." />
        </Show>

        <Show when={resourceQuery.error && !data}>
          <QueryErrorState error={resourceQuery.error} />
        </Show>

        <Show when={actionError}>
          <QueryErrorState error={actionError} />
        </Show>

        <Show when={data}>
          {(current: QueueResourceOverview) => (
            <Stack gap="3">
              <Show when={resourceQuery.refreshing}>
                <QueryRefreshingState description="Refreshing queue resource..." />
              </Show>

              <Card class="domain-resource-card" variant="raised">
                <CardHeader>
                  <Badge>Queue Resource</Badge>
                  <CardTitle>{current.detail.resource}</CardTitle>
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
                  { label: "Total messages", value: current.detail.messagesTotal },
                  { label: "Ready", value: current.detail.messagesReady },
                  { label: "Inflight", value: current.detail.messagesInflight },
                  { label: "Dead-lettered", value: current.detail.messagesDeadLettered },
                  { label: "Delayed", value: current.detail.messagesDelayed },
                  {
                    label: "Oldest age",
                    value: humanizeSeconds(current.detail.oldestMessageAgeSeconds),
                  },
                ]}
              />

              <Section size="3">
                <div class="domain-section-header">
                  <div>
                    <p class="eyebrow">Inflight</p>
                    <h2>{current.inflight.length} entries</h2>
                  </div>
                </div>

                <Show
                  when={current.inflight.length === 0}
                  fallback={<QueueInflightTable messages={current.inflight} />}
                >
                  <QueryEmptyState description="No inflight messages are visible for this resource." />
                </Show>
              </Section>

              <Section size="3">
                <div class="domain-section-header">
                  <div>
                    <p class="eyebrow">Dead letters</p>
                    <h2>{current.deadLetters.length} messages</h2>
                  </div>
                </div>

                <Show
                  when={current.deadLetters.length === 0}
                  fallback={
                    <QueueDeadLetterTable
                      messages={current.deadLetters}
                      onReplay={(message) => openDeadLetterConfirmation("replay", message)}
                      onPurge={(message) => openDeadLetterConfirmation("purge", message)}
                      pendingAction={actionKind()}
                      pendingMessageId={actionMessageId()}
                    />
                  }
                >
                  <QueryEmptyState description="No dead-letter messages are visible for this resource." />
                </Show>
              </Section>

              <Section size="3">
                <div class="domain-section-header">
                  <div>
                    <p class="eyebrow">Timeline</p>
                    <h2>Recent transitions</h2>
                    <p>Bounded live events from the broker view for this resource.</p>
                  </div>
                  <Badge>{current.timeline.derived ? "Derived" : "Live"}</Badge>
                </div>

                <Show
                  when={current.timeline.events.length === 0}
                  fallback={
                    <Stack gap="3">
                      <Card class="domain-resource-card" variant="raised">
                        <CardHeader>
                          <Badge>Live timeline</Badge>
                          <CardTitle>{current.timeline.limit} event window</CardTitle>
                        </CardHeader>
                        <CardContent>
                          <p>
                            Recent state changes, retries, failures, and ownership flips from the
                            current broker snapshot.
                          </p>
                        </CardContent>
                      </Card>

                      <Stack gap="3">
                        <For
                          each={current.timeline.events}
                          by={(event: QueueResourceTimelineEvent) =>
                            `${event.observedAt}:${event.summary}`
                          }
                        >
                          {(event: QueueResourceTimelineEvent) => (
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
                                  {event.operation
                                    ? `Operation: ${event.operation}`
                                    : "Operation: unknown"}
                                  {event.messageId != null ? ` | Message ${event.messageId}` : ""}
                                </p>
                                <p>
                                  {event.ownerSession
                                    ? `Owner: ${event.ownerSession}`
                                    : "Owner: unknown"}
                                  {event.workerSession ? ` | Worker: ${event.workerSession}` : ""}
                                </p>
                                {event.correlationId ? (
                                  <p>Correlation: {event.correlationId}</p>
                                ) : null}
                              </CardContent>
                            </Card>
                          )}
                        </For>
                      </Stack>
                    </Stack>
                  }
                >
                  <QueryEmptyState description="No recent queue transitions are visible for this resource." />
                </Show>
              </Section>

              <Section size="3">
                <div class="domain-section-header">
                  <div>
                    <p class="eyebrow">Compare</p>
                    <h2>Before / after snapshot</h2>
                    <p>Compare this resource against another queue scope.</p>
                  </div>
                </div>

                <Stack asChild gap="3">
                  <form onSubmit={onCompareSubmit}>
                  <div class="auth-field">
                    <Label for="compare-realm">Against realm</Label>
                    <Input
                      id="compare-realm"
                      value={compareRealmValue}
                      onInput={(event: Event) =>
                        setCompareRealmInput((event.target as HTMLInputElement).value)
                      }
                      placeholder="acme"
                    />
                  </div>

                  <div class="auth-field">
                    <Label for="compare-area">Against area</Label>
                    <Input
                      id="compare-area"
                      value={compareAreaValue}
                      onInput={(event: Event) =>
                        setCompareAreaInput((event.target as HTMLInputElement).value)
                      }
                      placeholder="payments"
                    />
                  </div>

                  <div class="auth-field">
                    <Label for="compare-resource">Against resource</Label>
                    <Input
                      id="compare-resource"
                      value={compareResourceValue}
                      onInput={(event: Event) =>
                        setCompareResourceInput((event.target as HTMLInputElement).value)
                      }
                      placeholder="inbox"
                    />
                  </div>

                  <div class="auth-field">
                    <Label for="compare-family">Against family</Label>
                    <Input
                      id="compare-family"
                      value={compareFamilyValue}
                      onInput={(event: Event) =>
                        setCompareFamilyInput((event.target as HTMLInputElement).value)
                      }
                      placeholder="Optional family"
                    />
                  </div>

                  <Inline gap="3" wrap="wrap">
                    <Button type="submit">Compare</Button>
                    <Button
                      type="button"
                      onPress={() => {
                        setCompareRealmInput("");
                        setCompareAreaInput("");
                        setCompareResourceInput("");
                        setCompareFamilyInput("");
                        navigate(`/queue/${realm}/${area}/${resource}`);
                      }}
                    >
                      Clear
                    </Button>
                  </Inline>
                  </form>
                </Stack>

                {compareTarget ? (
                  <QueueResourceComparisonResults
                    resourceRef={resourceRef}
                    compareTarget={compareTarget}
                  />
                ) : (
                  <QueryEmptyState description="Enter another queue scope to compare snapshots." />
                )}
              </Section>
            </Stack>
          )}
        </Show>

        {confirmationMessage ? (
          <AlertDialog
            open
            onOpenChange={(open) => {
              if (!open) {
                setConfirmKind(null);
                setConfirmMessage(null);
              }
            }}
          >
            <AlertDialogPortal>
              <AlertDialogOverlay class="dialog-overlay" />
              <AlertDialogContent class="dialog-content" role="alertdialog">
                <AlertDialogTitle>
                  {confirmationKind === "purge"
                    ? "Purge dead-letter message?"
                    : "Replay dead-letter message?"}
                </AlertDialogTitle>
                <AlertDialogDescription>
                  {`${confirmationKind === "purge" ? "Purge" : "Replay"} message ${confirmationMessage.messageId} in ${realm} / ${area} / ${resource}.`}
                </AlertDialogDescription>
                <Inline gap="3" wrap="wrap">
                  <AlertDialogCancel>Cancel</AlertDialogCancel>
                  <AlertDialogAction
                    disabled={actionPending}
                    onPress={() => {
                      const nextKind = confirmKind();
                      const nextMessage = confirmMessage();
                      if (nextKind && nextMessage) {
                        void runDeadLetterAction(nextKind, nextMessage);
                      }
                    }}
                  >
                    {actionPending ? "Working..." : "Confirm"}
                  </AlertDialogAction>
                </Inline>
              </AlertDialogContent>
            </AlertDialogPortal>
          </AlertDialog>
        ) : null}
      </Stack>
    </DomainPageFrame>
  );
}
