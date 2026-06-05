import { state } from "@askrjs/askr";
import { For } from "@askrjs/askr/control";
import { currentRoute, Link, navigate } from "@askrjs/askr/router";
import {
  Input,
  Label,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeaderCell,
  TableRow,
} from "@askrjs/ui";
import { Button } from "@askrjs/themes/controls";
import { Flex, Section, Stack } from "@askrjs/themes/layouts";
import DomainHeader from "@/components/shared/domain-header";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import { createDomainSidebar } from "@/components/shared/domain-sidebar";
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
import type { QueueResourceTimelineEvent } from "@/features/queue/queue-resource-models";

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

          <DomainMetricTable
            title="Comparison summary"
            metrics={[
              { label: "Summary", value: comparison.summary },
              { label: "Mode", value: comparison.comparisonMode },
              { label: "Source", value: comparison.derived ? "Derived" : "Live" },
            ]}
          />

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
  const [debugDialogOpen, setDebugDialogOpen] = state(false);
  const data = resourceQuery.data;
  const resourceQueryError = resourceQuery.error as Error | null;
  const actionError = replayMutation.error ?? purgeMutation.error;
  const confirmationMessage = confirmMessage();
  const confirmationKind = confirmKind();
  const debugDialogOpenValue = debugDialogOpen();
  console.log(
    "QUEUE-RESOURCE CONFIRM MESSAGE",
    confirmationMessage?.messageId,
    confirmationKind,
    debugDialogOpenValue,
  );
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

  if (!data) {
    return (
      <DomainPageFrame sidebar={sidebar}>
        <Stack gap="3">
          <DomainHeader
            title="Resource drill-down"
            description={`${realm} / ${area} / ${resource}`}
            onRefresh={() => resourceQuery.refresh()}
          />

          <div id="queue-resource-debug">
            Debug dialog open: {debugDialogOpenValue ? "yes" : "no"}
          </div>

          {resourceQuery.loading && !data ? (
            <QueryLoadingState description="Loading queue resource..." />
          ) : null}

          {resourceQuery.error && !data ? <QueryErrorState error={resourceQuery.error} /> : null}

          {actionError ? <QueryErrorState error={actionError} /> : null}
        </Stack>
      </DomainPageFrame>
    );
  }

  const current = data;

  function openDeadLetterConfirmation(kind: "replay" | "purge", message: DeadLetterMessage) {
    console.log("QUEUE-RESOURCE OPEN CONFIRM", kind, message.messageId);
    console.log(
      "QUEUE-RESOURCE SET CONFIRM",
      typeof setConfirmMessage,
      setConfirmMessage === undefined,
      setConfirmMessage?.toString?.().slice(0, 120),
    );
    setConfirmKind(kind);
    setConfirmMessage(message);
    setDebugDialogOpen(true);
    console.log(
      "QUEUE-RESOURCE OPEN CONFIRM AFTER SET",
      confirmationMessage?.messageId,
      confirmationKind,
      debugDialogOpenValue,
    );
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
          title="Resource drill-down"
          description={`${realm} / ${area} / ${resource}`}
          onRefresh={() => resourceQuery.refresh()}
        />

        <div id="queue-resource-debug">
          Debug dialog open: {debugDialogOpenValue ? "yes" : "no"}
        </div>

        {resourceQuery.loading && !data ? (
          <QueryLoadingState description="Loading queue resource..." />
        ) : null}

        {resourceQueryError && !data ? <QueryErrorState error={resourceQueryError} /> : null}

        {actionError ? <QueryErrorState error={actionError} /> : null}

        <Stack gap="3">
          {resourceQuery.refreshing ? (
            <QueryRefreshingState description="Refreshing queue resource..." />
          ) : null}

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
              <h2>Inflight</h2>
              <span>{current.inflight.length} entries</span>
            </div>

            {current.inflight.length === 0 ? (
              <QueryEmptyState description="No inflight messages are visible for this resource." />
            ) : (
              <QueueInflightTable messages={current.inflight} />
            )}
          </Section>

          <Section size="3">
            <div class="domain-section-header">
              <h2>Dead letters</h2>
              <span>{current.deadLetters.length} messages</span>
            </div>

            {console.log(
              "QUEUE-RESOURCE DEAD LETTER",
              JSON.stringify({
                length: current.deadLetters.length,
                messages: current.deadLetters,
              }),
            )}

            {current.deadLetters.length === 0 ? (
              <QueryEmptyState description="No dead-letter messages are visible for this resource." />
            ) : (
              <QueueDeadLetterTable
                messages={current.deadLetters}
                onReplay={(message) => openDeadLetterConfirmation("replay", message)}
                onPurge={(message) => openDeadLetterConfirmation("purge", message)}
                pendingAction={actionKind()}
                pendingMessageId={actionMessageId()}
              />
            )}
          </Section>

          <Section size="3">
            <div class="domain-section-header">
              <h2>Timeline</h2>
              <span>{current.timeline.derived ? "Derived" : "Live"}</span>
            </div>

            {current.timeline.events.length === 0 ? (
              <QueryEmptyState description="No recent queue transitions are visible for this resource." />
            ) : (
              <div class="domain-table-wrap">
                <Table class="domain-table">
                  <TableHead>
                    <TableRow>
                      <TableHeaderCell>Kind</TableHeaderCell>
                      <TableHeaderCell>Summary</TableHeaderCell>
                      <TableHeaderCell>Observed</TableHeaderCell>
                      <TableHeaderCell>Age</TableHeaderCell>
                      <TableHeaderCell>Context</TableHeaderCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    <For
                      each={current.timeline.events}
                      by={(event: QueueResourceTimelineEvent) =>
                        `${event.observedAt}:${event.summary}`
                      }
                    >
                      {(event: QueueResourceTimelineEvent) => (
                        <TableRow>
                          <TableCell>{formatTimelineKind(event.kind)}</TableCell>
                          <TableCell>{event.summary}</TableCell>
                          <TableCell>{event.observedAt}</TableCell>
                          <TableCell>
                            {event.ageSeconds == null
                              ? "Unknown"
                              : humanizeSeconds(event.ageSeconds)}
                          </TableCell>
                          <TableCell>
                            {event.operation ? `Operation: ${event.operation}` : ""}
                            {event.messageId != null ? ` Message: ${event.messageId}` : ""}
                            {event.ownerSession ? ` Owner: ${event.ownerSession}` : ""}
                            {event.workerSession ? ` Worker: ${event.workerSession}` : ""}
                          </TableCell>
                        </TableRow>
                      )}
                    </For>
                  </TableBody>
                </Table>
              </div>
            )}
          </Section>

          <Section size="3">
            <div class="domain-section-header">
              <h2>Compare</h2>
            </div>

            <Stack asChild gap="3">
              <form onSubmit={onCompareSubmit}>
                <div class="form-grid">
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
                </div>

                <Flex gap="1" wrap="wrap">
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
                </Flex>
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

        {confirmationMessage ? (
          <div
            role="alertdialog"
            aria-modal="true"
            aria-labelledby="queue-dead-letter-dialog-title"
            aria-describedby="queue-dead-letter-dialog-description"
            class="dialog dialog-alert"
          >
            <h2 id="queue-dead-letter-dialog-title">
              {confirmationKind === "replay"
                ? "Replay dead-letter message?"
                : "Purge dead-letter message?"}
            </h2>
            <p id="queue-dead-letter-dialog-description">
              {confirmationKind === "replay"
                ? `Replay message ${confirmationMessage.messageId} in ${realm} / ${area} / ${resource}.`
                : `Purge message ${confirmationMessage.messageId} from ${realm} / ${area} / ${resource}.`}
            </p>
            <Flex gap="2" align="end">
              <Button
                variant="secondary"
                type="button"
                onPress={() => {
                  setConfirmKind(null);
                  setConfirmMessage(null);
                }}
                disabled={actionPending}
              >
                Cancel
              </Button>
              <Button
                type="button"
                onPress={() => runDeadLetterAction(confirmationKind!, confirmationMessage)}
                disabled={actionPending}
                aria-busy={actionPending}
              >
                {actionPending
                  ? confirmationKind === "replay"
                    ? "Replaying..."
                    : "Purging..."
                  : confirmationKind === "replay"
                    ? "Replay"
                    : "Purge"}
              </Button>
            </Flex>
          </div>
        ) : null}
      </Stack>
    </DomainPageFrame>
  );
}
