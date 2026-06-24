import { state } from "@askrjs/askr";
import { For } from "@askrjs/askr/control";
import { currentRoute, Link, navigate } from "@askrjs/askr/router";
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogOverlay,
  DialogPortal,
  DialogTitle,
  Input,
  Label,
  VirtualTable,
  type VirtualTableColumn,
} from "@askrjs/ui";
import { Button } from "@askrjs/themes/controls";
import { Flex, Stack } from "@askrjs/themes/layouts";
import {
  Badge,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@askrjs/themes/surfaces";
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
import type {
  QueueResourceDetail,
  QueueResourceRef,
  QueueResourceTimelineEvent,
} from "@/features/queue/queue-resource-models";
import { domainResourceHref } from "@/shared/navigation/domains";

interface QueueComparisonTarget {
  area: string;
  family: number | null;
  realm: string;
  resource: string;
}

interface ParsedQueueFamily {
  valid: boolean;
  value: number | null;
}

type QueueStateTone = "info" | "success" | "warning" | "danger";

function trimmedOrNull(value: string) {
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function parseFamilyInput(value: string): ParsedQueueFamily {
  const trimmed = value.trim();

  if (trimmed.length === 0) {
    return { value: null, valid: true };
  }

  if (!/^\d+$/.test(trimmed)) {
    return { value: null, valid: false };
  }

  const parsed = Number(trimmed);

  if (!Number.isSafeInteger(parsed)) {
    return { value: null, valid: false };
  }

  return { value: parsed, valid: true };
}

function humanizeSeconds(seconds: number) {
  if (seconds < 60) {
    return `${seconds}s`;
  }

  const minutes = Math.floor(seconds / 60);

  if (minutes < 60) {
    return `${minutes}m`;
  }

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

function formatQueueScope(
  scope: Pick<QueueComparisonTarget, "area" | "realm" | "resource"> & { family?: number | null },
) {
  const base = `${scope.realm} / ${scope.area} / ${scope.resource}`;
  return scope.family == null ? base : `${base} / family ${scope.family}`;
}

function describeQueueState(
  detail: QueueResourceDetail,
  compareTarget: QueueComparisonTarget | null,
): { detail: string; label: string; tone: QueueStateTone } {
  const counts = [
    detail.messagesReady > 0 ? `${detail.messagesReady} ready` : null,
    detail.messagesInflight > 0 ? `${detail.messagesInflight} inflight` : null,
    detail.messagesDelayed > 0 ? `${detail.messagesDelayed} delayed` : null,
    detail.messagesDeadLettered > 0 ? `${detail.messagesDeadLettered} dead-lettered` : null,
  ].filter((value): value is string => value !== null);

  const snapshotSentence = counts.length
    ? `Current scope has ${counts.join(", ")}.`
    : "No ready, inflight, delayed, or dead-lettered messages are visible.";

  const ageSentence = `Oldest visible message age: ${humanizeSeconds(
    detail.oldestMessageAgeSeconds,
  )}.`;
  const compareSentence = compareTarget
    ? ` Comparing against ${formatQueueScope(compareTarget)}.`
    : "";

  if (detail.messagesDeadLettered > 0) {
    return {
      detail: `${snapshotSentence} ${ageSentence} Dead letters need attention.${compareSentence}`,
      label: "Attention",
      tone: "danger",
    };
  }

  if (detail.messagesDelayed > 0) {
    return {
      detail: `${snapshotSentence} ${ageSentence} Delayed work is visible.${compareSentence}`,
      label: "Warning",
      tone: "warning",
    };
  }

  if (
    detail.messagesReady === 0 &&
    detail.messagesInflight === 0 &&
    detail.messagesDelayed === 0 &&
    detail.messagesDeadLettered === 0
  ) {
    return {
      detail: `${snapshotSentence} ${ageSentence}${compareSentence}`,
      label: "Healthy",
      tone: "success",
    };
  }

  return {
    detail: `${snapshotSentence} ${ageSentence} The queue is active and moving messages.${compareSentence}`,
    label: "Healthy",
    tone: "success",
  };
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

function formatTimelineContext(event: QueueResourceTimelineEvent) {
  return [
    `Scope: ${event.realm} / ${event.area} / ${event.resource}`,
    event.operation ? `Operation: ${event.operation}` : null,
    event.messageId != null ? `Message: ${event.messageId}` : null,
    event.attempts != null ? `Attempts: ${event.attempts}` : null,
    event.ownerSession ? `Owner session: ${event.ownerSession}` : null,
    event.workerSession ? `Worker session: ${event.workerSession}` : null,
    event.correlationId ? `Correlation: ${event.correlationId}` : null,
  ].filter((value): value is string => value !== null);
}

interface QueueResourceComparisonResultsProps {
  compareTarget: QueueComparisonTarget;
  resourceRef: QueueResourceRef;
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
        <QueryErrorState
          error={comparisonQuery.error}
          onRetry={() => comparisonQuery.refresh()}
          title="Unable to compare"
        />
      ) : null}

      {comparison ? (
        <Stack gap="3">
          <DomainMetricTable
            title="Comparison summary"
            description={`Current scope: ${formatQueueScope(resourceRef)}. Target scope: ${formatQueueScope(compareTarget)}.`}
            metrics={[
              { label: "Summary", value: comparison.summary },
              { label: "Mode", value: comparison.comparisonMode },
              { label: "Source", value: comparison.derived ? "Derived" : "Live" },
            ]}
          />

          <DomainMetricTable
            title="Current scope"
            description={`Live metrics for ${formatQueueScope(comparison.left.scope)}.`}
            metrics={[
              { label: "Backlog", value: comparison.left.metrics.backlog ?? "n/a" },
              { label: "Inflight", value: comparison.left.metrics.inflight ?? "n/a" },
              { label: "Ready", value: comparison.left.metrics.ready ?? "n/a" },
              { label: "Delayed", value: comparison.left.metrics.delayed ?? "n/a" },
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
            title="Target scope"
            description={`Live metrics for ${formatQueueScope(comparison.right.scope)}.`}
            metrics={[
              { label: "Backlog", value: comparison.right.metrics.backlog ?? "n/a" },
              { label: "Inflight", value: comparison.right.metrics.inflight ?? "n/a" },
              { label: "Ready", value: comparison.right.metrics.ready ?? "n/a" },
              { label: "Delayed", value: comparison.right.metrics.delayed ?? "n/a" },
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
            title="Difference"
            description="Positive values mean the current scope is ahead of the target."
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

      {comparison && comparisonQuery.refreshing ? (
        <QueryRefreshingState description="Updating queue comparison..." />
      ) : null}
    </>
  );
}

export default function QueueResourcePage() {
  const { realm, area, resource } = currentRoute().params;
  const resourceRef = { realm, area, resource };
  const resourceQuery = createQueueResourceQuery(resourceRef);
  const scopeLabel = formatQueueScope(resourceRef);

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
  const compareFamilyParsed = parseFamilyInput(compareFamilyValue);

  const compareTarget =
    compareRealmTrimmed && compareAreaTrimmed && compareResourceTrimmed && compareFamilyParsed.valid
      ? {
          area: compareAreaTrimmed,
          family: compareFamilyParsed.value,
          realm: compareRealmTrimmed,
          resource: compareResourceTrimmed,
        }
      : null;

  const compareTargetReady =
    Boolean(compareRealmTrimmed && compareAreaTrimmed && compareResourceTrimmed) &&
    compareFamilyParsed.valid;
  const compareHasInput =
    compareRealmTrimmed != null ||
    compareAreaTrimmed != null ||
    compareResourceTrimmed != null ||
    compareFamilyValue.trim().length > 0;

  const compareHint =
    compareFamilyValue.trim().length > 0 && !compareFamilyParsed.valid
      ? "Family must be a non-negative integer if provided."
      : compareHasInput && !compareTargetReady
        ? "Target realm, area, and resource are required to compare."
        : null;

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
  const stateSummary = data ? describeQueueState(data.detail, compareTarget) : null;
  const timelineColumns: readonly VirtualTableColumn<QueueResourceTimelineEvent>[] = [
    {
      id: "kind",
      header: "Kind",
      width: "14%",
      cellComponent: ({ row }) => (
        <span class="domain-table-cell-truncate" title={formatTimelineKind(row.kind)}>
          {formatTimelineKind(row.kind)}
        </span>
      ),
    },
    {
      id: "summary",
      header: "Summary",
      width: "30%",
      cellComponent: ({ row }) => (
        <span class="domain-table-cell-truncate" title={row.summary}>
          {row.summary}
        </span>
      ),
    },
    {
      id: "observed",
      header: "Observed",
      width: "18%",
      cellComponent: ({ row }) => (
        <span class="domain-table-cell-truncate" title={row.observedAt}>
          {row.observedAt}
        </span>
      ),
    },
    {
      id: "age",
      header: "Age",
      width: "12%",
      cellComponent: ({ row }) => (
        <span>{row.ageSeconds == null ? "Unknown" : humanizeSeconds(row.ageSeconds)}</span>
      ),
    },
    {
      id: "context",
      header: "Context",
      width: "26%",
      cellComponent: ({ row }) => {
        const timelineContext = formatTimelineContext(row);

        return (
          <div class="queue-timeline-context">
            {timelineContext.length > 0 ? (
              <For each={timelineContext.slice(0, 2)} by={(line) => line}>
                {(line) => <span title={line}>{line}</span>}
              </For>
            ) : (
              <span>Context unavailable</span>
            )}
          </div>
        );
      },
    },
  ];

  const headerStatus = {
    detail: stateSummary?.detail ?? "Inspecting queue state and comparison tools.",
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

  const sidebar = createDomainSidebar({
    data,
    title: "Scope summary",
    description: scopeLabel,
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
        note: "Point-in-time snapshot",
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
            eyebrow="Queue resource"
            title="Queue resource inspection"
            description={`${scopeLabel}`}
            primaryAction={{
              label: "Refresh resource",
              onPress: () => resourceQuery.refresh(),
            }}
            status={headerStatus}
          />

          {resourceQuery.loading ? (
            <QueryLoadingState description="Loading queue resource..." />
          ) : null}

          {resourceQueryError ? (
            <QueryErrorState error={resourceQueryError} onRetry={() => resourceQuery.refresh()} />
          ) : null}

          {actionError ? <QueryErrorState error={actionError} /> : null}
        </Stack>
      </DomainPageFrame>
    );
  }

  const current = data;

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

    if (!compareTargetReady) {
      return;
    }

    const nextQuery = new URLSearchParams();

    if (compareRealmTrimmed) {
      nextQuery.set("againstRealm", compareRealmTrimmed);
    }

    if (compareAreaTrimmed) {
      nextQuery.set("againstArea", compareAreaTrimmed);
    }

    if (compareResourceTrimmed) {
      nextQuery.set("againstResource", compareResourceTrimmed);
    }

    if (compareFamilyParsed.value != null) {
      nextQuery.set("againstFamily", String(compareFamilyParsed.value));
    }

    navigate(`${domainResourceHref("queue", resourceRef)}?${nextQuery.toString()}`);
  }

  return (
    <DomainPageFrame sidebar={sidebar}>
      <Stack gap="3">
        <DomainHeader
          eyebrow="Queue resource"
          title="Queue resource inspection"
          description={`${scopeLabel}. Inspect live and historical signals for this queue resource.`}
          primaryAction={{
            label: "Refresh resource",
            onPress: () => resourceQuery.refresh(),
          }}
          status={headerStatus}
        />

        {resourceQuery.refreshing ? (
          <QueryRefreshingState description="Refreshing queue resource..." />
        ) : null}

        {resourceQuery.loading ? (
          <QueryLoadingState description="Loading queue resource..." />
        ) : null}

        {resourceQueryError ? (
          <QueryErrorState error={resourceQueryError} onRetry={() => resourceQuery.refresh()} />
        ) : null}

        {actionError ? <QueryErrorState error={actionError} /> : null}

        <Stack gap="3">
          <DomainMetricTable
            title="Current values"
            description="Point-in-time queue counters for this scope."
            metrics={[
              { label: "Ready", value: current.detail.messagesReady },
              {
                label: "Delayed",
                value: current.detail.messagesDelayed,
                caption:
                  current.detail.messagesDelayed > 0 ? "Delayed messages visible" : undefined,
              },
              { label: "Inflight", value: current.detail.messagesInflight },
              {
                label: "Dead letters",
                value: current.detail.messagesDeadLettered,
                caption:
                  current.detail.messagesDeadLettered > 0
                    ? "Needs action before work resumes"
                    : undefined,
              },
              { label: "Total messages", value: current.detail.messagesTotal },
              {
                label: "Oldest age",
                value: humanizeSeconds(current.detail.oldestMessageAgeSeconds),
                caption: "Live snapshot",
              },
            ]}
          />

          <Card variant="raised">
            <CardHeader>
              <CardTitle>Compare scopes</CardTitle>
              <CardDescription>
                Enter target realm, area, and resource values. Family is optional.
              </CardDescription>
            </CardHeader>
            <CardContent>
              <Stack gap="3">
                <form onSubmit={onCompareSubmit}>
                  <div class="form-grid">
                    <div class="auth-field">
                      <Label for="compare-realm">Target realm</Label>
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
                      <Label for="compare-area">Target area</Label>
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
                      <Label for="compare-resource">Target resource</Label>
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
                      <Label for="compare-family">Target family (optional)</Label>
                      <Input
                        id="compare-family"
                        value={compareFamilyValue}
                        onInput={(event: Event) =>
                          setCompareFamilyInput((event.target as HTMLInputElement).value)
                        }
                        placeholder="2"
                      />
                    </div>
                  </div>

                  {compareHint ? <p class="domain-muted">{compareHint}</p> : null}

                  <Flex gap="2" wrap="wrap">
                    <Button type="submit" disabled={!compareTargetReady}>
                      Compare scope
                    </Button>
                    <Button
                      type="button"
                      variant="outline"
                      onPress={() => {
                        setCompareRealmInput("");
                        setCompareAreaInput("");
                        setCompareResourceInput("");
                        setCompareFamilyInput("");
                        navigate(domainResourceHref("queue", resourceRef));
                      }}
                    >
                      Clear comparison
                    </Button>
                  </Flex>
                </form>

                {compareTarget ? (
                  <QueueResourceComparisonResults
                    resourceRef={resourceRef}
                    compareTarget={compareTarget}
                  />
                ) : (
                  <QueryEmptyState
                    title="No comparison active"
                    description="Enter a target realm, area, and resource. Family is optional."
                  />
                )}
              </Stack>
            </CardContent>
          </Card>

          <Card variant="raised">
            <CardHeader>
              <Flex justify="between" gap="3" align="start" wrap="wrap">
                <Stack gap="1">
                  <CardTitle>Inflight</CardTitle>
                  <CardDescription>
                    Messages currently owned by active queue sessions.
                  </CardDescription>
                </Stack>
                <Badge variant="info">{current.inflight.length} entries</Badge>
              </Flex>
            </CardHeader>

            <CardContent>
              {current.inflight.length === 0 ? (
                <QueryEmptyState description="No inflight messages are visible for this resource." />
              ) : (
                <QueueInflightTable messages={current.inflight} />
              )}
            </CardContent>
          </Card>

          <Card variant="raised">
            <CardHeader>
              <Flex justify="between" gap="3" align="start" wrap="wrap">
                <Stack gap="1">
                  <CardTitle>Dead letters</CardTitle>
                  <CardDescription>
                    Messages that need explicit replay or purge decisions.
                  </CardDescription>
                </Stack>
                <Badge variant={current.deadLetters.length > 0 ? "warning" : "success"}>
                  {current.deadLetters.length} messages
                </Badge>
              </Flex>
            </CardHeader>

            <CardContent>
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
            </CardContent>
          </Card>

          <Card variant="raised">
            <CardHeader>
              <Flex justify="between" gap="3" align="start" wrap="wrap">
                <Stack gap="1">
                  <CardTitle>Timeline</CardTitle>
                  <CardDescription>
                    {current.timeline.derived
                      ? "Derived timeline built from surrounding evidence."
                      : "Live queue transitions observed for this resource."}
                  </CardDescription>
                </Stack>
                <Badge variant={current.timeline.derived ? "info" : "success"}>
                  {current.timeline.derived ? "Derived" : "Live"}
                </Badge>
              </Flex>
            </CardHeader>

            <CardContent>
              {current.timeline.events.length === 0 ? (
                <QueryEmptyState
                  title={current.timeline.derived ? "Derived timeline" : "Live timeline"}
                  description="No recent transitions are visible for this resource. Use current metrics for context."
                />
              ) : (
                <VirtualTable<QueueResourceTimelineEvent>
                  aria-label="Queue resource timeline"
                  class="queue-resource-virtual-table"
                  columns={timelineColumns}
                  getKey={(event) => `${event.observedAt}:${event.summary}`}
                  headerHeight={44}
                  overscan={8}
                  rowHeight={56}
                  rows={current.timeline.events}
                  style={{
                    height: `${Math.min(456, Math.max(156, 44 + current.timeline.events.length * 56))}px`,
                  }}
                />
              )}
            </CardContent>
          </Card>
        </Stack>

        <Dialog
          open={confirmationMessage != null}
          modal
          onOpenChange={(open) => {
            if (!open && !actionPending) {
              setConfirmKind(null);
              setConfirmMessage(null);
            }
          }}
        >
          <DialogPortal>
            <DialogOverlay class="dialog-overlay" />
            {confirmationMessage ? (
              <DialogContent class="dialog-content" role="alertdialog">
                <DialogTitle>
                  {confirmationKind === "replay"
                    ? "Replay dead-letter message?"
                    : "Purge dead-letter message?"}
                </DialogTitle>

                <DialogDescription>
                  {confirmationKind === "replay"
                    ? `Replay message ${confirmationMessage.messageId} in ${scopeLabel}.`
                    : `Purge message ${confirmationMessage.messageId} from ${scopeLabel}. This is permanent.`}
                </DialogDescription>

                <Flex gap="2" justify="end" wrap="wrap">
                  <DialogClose asChild>
                    <Button variant="secondary" type="button" disabled={actionPending}>
                      Cancel
                    </Button>
                  </DialogClose>

                  <Button
                    variant={confirmationKind === "purge" ? "destructive" : undefined}
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
                        ? "Replay message"
                        : "Purge message"}
                  </Button>
                </Flex>
              </DialogContent>
            ) : null}
          </DialogPortal>
        </Dialog>
      </Stack>
    </DomainPageFrame>
  );
}
