import { state } from "@askrjs/askr";
import { currentRoute, navigate } from "@askrjs/askr/router";
import { Stack } from "@askrjs/themes/components";
import DomainHeader from "@/components/shared/domain-header";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import OperatorScopeStrip from "@/components/shared/operator-scope-strip";
import PageActionBar from "@/components/shared/page-action-bar";
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
import QueueResourceComparePanel from "@/features/queue/queue-resource-compare-panel";
import {
  QueueResourceCurrentValuesPanel,
  QueueResourceDeadLettersPanel,
  QueueResourceInflightPanel,
  QueueResourceTimelinePanel,
} from "@/features/queue/queue-resource-panels";
import { createQueueResourceSidebar } from "@/features/queue/queue-resource-sidebar";
import {
  describeQueueState,
  formatQueueScope,
  parseFamilyInput,
  trimmedOrNull,
} from "@/features/queue/queue-resource-presenters";
import { domainHref, domainResourceHref, domainScopeHref } from "@/shared/navigation/domains";

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
  const resourceRef: QueueResourceRef = { realm, area, resource };
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

  const sidebar = createQueueResourceSidebar({
    data,
    onRefresh: () => resourceQuery.refresh(),
    scopeLabel,
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

    if (typeof window === "undefined" || !compareTargetReady) {
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

  function clearComparison() {
    setCompareRealmInput("");
    setCompareAreaInput("");
    setCompareResourceInput("");
    setCompareFamilyInput("");
    navigate(domainResourceHref("queue", resourceRef));
  }

  function ComparePanel() {
    return (
      <QueueResourceComparePanel
        compareAreaValue={compareAreaValue}
        compareFamilyValue={compareFamilyValue}
        compareHint={compareHint}
        compareRealmValue={compareRealmValue}
        compareResourceValue={compareResourceValue}
        compareTarget={compareTarget}
        compareTargetReady={compareTargetReady}
        onClear={clearComparison}
        onCompareSubmit={onCompareSubmit}
        resourceRef={resourceRef}
        setCompareAreaInput={setCompareAreaInput}
        setCompareFamilyInput={setCompareFamilyInput}
        setCompareRealmInput={setCompareRealmInput}
        setCompareResourceInput={setCompareResourceInput}
      />
    );
  }

  return (
    <DomainPageFrame sidebar={sidebar}>
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
        <PageActionBar
          description="Resolve current backlog first, then review dead letters, live reservations, and recent queue transitions before running comparison."
          actions={[
            { label: "Back to Queue inventory", href: domainHref("queue") },
            {
              label: "Back to area",
              href: domainScopeHref("queue", { area: resourceRef.area, realm: resourceRef.realm }),
            },
          ]}
        />

        {resourceQuery.refreshing && data ? (
          <QueryRefreshingState description="Refreshing queue resource..." />
        ) : null}

        {resourceQuery.loading && !data ? (
          <QueryLoadingState description="Loading queue resource..." />
        ) : null}

        {resourceQueryError && !data ? (
          <QueryErrorState error={resourceQueryError} onRetry={() => resourceQuery.refresh()} />
        ) : null}

        {resourceQueryError && data ? (
          <QueryErrorState error={resourceQueryError} onRetry={() => resourceQuery.refresh()} />
        ) : null}

        {actionError ? <QueryErrorState error={actionError} /> : null}

        {data ? (
          <Stack gap="3">
            <QueueResourceCurrentValuesPanel detail={data.detail} />

            {compareTarget ? <ComparePanel /> : null}

            <QueueResourceDeadLettersPanel
              messages={data.deadLetters}
              onReplay={(message) => openDeadLetterConfirmation("replay", message)}
              onPurge={(message) => openDeadLetterConfirmation("purge", message)}
              pendingAction={actionKind()}
              pendingMessageId={actionMessageId()}
            />
            <QueueResourceInflightPanel messages={data.inflight} />
            <QueueResourceTimelinePanel timeline={data.timeline} />
            {compareTarget ? null : <ComparePanel />}

            <QueueDeadLetterDialog
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
        ) : null}
      </Stack>
    </DomainPageFrame>
  );
}
