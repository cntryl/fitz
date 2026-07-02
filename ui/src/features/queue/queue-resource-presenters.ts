import type {
  QueueResourceDetail,
  QueueResourceRef,
  QueueResourceTimelineEvent,
} from "@/features/queue/queue-resource-models";

export interface QueueComparisonTarget extends QueueResourceRef {
  family: number | null;
}

export interface ParsedQueueFamily {
  valid: boolean;
  value: number | null;
}

export type QueueStateTone = "info" | "success" | "warning" | "danger";

export function trimmedOrNull(value: string) {
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

export function parseFamilyInput(value: string): ParsedQueueFamily {
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

export function humanizeSeconds(seconds: number) {
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

export function formatRate(value: number) {
  return value.toFixed(2);
}

export function formatStatus(status: QueueResourceDetail["status"]) {
  switch (status) {
    case "falling_behind":
      return "Falling behind";
    case "backlogged":
      return "Backlogged";
    case "draining":
      return "Draining";
    case "idle":
      return "Idle";
  }
}

export function formatTimelineKind(kind: string) {
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

export function formatComparisonValue(value: number | null | undefined) {
  if (value == null) {
    return "n/a";
  }

  if (value === 0) {
    return "0";
  }

  return value > 0 ? `+${value}` : `${value}`;
}

export function formatQueueScope(
  scope: Pick<QueueComparisonTarget, "area" | "realm" | "resource"> & {
    family?: number | null;
  },
) {
  const base = `${scope.realm} / ${scope.area} / ${scope.resource}`;
  return scope.family == null ? base : `${base} / family ${scope.family}`;
}

export function describeQueueState(
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

export function formatTimelineContext(event: QueueResourceTimelineEvent) {
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
