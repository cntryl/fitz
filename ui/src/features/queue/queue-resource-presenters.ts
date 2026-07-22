import type {
  QueueResourceDetail,
  QueueResourceRef,
  QueueResourceTimelineEvent,
} from "@/features/queue/queue-resource-models";

export type QueueStateTone = "info" | "success" | "warning" | "danger";

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

export function formatQueueScope(
  scope: Pick<QueueResourceRef, "area" | "realm" | "resource"> & {
    family?: number | null;
  },
) {
  const base = `${scope.realm} / ${scope.area} / ${scope.resource}`;
  return scope.family == null ? base : `${base} / family ${scope.family}`;
}

export function describeQueueState(detail: QueueResourceDetail): {
  detail: string;
  label: string;
  tone: QueueStateTone;
} {
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
  if (detail.messagesDeadLettered > 0) {
    return {
      detail: `${snapshotSentence} ${ageSentence} Dead letters need attention.`,
      label: "Attention",
      tone: "danger",
    };
  }

  if (detail.messagesDelayed > 0) {
    return {
      detail: `${snapshotSentence} ${ageSentence} Delayed work is visible.`,
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
      detail: `${snapshotSentence} ${ageSentence}`,
      label: "Healthy",
      tone: "success",
    };
  }

  return {
    detail: `${snapshotSentence} ${ageSentence} The queue is active and moving messages.`,
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
