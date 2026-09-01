import { formatRelativeTime, formatTimestamp } from "@/shared/format";

export function decodeScheduleParam(value: string | undefined) {
  if (!value) return "";

  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

export function formatScheduleTimestamp(value?: string | null) {
  return value ? formatTimestamp(value) : "--";
}

export function formatScheduleTiming(value?: string | null, reference = Date.now()) {
  if (!value) return "No next run scheduled";

  const timestamp = new Date(value).getTime();
  if (Number.isNaN(timestamp)) return value;

  const relative = formatRelativeTime(value, reference);
  return timestamp >= reference ? `Next run ${relative}` : `Scheduled run was ${relative}`;
}

export function scheduleTimingMetric(value?: string | null) {
  if (!value) {
    return {
      label: "Next run",
      value: "No next run scheduled",
    };
  }

  const timestamp = new Date(value).getTime();
  if (Number.isNaN(timestamp)) {
    return {
      label: "Scheduled value",
      value,
    };
  }

  return {
    caption: formatScheduleTiming(value),
    label: timestamp >= Date.now() ? "Next run" : "Scheduled run",
    value: formatTimestamp(value),
  };
}
