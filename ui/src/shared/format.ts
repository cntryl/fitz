export function formatNumber(value: number) {
  return new Intl.NumberFormat("en-US").format(value);
}

export function formatCount(value: number, singular: string, plural = `${singular}s`) {
  return `${formatNumber(value)} ${value === 1 ? singular : plural}`;
}

export function formatDisplayValue(value: string | number) {
  return typeof value === "number" ? formatNumber(value) : value;
}

function formatDateTime(date: Date) {
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "medium",
  }).format(date);
}

export function formatTimestamp(value?: string) {
  if (!value) {
    return "Unknown";
  }

  const date = new Date(value);

  if (Number.isNaN(date.getTime())) {
    return value;
  }

  return formatDateTime(date);
}

export function formatTimestampMs(value?: number | null) {
  if (value == null || !Number.isFinite(value) || value <= 0) {
    return "Unknown";
  }

  return formatDateTime(new Date(value));
}

export function formatRelativeTime(value?: string, reference = Date.now()) {
  if (!value) {
    return "Unknown";
  }

  const date = new Date(value);

  if (Number.isNaN(date.getTime())) {
    return value;
  }

  const deltaSeconds = Math.round((date.getTime() - reference) / 1000);
  const absoluteSeconds = Math.abs(deltaSeconds);

  if (absoluteSeconds < 45) {
    return deltaSeconds >= 0 ? "in moments" : "moments ago";
  }

  const units = [
    { label: "year", seconds: 60 * 60 * 24 * 365 },
    { label: "month", seconds: 60 * 60 * 24 * 30 },
    { label: "day", seconds: 60 * 60 * 24 },
    { label: "hour", seconds: 60 * 60 },
    { label: "minute", seconds: 60 },
    { label: "second", seconds: 1 },
  ] as const;

  for (const unit of units) {
    if (absoluteSeconds >= unit.seconds || unit.label === "second") {
      const valueInUnit = Math.round(deltaSeconds / unit.seconds);
      const formatter = new Intl.RelativeTimeFormat("en", { numeric: "auto" });
      return formatter.format(valueInUnit, unit.label);
    }
  }

  return formatTimestamp(value);
}

export function formatDurationSeconds(value: number) {
  if (!Number.isFinite(value) || value < 0) {
    return "0s";
  }

  const seconds = Math.floor(value);
  if (seconds < 60) {
    return `${seconds}s`;
  }

  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) {
    return `${minutes}m`;
  }

  const hours = Math.floor(minutes / 60);
  if (hours < 24) {
    return `${hours}h`;
  }

  const days = Math.floor(hours / 24);
  return `${days}d`;
}

export function formatBytes(value: number) {
  if (value < 1024) return `${formatNumber(value)} B`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KiB`;
  if (value < 1024 * 1024 * 1024) return `${(value / (1024 * 1024)).toFixed(1)} MiB`;
  return `${(value / (1024 * 1024 * 1024)).toFixed(1)} GiB`;
}
