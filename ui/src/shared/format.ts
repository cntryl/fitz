export function formatNumber(value: number) {
  return new Intl.NumberFormat("en-US").format(value);
}

export function formatDisplayValue(value: string | number) {
  return typeof value === "number" ? formatNumber(value) : value;
}

export function formatTimestamp(value?: string) {
  if (!value) {
    return "Unknown";
  }

  const date = new Date(value);

  if (Number.isNaN(date.getTime())) {
    return value;
  }

  return date.toLocaleString();
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
