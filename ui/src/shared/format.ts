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
