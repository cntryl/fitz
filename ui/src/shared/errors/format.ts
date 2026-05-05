export function formatUnknownError(value: unknown) {
  if (value instanceof Error) {
    return value.message;
  }

  if (typeof value === "string") {
    return value;
  }

  if (typeof value === "number" || typeof value === "boolean" || typeof value === "bigint") {
    return `${value}`;
  }

  if (value == null) {
    return "Unknown error";
  }

  try {
    return JSON.stringify(value);
  } catch {
    return "Unknown error";
  }
}
