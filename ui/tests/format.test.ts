import { describe, expect, it } from "vite-plus/test";
import {
  formatDisplayValue,
  formatDurationSeconds,
  formatNumber,
  formatTimestamp,
  formatTimestampMs,
} from "@/shared/format";

describe("shared format helpers", () => {
  it("formats display values and numbers", () => {
    expect(formatDisplayValue("live")).toBe("live");
    expect(formatDisplayValue(1234567)).toBe("1,234,567");
    expect(formatNumber(42)).toBe("42");
  });

  it("formats timestamps and preserves invalid values", () => {
    const timestamp = "2026-05-22T12:34:56.000Z";
    const expected = new Intl.DateTimeFormat(undefined, {
      dateStyle: "medium",
      timeStyle: "medium",
    }).format(new Date(timestamp));

    expect(formatTimestamp(timestamp)).toBe(expected);
    expect(formatTimestampMs(new Date(timestamp).getTime())).toBe(expected);
    expect(formatTimestampMs()).toBe("Unknown");
    expect(formatTimestamp("not-a-date")).toBe("not-a-date");
    expect(formatTimestamp()).toBe("Unknown");
  });

  it("formats durations into readable units", () => {
    expect(formatDurationSeconds(0)).toBe("0s");
    expect(formatDurationSeconds(42)).toBe("42s");
    expect(formatDurationSeconds(125)).toBe("2m");
    expect(formatDurationSeconds(3700)).toBe("1h");
    expect(formatDurationSeconds(172_800)).toBe("2d");
  });
});
