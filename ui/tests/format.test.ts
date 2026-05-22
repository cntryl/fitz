import { describe, expect, it } from "vite-plus/test";
import { formatDisplayValue, formatNumber, formatTimestamp } from "@/shared/format";

describe("shared format helpers", () => {
  it("formats display values and numbers", () => {
    expect(formatDisplayValue("live")).toBe("live");
    expect(formatDisplayValue(1234567)).toBe("1,234,567");
    expect(formatNumber(42)).toBe("42");
  });

  it("formats timestamps and preserves invalid values", () => {
    const timestamp = "2026-05-22T12:34:56.000Z";

    expect(formatTimestamp(timestamp)).toBe(new Date(timestamp).toLocaleString());
    expect(formatTimestamp("not-a-date")).toBe("not-a-date");
    expect(formatTimestamp()).toBe("Unknown");
  });
});
