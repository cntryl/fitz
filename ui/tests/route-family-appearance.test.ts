import { describe, expect, it } from "vite-plus/test";
import { routeFamilyIconSlot } from "@/shared/route-family-appearance";

describe("route family appearance", () => {
  it("uses a stable identity slot without status semantics", () => {
    expect(routeFamilyIconSlot("7")).toBe("identity-0");
  });
});
