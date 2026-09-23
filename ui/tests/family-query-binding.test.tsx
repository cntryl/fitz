import { afterEach, describe, expect, it } from "vite-plus/test";
import { getDefaultDataRuntime, queryScope } from "@askrjs/askr/data";
import { serviceContractMocks } from "./service-contract/setup";

const mocks = serviceContractMocks();

afterEach(() => {
  window.history.pushState({}, "", "/");
});

describe("family-scoped query binding", () => {
  it("keeps a cached family A inventory in family A when refreshed from a family B URL", async () => {
    const { createResourceInventoryQuery } = await import("@/features/resource/resource-query");
    const runtime = getDefaultDataRuntime();
    const keyForA = queryScope("resource").key("inventory", "kv", "7");
    const keyForB = queryScope("resource").key("inventory", "kv", "8");
    window.history.pushState({}, "", "/admin/7/kv");
    const queryA = createResourceInventoryQuery("kv");
    await queryA.refresh();
    expect(queryA.data?.realms[0]?.realm).toBe("default");
    expect(runtime.queryCache.has(keyForA)).toBe(true);

    window.history.pushState({}, "", "/admin/8/kv");
    await queryA.refresh();

    expect(runtime.queryCache.has(keyForA)).toBe(true);
    expect(runtime.queryCache.has(keyForB)).toBe(false);
    expect(mocks.apiv1.listKvRealms).toHaveBeenLastCalledWith(
      expect.objectContaining({ params: { family: "7" } }),
    );
    expect(mocks.apiv1.listKvAreas).toHaveBeenLastCalledWith(
      expect.objectContaining({ params: { family: "7", realm: "default" } }),
    );
    expect(mocks.apiv1.listKvResources).toHaveBeenLastCalledWith(
      expect.objectContaining({
        params: { area: "ops", family: "7", realm: "default" },
      }),
    );
  });
  it("keeps a Queue resource request in its captured family after navigation", async () => {
    const { createQueueResourceQuery } = await import("@/features/queue/queue-resource-query");
    window.history.pushState({}, "", "/admin/7/queue/default/ops/primary");
    const queryA = createQueueResourceQuery({
      area: "ops",
      realm: "default",
      resource: "primary",
    });
    window.history.pushState({}, "", "/admin/8/queue/default/ops/primary");

    await queryA.refresh();

    expect(mocks.apiv1.getQueueResource).toHaveBeenLastCalledWith(
      expect.objectContaining({
        params: { area: "ops", family: "7", realm: "default", resource: "primary" },
      }),
    );
  });
});
