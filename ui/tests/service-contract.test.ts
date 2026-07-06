import { describe, expect, it } from "vite-plus/test";
import { serviceContractMocks } from "./service-contract/setup";

const mocks = serviceContractMocks();

describe("service endpoint contracts", () => {
  it("fans out generic resource inventory requests through the domain endpoints", async () => {
    const { resourceService } = await import("@/features/resource/resource-service");

    await expect(resourceService.getResourceInventory("kv")).resolves.toEqual({
      domain: "kv",
      realms: [
        {
          areas: [
            {
              area: "ops",
              resourceEntries: [
                {
                  resource: "primary",
                },
              ],
              resources: ["primary"],
            },
          ],
          realm: "default",
        },
      ],
    });

    expect(mocks.apiv1.listKvRealms).toHaveBeenCalledTimes(1);
    expect(mocks.apiv1.listKvAreas).toHaveBeenCalledWith("1", "default", {});
    expect(mocks.apiv1.listKvResources).toHaveBeenCalledWith("1", "default", "ops", {});
  });
  it("fans out generic resource detail requests through detail, timeline, and related endpoints", async () => {
    const { resourceService } = await import("@/features/resource/resource-service");

    await resourceService.getResource(
      "kv",
      { area: "ops", realm: "default", resource: "primary" },
      null,
    );

    expect(mocks.apiv1.getKvResource).toHaveBeenCalledWith("1", "default", "ops", "primary", {});
    expect(mocks.apiv1.listKvResourceEvents).toHaveBeenCalledWith(
      "1",
      "default",
      "ops",
      "primary",
      { limit: 20 },
      {},
    );
    expect(mocks.apiv1.listKvTransactions).toHaveBeenCalledWith(
      "1",
      "default",
      "ops",
      "primary",
      {},
    );
  });
  it("loads committed KV exact key reads through the committed value endpoint", async () => {
    const { kvService } = await import("@/features/kv/kv-service");

    await expect(
      kvService.getCommittedValue(
        { area: "ops", realm: "default", resource: "primary", routeFamily: 7 },
        "user:1",
        "utf8",
      ),
    ).resolves.toMatchObject({
      found: true,
      routeFamily: 7,
      value: { utf8: "alice" },
    });

    expect(mocks.apiv1.getKvCommittedValue).toHaveBeenCalledWith(
      "7",
      "default",
      "ops",
      "primary",
      {
        key: "user:1",
        key_encoding: "utf8",
      },
      {},
    );
  });
  it("loads committed KV prefix scans through the prefix endpoint", async () => {
    const { kvService } = await import("@/features/kv/kv-service");

    await expect(
      kvService.scanCommittedPrefix(
        { area: "ops", realm: "default", resource: "primary", routeFamily: 7 },
        "user:",
        "utf8",
        25,
      ),
    ).resolves.toMatchObject({
      hasMore: false,
      items: [{ key: { utf8: "user:1" }, value: { utf8: "alice" } }],
      routeFamily: 7,
    });

    expect(mocks.apiv1.scanKvCommittedPrefix).toHaveBeenCalledWith(
      "7",
      "default",
      "ops",
      "primary",
      {
        key_encoding: "utf8",
        limit: 25,
        prefix: "user:",
      },
      {},
    );
  });
  it("loads queue resource detail from the expected queue resource endpoints", async () => {
    const { queueResourceService } = await import("@/features/queue/queue-resource-service");

    await queueResourceService.getResource({
      area: "ops",
      realm: "default",
      resource: "primary",
    });

    expect(mocks.apiv1.getQueueResource).toHaveBeenCalledWith("1", "default", "ops", "primary", {});
    expect(mocks.apiv1.listQueueInflightEntries).toHaveBeenCalledWith(
      "1",
      "default",
      "ops",
      "primary",
      {},
    );
    expect(mocks.apiv1.listQueueDeadLetters).toHaveBeenCalledWith(
      "1",
      "default",
      "ops",
      "primary",
      {},
    );
    expect(mocks.apiv1.listQueueResourceEvents).toHaveBeenCalledWith(
      "1",
      "default",
      "ops",
      "primary",
      { limit: 8 },
      {},
    );
  });
  it("loads queue drill-down rollups from queue-specific endpoints", async () => {
    const { queueService } = await import("@/features/queue/queue-service");

    await expect(queueService.getOverview()).resolves.toMatchObject({
      realms: [{ realm: "default", status: "falling_behind", subscriptionsActive: 2 }],
    });
    await expect(queueService.getRealm("default")).resolves.toMatchObject({
      areas: [{ area: "ops" }],
      queues: [{ resource: "primary" }],
      realm: "default",
    });
    await expect(queueService.getArea("default", "ops")).resolves.toMatchObject({
      area: "ops",
      queues: [{ resource: "primary" }],
      realm: "default",
    });

    expect(mocks.apiv1.listQueueRealms).toHaveBeenCalledWith("1", {});
    expect(mocks.apiv1.getQueueStats).toHaveBeenCalledWith("1", {});
    expect(mocks.apiv1.getQueueRealm).toHaveBeenCalledWith("1", "default", {});
    expect(mocks.apiv1.getQueueArea).toHaveBeenCalledWith("1", "default", "ops", {});
  });
  it("loads lease overview and drill-down inventory through lease-specific scoped endpoints", async () => {
    const { leaseService } = await import("@/features/lease/lease-service");

    window.history.pushState({}, "", "/admin/9/lease");
    try {
      await expect(leaseService.getOverview()).resolves.toMatchObject({
        realms: [{ realm: "default" }],
      });
      await expect(leaseService.listRealmResources("default")).resolves.toMatchObject({
        realm: "default",
        areas: [{ area: "ops" }],
      });
      await expect(leaseService.listAreaResources("default", "ops")).resolves.toMatchObject({
        area: "ops",
        realm: "default",
      });
    } finally {
      window.history.pushState({}, "", "/");
    }

    expect(mocks.apiv1.listLeaseRealms).toHaveBeenCalledWith("9", {});
    expect(mocks.apiv1.getLeaseStats).toHaveBeenCalledWith("9", {});
    expect(mocks.apiv1.listLeaseAreas).toHaveBeenCalledWith("9", "default", {});
    expect(mocks.apiv1.listLeaseResources).toHaveBeenCalledWith("9", "default", "ops", {});
  });
  it("loads lease ownership rows through the search endpoint with active route-family scope", async () => {
    const { leaseService } = await import("@/features/lease/lease-service");

    window.history.pushState({}, "", "/admin/11/lease/default/ops/primary");
    try {
      await leaseService.searchRows({
        area: "ops",
        limit: 10,
        realm: "default",
        resource: "primary",
      });
    } finally {
      window.history.pushState({}, "", "/");
    }

    expect(mocks.apiv1.searchLeaseOwnership).toHaveBeenCalledWith(
      "11",
      {
        area: "ops",
        limit: 10,
        owner: undefined,
        realm: "default",
        resource: "primary",
        state: undefined,
      },
      {},
    );
  });
  it("loads queue resource comparisons from the comparison endpoint", async () => {
    const { queueResourceService } = await import("@/features/queue/queue-resource-service");

    await queueResourceService.compareResource(
      { area: "ops", realm: "default", resource: "primary" },
      { area: "ops", family: 7, realm: "default", resource: "secondary" },
    );

    expect(mocks.apiv1.compareQueueResourceSnapshots).toHaveBeenCalledWith(
      "1",
      "default",
      "ops",
      "primary",
      {
        against_area: "ops",
        against_family: 7,
        against_realm: "default",
        against_resource: "secondary",
      },
      {},
    );
  });
});
