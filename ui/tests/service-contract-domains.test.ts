import { describe, expect, it } from "vite-plus/test";
import { serviceContractMocks } from "./service-contract/setup";

const mocks = serviceContractMocks();
const params = (value: Record<string, unknown>) => ({ params: value });
const paramsQuery = (paramsValue: Record<string, unknown>, query: Record<string, unknown>) => ({
  params: paramsValue,
  query,
});

describe("service endpoint contracts", () => {
  it("loads stream records through route-family scoped stream endpoints", async () => {
    const { streamService } = await import("@/features/stream/stream-service");

    await streamService.searchRecords({
      area: "ops",
      discriminator: "invoice",
      fromOffset: 3,
      limit: 10,
      realm: "default",
      resource: "events",
      routeFamily: 7,
    });
    await streamService.readResourceRecords({
      area: "ops",
      discriminator: "invoice",
      fromOffset: 0,
      limit: 10,
      realm: "default",
      resource: "events",
      routeFamily: 7,
    });

    expect(mocks.apiv1.searchStreamRecords).toHaveBeenCalledWith(
      paramsQuery(
        { family: "7" },
        {
          area: "ops",
          discriminator: "invoice",
          from_offset: 3,
          limit: 10,
          realm: "default",
          resource: "events",
        },
      ),
    );
    expect(mocks.apiv1.readStreamResourceRecords).toHaveBeenCalledWith(
      paramsQuery(
        { area: "ops", family: "7", realm: "default", resource: "events" },
        {
          discriminator: "invoice",
          from_offset: 0,
          limit: 10,
        },
      ),
    );
  });
  it("loads schedule execution and missed handoff observations through schedule endpoints", async () => {
    const { scheduleService } = await import("@/features/schedule/schedule-service");

    await scheduleService.listExecutionObservations({
      area: "ops",
      limit: 10,
      realm: "default",
      resource: "reconcile",
      routeFamily: 7,
    });
    await scheduleService.searchMissedHandoffs({
      area: "ops",
      limit: 10,
      realm: "default",
      resource: "reconcile",
      routeFamily: 7,
    });

    expect(mocks.apiv1.listScheduleExecutionObservations).toHaveBeenCalledWith(
      paramsQuery(
        { area: "ops", family: "7", realm: "default", resource: "reconcile" },
        {
          limit: 10,
        },
      ),
    );
    expect(mocks.apiv1.searchScheduleMissedHandoffs).toHaveBeenCalledWith(
      paramsQuery(
        { family: "7" },
        {
          area: "ops",
          limit: 10,
          realm: "default",
          resource: "reconcile",
        },
      ),
    );
  });
  it("loads schedule hierarchy and resource detail through schedule endpoints", async () => {
    const { scheduleService } = await import("@/features/schedule/schedule-service");

    await scheduleService.listScheduleAreas("default");
    await scheduleService.listScheduleResources("default", "ops");
    await scheduleService.getScheduleResource({
      area: "ops",
      limit: 10,
      realm: "default",
      resource: "reconcile",
      routeFamily: 7,
    });

    expect(mocks.apiv1.listScheduleAreas).toHaveBeenCalledWith(
      params({ family: "1", realm: "default" }),
    );
    expect(mocks.apiv1.listScheduleResources).toHaveBeenCalledWith(
      params({ area: "ops", family: "1", realm: "default" }),
    );
    expect(mocks.apiv1.getScheduleResource).toHaveBeenCalledWith(
      params({ area: "ops", family: "7", realm: "default", resource: "reconcile" }),
    );
    expect(mocks.apiv1.listScheduleExecutionObservations).toHaveBeenCalledWith(
      paramsQuery(
        { area: "ops", family: "7", realm: "default", resource: "reconcile" },
        { limit: 10 },
      ),
    );
    expect(mocks.apiv1.searchScheduleMissedHandoffs).toHaveBeenCalledWith(
      paramsQuery(
        { family: "7" },
        {
          area: "ops",
          limit: 10,
          realm: "default",
          resource: "reconcile",
        },
      ),
    );
  });
  it("loads lease ownership searches through the lease search endpoint", async () => {
    const { leaseService } = await import("@/features/lease/lease-service");

    await leaseService.searchOwnership({
      area: "ops",
      limit: 10,
      owner: "worker-1",
      realm: "default",
      resource: "settlement",
      routeFamily: 7,
      state: "contention",
    });

    expect(mocks.apiv1.searchLeaseOwnership).toHaveBeenCalledWith(
      paramsQuery(
        { family: "7" },
        {
          area: "ops",
          limit: 10,
          owner: "worker-1",
          realm: "default",
          resource: "settlement",
          state: "contention",
        },
      ),
    );
  });
  it("loads communication evidence through notice delivery and RPC call endpoints", async () => {
    const { noticeService } = await import("@/features/notice/notice-service");
    const { rpcService } = await import("@/features/rpc/rpc-service");

    await noticeService.searchDeliveries({
      area: "ops",
      limit: 10,
      query: "events",
      realm: "default",
      resource: "events",
      routeFamily: 7,
    });
    await rpcService.searchCalls({
      area: "ops",
      correlationId: "corr-1",
      limit: 10,
      operation: "sync",
      query: "profile",
      realm: "default",
      resource: "profile",
      routeFamily: 7,
    });

    expect(mocks.apiv1.searchNoticeDeliveries).toHaveBeenCalledWith(
      paramsQuery(
        { family: "7" },
        {
          area: "ops",
          limit: 10,
          q: "events",
          realm: "default",
          resource: "events",
        },
      ),
    );
    expect(mocks.apiv1.searchRpcCalls).toHaveBeenCalledWith(
      paramsQuery(
        { family: "7" },
        {
          area: "ops",
          correlation_id: "corr-1",
          limit: 10,
          operation: "sync",
          q: "profile",
          realm: "default",
          resource: "profile",
        },
      ),
    );
  });
  it("loads notice scoped inventory and operation delivery detail with route-family scope", async () => {
    const { noticeService } = await import("@/features/notice/notice-service");

    await noticeService.listNoticeAreas("default", {
      routeFamily: 7,
    });
    await noticeService.listNoticeResources("default", "ops", {
      routeFamily: 7,
    });
    await noticeService.searchResourceRows(
      {
        area: "ops",
        limit: 50,
        realm: "default",
        resource: "primary",
        routeFamily: 7,
      },
      {},
    );
    await noticeService.searchOperationRows(
      {
        area: "ops",
        limit: 25,
        operation: "GetStatus",
        realm: "default",
        resource: "primary",
        routeFamily: 7,
      },
      {},
    );

    expect(mocks.apiv1.listNoticeAreas).toHaveBeenCalledWith(
      params({ family: "7", realm: "default" }),
    );
    expect(mocks.apiv1.listNoticeResources).toHaveBeenCalledWith(
      params({ area: "ops", family: "7", realm: "default" }),
    );
    expect(mocks.apiv1.searchNoticeDeliveries).toHaveBeenCalledWith(
      paramsQuery(
        { family: "7" },
        {
          area: "ops",
          limit: 50,
          q: undefined,
          realm: "default",
          resource: "primary",
        },
      ),
    );
    expect(mocks.apiv1.searchNoticeDeliveries).toHaveBeenCalledWith(
      paramsQuery(
        { family: "7" },
        {
          area: "ops",
          limit: 25,
          q: "GetStatus",
          realm: "default",
          resource: "primary",
        },
      ),
    );
  });
  it("loads RPC scoped inventory, resource operations, and operation evidence", async () => {
    const { rpcService } = await import("@/features/rpc/rpc-service");

    window.history.pushState({}, "", "/admin/7/rpc/default/ops/primary/GetStatus");
    try {
      await rpcService.listRpcAreas("default");
      await rpcService.listRpcResources("default", "ops");
      await rpcService.getResourceOperations("default", "ops", "primary");
      await rpcService.getOperationView({
        area: "ops",
        limit: 25,
        operation: "GetStatus",
        realm: "default",
        resource: "primary",
        routeFamily: 7,
      });
    } finally {
      window.history.pushState({}, "", "/");
    }

    expect(mocks.apiv1.listRpcAreas).toHaveBeenCalledWith(
      params({ family: "7", realm: "default" }),
    );
    expect(mocks.apiv1.listRpcResources).toHaveBeenCalledWith(
      params({ area: "ops", family: "7", realm: "default" }),
    );
    expect(mocks.apiv1.getRpcResource).toHaveBeenCalledWith(
      params({ area: "ops", family: "7", realm: "default", resource: "primary" }),
    );
    expect(mocks.apiv1.getRpcOperation).toHaveBeenCalledWith(
      params({
        area: "ops",
        family: "7",
        operation: "GetStatus",
        realm: "default",
        resource: "primary",
      }),
    );
    expect(mocks.apiv1.searchRpcCalls).toHaveBeenCalledWith(
      paramsQuery(
        { family: "7" },
        {
          area: "ops",
          correlation_id: undefined,
          limit: 25,
          operation: "GetStatus",
          q: undefined,
          realm: "default",
          resource: "primary",
        },
      ),
    );
  });
  it("loads Stream rollups and resource records through scoped stream endpoints", async () => {
    const { streamService } = await import("@/features/stream/stream-service");

    window.history.pushState({}, "", "/admin/7/stream/default/ops/events");
    try {
      await streamService.getRealmRollup("default");
      await streamService.getAreaRollup("default", "ops");
      await streamService.getResourceView({
        area: "ops",
        discriminator: "invoice",
        fromOffset: 4,
        limit: 25,
        realm: "default",
        resource: "events",
        routeFamily: 7,
      });
    } finally {
      window.history.pushState({}, "", "/");
    }

    expect(mocks.apiv1.listStreamAreas).toHaveBeenCalledWith(
      params({ family: "7", realm: "default" }),
    );
    expect(mocks.apiv1.listStreamResources).toHaveBeenCalledWith(
      params({ area: "ops", family: "7", realm: "default" }),
    );
    expect(mocks.apiv1.getStreamRealmWatermarks).toHaveBeenCalledWith(
      params({ family: "7", realm: "default" }),
    );
    expect(mocks.apiv1.getStreamAreaWatermarks).toHaveBeenCalledWith(
      params({ area: "ops", family: "7", realm: "default" }),
    );
    expect(mocks.apiv1.getStreamResource).toHaveBeenCalledWith(
      params({ area: "ops", family: "7", realm: "default", resource: "events" }),
    );
    expect(mocks.apiv1.readStreamResourceRecords).toHaveBeenCalledWith(
      paramsQuery(
        { area: "ops", family: "7", realm: "default", resource: "events" },
        {
          discriminator: "invoice",
          from_offset: 4,
          limit: 25,
        },
      ),
    );
  });
  it("loads global admin search through the search endpoint with route-family scope", async () => {
    const { searchService } = await import("@/features/search/search-service");

    await expect(
      searchService.searchAdminState({
        area: "ops",
        domain: "queue",
        limit: 10,
        query: "settlement",
        realm: "default",
        resource: "settlement",
        routeFamily: "7",
      }),
    ).resolves.toMatchObject({
      results: [{ routeFamily: "7", title: "settlement" }],
      routeFamily: "7",
      total: 1,
    });

    expect(mocks.apiv1.searchAdminState).toHaveBeenCalledWith({
      query: {
        area: "ops",
        domain: "queue",
        limit: 10,
        operation: undefined,
        q: "settlement",
        realm: "default",
        resource: "settlement",
        route_family: "7",
      },
    });
  });
});
