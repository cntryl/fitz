import { describe, expect, it } from "vite-plus/test";
import {
  queueOverviewQueryKey,
  queueAreaQueryKey,
  queueDeadLettersQueryKey,
  queueDeadLettersQueryPrefix,
  queueRealmQueryKey,
} from "@/features/queue/queue-query";
import {
  queueResourceQueryKey,
  queueResourceInflightQueryKey,
  queueResourceTimelineQueryKey,
} from "@/features/queue/queue-resource-query";
import {
  leaseAreaQueryKey,
  leaseRealmQueryKey,
  leaseResourceRowsQueryKey,
} from "@/features/lease/lease-query";
import {
  noticeAreaQueryKey,
  noticeOperationRowsQueryKey,
  noticeRealmQueryKey,
  noticeResourceRowsQueryKey,
} from "@/features/notice/notice-query";
import {
  rpcAreaQueryKey,
  rpcOperationQueryKey,
  rpcRealmQueryKey,
  rpcResourceQueryKey,
} from "@/features/rpc/rpc-query";
import {
  scheduleAreaQueryKey,
  scheduleExecutionObservationsQueryKey,
  scheduleMissedHandoffsQueryKey,
  scheduleOperationQueryKey,
  scheduleRealmQueryKey,
  scheduleResourceQueryKey,
} from "@/features/schedule/schedule-query";
import {
  streamAreaQueryKey,
  streamRealmQueryKey,
  streamResourceQueryKey,
} from "@/features/stream/stream-query";
import { affectedQueueKeys } from "@/features/queue/queue-actions";

describe("Data query layer", () => {
  it("builds prefix-friendly queue mutation affected keys", () => {
    const ref = {
      area: "ops",
      realm: "default",
      resource: "primary",
    };

    expect(queueResourceTimelineQueryKey(ref).startsWith(queueResourceQueryKey(ref))).toBe(true);
    expect(queueResourceInflightQueryKey(ref).startsWith(queueResourceQueryKey(ref))).toBe(true);
    expect(queueDeadLettersQueryKey(ref).startsWith(queueDeadLettersQueryPrefix(ref))).toBe(true);
    expect(
      queueDeadLettersQueryKey(ref, { family: 4 }).startsWith(queueDeadLettersQueryPrefix(ref)),
    ).toBe(true);
    expect(affectedQueueKeys(ref)).toEqual([
      queueOverviewQueryKey(),
      queueRealmQueryKey(ref.realm),
      queueAreaQueryKey(ref.realm, ref.area),
      queueResourceQueryKey(ref),
      queueResourceInflightQueryKey(ref),
      queueResourceTimelineQueryKey(ref),
      queueDeadLettersQueryPrefix(ref),
    ]);
  });
  it("builds hierarchical lease query keys", () => {
    const ref = {
      area: "ops",
      realm: "default",
      resource: "primary",
    };

    expect(leaseRealmQueryKey("default", "1")).not.toBe(leaseRealmQueryKey("default", "2"));
    expect(leaseRealmQueryKey(ref.realm)).not.toBe(leaseAreaQueryKey(ref.realm, ref.area));
    expect(leaseAreaQueryKey(ref.realm, ref.area)).not.toBe(
      leaseResourceRowsQueryKey(ref.realm, ref.area, ref.resource),
    );
    expect(leaseResourceRowsQueryKey(ref.realm, ref.area, ref.resource, 50)).toBe(
      leaseResourceRowsQueryKey(ref.realm, ref.area, ref.resource, 50),
    );
    expect(leaseResourceRowsQueryKey(ref.realm, ref.area, ref.resource, 50)).not.toBe(
      leaseResourceRowsQueryKey(ref.realm, ref.area, ref.resource, 51),
    );
  });
  it("builds hierarchical notice query keys", () => {
    const ref = {
      area: "ops",
      realm: "default",
      operation: "GetStatus",
      resource: "primary",
    };

    expect(noticeRealmQueryKey(ref.realm, "1")).toBe(noticeRealmQueryKey(ref.realm, "1"));
    expect(noticeRealmQueryKey(ref.realm, "1")).not.toBe(noticeRealmQueryKey(ref.realm, "2"));
    expect(noticeRealmQueryKey(ref.realm)).not.toBe(noticeAreaQueryKey(ref.realm, ref.area));
    expect(noticeAreaQueryKey(ref.realm, ref.area)).not.toBe(
      noticeResourceRowsQueryKey(ref.realm, ref.area, ref.resource),
    );
    expect(
      noticeOperationRowsQueryKey(ref.realm, ref.area, ref.resource, ref.operation, 50),
    ).not.toBe(noticeOperationRowsQueryKey(ref.realm, ref.area, "secondary", ref.operation, 50));
    expect(
      noticeOperationRowsQueryKey(ref.realm, ref.area, ref.resource, ref.operation, 50),
    ).not.toBe(noticeOperationRowsQueryKey(ref.realm, ref.area, ref.resource, "List", 50));
    expect(
      noticeOperationRowsQueryKey(ref.realm, ref.area, ref.resource, ref.operation, 50, "1"),
    ).not.toBe(
      noticeOperationRowsQueryKey(ref.realm, ref.area, ref.resource, ref.operation, 50, "2"),
    );
    expect(noticeResourceRowsQueryKey(ref.realm, ref.area, ref.resource, 50, "1")).not.toBe(
      noticeResourceRowsQueryKey(ref.realm, ref.area, ref.resource, 51, "1"),
    );
  });
  it("builds hierarchical RPC query keys", () => {
    const ref = {
      area: "ops",
      operation: "GetStatus",
      realm: "default",
      resource: "primary",
    };

    expect(rpcRealmQueryKey(ref.realm, "1")).not.toBe(rpcRealmQueryKey(ref.realm, "2"));
    expect(rpcRealmQueryKey(ref.realm)).not.toBe(rpcAreaQueryKey(ref.realm, ref.area));
    expect(rpcAreaQueryKey(ref.realm, ref.area)).not.toBe(
      rpcResourceQueryKey(ref.realm, ref.area, ref.resource),
    );
    expect(rpcResourceQueryKey(ref.realm, ref.area, ref.resource)).not.toBe(
      rpcOperationQueryKey(ref.realm, ref.area, ref.resource, ref.operation),
    );
    expect(rpcOperationQueryKey(ref.realm, ref.area, ref.resource, ref.operation, 50)).not.toBe(
      rpcOperationQueryKey(ref.realm, ref.area, ref.resource, ref.operation, 51),
    );
  });
  it("builds hierarchical Stream query keys", () => {
    const ref = {
      area: "ops",
      discriminator: "invoice",
      realm: "default",
      resource: "events",
    };

    expect(streamRealmQueryKey(ref.realm, "1")).not.toBe(streamRealmQueryKey(ref.realm, "2"));
    expect(streamRealmQueryKey(ref.realm)).not.toBe(streamAreaQueryKey(ref.realm, ref.area));
    expect(streamAreaQueryKey(ref.realm, ref.area)).not.toBe(
      streamResourceQueryKey({ ...ref, fromOffset: 0, limit: 50 }),
    );
    expect(streamResourceQueryKey({ ...ref, fromOffset: 0, limit: 50 })).not.toBe(
      streamResourceQueryKey({ ...ref, fromOffset: 1, limit: 50 }),
    );
    expect(
      streamResourceQueryKey({ ...ref, discriminator: "a", fromOffset: 0, limit: 50 }),
    ).not.toBe(streamResourceQueryKey({ ...ref, discriminator: "b", fromOffset: 0, limit: 50 }));
    expect(streamResourceQueryKey({ ...ref, fromOffset: 0, limit: 50 })).not.toBe(
      streamResourceQueryKey({ ...ref, fromOffset: 0, limit: 51 }),
    );
  });
  it("builds hierarchical Schedule query keys", () => {
    const ref = {
      area: "ops",
      realm: "default",
      resource: "reconcile",
    };

    expect(scheduleRealmQueryKey(ref.realm, "1")).not.toBe(scheduleRealmQueryKey(ref.realm, "2"));
    expect(scheduleRealmQueryKey(ref.realm)).not.toBe(scheduleAreaQueryKey(ref.realm, ref.area));
    expect(scheduleAreaQueryKey(ref.realm, ref.area)).not.toBe(
      scheduleResourceQueryKey({ ...ref, limit: 20 }),
    );
    expect(scheduleResourceQueryKey({ ...ref, limit: 20 })).not.toBe(
      scheduleResourceQueryKey({ ...ref, limit: 21 }),
    );
    expect(scheduleResourceQueryKey({ ...ref, limit: 20 })).not.toBe(
      scheduleResourceQueryKey({ ...ref, limit: 20, offset: 20 }),
    );
    expect(scheduleResourceQueryKey({ ...ref, limit: 20 })).not.toBe(
      scheduleOperationQueryKey({ ...ref, limit: 20, operation: "handoff" }),
    );
    expect(scheduleExecutionObservationsQueryKey({ ...ref, limit: 20 })).not.toBe(
      scheduleMissedHandoffsQueryKey({ ...ref, limit: 20 }),
    );
    expect(scheduleMissedHandoffsQueryKey({ ...ref, limit: 20 })).not.toBe(
      scheduleMissedHandoffsQueryKey({ ...ref, limit: 21 }),
    );
  });
});
