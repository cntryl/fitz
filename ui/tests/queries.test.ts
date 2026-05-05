import { describe, expect, it } from "vite-plus/test";
import {
  createQueueDeadLettersQuery,
  queueDeadLettersQueryKey,
} from "@/features/queue/queue-query";
import { createCurrentSessionQuery } from "@/features/session/session-query";
import { createSignInMutation, createSignOutMutation } from "@/features/session/session-mutation";
import { mapQueueDeadLetter } from "@/features/queue/queue-mappers";

describe("Data query layer", () => {
  it("exports session query helpers", () => {
    expect(createCurrentSessionQuery).toBeDefined();
    expect(typeof createCurrentSessionQuery).toBe("function");
    expect(createSignInMutation).toBeDefined();
    expect(typeof createSignInMutation).toBe("function");
    expect(createSignOutMutation).toBeDefined();
    expect(typeof createSignOutMutation).toBe("function");
  });

  it("exports queue dead-letter query helpers", () => {
    expect(createQueueDeadLettersQuery).toBeDefined();
    expect(typeof createQueueDeadLettersQuery).toBe("function");
    expect(queueDeadLettersQueryKey({ realm: "r", area: "a", resource: "q" })).toBe(
      "queue:dead-letters:r:a:q:all",
    );
  });

  it("maps queue DTOs to camelCase app models", () => {
    expect(
      mapQueueDeadLetter({
        realm: "r",
        area: "a",
        resource: "q",
        family: 4,
        message_id: 42,
        attempts: 3,
        reason: "exhausted retries",
        dead_lettered_at: "2026-05-04T12:00:00Z",
      }),
    ).toEqual({
      realm: "r",
      area: "a",
      resource: "q",
      family: 4,
      messageId: 42,
      attempts: 3,
      reason: "exhausted retries",
      deadLetteredAt: "2026-05-04T12:00:00Z",
    });
  });
});
