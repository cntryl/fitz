import { describe, expect, it } from "vite-plus/test";
import {
  createQueueDeadLettersQuery,
  queueDeadLettersQueryKey,
} from "../src/queries/queue.query";
import {
  createCurrentSessionQuery,
  signInAdmin,
  signOutAdmin,
} from "../src/queries/session.query";
import { mapQueueDeadLetter } from "../src/services/queue.mappers";

describe("Data query layer", () => {
  it("exports session query helpers", () => {
    expect(createCurrentSessionQuery).toBeDefined();
    expect(typeof createCurrentSessionQuery).toBe("function");
    expect(signInAdmin).toBeDefined();
    expect(typeof signInAdmin).toBe("function");
    expect(signOutAdmin).toBeDefined();
    expect(typeof signOutAdmin).toBe("function");
  });

  it("exports queue dead-letter query helpers", () => {
    expect(createQueueDeadLettersQuery).toBeDefined();
    expect(typeof createQueueDeadLettersQuery).toBe("function");
    expect(queueDeadLettersQueryKey({ realm: "r", area: "a", resource: "q" })).toEqual([
      "queueDeadLetters",
      "r",
      "a",
      "q",
      "all",
    ]);
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
