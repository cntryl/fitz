import { describe, expect, it } from "vite-plus/test";
import { createSession, deleteSession, fetchSession } from "../src/resources/session";

describe("Session resources", () => {
  it("exports the session fetcher", () => {
    expect(fetchSession).toBeDefined();
    expect(typeof fetchSession).toBe("function");
  });

  it("exports the session creator", () => {
    expect(createSession).toBeDefined();
    expect(typeof createSession).toBe("function");
  });

  it("exports the session deleter", () => {
    expect(deleteSession).toBeDefined();
    expect(typeof deleteSession).toBe("function");
  });
});
