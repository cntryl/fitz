import { describe, expect, it } from "vite-plus/test";
import { mountRoute, pageSmokeMocks } from "./page-smoke/harness";

pageSmokeMocks();

function linkQuery(root: Element, label: string) {
  const link = Array.from(root.querySelectorAll("a")).find(
    (anchor) => anchor.textContent?.trim() === label,
  );
  const href = link?.getAttribute("href");
  expect(href, label).toBeTruthy();
  return new URL(href ?? "", "http://fitz.test").searchParams;
}

describe("evidence links keep the sections already on screen", () => {
  it("keeps loaded queue messages when inspecting transitions", async () => {
    // Arrange
    const { default: QueueResourcePage } = await import("@/pages/app/queue-resource");

    // Act
    const root = await mountRoute(
      "/queue/default/ops/primary?rows=1",
      "/queue/{realm}/{area}/{resource}",
      QueueResourcePage,
    );
    const query = linkQuery(root, "Inspect transitions");

    // Assert
    expect(query.get("rows")).toBe("1");
    expect(query.get("timeline")).toBe("1");
  });

  it("keeps loaded queue transitions when inspecting messages", async () => {
    // Arrange
    const { default: QueueResourcePage } = await import("@/pages/app/queue-resource");

    // Act
    const root = await mountRoute(
      "/queue/default/ops/primary?timeline=1",
      "/queue/{realm}/{area}/{resource}",
      QueueResourcePage,
    );
    const query = linkQuery(root, "Inspect messages");

    // Assert
    expect(query.get("timeline")).toBe("1");
    expect(query.get("rows")).toBe("1");
  });

  it("keeps KV row paging when inspecting active transactions", async () => {
    // Arrange
    const { default: KvResourcePage } = await import("@/pages/app/kv-resource");

    // Act
    const root = await mountRoute(
      "/admin/1/kv/default/ops/primary?rows=1&startsWith=user%3A&cursor=cursor-2&cursorTrail=&cursorTrail=cursor-1&limit=25",
      "/admin/{family}/kv/{realm}/{area}/{resource}",
      KvResourcePage,
    );
    const query = linkQuery(root, "Inspect active transactions");

    // Assert
    expect(query.get("rows")).toBe("1");
    expect(query.get("startsWith")).toBe("user:");
    expect(query.get("cursor")).toBe("cursor-2");
    expect(query.getAll("cursorTrail")).toEqual(["", "cursor-1"]);
    expect(query.get("limit")).toBe("25");
    expect(query.get("transactions")).toBe("1");
  });
});
