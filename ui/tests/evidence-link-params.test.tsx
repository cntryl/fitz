import { describe, expect, it, vi } from "vite-plus/test";
import { queryState, submit, type } from "@askrjs/askr/testing";
import { mountRoute, pageSmokeMocks, queryOptions } from "./page-smoke/harness";
import { kvRows } from "./page-smoke/fixtures";

const mocks = pageSmokeMocks();

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
      "/admin/1/kv/default/ops/primary?rows=1&startsWith=user%3A&cursor=cursor-2&cursorTrail=&cursorTrail=cursor-1&limit=25&note=a%2Bb%26c%3D%252F+%E2%98%83",
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
    expect(query.get("note")).toBe("a+b&c=%2F ☃");
  });

  it("keeps inspected KV transactions when paging rows", async () => {
    // Arrange
    const { default: KvResourcePage } = await import("@/pages/app/kv-resource");

    // Act
    const root = await mountRoute(
      "/admin/1/kv/default/ops/primary?rows=1&transactions=1&cursor=cursor-2&cursorTrail=cursor-1",
      "/admin/{family}/kv/{realm}/{area}/{resource}",
      KvResourcePage,
    );
    const firstPage = linkQuery(root, "First page");
    const previousPage = linkQuery(root, "Previous page");

    // Assert
    expect(firstPage.get("transactions")).toBe("1");
    expect(previousPage.get("transactions")).toBe("1");
    expect(previousPage.get("cursor")).toBe("cursor-1");
  });

  it("keeps inspected KV transactions when advancing rows", async () => {
    // Arrange
    const { default: KvResourcePage } = await import("@/pages/app/kv-resource");
    mocks.queryStates.kvRows = queryState.fresh(
      { ...kvRows, hasMore: true, nextCursor: "cursor-3" },
      queryOptions(),
    );

    // Act
    const root = await mountRoute(
      "/admin/1/kv/default/ops/primary?rows=1&transactions=1&startsWith=user%3A&limit=25&cursor=cursor-2&cursorTrail=&cursorTrail=cursor-1",
      "/admin/{family}/kv/{realm}/{area}/{resource}",
      KvResourcePage,
    );
    const nextPage = linkQuery(root, "Next page");

    // Assert
    expect(nextPage.get("transactions")).toBe("1");
    expect(nextPage.get("rows")).toBe("1");
    expect(nextPage.get("startsWith")).toBe("user:");
    expect(nextPage.get("limit")).toBe("25");
    expect(nextPage.get("cursor")).toBe("cursor-3");
    expect(nextPage.getAll("cursorTrail")).toEqual(["", "cursor-1", "cursor-2"]);
  });

  it("keeps inspected KV transactions when loading rows", async () => {
    // Arrange
    const { default: KvResourcePage } = await import("@/pages/app/kv-resource");

    // Act
    const root = await mountRoute(
      "/admin/1/kv/default/ops/primary?transactions=1&limit=25",
      "/admin/{family}/kv/{realm}/{area}/{resource}",
      KvResourcePage,
    );
    const query = linkQuery(root, "Load rows");

    // Assert
    expect(query.get("transactions")).toBe("1");
    expect(query.get("rows")).toBe("1");
    expect(query.get("limit")).toBe("25");
  });

  it("keeps inspected KV transactions when applying row filters", async () => {
    // Arrange
    const { default: KvResourcePage } = await import("@/pages/app/kv-resource");
    const root = await mountRoute(
      "/admin/1/kv/default/ops/primary?rows=1&transactions=1&cursor=cursor-2&cursorTrail=&cursorTrail=cursor-1",
      "/admin/{family}/kv/{realm}/{area}/{resource}",
      KvResourcePage,
    );
    const prefix = root.querySelector<HTMLInputElement>("#kv-starts-with");
    const limit = root.querySelector<HTMLInputElement>("#kv-limit");
    const form = prefix?.closest("form");
    expect(prefix).toBeTruthy();
    expect(limit).toBeTruthy();
    expect(form).toBeTruthy();

    // Act
    type(prefix!, "settings:");
    limit!.value = "";
    type(limit!, "25");
    submit(form!);

    // Assert
    await vi.waitFor(() => {
      const query = new URLSearchParams(window.location.search);
      expect(query.get("transactions")).toBe("1");
      expect(query.get("rows")).toBe("1");
      expect(query.get("startsWith")).toBe("settings:");
      expect(query.get("limit")).toBe("25");
      expect(query.has("cursor")).toBe(false);
      expect(query.has("cursorTrail")).toBe(false);
      expect(root.textContent).toContain("Active transactions");
    });
  });
});
