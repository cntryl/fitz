import { describe, expect, it } from "vite-plus/test";
import { cleanupApp } from "@askrjs/askr/boot";
import { queryState } from "@askrjs/askr/testing";
import { mountRoute, pageSmokeMocks, queryOptions } from "./page-smoke/harness";
import { queueInventory, queueResource, resourceDetail } from "./page-smoke/fixtures";

const mocks = pageSmokeMocks();

describe("admin page smoke tests", () => {
  it("renders queue resource links for overview, realm, and area routes", async () => {
    const { default: QueuePage } = await import("@/pages/app/queue");
    mocks.queryStates.queueInventory = queryState.fresh(
      {
        ...queueInventory,
        realms: [
          ...queueInventory.realms,
          {
            realm: "globex",
            areas: [{ area: "support", resources: ["tickets"] }],
          },
        ],
      },
      queryOptions(),
    );

    let root = await mountRoute("/admin/1/queue", "/admin/{family}/queue", QueuePage);
    expect(root.textContent).toContain("Queue inventory");
    expect(root.querySelector('a[href="/admin/1/queue/default"]')).toBeNull();
    expect(
      root.querySelector('a[href="/admin/1/queue/default/ops/primary"]')?.textContent,
    ).toContain("queue://default/ops/primary");
    expect(root.textContent).toContain("queue://globex/support/tickets");

    cleanupApp(root);
    document.body.innerHTML = "";

    root = await mountRoute("/admin/1/queue/default", "/admin/{family}/queue/{realm}", QueuePage);
    expect(root.textContent).toContain("Queue inventory");
    expect(root.querySelector('a[href="/admin/1/queue/default/ops"]')).toBeNull();
    expect(
      root.querySelector('a[href="/admin/1/queue/default/ops/primary"]')?.textContent,
    ).toContain("queue://default/ops/primary");
    expect(root.textContent).not.toContain("queue://globex/support/tickets");

    cleanupApp(root);
    document.body.innerHTML = "";

    root = await mountRoute(
      "/admin/1/queue/default/ops",
      "/admin/{family}/queue/{realm}/{area}",
      QueuePage,
    );
    expect(root.textContent).toContain("Queue inventory");
    expect(
      root.querySelector('a[href="/admin/1/queue/default/ops/primary"]')?.textContent,
    ).toContain("queue://default/ops/primary");
    expect(root.textContent).not.toContain("queue://globex/support/tickets");
  });
  it("removes queue comparison controls and preserves generic resource flows", async () => {
    const { default: QueueResourcePage } = await import("@/pages/app/queue-resource");
    let root = await mountRoute(
      "/queue/default/ops/primary?againstRealm=default&againstArea=ops&againstResource=secondary",
      "/queue/{realm}/{area}/{resource}",
      QueueResourcePage,
    );

    expect(root.textContent).not.toContain("Compare scopes");
    expect(root.textContent).not.toContain("Comparison summary");
    expect(root.querySelector("#compare-realm")).toBeNull();
    expect(root.querySelector("#compare-family")).toBeNull();
    expect(root.textContent).toContain(
      "No dead-letter messages are visible for this resource. No replay or purge action is needed.",
    );

    const text = root.textContent ?? "";
    const order = ["Current values", "Dead letters", "Inflight", "Timeline"];
    let cursor = -1;
    for (const label of order) {
      const index = text.indexOf(label, cursor + 1);
      expect(index).toBeGreaterThan(cursor);
      cursor = index;
    }

    cleanupApp(root);
    document.body.innerHTML = "";

    mocks.queryStates.resource = queryState.fresh(
      {
        ...resourceDetail,
        comparison: {
          comparisonMode: "resource",
          derived: false,
          metrics: [{ label: "Delta", value: 0 }],
          leftScope: {
            area: "ops",
            realm: "default",
            resource: "primary",
          },
          rightScope: {
            area: "ops",
            realm: "default",
            resource: "secondary",
          },
          summary: "No material difference",
        },
      },
      queryOptions(),
    );

    const { default: KvResourcePage } = await import("@/pages/app/kv-resource");
    root = await mountRoute(
      "/admin/1/kv/default/ops/primary?startsWith=user%3A&cursor=cursor-2&cursorTrail=",
      "/admin/{family}/kv/{realm}/{area}/{resource}",
      KvResourcePage,
    );

    expect(root.textContent).toContain("Key preview");
    expect(root.textContent).toContain("user:1");
    expect(root.textContent).toContain("alice");
    expect(
      root.querySelector('a[href="/admin/1/kv/default/ops/primary?startsWith=user%3A"]')
        ?.textContent,
    ).toContain("First page");
    expect(root.textContent).toContain("Previous page");

    const exactKey = root.querySelector<HTMLInputElement>("#kv-exact-key");
    if (exactKey) {
      exactKey.value = "user:1";
      exactKey.dispatchEvent(new Event("input", { bubbles: true }));
    }
    root
      .querySelector<HTMLFormElement>("#kv-exact-key")
      ?.closest("form")
      ?.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
    await new Promise<void>((resolve) => queueMicrotask(() => resolve()));

    expect(root.textContent).toContain("Exact key result");
    expect(root.querySelector('button[aria-label="Copy exact key"]')).toBeTruthy();
    expect(root.querySelector('button[aria-label="Copy exact value"]')).toBeTruthy();
  });
  it("offers first and previous controls for a later Stream window", async () => {
    const { default: StreamResourcePage } = await import("@/pages/app/stream-resource");
    const root = await mountRoute(
      "/admin/1/stream/default/ops/primary?fromOffset=100&limit=50",
      "/admin/{family}/stream/{realm}/{area}/{resource}",
      StreamResourcePage,
    );

    expect(
      root.querySelector('a[href="/admin/1/stream/default/ops/primary"]')?.textContent,
    ).toContain("First page");
    expect(
      root.querySelector('a[href="/admin/1/stream/default/ops/primary?fromOffset=50"]')
        ?.textContent,
    ).toContain("Previous page");
    expect(root.querySelector('button[aria-label^="Copy body at offset"]')).toBeTruthy();
  });
  it("opens an accessible queue dead-letter confirmation dialog", async () => {
    const { default: QueueResourcePage } = await import("@/pages/app/queue-resource");
    mocks.queryStates.queueResource = queryState.fresh(
      {
        ...queueResource,
        deadLetters: [
          {
            attempts: 2,
            deadLetteredAt: "2026-05-21T13:05:00Z",
            family: 1,
            messageId: 42,
            reason: "handler failed",
          },
        ],
      },
      queryOptions(),
    );

    const root = await mountRoute(
      "/queue/default/ops/primary",
      "/queue/{realm}/{area}/{resource}",
      QueueResourcePage,
    );
    const replay = Array.from(root.querySelectorAll("button")).find(
      (button) => button.textContent === "Replay",
    );

    expect(replay).toBeDefined();

    replay?.click();
    await new Promise<void>((resolve) => queueMicrotask(() => resolve()));

    expect(root.textContent).toContain("Replay dead-letter message?");
    expect(root.textContent).toContain("Replay message 42 in default / ops / primary.");
    expect(root.querySelector('[role="alertdialog"]')).toBeTruthy();

    mocks.mutation.error = new Error("Replay service unavailable");
    mocks.mutation.execute.mockRejectedValueOnce(new Error("Replay service unavailable"));
    const confirm = Array.from(root.querySelectorAll("button")).find(
      (button) => button.textContent === "Replay message",
    );
    confirm?.click();
    await new Promise<void>((resolve) => setTimeout(resolve, 0));

    expect(root.querySelector('[role="alertdialog"]')).toBeTruthy();
    expect(root.textContent).toContain("Replay failed");
    expect(root.textContent).toContain("Replay service unavailable");
  });
  it("uses mutation-owned login pending and error states", async () => {
    const { default: Login } = await import("@/pages/auth/login");

    mocks.mutation.pending = true;
    let root = await mountRoute("/login", "/login", Login);

    expect(root.textContent).toContain("Signing in...");

    cleanupApp(root);
    document.body.innerHTML = "";

    mocks.mutation.pending = false;
    mocks.mutation.error = new Error("Bad credentials");
    root = await mountRoute("/login", "/login", Login);

    expect(root.textContent).toContain("Bad credentials");
  });
  it("starts logout on entry and exposes retry on error", async () => {
    const { default: Logout } = await import("@/pages/auth/logout");

    mocks.mutation.execute.mockImplementationOnce(() => new Promise<void>(() => {}));

    let root = await mountRoute("/logout", "/logout", Logout);

    expect(root.textContent).toContain("Fitz Admin");
    await new Promise<void>((resolve) => setTimeout(resolve, 0));
    expect(root.textContent).toContain("Signing out");
    expect(root.textContent).toContain("Clearing your Fitz Admin session.");

    cleanupApp(root);
    document.body.innerHTML = "";

    mocks.mutation.execute.mockResolvedValueOnce(undefined);
    root = await mountRoute("/logout", "/logout", Logout);
    await new Promise<void>((resolve) => setTimeout(resolve, 0));

    expect(mocks.mutation.execute).toHaveBeenCalledWith(undefined);

    cleanupApp(root);
    document.body.innerHTML = "";

    mocks.mutation.execute.mockRejectedValueOnce(new Error("Logout failed"));
    root = await mountRoute("/logout", "/logout", Logout);
    await new Promise<void>((resolve) => setTimeout(resolve, 0));

    expect(root.textContent).toContain("Sign out failed");
    expect(root.textContent).toContain("Logout failed");
    expect(root.textContent).toContain("Retry");
  });
});
