import { describe, expect, it } from "vite-plus/test";
import { cleanupApp } from "@askrjs/askr/boot";
import { queryState } from "@askrjs/askr/testing";
import { mountRoute, pageSmokeMocks, queryOptions } from "./page-smoke/harness";
import { queueResource, resourceDetail } from "./page-smoke/fixtures";

const mocks = pageSmokeMocks();

describe("admin page smoke tests", () => {
  it("renders queue resource links for overview, realm, and area routes", async () => {
    const { default: QueuePage } = await import("@/pages/app/queue");

    let root = await mountRoute("/admin/1/queue", "/admin/{family}/queue", QueuePage);
    expect(root.textContent).toContain("Queue inventory");
    expect(root.querySelector('a[href="/admin/1/queue/default"]')).toBeNull();
    expect(
      root.querySelector('a[href="/admin/1/queue/default/ops/primary"]')?.textContent,
    ).toContain("queue://default/ops/primary");

    cleanupApp(root);
    document.body.innerHTML = "";

    root = await mountRoute("/admin/1/queue/default", "/admin/{family}/queue/{realm}", QueuePage);
    expect(root.textContent).toContain("Queue inventory");
    expect(root.querySelector('a[href="/admin/1/queue/default/ops"]')).toBeNull();
    expect(
      root.querySelector('a[href="/admin/1/queue/default/ops/primary"]')?.textContent,
    ).toContain("queue://default/ops/primary");

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
  });
  it("mounts queue comparison and generic resource comparison flows", async () => {
    const { default: QueueResourcePage } = await import("@/pages/app/queue-resource");
    let root = await mountRoute(
      "/queue/default/ops/primary?againstRealm=default&againstArea=ops&againstResource=secondary",
      "/queue/{realm}/{area}/{resource}",
      QueueResourcePage,
    );

    expect(root.textContent).toContain("Comparison summary");
    expect(root.textContent).toContain("Current scope");
    expect(root.textContent).toContain("Target scope");
    expect(root.textContent).toContain("Difference");
    expect(root.textContent).toContain("Point-in-time durable backlog");
    expect(root.textContent).toContain("Snapshots match");
    expect(root.textContent).toContain(
      "No dead-letter messages are visible for this resource. No replay or purge action is needed.",
    );

    let text = root.textContent ?? "";
    let order = ["Current values", "Compare scopes", "Dead letters", "Inflight", "Timeline"];
    let cursor = -1;
    for (const label of order) {
      const index = text.indexOf(label, cursor + 1);
      expect(index).toBeGreaterThan(cursor);
      cursor = index;
    }

    cleanupApp(root);
    document.body.innerHTML = "";

    root = await mountRoute(
      "/queue/default/ops/primary",
      "/queue/{realm}/{area}/{resource}",
      QueueResourcePage,
    );
    text = root.textContent ?? "";
    order = ["Current values", "Dead letters", "Inflight", "Timeline", "Compare scopes"];
    cursor = -1;
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
      "/admin/1/kv/default/ops/primary?startsWith=user%3A",
      "/admin/{family}/kv/{realm}/{area}/{resource}",
      KvResourcePage,
    );

    expect(root.textContent).toContain("Key preview");
    expect(root.textContent).toContain("user:1");
    expect(root.textContent).toContain("alice");
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
  it("uses mutation-owned logout pending, success, and error states", async () => {
    const { default: Logout } = await import("@/pages/auth/logout");

    mocks.mutation.execute.mockImplementationOnce(() => new Promise<void>(() => {}));

    let root = await mountRoute("/logout", "/logout", Logout);

    expect(root.textContent).toContain("Signing out");
    expect(root.textContent).toContain("Clearing your session.");

    cleanupApp(root);
    document.body.innerHTML = "";

    mocks.mutation.execute.mockResolvedValueOnce(undefined);
    root = await mountRoute("/logout", "/logout", Logout);
    await new Promise<void>((resolve) => setTimeout(resolve, 0));

    expect(root.textContent).toContain("Signed out");
    expect(root.textContent).toContain("Go to sign in");

    cleanupApp(root);
    document.body.innerHTML = "";

    mocks.mutation.execute.mockRejectedValueOnce(new Error("Logout failed"));
    root = await mountRoute("/logout", "/logout", Logout);
    await new Promise<void>((resolve) => setTimeout(resolve, 0));

    expect(root.textContent).toContain("Sign out failed");
    expect(root.textContent).toContain("Logout failed");
    expect(root.textContent).toContain("Try again");
  });
});
