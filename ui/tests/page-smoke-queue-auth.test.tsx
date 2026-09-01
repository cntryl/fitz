import { describe, expect, it } from "vite-plus/test";
import { cleanupApp } from "@askrjs/askr/boot";
import { queryState, submit, type } from "@askrjs/askr/testing";
import { mountRoute, pageSmokeMocks, queryOptions } from "./page-smoke/harness";
import { queueInventory, queueResource } from "./page-smoke/fixtures";

const mocks = pageSmokeMocks();

describe("admin page smoke tests", () => {
  it("renders progressive queue links for overview, realm, and area routes", async () => {
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
    expect(root.querySelector('a[href="/admin/1/queue/default"]')).toBeTruthy();
    expect(root.querySelector('a[href="/admin/1/queue/globex"]')).toBeTruthy();
    expect(root.querySelector('a[href="/admin/1/queue/default/ops/primary"]')).toBeNull();

    cleanupApp(root);
    document.body.innerHTML = "";

    root = await mountRoute("/admin/1/queue/default", "/admin/{family}/queue/{realm}", QueuePage);
    expect(root.textContent).toContain("Queue realm");
    expect(root.querySelector('a[href="/admin/1/queue/default/ops"]')).toBeTruthy();
    expect(root.querySelector('a[href="/admin/1/queue/default/ops/primary"]')).toBeNull();
    expect(root.textContent).not.toContain("queue://globex/support/tickets");

    cleanupApp(root);
    document.body.innerHTML = "";

    root = await mountRoute(
      "/admin/1/queue/default/ops",
      "/admin/{family}/queue/{realm}/{area}",
      QueuePage,
    );
    expect(root.textContent).toContain("Queue area");
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

    const { default: KvResourcePage } = await import("@/pages/app/kv-resource");
    root = await mountRoute(
      "/admin/1/kv/default/ops/primary?startsWith=user%3A&cursor=cursor-2&cursorTrail=",
      "/admin/{family}/kv/{realm}/{area}/{resource}",
      KvResourcePage,
    );

    expect(root.textContent).toContain("Key preview");
    expect(root.textContent).toContain("user:1");
    expect(root.textContent).toContain("alice");
    const committedRows = root.querySelector('[aria-label="Committed KV rows"]');
    expect(committedRows?.querySelectorAll('button[aria-label="Copy value"]')).toHaveLength(1);
    expect(committedRows?.querySelector('button[aria-label="Copy key"]')).toBeNull();
    expect(committedRows?.textContent).not.toContain("Copy value");
    expect(
      root.querySelector('a[href="/admin/1/kv/default/ops/primary?startsWith=user%3A"]')
        ?.textContent,
    ).toContain("First page");
    expect(root.textContent).toContain("Previous page");

    const exactKey = root.querySelector<HTMLInputElement>("#kv-exact-key");
    if (exactKey) {
      type(exactKey, "user:1");
    }
    const exactKeyForm = root.querySelector<HTMLInputElement>("#kv-exact-key")?.closest("form");
    if (exactKeyForm) submit(exactKeyForm);
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
    expect(root.textContent).not.toContain("Copy body");
    const headers = Array.from(
      root.querySelectorAll('table[aria-label="Stream records"] [data-slot="table-header-cell"]'),
    ).map((header) => header.textContent?.trim());
    expect(headers).toEqual(["Offset", "Created", "Body", "Action"]);
  });
  it("uses tables for queue message state and a list for timeline evidence", async () => {
    mocks.queryStates.queueResource = queryState.fresh(
      {
        ...queueResource,
        deadLetters: [
          {
            area: "ops",
            attempts: 2,
            deadLetteredAt: "2026-05-21T13:05:00Z",
            family: 1,
            messageId: 42,
            realm: "default",
            reason: "handler failed",
            resource: "primary",
          },
        ],
        inflight: [
          {
            area: "ops",
            attempts: 1,
            expiresAt: "2026-05-21T13:06:00Z",
            family: 1,
            inflightToken: "token-1",
            messageId: 41,
            realm: "default",
            resource: "primary",
            sessionId: "session-1",
          },
        ],
        timeline: {
          ...queueResource.timeline,
          events: [
            {
              ageSeconds: 2,
              area: "ops",
              attempts: 1,
              correlationId: "correlation-1",
              kind: "transition" as const,
              messageId: 41,
              observedAt: "2026-05-21T13:00:00Z",
              operation: "Peek",
              ownerSession: "session-1",
              realm: "default",
              resource: "primary",
              summary: "Queue worker activity observed.",
              workerSession: "worker-1",
            },
          ],
        },
      },
      queryOptions(),
    );

    const { default: QueueResourcePage } = await import("@/pages/app/queue-resource");
    const root = await mountRoute(
      "/queue/default/ops/primary",
      "/queue/{realm}/{area}/{resource}",
      QueueResourcePage,
    );

    expect(root.querySelector('table[aria-label="Dead-letter queue messages"]')).toBeTruthy();
    expect(root.querySelector('table[aria-label="Inflight queue messages"]')).toBeTruthy();
    const queueHeaders = Array.from(
      root.querySelectorAll(
        'table[aria-label="Dead-letter queue messages"] [data-slot="table-header-cell"], table[aria-label="Inflight queue messages"] [data-slot="table-header-cell"]',
      ),
    ).map((header) => header.textContent?.trim());
    expect(queueHeaders).not.toContain("Family");
    const timeline = root.querySelector('ul[aria-label="Queue resource timeline"]');
    expect(timeline?.querySelectorAll('[data-slot="item"]')).toHaveLength(1);
    expect(root.querySelector("#queue-timeline [data-slot='table']")).toBeNull();
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
