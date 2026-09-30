import { describe, expect, it, vi } from "vite-plus/test";
import { cleanupApp } from "@askrjs/askr/boot";
import { click, queryState, submit, type } from "@askrjs/askr/testing";
import { mountRoute, pageSmokeMocks, queryOptions } from "./page-smoke/harness";
import { queueInventory, queueResource, queueResourceRow } from "./page-smoke/fixtures";

const mocks = pageSmokeMocks();

describe("admin page smoke tests", () => {
  it("shows and sorts live subscription counts in queue inventory and scope rollups", async () => {
    const { default: QueuePage } = await import("@/pages/app/queue");
    mocks.queryStates.queueInventory = queryState.fresh(
      {
        ...queueInventory,
        realms: [
          {
            realm: "default",
            areas: [
              {
                area: "ops",
                resources: ["unconsumed", "consumed"],
                resourceEntries: [
                  { ...queueResourceRow, resource: "unconsumed", subscriptionsActive: 0 },
                  { ...queueResourceRow, resource: "consumed", subscriptionsActive: 3 },
                ],
              },
            ],
          },
        ],
      },
      queryOptions(),
    );

    const root = await mountRoute(
      "/admin/1/queue/default/ops",
      "/admin/{family}/queue/{realm}/{area}",
      QueuePage,
    );
    const table = root.querySelector<HTMLTableElement>("#queue-inventory-table");

    expect(root.textContent).toMatch(/Active subscriptions\s*3/);
    expect(table?.querySelector('th[data-column-id="subscriptions"]')).toBeTruthy();
    expect(
      Array.from(table?.querySelectorAll('tbody td[data-column-id="subscriptions"]') ?? []).map(
        (cell) => cell.textContent?.trim(),
      ),
    ).toEqual(["0", "3"]);

    const sort = root.querySelector<HTMLButtonElement>(
      'button[aria-label="Sort by Active subscriptions, not sorted"]',
    );
    expect(sort).toBeTruthy();
    click(sort!);
    await new Promise<void>((resolve) => queueMicrotask(resolve));

    expect(
      root.querySelector('button[aria-label="Sort by Active subscriptions, descending"]'),
    ).toBeTruthy();
    expect(
      Array.from(
        root
          .querySelector<HTMLTableElement>("#queue-inventory-table")
          ?.querySelectorAll('tbody td[data-column-id="subscriptions"]') ?? [],
      ).map((cell) => cell.textContent?.trim()),
    ).toEqual(["3", "0"]);

    cleanupApp(root);
    document.body.innerHTML = "";
  });

  it("waits for an explicit request before loading data rows on detail pages", async () => {
    const pages = [
      {
        loadLabel: "Load messages",
        module: () => import("@/pages/app/queue-resource"),
        path: "/admin/1/queue/default/ops/primary",
        routePath: "/admin/{family}/queue/{realm}/{area}/{resource}",
        table: 'table[aria-label="Dead-letter queue messages"]',
      },
      {
        loadLabel: "Load rows",
        module: () => import("@/pages/app/kv-resource"),
        path: "/admin/1/kv/default/ops/primary",
        routePath: "/admin/{family}/kv/{realm}/{area}/{resource}",
        table: '[aria-label="Committed KV rows"]',
      },
      {
        loadLabel: "Load records",
        module: () => import("@/pages/app/stream-resource"),
        path: "/admin/1/stream/default/ops/primary",
        routePath: "/admin/{family}/stream/{realm}/{area}/{resource}",
        table: 'table[aria-label="Stream records"]',
      },
    ];

    for (const page of pages) {
      // Arrange
      const { default: Component } = await page.module();

      // Act
      const root = await mountRoute(page.path, page.routePath, Component);
      const load = Array.from(root.querySelectorAll("a")).find(
        (link) => link.textContent?.trim() === page.loadLabel,
      );

      // Assert
      expect(root.querySelector(page.table), page.path).toBeNull();
      expect(load?.getAttribute("href"), page.path).toBe(`${page.path}?rows=1`);

      cleanupApp(root);
      document.body.innerHTML = "";
    }
  });

  it("creates no data-row query until the operator asks for rows", async () => {
    // Arrange
    const { createQueueDeadLettersQuery } = await import("@/features/queue/queue-query");
    const { createQueueResourceInflightQuery } =
      await import("@/features/queue/queue-resource-query");
    const { createKvRowsQuery } = await import("@/features/kv/kv-rows-query");
    const { createKvValueQuery } = await import("@/features/kv/kv-value-query");
    const { createStreamRecordsQuery } = await import("@/features/stream/stream-query");
    const factories = [
      createQueueDeadLettersQuery,
      createQueueResourceInflightQuery,
      createKvRowsQuery,
      createKvValueQuery,
      createStreamRecordsQuery,
    ].map((factory) => vi.mocked(factory));
    for (const factory of factories) factory.mockClear();
    const pages = [
      ["@/pages/app/queue-resource", "/admin/1/queue/default/ops/primary", "queue"],
      ["@/pages/app/kv-resource", "/admin/1/kv/default/ops/primary", "kv"],
      ["@/pages/app/stream-resource", "/admin/1/stream/default/ops/primary", "stream"],
    ] as const;

    // Act
    for (const [module, path, domain] of pages) {
      const { default: Component } = await import(module);
      const root = await mountRoute(
        path,
        `/admin/{family}/${domain}/{realm}/{area}/{resource}`,
        Component,
      );
      cleanupApp(root);
      document.body.innerHTML = "";
    }

    // Assert
    expect(factories.map((factory) => factory.mock.calls.length)).toEqual([0, 0, 0, 0, 0]);
  });

  it("fits queue realms without structural columns or a scroll hint", async () => {
    // Arrange
    const { default: QueuePage } = await import("@/pages/app/queue");

    // Act
    const root = await mountRoute("/admin/1/queue", "/admin/{family}/queue", QueuePage);
    const table = root.querySelector<HTMLTableElement>("#queue-inventory-table");

    // Assert
    expect(table?.querySelector('th[data-column-id="areas"]')).toBeNull();
    expect(table?.querySelector('th[data-column-id="resources"]')).toBeNull();
    expect(root.textContent).not.toContain("Scroll horizontally");

    cleanupApp(root);
    document.body.innerHTML = "";
  });

  it("folds secondary queue metrics into the route cell detail line", async () => {
    // Arrange
    const { default: QueuePage } = await import("@/pages/app/queue");

    // Act
    const root = await mountRoute("/admin/1/queue", "/admin/{family}/queue", QueuePage);
    const table = root.querySelector<HTMLTableElement>("#queue-inventory-table");
    const secondary = Array.from(
      table?.querySelectorAll('th[data-priority="secondary"]') ?? [],
    ).map((cell) => cell.getAttribute("data-column-id"));

    // Assert
    expect(secondary).toEqual(["delayed", "inflight", "subscriptions"]);
    expect(
      table?.querySelector('th[data-column-id="dead-lettered"]')?.hasAttribute("data-priority"),
    ).toBe(false);
    expect(
      table?.querySelector('tbody td[data-column-id="route"] .domain-row-detail')?.textContent,
    ).toMatch(/^Delayed \S+ · In flight \S+ · Active subscriptions \S+$/);

    cleanupApp(root);
    document.body.innerHTML = "";
  });

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
      "/queue/default/ops/primary?rows=1&againstRealm=default&againstArea=ops&againstResource=secondary",
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

    expect(
      Array.from(
        root.querySelectorAll('[aria-label="Committed KV rows"] [data-slot="table-header-cell"]'),
      ).map((header) => header.textContent?.trim()),
    ).toEqual(["Key", "Action"]);
    expect(root.textContent).toContain("user:1");
    expect(root.textContent).toContain("alice");
    const committedRows = root.querySelector('[aria-label="Committed KV rows"]');
    expect(committedRows?.querySelectorAll('button[aria-label="Copy value"]')).toHaveLength(1);
    expect(committedRows?.querySelector('button[aria-label="Copy key"]')).toBeNull();
    expect(committedRows?.textContent).not.toContain("Copy value");
    expect(
      root.querySelector('a[href="/admin/1/kv/default/ops/primary?rows=1&startsWith=user%3A"]')
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
      root.querySelector('a[href="/admin/1/stream/default/ops/primary?rows=1"]')?.textContent,
    ).toContain("First page");
    expect(
      root.querySelector('a[href="/admin/1/stream/default/ops/primary?rows=1&fromOffset=50"]')
        ?.textContent,
    ).toContain("Previous page");
    expect(root.querySelector('button[aria-label^="Copy body at offset"]')).toBeTruthy();
    expect(root.textContent).not.toContain("Copy body");
    const headers = Array.from(
      root.querySelectorAll('table[aria-label="Stream records"] [data-slot="table-header-cell"]'),
    ).map((header) => header.textContent?.trim());
    expect(headers).toEqual(["Offset", "Body", "Action"]);
  });
  it("uses tables for queue message state and a list for timeline evidence", async () => {
    mocks.queryStates.queueDeadLetters = queryState.fresh(
      [
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
      queryOptions(),
    );
    mocks.queryStates.queueInflight = queryState.fresh(
      [
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
      queryOptions(),
    );
    mocks.queryStates.queueTimeline = queryState.fresh(
      {
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
      queryOptions(),
    );

    const { default: QueueResourcePage } = await import("@/pages/app/queue-resource");
    const root = await mountRoute(
      "/queue/default/ops/primary?rows=1",
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
    expect(queueHeaders).toEqual(["Message", "Attempts", "Actions", "Message", "Attempts"]);
    expect(root.textContent).not.toMatch(/horizontally/i);
    expect(
      root.querySelector(
        'table[aria-label="Inflight queue messages"] td[data-column-id="message"] .titled-cell-subtitle',
      ),
    ).toBeTruthy();
    const timeline = root.querySelector('ul[aria-label="Queue resource timeline"]');
    expect(timeline?.querySelectorAll('[data-slot="item"]')).toHaveLength(1);
    expect(root.querySelector("#queue-timeline [data-slot='table']")).toBeNull();
  });
  it("keeps queue backlog and dead-letter actions usable when the timeline fails", async () => {
    const timelineRetry = vi.fn(async () => undefined);
    mocks.queryStates.queueResource = queryState.fresh(queueResource.detail, queryOptions());
    mocks.queryStates.queueInflight = queryState.fresh(
      [
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
      queryOptions(),
    );
    mocks.queryStates.queueDeadLetters = queryState.fresh(
      [
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
      queryOptions(),
    );
    mocks.queryStates.queueTimeline = queryState.error(
      new Error("Timeline read failed"),
      undefined,
      { refresh: timelineRetry },
    );

    const { default: QueueResourcePage } = await import("@/pages/app/queue-resource");
    const root = await mountRoute(
      "/queue/default/ops/primary?rows=1",
      "/queue/{realm}/{area}/{resource}",
      QueueResourcePage,
    );

    expect(root.textContent).toContain("Current values");
    expect(root.querySelector('table[aria-label="Inflight queue messages"]')).toBeTruthy();
    expect(root.querySelector('table[aria-label="Dead-letter queue messages"]')).toBeTruthy();
    expect(
      Array.from(root.querySelectorAll("button")).some((button) => button.textContent === "Replay"),
    ).toBe(true);
    expect(root.textContent).toContain("Timeline read failed");
    const retry = Array.from(root.querySelectorAll<HTMLButtonElement>("button")).find(
      (button) => button.textContent?.trim() === "Retry",
    );
    expect(retry?.textContent).toContain("Retry");
    retry?.click();
    expect(timelineRetry).toHaveBeenCalledTimes(1);
  });
  it("opens an accessible queue dead-letter confirmation dialog", async () => {
    const { default: QueueResourcePage } = await import("@/pages/app/queue-resource");
    mocks.queryStates.queueDeadLetters = queryState.fresh(
      [
        {
          attempts: 2,
          deadLetteredAt: "2026-05-21T13:05:00Z",
          family: 1,
          messageId: 42,
          reason: "handler failed",
        },
      ],
      queryOptions(),
    );

    const root = await mountRoute(
      "/queue/default/ops/primary?rows=1",
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
