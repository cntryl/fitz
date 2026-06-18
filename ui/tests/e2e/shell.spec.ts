import { expect, test, type Page } from "@playwright/test";

async function openDashboard(page: Page, theme: "light" | "dark" = "light") {
  if (theme === "dark") {
    await page.addInitScript(() => {
      localStorage.setItem("fitz-admin-theme", "dark");
    });
  }

  await page.goto("/admin");

  await expect(page.locator("main#main-content")).toHaveCount(1);
  const viewport = page.viewportSize();
  if ((viewport?.width ?? 0) < 768) {
    await expect(page.getByRole("button", { name: "Menu" })).toBeVisible();
    return;
  }
  await expect(page.getByRole("link", { name: "Fitz admin home" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Domains" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Toggle color theme" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Sign out" })).toBeVisible();
}

type DomainOverviewFixture = {
  realms: {
    realm: string;
  }[];
  stats: Record<string, number | Record<string, number>>;
};

const adminFeatures = {
  admin_auth_required: false,
  admin_auth_mode: "open" as const,
};

const domainOverviewPages = [
  {
    path: "/kv",
    domain: "kv",
    heading: "KV overview",
  },
  {
    path: "/lease",
    domain: "lease",
    heading: "Lease overview",
  },
  {
    path: "/notice",
    domain: "notice",
    heading: "Notice overview",
  },
  {
    path: "/rpc",
    domain: "rpc",
    heading: "RPC overview",
  },
  {
    path: "/schedule",
    domain: "schedule",
    heading: "Schedule overview",
  },
  {
    path: "/stream",
    domain: "stream",
    heading: "Stream overview",
  },
  {
    path: "/queue",
    domain: "queue",
    heading: "Queue overview",
  },
];

const domainOverviewData: Record<string, DomainOverviewFixture> = {
  kv: {
    realms: [
      {
        realm: "default",
      },
      {
        realm: "analytics",
      },
    ],
    stats: {
      commits_failed_total: 0,
      invalid_transaction_rejects_total: 0,
      keys_total: 1280,
      operations_per_second: 2.75,
      rollbacks_total: 0,
      transactions_active: 22,
    },
  },
  lease: {
    realms: [{ realm: "default" }],
    stats: {
      acquire_timeouts_total: 0,
      forced_releases_total: 0,
      invalid_token_rejects_total: 1,
      leases_active: 18,
      oldest_lease_age_seconds: 47,
      operations_per_second: 1.9,
      waiter_depth: 3,
    },
  },
  notice: {
    realms: [{ realm: "default" }],
    stats: {
      delivery_drops_total: 0,
      publishes_per_second: 4.1,
      routes_active: 2,
      wildcard_limit_rejects_total: 0,
      subscriptions_active: 9,
      max_route_subscribers: 8,
    },
  },
  rpc: {
    realms: [{ realm: "default" }],
    stats: {
      invalid_sequence_errors_dropped_total: 0,
      invalid_sequence_errors_forwarded_total: 0,
      invalid_sequence_responses_total: 0,
      operations_per_second: 6.2,
      requests_pending: 4,
      pending_routes_active: 1,
      responses_dropped_closed_caller_total: 0,
      responses_missing_pending_total: 0,
      request_timeouts_total: 0,
      workers_registered: 18,
    },
  },
  queue: {
    realms: [{ realm: "default" }],
    stats: {
      inflight_active: 3,
      messages_dead_lettered: 0,
      messages_delayed: 6,
      messages_pending: 12,
      messages_ready: 7,
      operations_per_second: 14.8,
    },
  },
  schedule: {
    realms: [{ realm: "default" }],
    stats: {
      ack_failures_total: 0,
      cancel_persistence_failures_total: 0,
      create_persistence_failures_total: 0,
      executions_per_minute: 9.5,
      notify_failures_total: 0,
      overdue_normalizations_total: 0,
      pending_fire_claims: 1,
      schedules_active: 27,
      subscriptions_active: 3,
      upsert_persistence_failures_total: 0,
    },
  },
  stream: {
    realms: [{ realm: "default" }],
    stats: {
      events_total: 840,
      operations_per_second: 3.5,
      streams_active: 12,
      subscriptions_active: 4,
      watermark_lag_buckets: {
        caught_up: 11,
        over_100: 0,
        under_10: 6,
        under_100: 2,
      },
    },
  },
};

type DomainOverviewOverride = Partial<DomainOverviewFixture>;

function applyLeaseOverride(overrides?: DomainOverviewOverride) {
  const base = domainOverviewData.lease;
  return {
    ...base,
    ...overrides,
    realms: overrides?.realms ?? base.realms,
    stats: {
      ...base.stats,
      ...overrides?.stats,
    },
  };
}

function domainAreasByRealm(_domain: string, _realm: string) {
  return ["default", "analytics"];
}

function domainResourcesByArea(domain: string, realm: string, area: string) {
  return (
    {
      [domain]: {
        [realm]: {
          [area]: [{ resource: "primary" }, { resource: "tenant-dashboard-stateful-resource" }],
        },
      },
    }[domain]?.[realm]?.[area] ?? [{ resource: "primary" }]
  );
}

async function mockDomainOverviewApis(
  page: Page,
  overrides: Partial<Record<string, DomainOverviewOverride>> = {},
) {
  await page.route("**/api/v1/features", async (route) => {
    await route.fulfill({
      json: adminFeatures,
    });
  });

  await page.route("**/api/v1/*", async (route) => {
    const parsed = new URL(route.request().url());
    const segments = parsed.pathname.split("/").filter(Boolean);
    if (segments.length < 3) {
      await route.continue();
      return;
    }

    const domain = segments[2] ?? "";
    const baseFixture = domainOverviewData[domain as keyof typeof domainOverviewData];
    const override = overrides[domain];
    const domainFixture = baseFixture
      ? {
          ...baseFixture,
          ...override,
          realms: override?.realms ?? baseFixture.realms,
          stats: {
            ...baseFixture.stats,
            ...override?.stats,
          },
        }
      : null;
    if (!domainFixture) {
      await route.continue();
      return;
    }

    const detail = segments[segments.length - 1];
    if (segments.length === 4 && detail === "stats") {
      await route.fulfill({
        json: domainFixture.stats,
      });
      return;
    }

    if (segments.length === 4 && detail === "realms") {
      await route.fulfill({
        json: { realms: domainFixture.realms },
      });
      return;
    }

    if (segments.length === 6 && detail === "areas") {
      const realm = decodeURIComponent(segments[3] ?? "");
      const areas = domainAreasByRealm(domain, realm).map((entry) => ({ area: entry }));
      await route.fulfill({
        json: { areas },
      });
      return;
    }

    if (segments.length === 8 && detail === "resources") {
      const realm = decodeURIComponent(segments[3] ?? "");
      const area = decodeURIComponent(segments[5] ?? "");
      await route.fulfill({
        json: {
          resources: domainResourcesByArea(domain, realm, area),
        },
      });
      return;
    }

    await route.continue();
  });
}

type SessionsPayload = {
  sessions: Array<{
    connected_at?: string;
    idle_seconds?: number;
    identity_claim?: string;
    identity_value?: string;
    messages_received?: number;
    messages_sent?: number;
    remote_addr?: string;
    route_family?: number;
    session_id?: string;
    subject?: string;
    transport?: string;
  }>;
};

const sessionsWithData: SessionsPayload = {
  sessions: [
    {
      connected_at: "2026-05-21T13:00:00Z",
      idle_seconds: 12,
      identity_claim: "tid",
      identity_value: "default",
      messages_received: 2,
      messages_sent: 3,
      remote_addr: "127.0.0.1",
      route_family: 1,
      session_id: "session-1",
      subject: "user:1",
      transport: "ws",
    },
    {
      connected_at: "2026-05-21T13:01:00Z",
      idle_seconds: 45,
      identity_claim: "tenant",
      identity_value: "ops",
      messages_received: 4,
      messages_sent: 8,
      remote_addr: "2001:db8::1ff:fe23:4567:890a",
      route_family: 2,
      session_id: "session-long-id-2",
      subject: "user:2",
      transport: "http",
    },
  ],
};

const sessionsEmpty: SessionsPayload = {
  sessions: [],
};

const metricsPayload = `# HELP fitz_broker_uptime_seconds Broker up
# TYPE fitz_broker_uptime_seconds gauge
fitz_broker_uptime_seconds 120
# HELP fitz_queue_ready Gauge
# TYPE fitz_queue_ready gauge
fitz_queue_ready{realm="default",area="jobs"} 7
# HELP fitz_rpc_requests_total rpc requests
# TYPE fitz_rpc_requests_total counter
fitz_rpc_requests_total{realm="default"} 19
`;

async function mockSessionsApi(page: Page, payload: SessionsPayload) {
  await page.route("**/api/v1/features", async (route) => {
    await route.fulfill({
      json: adminFeatures,
    });
  });

  await page.route("**/api/v1/sessions", async (route) => {
    await route.fulfill({
      json: payload,
    });
  });
}

async function mockMetricsApi(page: Page, payload = metricsPayload) {
  await page.route("**/api/v1/features", async (route) => {
    await route.fulfill({
      json: adminFeatures,
    });
  });

  await page.route(
    (url) => {
      const parsedUrl = new URL(url);
      return parsedUrl.pathname === "/metrics";
    },
    async (route) => {
      await route.fulfill({
        body: payload,
        contentType: "text/plain; charset=utf-8",
      });
    },
  );
}

test("captures the desktop dashboard shell", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  await openDashboard(page);

  await expect(page.getByRole("heading", { name: "Broker status" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Domain signals" })).toBeVisible();
  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("dashboard-desktop.png"),
    animations: "disabled",
  });
});

test("captures the tablet dashboard shell", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1024, height: 1200 });
  await openDashboard(page);

  await expect(page.getByRole("heading", { name: "Broker status" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Domain signals" })).toBeVisible();
  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("dashboard-tablet.png"),
    animations: "disabled",
  });
});

test("captures the desktop dashboard shell in dark mode", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  await openDashboard(page, "dark");

  await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");
  await expect(page.getByRole("heading", { name: "Broker status" })).toBeVisible();
  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("dashboard-dark.png"),
    animations: "disabled",
  });
});

test("captures the dashboard refreshing state", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });

  let releaseRefresh: (() => void) | undefined;
  let topologyRequests = 0;

  await page.route("**/api/v1/topology", async (route) => {
    topologyRequests += 1;

    if (topologyRequests > 1) {
      await new Promise<void>((resolve) => {
        releaseRefresh = resolve;
      });
    }

    await route.continue();
  });

  await openDashboard(page);
  await expect(page.getByRole("heading", { name: "Domain signals" })).toBeVisible();

  await page.getByRole("button", { name: "Refresh topology" }).click();
  await expect(page.locator('[data-slot="badge"]').filter({ hasText: "Refreshing" })).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("dashboard-refreshing.png"),
    animations: "disabled",
  });

  releaseRefresh?.();
  await page.waitForTimeout(100);
});

test("captures the desktop domain dropdown and closes on navigation", async ({
  page,
}, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  await openDashboard(page);

  await page.getByRole("button", { name: "Domains" }).click();
  const dropdown = page.locator('[data-slot="dropdown-content"]');

  await expect(dropdown).toBeVisible();
  await expect(page.getByText("Domain pages")).toBeVisible();
  await expect(page.getByRole("link", { name: /Queue/ }).first()).toBeVisible();
  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("domains-dropdown-open.png"),
    animations: "disabled",
  });

  await dropdown.locator('a[href="/queue"]').click();
  await expect(page).toHaveURL(/\/queue$/);
  await expect(page.locator("main#main-content")).toHaveCount(1);
  await expect(dropdown).toBeHidden();
});

test("captures a sidebar domain page", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  await page.goto("/queue");

  await expect(page.locator("main#main-content")).toHaveCount(1);
  await expect(page.locator(".page-frame-sidebar")).toBeVisible();
  await expect(page.getByRole("heading", { name: "Queue overview" })).toBeVisible();
  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("queue-sidebar.png"),
    animations: "disabled",
  });
});

test("captures lease overview empty state", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  await mockDomainOverviewApis(page, {
    lease: applyLeaseOverride({ realms: [] }),
  });

  await page.goto("/lease");
  await expect(page.getByRole("heading", { name: "Lease overview" })).toBeVisible();
  await expect(page.getByText("No lease realms are currently visible.")).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("lease-empty-desktop.png"),
    animations: "disabled",
  });
});

test("captures notice overview empty state", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  await mockDomainOverviewApis(page, {
    notice: {
      realms: [],
      stats: {
        delivery_drops_total: 0,
        publishes_per_second: 0,
        routes_active: 0,
        wildcard_limit_rejects_total: 0,
        subscriptions_active: 0,
        max_route_subscribers: 0,
      },
    },
  });

  await page.goto("/notice");
  await expect(page.getByRole("heading", { name: "Notice overview" })).toBeVisible();
  await expect(page.getByText("No notice realms are currently visible.")).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("notice-empty-desktop.png"),
    animations: "disabled",
  });
});

test("captures rpc overview empty state", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  await mockDomainOverviewApis(page, {
    rpc: {
      realms: [],
      stats: {
        invalid_sequence_errors_dropped_total: 0,
        invalid_sequence_errors_forwarded_total: 0,
        invalid_sequence_responses_total: 0,
        operations_per_second: 0,
        pending_routes_active: 0,
        request_timeouts_total: 0,
        requests_pending: 0,
        responses_dropped_closed_caller_total: 0,
        responses_missing_pending_total: 0,
        workers_registered: 0,
      },
    },
  });

  await page.goto("/rpc");
  await expect(page.getByRole("heading", { name: "RPC overview" })).toBeVisible();
  await expect(page.getByText("No RPC realms are currently visible.")).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("rpc-empty-desktop.png"),
    animations: "disabled",
  });
});

test("captures schedule overview empty state", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  await mockDomainOverviewApis(page, {
    schedule: {
      realms: [],
      stats: {
        ack_failures_total: 0,
        cancel_persistence_failures_total: 0,
        create_persistence_failures_total: 0,
        executions_per_minute: 0,
        notify_failures_total: 0,
        overdue_normalizations_total: 0,
        pending_fire_claims: 0,
        schedules_active: 0,
        subscriptions_active: 0,
        upsert_persistence_failures_total: 0,
      },
    },
  });

  await page.goto("/schedule");
  await expect(page.getByRole("heading", { name: "Schedule overview" })).toBeVisible();
  await expect(page.getByText("No schedule realms are currently visible.")).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("schedule-empty-desktop.png"),
    animations: "disabled",
  });
});

test.describe("captures domain overview templates", () => {
  for (const overviewPage of domainOverviewPages) {
    test(`captures ${overviewPage.domain} overview on desktop`, async ({ page }, testInfo) => {
      await page.setViewportSize({ width: 1440, height: 1200 });
      await mockDomainOverviewApis(page);
      await page.goto(overviewPage.path);

      await expect(page.getByRole("heading", { name: overviewPage.heading })).toBeVisible();
      await expect(page.locator("main#main-content")).toHaveCount(1);

      await page.screenshot({
        fullPage: true,
        path: testInfo.outputPath(`${overviewPage.domain}-desktop.png`),
        animations: "disabled",
      });
    });

    test(`captures ${overviewPage.domain} overview on mobile`, async ({ page }, testInfo) => {
      await page.setViewportSize({ width: 390, height: 844 });
      await mockDomainOverviewApis(page);
      await page.goto(overviewPage.path);

      await expect(page.getByRole("heading", { name: overviewPage.heading })).toBeVisible();
      await expect(page.locator("main#main-content")).toHaveCount(1);

      await page.screenshot({
        fullPage: true,
        path: testInfo.outputPath(`${overviewPage.domain}-mobile.png`),
        animations: "disabled",
      });
    });
  }
});

test("captures the mobile navbar panel", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await openDashboard(page);

  await page.getByRole("button", { name: "Menu" }).click();
  await expect(page.getByRole("link", { name: "Dashboard" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Domains" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Toggle color theme" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Sign out" })).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("mobile-nav-open.png"),
    animations: "disabled",
  });
});

test("captures sessions data state", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  await mockSessionsApi(page, sessionsWithData);

  await page.goto("/sessions");
  await expect(page.getByRole("heading", { name: "Active sessions", exact: true })).toBeVisible();
  await expect(page.locator("table tbody tr").first()).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("sessions-desktop.png"),
    animations: "disabled",
  });
});

test("captures sessions empty state", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  await mockSessionsApi(page, sessionsEmpty);

  await page.goto("/sessions");
  await expect(page.getByRole("heading", { name: "Active sessions", exact: true })).toBeVisible();
  await expect(
    page.getByRole("heading", { name: "No active sessions", exact: true }),
  ).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("sessions-empty-desktop.png"),
    animations: "disabled",
  });
});

test("captures sessions on mobile", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await mockSessionsApi(page, sessionsWithData);

  await page.goto("/sessions");
  await expect(page.getByRole("heading", { name: "Active sessions" })).toBeVisible();
  await expect(page.locator("ul.session-mobile-list li").first()).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("sessions-mobile.png"),
    animations: "disabled",
  });
});

test("captures metrics desktop", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  await mockMetricsApi(page);
  await page.goto("/admin/metrics");

  await expect(page.getByRole("heading", { name: "Metrics explorer" })).toBeVisible();
  await expect(page.locator('input[aria-label="Filter metrics"]')).toBeVisible();
  await expect(page.getByRole("button", { name: "Refresh metrics" })).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("metrics-desktop.png"),
    animations: "disabled",
  });
});

test("captures metrics filtered empty state", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  await mockMetricsApi(page);
  await page.goto("/admin/metrics");

  const filter = page.locator('input[aria-label="Filter metrics"]');
  await filter.fill("does-not-exist");
  await expect(page.getByText("No matching metrics")).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("metrics-filtered-empty.png"),
    animations: "disabled",
  });
});

test("captures metrics on mobile", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await mockMetricsApi(page);
  await page.goto("/admin/metrics");

  await expect(page.getByRole("heading", { name: "Metrics explorer" })).toBeVisible();
  await expect(page.locator('input[aria-label="Filter metrics"]')).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("metrics-mobile.png"),
    animations: "disabled",
  });
});

test("captures metrics in dark mode", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1200 });
  await mockMetricsApi(page);
  await page.addInitScript(() => {
    localStorage.setItem("fitz-admin-theme", "dark");
  });

  await page.goto("/admin/metrics");

  await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");
  await expect(page.getByRole("heading", { name: "Metrics explorer" })).toBeVisible();

  await page.screenshot({
    fullPage: true,
    path: testInfo.outputPath("metrics-dark.png"),
    animations: "disabled",
  });
});
