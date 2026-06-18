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

const adminFeatures = {
  admin_auth_required: false,
  admin_auth_mode: "open" as const,
};

type SessionsPayload = {
  sessions: Array<{
    connectedAt?: string;
    idleSeconds?: number;
    identityClaim?: string;
    identityValue?: string;
    key: string;
    messagesReceived?: number;
    messagesSent?: number;
    remoteAddress?: string;
    routeFamily?: number;
    sessionId?: string;
    subject?: string;
    transport?: string;
  }>;
};

const sessionsWithData: SessionsPayload = {
  sessions: [
    {
      connectedAt: "2026-05-21T13:00:00Z",
      idleSeconds: 12,
      identityClaim: "tid",
      identityValue: "default",
      key: "session-1",
      messagesReceived: 2,
      messagesSent: 3,
      remoteAddress: "127.0.0.1",
      routeFamily: 1,
      sessionId: "session-1",
      subject: "user:1",
      transport: "ws",
    },
    {
      connectedAt: "2026-05-21T13:01:00Z",
      idleSeconds: 45,
      identityClaim: "tenant",
      identityValue: "ops",
      key: "session-2",
      messagesReceived: 4,
      messagesSent: 8,
      remoteAddress: "2001:db8::1ff:fe23:4567:890a",
      routeFamily: 2,
      sessionId: "session-long-id-2",
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
  await expect(page.getByRole("heading", { name: "Active sessions" })).toBeVisible();
  await expect(page.getByText("Session summary")).toBeVisible();

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
  await expect(page.getByRole("heading", { name: "Active sessions" })).toBeVisible();
  await expect(page.getByText("No active sessions")).toBeVisible();

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
  await expect(page.getByText("Route family")).toBeVisible();

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
