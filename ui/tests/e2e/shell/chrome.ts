import { expect, type Page } from "@playwright/test";
import {
  mockDiagnosticsApis,
  mockDomainOverviewApis,
  mockHomeRouteApis,
  mockMetricsApi,
  mockSessionsApi,
  sessionsWithData,
} from "./api-fixtures";
import { parseRouteResourceScope } from "./resource-fixtures";
import {
  mockQueueResourceApis,
  mockResourceDetailApis,
  mockScheduleResourceApis,
} from "./resource-mocks";

export async function openDashboard(
  page: Page,
  theme: "light" | "dark" = "light",
  setupApis = true,
) {
  if (theme === "dark") {
    await page.addInitScript(() => {
      localStorage.setItem("fitz-admin-theme", "dark");
    });
  }

  if (setupApis) {
    await mockHomeRouteApis(page);
  }

  await page.goto("/admin/1");

  await expect(page.locator("main#main-content")).toHaveCount(1);
  const primaryNav = page.getByRole("navigation", { name: "Primary navigation" });
  const contextNav = page.getByRole("navigation", { name: "Operator context" });
  const viewport = page.viewportSize();

  await expect(contextNav.getByRole("link", { name: "Fitz admin home" })).toBeVisible();
  await expect(contextNav.getByRole("button", { name: "Route Family selector" })).toBeVisible();
  await expect(contextNav.getByRole("button", { name: "Toggle color theme" })).toBeVisible();

  if ((viewport?.width ?? 0) < 768) {
    await expect(primaryNav.getByRole("button", { name: /Menu|Navigation/ })).toBeVisible();
    return;
  }

  await expect(primaryNav.getByRole("link", { name: "Overview" })).toBeVisible();
  await expect(primaryNav.getByRole("link", { name: "Diagnostics" })).toBeVisible();
  await expect(primaryNav.getByRole("link", { name: "Metrics" })).toBeVisible();
  await expect(primaryNav.getByRole("link", { name: "Queue" })).toBeVisible();
}

export type RouteChrome = "app" | "auth";

export type ThemeMode = "light" | "dark";

export type ViewportPreset = {
  height: number;
  isMobile: boolean;
  key: "desktop" | "mobile" | "tablet";
  width: number;
};

export type RouteScenario = {
  path: string;
  setup: (page: Page) => Promise<void>;
  shell: RouteChrome;
  title: string | RegExp;
};

export function exactHeadingMatcher(title: string | RegExp): string | RegExp {
  if (title instanceof RegExp) {
    return title;
  }

  const escaped = title.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  return new RegExp(`^${escaped}(?:\\s.+)?$`);
}

export const viewportPresets: ViewportPreset[] = [
  {
    height: 1200,
    isMobile: false,
    key: "desktop",
    width: 1440,
  },
  {
    height: 1200,
    isMobile: false,
    key: "tablet",
    width: 1024,
  },
  {
    height: 844,
    isMobile: true,
    key: "mobile",
    width: 390,
  },
];

export const themeModes: ThemeMode[] = ["light", "dark"];

export function normalizeRoute(path: string) {
  if (path === "/") {
    return "home";
  }

  return path
    .replace(/^\//, "")
    .replace(/[^a-zA-Z0-9]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
}

export async function applyTheme(page: Page, theme: ThemeMode) {
  if (theme === "dark") {
    await page.addInitScript(() => {
      localStorage.setItem("fitz-admin-theme", "dark");
    });
    return;
  }

  await page.addInitScript(() => {
    localStorage.removeItem("fitz-admin-theme");
  });
}

export async function expectNoHorizontalOverflow(page: Page) {
  const visibleOverflow = await page.evaluate(() => {
    const viewportRight = window.innerWidth + 2;
    const viewportLeft = -2;
    const isClippedByScrollableAncestor = (element: HTMLElement) => {
      let parent = element.parentElement;

      while (parent && parent !== document.body) {
        const style = window.getComputedStyle(parent);
        const clipsInlineOverflow = ["auto", "clip", "hidden", "scroll"].includes(style.overflowX);

        if (clipsInlineOverflow) {
          const rect = parent.getBoundingClientRect();
          if (rect.left >= viewportLeft && rect.right <= viewportRight) {
            return true;
          }
        }

        parent = parent.parentElement;
      }

      return false;
    };
    const offenders = Array.from(document.body.querySelectorAll<HTMLElement>("*")).filter(
      (element) => {
        // askrjs/askr-charts#2 tracks sr-only chart tables inflating document width.
        if (element.closest(".ak-chart-sr-only")) {
          return false;
        }

        const style = window.getComputedStyle(element);
        if (style.display === "none" || style.visibility === "hidden") {
          return false;
        }

        const rect = element.getBoundingClientRect();
        if (rect.width === 0 || rect.height === 0) {
          return false;
        }

        if (isClippedByScrollableAncestor(element)) {
          return false;
        }

        return rect.right > viewportRight || rect.left < viewportLeft;
      },
    );

    return offenders.length === 0;
  });

  expect(visibleOverflow).toBe(true);
}

export async function expectRouteChrome(page: Page, route: RouteScenario) {
  const viewport = page.viewportSize();
  const isMobile = (viewport?.width ?? 0) < 768;
  const primaryNav = page.getByRole("navigation", { name: "Primary navigation" });
  const contextNav = page.getByRole("navigation", { name: "Operator context" });
  const headingOptions =
    route.shell === "app"
      ? { level: 1, name: exactHeadingMatcher(route.title) }
      : { name: exactHeadingMatcher(route.title) };

  await expect(page.locator("main#main-content")).toHaveCount(1);
  await expect(page.getByRole("heading", headingOptions)).toBeVisible();

  await expectNoHorizontalOverflow(page);

  if (route.shell === "app") {
    await expect(contextNav.getByRole("link", { name: "Fitz admin home" })).toBeVisible();
    await expect(contextNav.getByRole("button", { name: "Route Family selector" })).toBeVisible();
    await expect(contextNav.getByRole("button", { name: "Toggle color theme" })).toBeVisible();

    if (isMobile) {
      const menu = primaryNav.getByRole("button", { name: /Menu|Navigation/ });
      await expect(menu).toBeVisible();
      await menu.click();

      await expect(primaryNav.getByRole("link", { name: "Overview" })).toBeVisible();
      await expect(primaryNav.getByRole("link", { name: "Diagnostics" })).toBeVisible();
      await expect(primaryNav.getByRole("link", { name: "Metrics" })).toBeVisible();
      await expect(primaryNav.getByRole("link", { name: "Queue" })).toBeVisible();
      await expectNoHorizontalOverflow(page);

      await page.keyboard.press("Escape");
      return;
    }

    await expect(primaryNav.getByRole("link", { name: "Overview" })).toBeVisible();
    await expect(primaryNav.getByRole("link", { name: "Stream" })).toBeVisible();
    await expect(primaryNav.getByRole("link", { name: "KV" })).toBeVisible();
    await expect(primaryNav.getByRole("link", { name: "Queue" })).toBeVisible();
    await expect(primaryNav.getByRole("link", { name: "Diagnostics" })).toBeVisible();
    await expect(primaryNav.getByRole("link", { name: "Metrics" })).toBeVisible();
    await expectNoHorizontalOverflow(page);
    return;
  }

  await expect(primaryNav.getByRole("link", { name: "Fitz admin home" })).toBeVisible();
  await expect(primaryNav.getByRole("button", { name: "Toggle color theme" })).toBeVisible();
}

export const sprint16Routes: RouteScenario[] = [
  {
    path: "/admin/1",
    shell: "app",
    setup: mockHomeRouteApis,
    title: "Fitz status",
  },
  {
    path: "/admin/1/sessions",
    shell: "app",
    setup: (page) => mockSessionsApi(page, sessionsWithData),
    title: "Active sessions",
  },
  {
    path: "/admin/1/metrics",
    shell: "app",
    setup: mockMetricsApi,
    title: "Metrics explorer",
  },
  {
    path: "/admin/1/diagnostics",
    shell: "app",
    setup: mockDiagnosticsApis,
    title: "Diagnostics",
  },
  {
    path: "/admin/1/settings",
    shell: "app",
    setup: mockHomeRouteApis,
    title: "Settings",
  },
  {
    path: "/admin/1/lease",
    shell: "app",
    setup: (page) => mockDomainOverviewApis(page),
    title: "Lease inventory",
  },
  {
    path: "/admin/1/lease/default",
    shell: "app",
    setup: (page) => mockDomainOverviewApis(page),
    title: "Lease inventory",
  },
  {
    path: "/admin/1/lease/default/ops",
    shell: "app",
    setup: (page) => mockDomainOverviewApis(page),
    title: "Lease inventory",
  },
  {
    path: "/admin/1/notice",
    shell: "app",
    setup: (page) => mockDomainOverviewApis(page),
    title: "Notice inventory",
  },
  {
    path: "/admin/1/notice/default",
    shell: "app",
    setup: (page) => mockDomainOverviewApis(page),
    title: "Notice inventory",
  },
  {
    path: "/admin/1/notice/default/ops",
    shell: "app",
    setup: (page) => mockDomainOverviewApis(page),
    title: "Notice inventory",
  },
  {
    path: "/admin/1/notice/default/ops/primary",
    shell: "app",
    setup: (page) => mockDomainOverviewApis(page),
    title: "Notice operations",
  },
  {
    path: "/admin/1/notice/default/ops/primary/GetStatus",
    shell: "app",
    setup: (page) => mockDomainOverviewApis(page),
    title: "GetStatus",
  },
  {
    path: "/admin/1/rpc",
    shell: "app",
    setup: (page) => mockDomainOverviewApis(page),
    title: "RPC inventory",
  },
  {
    path: "/admin/1/schedule",
    shell: "app",
    setup: (page) => mockDomainOverviewApis(page),
    title: "Schedule inventory",
  },
  {
    path: "/admin/1/schedule/default",
    shell: "app",
    setup: (page) => mockDomainOverviewApis(page),
    title: "Schedule inventory",
  },
  {
    path: "/admin/1/schedule/default/ops",
    shell: "app",
    setup: (page) => mockDomainOverviewApis(page),
    title: "Schedule inventory",
  },
  {
    path: "/admin/1/queue",
    shell: "app",
    setup: (page) => mockDomainOverviewApis(page),
    title: "Queue inventory",
  },
  {
    path: "/admin/1/queue/default",
    shell: "app",
    setup: (page) => mockDomainOverviewApis(page),
    title: "Queue inventory",
  },
  {
    path: "/admin/1/queue/default/ops",
    shell: "app",
    setup: (page) => mockDomainOverviewApis(page),
    title: "Queue inventory",
  },
  {
    path: "/admin/1/stream",
    shell: "app",
    setup: (page) => mockDomainOverviewApis(page),
    title: "Stream inventory",
  },
  {
    path: "/admin/1/kv",
    shell: "app",
    setup: (page) => mockDomainOverviewApis(page),
    title: "KV tables",
  },
  {
    path: "/admin/1/queue/default/ops/primary",
    shell: "app",
    setup: (page) =>
      mockQueueResourceApis(page, parseRouteResourceScope("/admin/1/queue/default/ops/primary")),
    title: "Queue resource inspection",
  },
  {
    path: "/admin/1/kv/default/ops/primary",
    shell: "app",
    setup: (page) =>
      mockResourceDetailApis(
        page,
        "kv",
        parseRouteResourceScope("/admin/1/kv/default/ops/primary"),
      ),
    title: "primary",
  },
  {
    path: "/admin/1/lease/default/ops/primary",
    shell: "app",
    setup: (page) =>
      mockResourceDetailApis(
        page,
        "lease",
        parseRouteResourceScope("/admin/1/lease/default/ops/primary"),
      ),
    title: "primary",
  },
  {
    path: "/admin/7/lease/default/ops/primary",
    shell: "app",
    setup: (page) =>
      mockResourceDetailApis(page, "lease", {
        area: "ops",
        realm: "default",
        resource: "primary",
      }),
    title: "primary",
  },
  {
    path: "/admin/1/rpc/default/ops/primary",
    shell: "app",
    setup: (page) =>
      mockResourceDetailApis(
        page,
        "rpc",
        parseRouteResourceScope("/admin/1/rpc/default/ops/primary"),
      ),
    title: "primary",
  },
  {
    path: "/admin/1/schedule/default/ops/primary",
    shell: "app",
    setup: (page) =>
      mockScheduleResourceApis(
        page,
        parseRouteResourceScope("/admin/1/schedule/default/ops/primary"),
      ),
    title: "Schedule resource inspection",
  },
  {
    path: "/admin/1/stream/default/ops/primary",
    shell: "app",
    setup: (page) =>
      mockResourceDetailApis(
        page,
        "stream",
        parseRouteResourceScope("/admin/1/stream/default/ops/primary"),
      ),
    title: "primary",
  },
  {
    path: "/login",
    shell: "auth",
    setup: (page) => mockHomeRouteApis(page),
    title: "Sign in to Fitz Admin",
  },
  {
    path: "/logout",
    shell: "auth",
    setup: (page) => mockHomeRouteApis(page),
    title: /Signed out|Signing out/,
  },
];
