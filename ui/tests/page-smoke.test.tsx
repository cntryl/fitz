import { describe, expect, it } from "vite-plus/test";
import { cleanupApp } from "@askrjs/askr/boot";
import { queryState } from "@askrjs/askr/testing";
import { mountRoute, pageSmokeMocks, queryOptions, resetQueries } from "./page-smoke/harness";
import {
  domainOverviews,
  inventory,
  noticeAreaInventory,
  noticeHierarchyRoutes,
  noticeOperationRows,
  noticeOverview,
  noticeRealmInventory,
  noticeResourceRows,
  scheduleHierarchyRoutes,
} from "./page-smoke/fixtures";

const mocks = pageSmokeMocks();
const PAGE_SMOKE_TIMEOUT_MS = 30_000;

describe("admin page smoke tests", () => {
  it(
    "mounts every authenticated route with success data",
    async () => {
      const pages = [
        {
          assertText: "Fitz status",
          module: () => import("@/pages/app/home"),
          path: "/",
          routePath: "/",
        },
        {
          assertText: "Active sessions",
          module: () => import("@/pages/app/sessions"),
          path: "/sessions",
          routePath: "/sessions",
        },
        {
          assertText: "Metrics explorer",
          module: () => import("@/pages/app/metrics"),
          path: "/admin/metrics",
          routePath: "/admin/metrics",
        },
        {
          assertText: "Diagnostics",
          module: () => import("@/pages/app/diagnostics"),
          path: "/diagnostics",
          routePath: "/diagnostics",
        },
        {
          assertText: "Queue inventory",
          module: () => import("@/pages/app/queue"),
          path: "/queue",
          routePath: "/queue",
        },
        {
          assertText: "Queue inventory",
          module: () => import("@/pages/app/queue"),
          path: "/queue/default",
          routePath: "/queue/{realm}",
        },
        {
          assertText: "Queue inventory",
          module: () => import("@/pages/app/queue"),
          path: "/queue/default/ops",
          routePath: "/queue/{realm}/{area}",
        },
        {
          assertText: "Queue resource inspection",
          module: () => import("@/pages/app/queue-resource"),
          path: "/admin/1/queue/default/ops/primary",
          routePath: "/admin/{family}/queue/{realm}/{area}/{resource}",
        },
        {
          assertText: "KV tables",
          module: () => import("@/pages/app/kv"),
          path: "/kv",
          routePath: "/kv",
        },
        {
          assertText: "KV tables",
          module: () => import("@/pages/app/kv"),
          path: "/kv/default",
          routePath: "/kv/{realm}",
        },
        {
          assertText: "KV tables",
          module: () => import("@/pages/app/kv"),
          path: "/kv/default/ops",
          routePath: "/kv/{realm}/{area}",
        },
        {
          assertText: "KV resource",
          module: () => import("@/pages/app/kv-resource"),
          path: "/admin/1/kv/default/ops/primary",
          routePath: "/admin/{family}/kv/{realm}/{area}/{resource}",
        },
        {
          assertText: "Lease inventory",
          module: () => import("@/pages/app/lease"),
          path: "/lease",
          routePath: "/lease",
        },
        {
          assertText: "Lease inventory",
          module: () => import("@/pages/app/lease"),
          path: "/lease/default",
          routePath: "/lease/{realm}",
        },
        {
          assertText: "Lease inventory",
          module: () => import("@/pages/app/lease"),
          path: "/lease/default/ops",
          routePath: "/lease/{realm}/{area}",
        },
        {
          assertText: "primary",
          module: () => import("@/pages/app/lease-resource"),
          path: "/admin/1/lease/default/ops/primary",
          routePath: "/admin/{family}/lease/{realm}/{area}/{resource}",
        },
        {
          assertText: "Notice inventory",
          module: () => import("@/pages/app/notice"),
          path: "/notice",
          routePath: "/notice",
        },
        {
          assertText: "Notice inventory",
          module: () => import("@/pages/app/notice"),
          path: "/notice/default",
          routePath: "/notice/{realm}",
        },
        {
          assertText: "Notice inventory",
          module: () => import("@/pages/app/notice"),
          path: "/notice/default/ops",
          routePath: "/notice/{realm}/{area}",
        },
        {
          assertText: "RPC inventory",
          module: () => import("@/pages/app/rpc"),
          path: "/rpc",
          routePath: "/rpc",
        },
        {
          assertText: "Schedule inventory",
          module: () => import("@/pages/app/schedule"),
          path: "/schedule",
          routePath: "/schedule",
        },
        {
          assertText: "Stream inventory",
          module: () => import("@/pages/app/stream"),
          path: "/stream",
          routePath: "/stream",
        },
      ];

      for (const page of noticeHierarchyRoutes) {
        const { default: Component } = await page.module();
        const root = await mountRoute(page.path, page.routePath, Component);

        expect(root.textContent).toContain(page.assertText);
        expect(root.querySelectorAll("main#main-content")).toHaveLength(1);

        cleanupApp(root);
        document.body.innerHTML = "";
      }

      for (const page of scheduleHierarchyRoutes) {
        const { default: Component } = await page.module();
        const root = await mountRoute(page.path, page.routePath, Component);

        expect(root.textContent).toContain(page.assertText);
        expect(root.querySelectorAll("main#main-content")).toHaveLength(1);

        cleanupApp(root);
        document.body.innerHTML = "";
      }

      for (const page of pages) {
        const { default: Component } = await page.module();
        const root = await mountRoute(page.path, page.routePath, Component);

        expect(root.textContent).toContain(page.assertText);
        expect(root.textContent?.trim().length).toBeGreaterThan(0);
        expect(root.querySelector('[data-slot="shell"]')).toBeNull();
        expect(root.querySelectorAll("main#main-content")).toHaveLength(1);

        cleanupApp(root);
        document.body.innerHTML = "";
      }
    },
    PAGE_SMOKE_TIMEOUT_MS,
  );
  it(
    "renders all domain overviews with the shared frame contract",
    async () => {
      for (const page of domainOverviews) {
        const { default: Component } = await page.module();

        const root = await mountRoute(page.path, page.routePath, Component);
        const text = root.textContent ?? "";

        expect(root.querySelectorAll("main#main-content")).toHaveLength(1);
        expect(text).toContain(page.assertText);
        expect(text).toContain("Resource inventory");
        expect(text).toContain("Route");
        expect(text).toContain(`${page.path.slice(1)}://default/ops/primary`);
        for (const statLabel of page.statLabels) {
          expect(text).toContain(statLabel);
        }
        expect(text).toContain("Refresh");
        expect(text).toMatch(/Live|Healthy|Quiet|Pressure|Attention/);
        expect(root.querySelector('[data-slot="virtual-table"]')).toBeTruthy();
        expect(root.querySelector(`a[href="${page.resourceHref}"]`)).toBeTruthy();

        cleanupApp(root);
        document.body.innerHTML = "";
      }
    },
    PAGE_SMOKE_TIMEOUT_MS,
  );
  it(
    "renders the flat inventory for domain overview, realm, and area routes",
    async () => {
      for (const page of domainOverviews) {
        const { default: Component } = await page.module();
        const routeVariants = [
          { path: page.path, routePath: page.routePath },
          { path: `${page.path}/default`, routePath: `${page.routePath}/{realm}` },
          { path: `${page.path}/default/ops`, routePath: `${page.routePath}/{realm}/{area}` },
        ];

        for (const routeVariant of routeVariants) {
          resetQueries();
          const root = await mountRoute(routeVariant.path, routeVariant.routePath, Component);
          const text = root.textContent ?? "";

          expect(text).toContain(page.assertText);
          expect(text).toContain("Resource inventory");
          expect(text).toContain("Route");
          expect(text).toContain(`${page.path.slice(1)}://default/ops/primary`);
          if (routeVariant.path === page.path) {
            for (const statLabel of page.statLabels) {
              expect(text).toContain(statLabel);
            }
          }
          expect(root.querySelector(`a[href="${page.resourceHref}"]`)).toBeTruthy();

          cleanupApp(root);
          document.body.innerHTML = "";
        }
      }
    },
    PAGE_SMOKE_TIMEOUT_MS,
  );
  it("covers notice drill-down loading and error states", async () => {
    const { default: NoticePage } = await import("@/pages/app/notice");
    const { default: NoticeOperationPage } = await import("@/pages/app/notice-operation");

    mocks.queryStates.inventory = queryState.loading(queryOptions());
    const noticeOverviewRoot = await mountRoute("/notice", "/notice", NoticePage);
    expect(noticeOverviewRoot.textContent).toContain("Loading notice inventory...");
    cleanupApp(noticeOverviewRoot);
    document.body.innerHTML = "";

    mocks.queryStates.inventory = queryState.error(
      new Error("Notice inventory unavailable"),
      undefined,
      queryOptions(),
    );
    const noticeOverviewError = await mountRoute("/notice", "/notice", NoticePage);
    expect(noticeOverviewError.textContent).toContain("Unable to load notice inventory");
    cleanupApp(noticeOverviewError);
    document.body.innerHTML = "";

    mocks.queryStates.inventory = queryState.fresh(inventory, queryOptions());
    const noticeRealmRoot = await mountRoute("/notice/default", "/notice/{realm}", NoticePage);
    expect(noticeRealmRoot.textContent).toContain("Notice inventory");
    expect(noticeRealmRoot.textContent).toContain("Resource inventory");
    expect(noticeRealmRoot.textContent).toContain("notice://default/ops/primary");
    cleanupApp(noticeRealmRoot);
    document.body.innerHTML = "";

    const noticeAreaRoot = await mountRoute(
      "/notice/default/ops",
      "/notice/{realm}/{area}",
      NoticePage,
    );
    expect(noticeAreaRoot.textContent).toContain("Notice inventory");
    expect(noticeAreaRoot.textContent).toContain("Resource inventory");
    expect(noticeAreaRoot.textContent).toContain("notice://default/ops/primary");
    cleanupApp(noticeAreaRoot);
    document.body.innerHTML = "";

    mocks.queryStates.noticeResourceRows = queryState.loading(queryOptions());
    const noticeResourceRoot = await mountRoute(
      "/notice/default/ops/primary",
      "/notice/{realm}/{area}/{resource}",
      NoticePage,
    );
    expect(noticeResourceRoot.textContent).toContain("Loading notice operation rows...");
    cleanupApp(noticeResourceRoot);
    document.body.innerHTML = "";

    mocks.queryStates.noticeResourceRows = queryState.fresh(noticeResourceRows, queryOptions());
    mocks.queryStates.noticeOperationRows = queryState.loading(queryOptions());
    const noticeOperationRoot = await mountRoute(
      "/notice/default/ops/primary/GetStatus",
      "/notice/{realm}/{area}/{resource}/{operation}",
      NoticeOperationPage,
    );
    expect(noticeOperationRoot.textContent).toContain("Loading notice operation deliveries...");
    cleanupApp(noticeOperationRoot);
    document.body.innerHTML = "";

    mocks.queryStates.notice = queryState.fresh(noticeOverview, queryOptions());
    mocks.queryStates.noticeRealm = queryState.fresh(noticeRealmInventory, queryOptions());
    mocks.queryStates.noticeArea = queryState.fresh(noticeAreaInventory, queryOptions());
    mocks.queryStates.noticeResourceRows = queryState.fresh(noticeResourceRows, queryOptions());
    mocks.queryStates.noticeOperationRows = queryState.fresh(noticeOperationRows, queryOptions());
  });
  it("renders domain overview loading states consistently", async () => {
    for (const page of domainOverviews) {
      resetQueries();
      mocks.queryStates[page.inventoryKey] = queryState.loading(queryOptions());

      const { default: Component } = await page.module();
      const root = await mountRoute(page.path, page.routePath, Component);

      expect(root.textContent).toContain(page.loadingText);
      expect(root.querySelectorAll("main#main-content")).toHaveLength(1);

      cleanupApp(root);
      document.body.innerHTML = "";
    }
  });
});
