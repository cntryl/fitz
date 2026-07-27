import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import { cleanupApp, createSPA } from "@askrjs/askr/boot";
import { createRouteRegistry, group, type RouteHandler, route } from "@askrjs/askr/router";
import { Card, CardContent } from "@askrjs/themes/components";
import { ThemeScope } from "@askrjs/themes/theme";
import AppLayout from "@/pages/app/_layout";
import RouteFamilySelectorPage from "@/pages/app/route-family";
import AuthLayout from "@/pages/auth/_layout";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import DomainResourceInventoryTable from "@/components/shared/domain-resource-inventory-table";
import {
  domainResourceInventoryRows,
  PureDomainResourceInventoryTable,
  scopeDomainResourceInventoryRows,
} from "@/components/shared/domain-resource-inventory-table";
import DomainHeader from "@/components/shared/domain-header";
import OperatorScopeStrip from "@/components/shared/operator-scope-strip";
import QueueInflightTable from "@/components/shared/queue-inflight-table";
import {
  QueryCompactEmptyState,
  QueryEmptyState,
  QueryErrorState,
} from "@/components/shared/query-state";
import { domainLinks, pathWithRouteFamily } from "@/shared/navigation/domains";
import {
  createOperatorScopeSnapshot,
  OperatorScope,
  type OperatorScopeSnapshot,
} from "@/shared/operator-scope";
import { routeFamilyIconColor } from "@/shared/route-family-appearance";
import { topologyOverview } from "./fixtures/topology";

vi.mock("@/features/session/session-query", () => ({
  createCurrentSessionQuery: () => ({
    data: {
      authenticated: true,
      routeFamilies: ["1", "7"],
      routeFamiliesWildcard: false,
      username: "admin",
    },
  }),
}));

vi.mock("@/features/topology/topology-query", () => ({
  createMessagingTopologyQuery: () => ({
    data: null,
    error: null,
    loading: false,
    refresh: vi.fn(),
    refreshing: false,
    stale: false,
  }),
}));

async function mount(handler: RouteHandler, path = "/") {
  cleanupApp("app");
  document.body.innerHTML = '<div id="app"></div>';
  window.history.pushState({}, "", path);

  const root = document.getElementById("app");
  if (!root) {
    throw new Error("Missing test app root");
  }

  await createSPA({
    root,
    registry: createRouteRegistry(() => {
      route(path, (params: Record<string, string>, context?: { signal: AbortSignal }) => (
        <ThemeScope defaultTheme="system" storageKey="fitz-admin-theme">
          {handler(params, context)}
        </ThemeScope>
      ));
    }),
  });

  await new Promise<void>((resolve) => queueMicrotask(() => resolve()));
  return root;
}

function browserColor(value: string) {
  const element = document.createElement("span");
  element.style.color = value;
  return element.style.color;
}

afterEach(() => {
  cleanupApp("app");
  document.body.innerHTML = "";
});

describe("shared UI polish contracts", () => {
  it("renders route-family loading, error, and empty states", async () => {
    const base: OperatorScopeSnapshot = {
      retryRouteFamilies: vi.fn(),
      routeFamilies: [],
      routeFamilyError: null,
      routeFamilyState: "loading",
      routeFamiliesWildcard: false,
      selectedRouteFamily: { description: "Select a family", id: "", label: "Select Route Family" },
      selectedRouteFamilyId: "",
    };

    for (const state of ["loading", "error", "empty"] as const) {
      const snapshot = {
        ...base,
        routeFamilyError: state === "error" ? new Error("Family lookup failed") : null,
        routeFamilyState: state,
      };
      const root = await mount(() => (
        <OperatorScope value={snapshot}>
          <RouteFamilySelectorPage />
        </OperatorScope>
      ));

      expect(root.textContent).toContain(
        state === "loading"
          ? "Loading available Route Families"
          : state === "error"
            ? "Family lookup failed"
            : "No Route Families available",
      );
    }
  });

  it("uses the scoped URL as the only route-family selection state", async () => {
    const root = await mount(() => (
      <OperatorScope
        value={{
          retryRouteFamilies: vi.fn(),
          routeFamilies: [{ description: "Authorized family", id: "7", label: "Route Family 7" }],
          routeFamilyError: null,
          routeFamilyState: "ready",
          routeFamiliesWildcard: false,
          selectedRouteFamily: {
            description: "Select a family",
            id: "",
            label: "Select Route Family",
          },
          selectedRouteFamilyId: "",
        }}
      >
        <RouteFamilySelectorPage />
      </OperatorScope>
    ));

    const link = root.querySelector('a[aria-label="Open workspace for Route Family 7"]');
    expect(link?.getAttribute("href")).toBe("/admin/7");
    expect(link?.getAttribute("role")).toBeNull();
    expect(link?.getAttribute("tabindex")).toBeNull();
    expect(link?.textContent?.trim()).toBe("Route Family 7");
    expect(link?.textContent).not.toContain("Authorized family");
    expect(root.querySelector('[data-slot="card"]')).toBeNull();
    expect(document.title).toBe("Select Route Family · Fitz Admin");
    expect(document.activeElement).toBe(root.querySelector("main#main-content"));
  });

  it("accepts a concrete URL family through wildcard session access", () => {
    const snapshot = createOperatorScopeSnapshot(
      null,
      {
        authenticated: true,
        routeFamilies: [],
        routeFamiliesWildcard: true,
        username: "operator",
      },
      "42",
    );

    expect(snapshot.routeFamilyState).toBe("ready");
    expect(snapshot.routeFamiliesWildcard).toBe(true);
    expect(snapshot.selectedRouteFamilyId).toBe("42");
    expect(snapshot.selectedRouteFamily.label).toBe("Route Family 42");
  });

  it("does not add topology families outside the session allowlist", () => {
    const snapshot = createOperatorScopeSnapshot(
      {
        ...topologyOverview,
        sessionGroups: [
          ...topologyOverview.sessionGroups,
          { ...topologyOverview.sessionGroups[0]!, routeFamily: 42 },
        ],
      },
      {
        authRequired: true,
        authenticated: true,
        routeFamilies: ["1", "2", "3", "4", "5"],
        routeFamiliesWildcard: false,
        username: "admin",
      },
      "",
    );

    expect(snapshot.routeFamilies.map((family) => family.id)).toEqual(["1", "2", "3", "4", "5"]);
  });

  it("offers numeric Route Family entry to wildcard sessions", async () => {
    const root = await mount(() => (
      <OperatorScope
        value={{
          retryRouteFamilies: vi.fn(),
          routeFamilies: [],
          routeFamilyError: null,
          routeFamilyState: "ready",
          routeFamiliesWildcard: true,
          selectedRouteFamily: {
            description: "Select a family",
            id: "",
            label: "Select Route Family",
          },
          selectedRouteFamilyId: "",
        }}
      >
        <RouteFamilySelectorPage />
      </OperatorScope>
    ));

    const input = root.querySelector<HTMLInputElement>("#wildcard-route-family");
    expect(root.textContent).toContain("wildcard access");
    expect(input?.getAttribute("inputmode")).toBe("numeric");
    expect(input?.hasAttribute("required")).toBe(true);
  });

  it("omits numeric Route Family entry when wildcard sessions have a concrete list", async () => {
    const root = await mount(() => (
      <OperatorScope
        value={{
          retryRouteFamilies: vi.fn(),
          routeFamilies: [{ description: "Provisioned family", id: "1", label: "Route Family 1" }],
          routeFamilyError: null,
          routeFamilyState: "ready",
          routeFamiliesWildcard: true,
          selectedRouteFamily: {
            description: "Select a family",
            id: "",
            label: "Select Route Family",
          },
          selectedRouteFamilyId: "",
        }}
      >
        <RouteFamilySelectorPage />
      </OperatorScope>
    ));

    expect(root.textContent).toContain("Route Family 1");
    expect(root.querySelector("#wildcard-route-family")).toBeNull();
  });

  it("uses the page heading for the document title and moves focus to routed content", async () => {
    const root = await mount(
      () => (
        <DomainPageFrame>
          <DomainHeader
            title="Queue inventory"
            description="Durable work resources."
            status={{
              detail: "Verbose generated status summary.",
              label: "Live",
              tone: "success",
            }}
          />
        </DomainPageFrame>
      ),
      "/admin/1/queue",
    );
    const main = root.querySelector("main#main-content");

    expect(document.title).toBe("Queue inventory · Fitz Admin");
    expect(document.title).not.toContain("Live");
    expect(root.textContent).not.toContain("Durable work resources.");
    expect(root.textContent).not.toContain("Verbose generated status summary.");
    expect(document.activeElement).toBe(main);
  });

  it("clears route search through the owning URL-query callback", async () => {
    const onSearchChange = vi.fn();
    const root = await mount(() => (
      <main>
        <PureDomainResourceInventoryTable
          domain="notice"
          emptyDescription="No routes"
          onRowOpen={vi.fn()}
          onSearchChange={onSearchChange}
          rowHref={() => "/admin/1/notice/default/ops/primary"}
          rows={[{ area: "ops", realm: "default", resource: "primary" }]}
          searchValue="primary"
          title="Notice inventory"
        />
      </main>
    ));

    root.querySelector("button")?.dispatchEvent(new MouseEvent("click", { bubbles: true }));
    expect(onSearchChange).toHaveBeenCalledWith("");
  });

  it("sizes sparse resource inventories to their content minimum", async () => {
    const root = await mount(() => (
      <PureDomainResourceInventoryTable
        domain="notice"
        emptyDescription="No routes"
        onRowOpen={vi.fn()}
        onSearchChange={vi.fn()}
        rowHref={() => "/admin/1/notice/default/ops/primary"}
        rows={[{ area: "ops", realm: "default", resource: "primary" }]}
        searchValue=""
        title="Notice inventory"
      />
    ));

    expect(root.querySelector(".domain-resource-virtual-table")?.getAttribute("style")).toContain(
      "140px",
    );
  });

  it("scopes resource inventory rows to the route hierarchy", () => {
    const rows = domainResourceInventoryRows({
      realms: [
        {
          realm: "acme",
          areas: [
            { area: "payments", resources: ["orders"] },
            { area: "support", resources: ["tickets"] },
          ],
        },
        {
          realm: "globex",
          areas: [{ area: "payments", resources: ["billing"] }],
        },
      ],
    });

    expect(scopeDomainResourceInventoryRows(rows, { realm: "acme" })).toEqual([
      { area: "payments", realm: "acme", resource: "orders" },
      { area: "support", realm: "acme", resource: "tickets" },
    ]);
    expect(scopeDomainResourceInventoryRows(rows, { area: "payments", realm: "acme" })).toEqual([
      { area: "payments", realm: "acme", resource: "orders" },
    ]);
  });

  it("renders the compact secondary empty-state class", async () => {
    const root = await mount(() => (
      <QueryCompactEmptyState title="No suggested queries" description="Nothing recommended." />
    ));

    expect(root.querySelector(".domain-state-compact")).toBeTruthy();
  });

  it("disables and marks a refresh action busy while refreshing", async () => {
    const root = await mount(() => (
      <main>
        <DomainHeader
          title="Queue inventory"
          description="Durable work resources."
          primaryAction={{
            busy: true,
            disabled: true,
            label: "Refresh queue",
            onPress: vi.fn(),
          }}
        />
      </main>
    ));
    const refresh = root.querySelector('button[aria-label="Refresh queue"]');

    expect(refresh?.getAttribute("aria-busy")).toBe("true");
    expect(refresh?.hasAttribute("disabled")).toBe(true);
  });

  it("renders a route family selector when the URL has no valid family", async () => {
    const root = await mount(
      () => (
        <AppLayout>
          <DomainPageFrame>
            <section>Workspace</section>
          </DomainPageFrame>
        </AppLayout>
      ),
      "/admin",
    );

    expect(root.querySelector("main#main-content")?.textContent).toContain("Select Route Family");
    expect(root.textContent).not.toContain("Workspace");
    expect(root.querySelector('nav[aria-label="Primary navigation"]')).toBeNull();
    expect(root.querySelector('a[href="/admin/1"]')?.textContent).toContain("Route Family 1");
    expect(root.querySelector('a[href="/admin/7"]')?.textContent).toContain("Route Family 7");
    expect(root.querySelector('[data-slot="card"]')).toBeNull();
  });

  it("uses icon-backed shell controls with stable labels", async () => {
    const root = await mount(
      () => (
        <AppLayout>
          <DomainPageFrame>
            <section>Workspace</section>
          </DomainPageFrame>
        </AppLayout>
      ),
      "/admin/1",
    );

    const skipLink = root.querySelector('a[href="#main-content"]');
    const routeSurface = root.querySelector(".route-transition-surface");
    const main = root.querySelector("main#main-content");
    const themeToggle = root.querySelector('button[aria-label="Toggle color theme"]');
    const routeFamilySelector = root.querySelector('button[aria-label="Route Family selector"]');

    expect(root.textContent).toContain("Fitz");
    expect(root.textContent).toContain("Admin");
    expect(routeFamilySelector?.getAttribute("aria-label")).toBe("Route Family selector");
    expect(skipLink?.textContent).toContain("Skip to main content");
    expect(routeSurface?.tagName).toBe("DIV");
    expect(routeSurface?.parentElement?.classList.contains("operator-shell-layout")).toBe(true);
    expect(main?.getAttribute("tabindex")).toBe("-1");
    expect(main?.getAttribute("data-slot")).toBe("main");
    expect(root.querySelectorAll("main")).toHaveLength(1);
    expect(root.querySelectorAll('[data-slot="container"]')).toHaveLength(3);
    expect(root.querySelectorAll('[data-slot="container"][data-ak-layout="true"]')).toHaveLength(3);
    expect(root.querySelector('footer [href="https://github.com/cntryl/fitz"]')).toBeTruthy();
    expect(root.querySelector('footer [href="https://github.com/cntryl/fitz-ts"]')).toBeTruthy();
    expect(root.querySelector('footer [href="https://github.com/cntryl/fitz-go"]')).toBeTruthy();
    expect(root.textContent).toContain("fitz-ts");
    expect(root.textContent).toContain("fitz-go");
    expect(themeToggle).toBeTruthy();
    expect(themeToggle?.getAttribute("data-size")).toBe("icon");
    expect(themeToggle?.getAttribute("data-variant")).toBe("ghost");
    expect(themeToggle?.querySelector('[data-slot="icon"]')).toBeTruthy();
    expect(themeToggle?.textContent).not.toContain("☀");
    expect(themeToggle?.textContent).not.toContain("☾");
    expect(routeFamilySelector).toBeTruthy();
    expect(root.querySelector('form[role="search"]')).toBeNull();
    expect(routeFamilySelector?.querySelector('[data-slot="icon"]')).toBeTruthy();
    expect(routeFamilySelector?.querySelector<HTMLElement>(".route-family-icon")?.style.color).toBe(
      browserColor(routeFamilyIconColor("1")),
    );
    const signOut = root.querySelector('a[aria-label="Sign out"]');
    expect(signOut?.getAttribute("href")).toBe("/logout");
    expect(signOut?.textContent).toBe("");
    expect(root.textContent).not.toContain("Administration");
    expect(
      root
        .querySelector('[aria-label="Primary navigation"] a[href="/admin/1"]')
        ?.getAttribute("aria-current"),
    ).toBe("page");

    routeFamilySelector?.dispatchEvent(new MouseEvent("click", { bubbles: true }));
    await new Promise<void>((resolve) => queueMicrotask(() => resolve()));

    expect(document.body.textContent).toContain("Route Family");
    expect(
      document.body.querySelector<HTMLElement>(
        '.operator-route-family-option[data-route-family="7"] .route-family-icon',
      )?.style.color,
    ).toBe(browserColor(routeFamilyIconColor("7")));

    expect(root.querySelector('button[aria-label="Account menu"]')).toBeNull();
  });

  it("updates every sidebar link when the Route Family changes", async () => {
    cleanupApp("app");
    document.body.innerHTML = '<div id="app"></div>';
    window.history.pushState({}, "", "/admin/1/queue");
    const root = document.getElementById("app");

    if (!root) throw new Error("Missing test app root");

    const Workspace = () => (
      <DomainPageFrame>
        <section>Workspace</section>
      </DomainPageFrame>
    );
    const registry = createRouteRegistry(() => {
      group({ layout: AppLayout }, () => {
        route("/admin/{family}", Workspace);
        route("/admin/{family}/queue", Workspace);
      });
    });

    await createSPA({ root, registry });
    root.querySelector<HTMLButtonElement>('button[aria-label="Route Family selector"]')?.click();
    await new Promise<void>((resolve) => queueMicrotask(() => resolve()));
    document.body
      .querySelector<HTMLButtonElement>('.operator-route-family-option[data-route-family="7"]')
      ?.click();

    await vi.waitFor(() => expect(window.location.pathname).toBe("/admin/7/queue"));
    await vi.waitFor(() =>
      expect(
        root
          .querySelector('button[aria-label="Route Family selector"]')
          ?.getAttribute("aria-expanded"),
      ).toBe("false"),
    );
    expect(document.body.querySelector('[role="menu"]')).toBeNull();
    await vi.waitFor(() =>
      expect(root.querySelector('[aria-label="Primary navigation"]')).not.toBeNull(),
    );

    const sidebarHrefs = Array.from(
      root.querySelectorAll<HTMLAnchorElement>(
        '[aria-label="Primary navigation"] a[data-slot="sidebar-menu-button"]',
      ),
      (link) => link.getAttribute("href") ?? "",
    ).sort((first, second) => first.localeCompare(second));
    const expectedHrefs = [
      "/admin/7",
      "/admin/7/diagnostics",
      "/admin/7/metrics",
      "/admin/7/sessions",
      ...domainLinks.map((link) => `/admin/7/${link.segment}`),
    ].sort((first, second) => first.localeCompare(second));

    expect(sidebarHrefs).toEqual(expectedHrefs);
    expect(
      root
        .querySelector('a[data-slot="sidebar-menu-button"][href="/admin/7/queue"]')
        ?.getAttribute("aria-current"),
    ).toBe("page");
  });

  it("renders Sessions directly beneath the Route Family breadcrumb", async () => {
    const root = await mount(
      () => (
        <AppLayout>
          <DomainPageFrame>
            <section>Sessions content</section>
          </DomainPageFrame>
        </AppLayout>
      ),
      "/admin/1/sessions",
    );
    const breadcrumbs = root.querySelector('[aria-label="Resource hierarchy"]');

    expect(breadcrumbs?.textContent).toContain("Route Family 1");
    expect(breadcrumbs?.textContent).toContain("Sessions");
    expect(breadcrumbs?.textContent).not.toContain("Settings");
  });

  it("renders auth pages in a focused full-viewport shell", async () => {
    const root = await mount(
      () => (
        <AuthLayout>
          <section>Auth page</section>
        </AuthLayout>
      ),
      "/login",
    );

    expect(root.querySelector("main#main-content")?.textContent).toContain("Auth page");
    expect(root.querySelector("main#main-content")?.getAttribute("tabindex")).toBe("-1");
    expect(root.querySelector("main#main-content")?.classList.contains("auth-page")).toBe(true);
    expect(root.querySelector(".auth-panel")?.textContent).toContain("Auth page");
    expect(root.querySelector("header, footer")).toBeNull();
  });

  it("exposes individual domain pages from the app sidebar", async () => {
    const root = await mount(
      () => (
        <AppLayout>
          <DomainPageFrame>
            <section>Workspace</section>
          </DomainPageFrame>
        </AppLayout>
      ),
      "/admin/1",
    );

    const routeFamilySelector = root.querySelector('button[aria-label="Route Family selector"]');
    const contentBeforeOpen = root.querySelector('[data-slot="dropdown-content"]');
    const containers = document.querySelectorAll('[data-slot="container"]');

    expect(routeFamilySelector).toBeTruthy();
    expect(contentBeforeOpen).toBeNull();
    expect(containers.length).toBe(3);
    expect(
      document.querySelectorAll('[data-slot="container"][data-ak-layout="true"]'),
    ).toHaveLength(3);

    for (const link of domainLinks) {
      const item = root.querySelector(`a[href="${pathWithRouteFamily(link.href, "1")}"]`);

      expect(item?.textContent).toContain(link.title);
      expect(item?.querySelector('[data-slot="icon"]')).toBeTruthy();
    }
  });

  it("keeps the shared page frame to one full-width content surface", async () => {
    const root = await mount(
      () => (
        <AppLayout>
          <DomainPageFrame>
            <section>Workspace</section>
          </DomainPageFrame>
        </AppLayout>
      ),
      "/admin/1",
    );

    expect(root.querySelectorAll("main#main-content")).toHaveLength(1);
    expect(root.querySelectorAll(".page-frame-main")).toHaveLength(1);
    expect(root.querySelectorAll(".page-frame-sidebar")).toHaveLength(0);
    expect(root.querySelectorAll('[data-slot="container"][data-ak-layout="true"]')).toHaveLength(3);
    expect(root.textContent).toContain("Workspace");
  });

  it("renders query states as in-place surfaces instead of nested cards", async () => {
    const root = await mount(() => (
      <main>
        <QueryEmptyState title="No resources" description="Nothing is visible." />
        <QueryErrorState title="Unable to load" error={new Error("Network failed")} />
      </main>
    ));

    expect(root.querySelectorAll(".domain-state")).toHaveLength(2);
    expect(root.querySelector("[data-slot='card']")).toBeNull();
    expect(root.textContent).toContain("No resources");
    expect(root.textContent).toContain("Network failed");
    expect(root.querySelector('[role="alert"]')).toBeTruthy();
  });

  it("displays Route Family and realm as separate scope values without fallback", async () => {
    const root = await mount(() => (
      <main>
        <OperatorScopeStrip routeFamily="41" area="ops" resource="primary" freshness="Live" />
        <OperatorScopeStrip routeFamily="7" realm="default" area="ops" resource="primary" />
      </main>
    ));

    const strips = root.querySelectorAll(".operator-scope-strip");

    expect(strips[0]?.textContent).toContain("Route Family");
    expect(strips[0]?.textContent).toContain("Route Family 41");
    expect(strips[0]?.textContent).not.toContain("Realm41");
    expect(strips[0]?.textContent).not.toContain("RealmRoute Family 41");
    expect(strips[1]?.textContent).toContain("Route Family 7");
    expect(strips[1]?.textContent).toContain("Realmdefault");
  });

  it("renders flat resource inventory with virtual table links and metric typography", async () => {
    const root = await mount(() => (
      <main>
        <DomainResourceInventoryTable
          domain="kv"
          title="Resource inventory"
          emptyDescription="No resources are visible."
          inventory={{
            realms: [
              {
                realm: "default",
                areas: [
                  {
                    area: "ops",
                    resources: ["primary"],
                    resourceEntries: [
                      {
                        estimatedRecordCount: 300,
                        resource: "primary",
                      },
                    ],
                  },
                ],
              },
            ],
          }}
          metricColumns={[
            {
              id: "records",
              header: "Records",
              cell: (row) => row.estimatedRecordCount ?? "--",
            },
          ]}
        />
      </main>
    ));

    expect(root.textContent).toContain("Resource inventory");
    expect(root.textContent).toContain("Route");
    expect(root.textContent).toContain("Records");
    expect(root.textContent).toContain("kv://default/ops/primary");
    expect(root.querySelector('[data-slot="virtual-table"]')).toBeTruthy();
    expect(root.querySelector(".domain-resource-virtual-table")).toBeTruthy();
    expect(root.querySelector(".domain-resource-metric")?.getAttribute("data-font")).toBe("mono");
    expect(root.querySelector(".domain-resource-metric")?.getAttribute("data-numeric")).toBe(
      "tabular",
    );
    expect(root.querySelector('a[href="/admin/1/kv/default/ops/primary"]')).toBeTruthy();
  });

  it("uses accessible icons instead of visible sort-state prose", async () => {
    const root = await mount(() => (
      <main>
        <PureDomainResourceInventoryTable
          domain="queue"
          emptyDescription="No routes"
          metricColumns={[
            {
              id: "ready",
              header: "Ready",
              cell: () => 3,
              sortValue: () => 3,
            },
          ]}
          onRowOpen={vi.fn()}
          onSearchChange={vi.fn()}
          rowHref={() => "/admin/1/queue/default/ops/primary"}
          rows={[{ area: "ops", realm: "default", resource: "primary" }]}
          searchValue=""
          title="Queue inventory"
        />
      </main>
    ));
    const sort = root.querySelector('button[aria-label="Sort by Ready, not sorted"]');

    expect(sort?.querySelector('[data-slot="icon"]')).toBeTruthy();
    expect(sort?.textContent).toBe("Ready");
    expect(root.textContent).not.toContain("route order");

    sort?.dispatchEvent(new MouseEvent("click", { bubbles: true }));
    await new Promise<void>((resolve) => queueMicrotask(() => resolve()));
    expect(root.querySelector('button[aria-label="Sort by Ready, descending"]')).toBeTruthy();
  });

  it("uses AskR table, virtual table, and card styling without app-local table chrome", async () => {
    const root = await mount(() => (
      <main>
        <DomainMetricTable
          title="Current values"
          metrics={[
            {
              label: "Ready",
              value: 3,
            },
          ]}
        />
        <Card>
          <CardContent>
            <QueueInflightTable
              messages={[
                {
                  area: "payments",
                  attempts: 1,
                  expiresAt: "2026-06-18T12:00:00Z",
                  family: 7,
                  inflightToken: "token-1",
                  messageId: 42,
                  realm: "acme",
                  resource: "inbox",
                  sessionId: "session-1",
                },
              ]}
            />
          </CardContent>
        </Card>
      </main>
    ));

    const tables = root.querySelectorAll('[data-slot="table"]');
    const virtualTables = root.querySelectorAll('[data-slot="virtual-table"]');
    const cards = root.querySelectorAll('[data-slot="card"]');

    expect(tables).toHaveLength(1);
    expect(virtualTables).toHaveLength(1);
    expect(cards).toHaveLength(2);
    expect(root.querySelectorAll(".domain-table-wrap")).toHaveLength(1);
    expect(root.querySelector(".domain-table")).toBeNull();
    expect(root.querySelector(".domain-metric-card")).toBeNull();
    expect(root.querySelector(".domain-metric-value")?.getAttribute("data-font")).toBe("mono");
    expect(root.querySelector(".domain-metric-value")?.getAttribute("data-numeric")).toBe(
      "tabular",
    );
    expect(root.querySelector('[data-slot="card-title"]')?.tagName).toBe("H2");
    expect(root.textContent).toContain("Current values");
    expect(root.textContent).toContain("session-1");
  });
});
