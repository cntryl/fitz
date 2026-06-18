import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import { cleanupApp, createSPA } from "@askrjs/askr/boot";
import type { RouteHandler } from "@askrjs/askr/router";
import { Card, CardContent } from "@askrjs/themes/surfaces";
import { ThemeProvider } from "@askrjs/themes/theme";
import AppLayout from "@/pages/app/_layout";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import QueueInflightTable from "@/components/shared/queue-inflight-table";
import { QueryEmptyState, QueryErrorState } from "@/components/shared/query-state";
import { domainLinks } from "@/shared/navigation/domains";

vi.mock("@/features/session/session-query", () => ({
  createCurrentSessionQuery: () => ({
    data: {
      authenticated: true,
      username: "admin",
    },
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
    routes: [
      {
        handler: (params, context) => (
          <ThemeProvider defaultTheme="system" storageKey="fitz-admin-theme">
            {handler(params, context)}
          </ThemeProvider>
        ),
        path,
      },
    ],
  });

  await new Promise<void>((resolve) => queueMicrotask(() => resolve()));
  return root;
}

afterEach(() => {
  cleanupApp("app");
  document.body.innerHTML = "";
});

describe("shared UI polish contracts", () => {
  it("uses icon-backed shell controls with stable labels", async () => {
    const root = await mount(() => (
      <AppLayout>
        <DomainPageFrame>
          <section>Workspace</section>
        </DomainPageFrame>
      </AppLayout>
    ));

    const skipLink = root.querySelector('a[href="#main-content"]');
    const routeSurface = root.querySelector(".route-transition-surface");
    const main = root.querySelector("main#main-content");
    const themeToggle = root.querySelector('button[aria-label="Toggle color theme"]');
    const signOut = Array.from(root.querySelectorAll("button")).find((button) =>
      button.textContent?.includes("Sign out"),
    );

    expect(root.textContent).toContain("Fitz");
    expect(root.textContent).toContain("Admin");
    expect(skipLink?.textContent).toContain("Skip to main content");
    expect(routeSurface?.tagName).toBe("DIV");
    expect(main?.getAttribute("tabindex")).toBe("-1");
    expect(root.querySelectorAll("main")).toHaveLength(1);
    expect(root.querySelectorAll('[data-slot="container"]')).toHaveLength(2);
    expect(root.querySelectorAll('[data-slot="container"][data-size="initial:xl"]')).toHaveLength(
      2,
    );
    expect(themeToggle).toBeTruthy();
    expect(themeToggle?.getAttribute("data-size")).toBe("icon");
    expect(themeToggle?.getAttribute("data-variant")).toBe("ghost");
    expect(themeToggle?.querySelector('[data-slot="icon"]')).toBeTruthy();
    expect(themeToggle?.textContent).not.toContain("☀");
    expect(themeToggle?.textContent).not.toContain("☾");
    expect(signOut?.querySelector('[data-slot="icon"]')).toBeTruthy();
  });

  it("exposes individual domain pages from the app navbar", async () => {
    const root = await mount(() => (
      <AppLayout>
        <DomainPageFrame>
          <section>Workspace</section>
        </DomainPageFrame>
      </AppLayout>
    ));

    const dropdown = root.querySelector(".navbar-domain-menu");
    const trigger = root.querySelector('[data-slot="dropdown-trigger"]');
    const contentBeforeOpen = root.querySelector('[data-slot="dropdown-content"]');
    const containers = document.querySelectorAll('[data-slot="container"]');

    expect(dropdown).toBeTruthy();
    expect(trigger).toBeTruthy();
    expect(contentBeforeOpen).toBeNull();
    expect(containers.length).toBe(2);
    expect(
      document.querySelectorAll('[data-slot="container"][data-size="initial:xl"]'),
    ).toHaveLength(2);

    trigger?.dispatchEvent(new MouseEvent("click", { bubbles: true }));
    await new Promise<void>((resolve) => queueMicrotask(() => resolve()));

    const content = document.querySelector('[data-slot="dropdown-content"]');

    expect(content).toBeTruthy();

    for (const link of domainLinks) {
      const item =
        document.querySelector(`a[href="${link.href}"][data-slot="dropdown-item"]`) ??
        root.querySelector(`a[href="${link.href}"]`);

      expect(item?.textContent).toContain(link.title);
      expect(item?.textContent).toContain(link.description);
      expect(item?.querySelector('[data-slot="icon"]')).toBeTruthy();
    }
  });

  it("keeps the shared page frame to one main and one sidebar surface", async () => {
    const root = await mount(() => (
      <AppLayout>
        <DomainPageFrame sidebar={<section>Sidebar</section>}>
          <section>Workspace</section>
        </DomainPageFrame>
      </AppLayout>
    ));

    expect(root.querySelectorAll("main#main-content")).toHaveLength(1);
    expect(root.querySelectorAll(".page-frame-main")).toHaveLength(1);
    expect(root.querySelectorAll(".page-frame-sidebar")).toHaveLength(1);
    expect(root.querySelectorAll('[data-slot="container"][data-size="initial:xl"]')).toHaveLength(
      2,
    );
    expect(root.textContent).toContain("Workspace");
    expect(root.textContent).toContain("Sidebar");
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

  it("uses AskR table and card styling without app-local table chrome", async () => {
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
    const cards = root.querySelectorAll('[data-slot="card"]');

    expect(tables).toHaveLength(2);
    expect(cards).toHaveLength(2);
    expect(root.querySelectorAll(".domain-table-wrap")).toHaveLength(2);
    expect(root.querySelector(".domain-table")).toBeNull();
    expect(root.querySelector(".domain-metric-card")).toBeNull();
    expect(root.textContent).toContain("Current values");
    expect(root.textContent).toContain("session-1");
  });
});
