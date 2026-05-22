import { describe, expect, it } from "vite-plus/test";
import App from "@/pages/app/_layout";
import QueueDeadLettersPanel from "@/components/queue-dead-letters-panel";
import DomainHeader from "@/components/shared/domain-header";
import DomainIndex from "@/components/shared/domain-index";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import DomainRealmTable from "@/components/shared/domain-realm-table";
import DomainResourceBrowser from "@/components/shared/domain-resource-browser";
import DomainSidebar from "@/components/shared/domain-sidebar";
import ResourceWorkbench from "@/components/shared/resource-workbench";
import SessionTable from "@/components/shared/session-table";
import QueueDeadLetterTable from "@/components/shared/queue-dead-letter-table";
import Home from "@/pages/app/home";
import MetricsPage from "@/pages/app/metrics";
import QueuePage from "@/pages/app/queue";
import QueueResourcePage from "@/pages/app/queue-resource";
import ResourceDetailPage from "@/pages/app/resource-detail";
import KvPage from "@/pages/app/kv";
import LeasePage from "@/pages/app/lease";
import NoticePage from "@/pages/app/notice";
import RpcPage from "@/pages/app/rpc";
import SchedulePage from "@/pages/app/schedule";
import SessionsPage from "@/pages/app/sessions";
import StreamPage from "@/pages/app/stream";
import Logout from "@/pages/auth/logout";
import Login from "@/pages/auth/login";
import { getRoutes } from "@askrjs/askr/router";
import { EmptyState } from "@askrjs/themes/feedback";
import { NavBrand, NavGroup, NavItem, NavLink, Navbar } from "@askrjs/themes/navs";
import { Section } from "@askrjs/themes/layouts";
import { Card } from "@askrjs/themes/surfaces";
import { domainLinks, shellLinks } from "@/shared/navigation/domains";
import QueueInflightTable from "@/components/shared/queue-inflight-table";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import "@/pages/_routes";

describe("Admin UI", () => {
  it("defines the shared admin shell", () => {
    expect(App).toBeDefined();
    expect(typeof App).toBe("function");
    expect(DomainPageFrame).toBeDefined();
    expect(typeof DomainPageFrame).toBe("function");
    expect(Navbar).toBeDefined();
    expect(typeof Navbar).toBe("function");
    expect(NavBrand).toBeDefined();
    expect(typeof NavBrand).toBe("function");
    expect(NavGroup).toBeDefined();
    expect(typeof NavGroup).toBe("function");
    expect(NavItem).toBeDefined();
    expect(typeof NavItem).toBe("function");
  });

  it("defines the admin login page", () => {
    expect(Login).toBeDefined();
    expect(typeof Login).toBe("function");
  });

  it("defines the logout page", () => {
    expect(Logout).toBeDefined();
    expect(typeof Logout).toBe("function");
  });

  it("defines the landing and domain pages", () => {
    expect(Home).toBeDefined();
    expect(typeof Home).toBe("function");
    expect(QueuePage).toBeDefined();
    expect(typeof QueuePage).toBe("function");
    expect(QueueResourcePage).toBeDefined();
    expect(typeof QueueResourcePage).toBe("function");
    expect(ResourceDetailPage).toBeDefined();
    expect(typeof ResourceDetailPage).toBe("function");
    expect(KvPage).toBeDefined();
    expect(typeof KvPage).toBe("function");
    expect(LeasePage).toBeDefined();
    expect(typeof LeasePage).toBe("function");
    expect(NoticePage).toBeDefined();
    expect(typeof NoticePage).toBe("function");
    expect(RpcPage).toBeDefined();
    expect(typeof RpcPage).toBe("function");
    expect(SchedulePage).toBeDefined();
    expect(typeof SchedulePage).toBe("function");
    expect(SessionsPage).toBeDefined();
    expect(typeof SessionsPage).toBe("function");
    expect(StreamPage).toBeDefined();
    expect(typeof StreamPage).toBe("function");
    expect(MetricsPage).toBeDefined();
    expect(typeof MetricsPage).toBe("function");
  });

  it("defines the admin home page", () => {
    expect(Home).toBeDefined();
    expect(typeof Home).toBe("function");
  });

  it("defines the queue dead-letter sample component", () => {
    expect(QueueDeadLettersPanel).toBeDefined();
    expect(typeof QueueDeadLettersPanel).toBe("function");
  });

  it("defines the shared domain primitives", () => {
    expect(DomainHeader).toBeDefined();
    expect(typeof DomainHeader).toBe("function");
    expect(DomainIndex).toBeDefined();
    expect(typeof DomainIndex).toBe("function");
    expect(DomainMetricTable).toBeDefined();
    expect(typeof DomainMetricTable).toBe("function");
    expect(DomainRealmTable).toBeDefined();
    expect(typeof DomainRealmTable).toBe("function");
    expect(DomainResourceBrowser).toBeDefined();
    expect(typeof DomainResourceBrowser).toBe("function");
    expect(DomainSidebar).toBeDefined();
    expect(typeof DomainSidebar).toBe("function");
    expect(ResourceWorkbench).toBeDefined();
    expect(typeof ResourceWorkbench).toBe("function");
    expect(Section).toBeDefined();
    expect(typeof Section).toBe("function");
    expect(Card).toBeDefined();
    expect(typeof Card).toBe("function");
    expect(EmptyState).toBeDefined();
    expect(typeof EmptyState).toBe("function");
    expect(QueueDeadLetterTable).toBeDefined();
    expect(typeof QueueDeadLetterTable).toBe("function");
    expect(QueueInflightTable).toBeDefined();
    expect(typeof QueueInflightTable).toBe("function");
    expect(SessionTable).toBeDefined();
    expect(typeof SessionTable).toBe("function");
    expect(NavLink).toBeDefined();
    expect(typeof NavLink).toBe("function");
  });

  it("uses icons for the shell and domain navigation", () => {
    expect(shellLinks.every((link) => typeof link.icon === "function")).toBe(true);
    expect(domainLinks.every((link) => typeof link.icon === "function")).toBe(true);
  });

  it("registers the expected routes", () => {
    const paths = getRoutes().map((route) => route.path);

    expect(paths).toEqual(
      expect.arrayContaining([
        "/",
        "/admin",
        "/logout",
        "/login",
        "/sessions",
        "/metrics",
        "/queue",
        "/queue/{realm}/{area}/{resource}",
        "/kv",
        "/kv/{realm}/{area}/{resource}",
        "/lease",
        "/lease/{realm}/{area}/{resource}",
        "/notice",
        "/notice/{realm}/{area}/{resource}",
        "/rpc",
        "/rpc/{realm}/{area}/{resource}",
        "/schedule",
        "/schedule/{realm}/{area}/{resource}",
        "/stream",
        "/stream/{realm}/{area}/{resource}",
      ]),
    );
  });
});
