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
import DiagnosticsPage from "@/pages/app/diagnostics";
import MetricsPage from "@/pages/app/metrics";
import QueuePage from "@/pages/app/queue";
import QueueResourcePage from "@/pages/app/queue-resource";
import ResourceDetailPage from "@/pages/app/resource-detail";
import KvPage from "@/pages/app/kv";
import LeasePage from "@/pages/app/lease";
import NoticePage from "@/pages/app/notice";
import NoticeOperationPage from "@/pages/app/notice-operation";
import RpcPage from "@/pages/app/rpc";
import RpcOperationPage from "@/pages/app/rpc-operation";
import RpcResourcePage from "@/pages/app/rpc-resource";
import SchedulePage from "@/pages/app/schedule";
import ScheduleResourcePage from "@/pages/app/schedule-resource";
import SessionsPage from "@/pages/app/sessions";
import StreamPage from "@/pages/app/stream";
import StreamResourcePage from "@/pages/app/stream-resource";
import Logout from "@/pages/auth/logout";
import Login from "@/pages/auth/login";
import { getRoutes } from "@askrjs/askr/router";
import { EmptyState } from "@askrjs/themes/components";
import { NavBrand, NavGroup, NavItem, NavLink, Navbar } from "@askrjs/themes/components";
import { Section } from "@askrjs/themes/components";
import { Card } from "@askrjs/themes/components";
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
    expect(NoticeOperationPage).toBeDefined();
    expect(typeof NoticeOperationPage).toBe("function");
    expect(RpcPage).toBeDefined();
    expect(typeof RpcPage).toBe("function");
    expect(RpcResourcePage).toBeDefined();
    expect(typeof RpcResourcePage).toBe("function");
    expect(RpcOperationPage).toBeDefined();
    expect(typeof RpcOperationPage).toBe("function");
    expect(SchedulePage).toBeDefined();
    expect(typeof SchedulePage).toBe("function");
    expect(ScheduleResourcePage).toBeDefined();
    expect(typeof ScheduleResourcePage).toBe("function");
    expect(SessionsPage).toBeDefined();
    expect(typeof SessionsPage).toBe("function");
    expect(StreamPage).toBeDefined();
    expect(typeof StreamPage).toBe("function");
    expect(StreamResourcePage).toBeDefined();
    expect(typeof StreamResourcePage).toBe("function");
    expect(DiagnosticsPage).toBeDefined();
    expect(typeof DiagnosticsPage).toBe("function");
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
        "/admin/{family}",
        "/logout",
        "/login",
        "/sessions",
        "/admin/{family}/sessions",
        "/diagnostics",
        "/admin/{family}/diagnostics",
        "/settings",
        "/admin/{family}/settings",
        "/admin/metrics",
        "/admin/{family}/metrics",
        "/queue",
        "/admin/{family}/queue",
        "/queue/{realm}",
        "/admin/{family}/queue/{realm}",
        "/queue/{realm}/{area}",
        "/admin/{family}/queue/{realm}/{area}",
        "/admin/{family}/queue/{realm}/{area}/{resource}",
        "/kv",
        "/admin/{family}/kv",
        "/kv/{realm}",
        "/admin/{family}/kv/{realm}",
        "/kv/{realm}/{area}",
        "/admin/{family}/kv/{realm}/{area}",
        "/admin/{family}/kv/{realm}/{area}/{resource}",
        "/lease",
        "/admin/{family}/lease",
        "/lease/{realm}",
        "/admin/{family}/lease/{realm}",
        "/lease/{realm}/{area}",
        "/admin/{family}/lease/{realm}/{area}",
        "/admin/{family}/lease/{realm}/{area}/{resource}",
        "/notice",
        "/admin/{family}/notice",
        "/notice/{realm}",
        "/admin/{family}/notice/{realm}",
        "/notice/{realm}/{area}",
        "/admin/{family}/notice/{realm}/{area}",
        "/admin/{family}/notice/{realm}/{area}/{resource}",
        "/admin/{family}/notice/{realm}/{area}/{resource}/{operation}",
        "/rpc",
        "/admin/{family}/rpc",
        "/rpc/{realm}",
        "/admin/{family}/rpc/{realm}",
        "/rpc/{realm}/{area}",
        "/admin/{family}/rpc/{realm}/{area}",
        "/admin/{family}/rpc/{realm}/{area}/{resource}",
        "/admin/{family}/rpc/{realm}/{area}/{resource}/{operation}",
        "/schedule",
        "/admin/{family}/schedule",
        "/schedule/{realm}",
        "/admin/{family}/schedule/{realm}",
        "/schedule/{realm}/{area}",
        "/admin/{family}/schedule/{realm}/{area}",
        "/admin/{family}/schedule/{realm}/{area}/{resource}",
        "/stream",
        "/admin/{family}/stream",
        "/stream/{realm}",
        "/admin/{family}/stream/{realm}",
        "/stream/{realm}/{area}",
        "/admin/{family}/stream/{realm}/{area}",
        "/admin/{family}/stream/{realm}/{area}/{resource}",
      ]),
    );
  });
});
