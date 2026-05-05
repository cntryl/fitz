import { describe, expect, it } from "vite-plus/test";
import App from "@/app";
import QueueDeadLettersPanel from "@/components/queue-dead-letters-panel";
import DomainHeader from "@/components/shared/domain-header";
import DomainIndex from "@/components/shared/domain-index";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import DomainRealmTable from "@/components/shared/domain-realm-table";
import DomainState from "@/components/shared/domain-state";
import DomainSidebar from "@/components/shared/domain-sidebar";
import PageShell from "@/components/shared/page-shell";
import QueueDeadLetterTable from "@/components/shared/queue-dead-letter-table";
import AdminHome from "@/pages/admin-home";
import AdminLogin from "@/pages/admin-login";
import Home from "@/pages/home";
import QueuePage from "@/pages/queue";
import QueueResourcePage from "@/pages/queue-resource";
import KvPage from "@/pages/kv";
import LeasePage from "@/pages/lease";
import NoticePage from "@/pages/notice";
import RpcPage from "@/pages/rpc";
import SchedulePage from "@/pages/schedule";
import StreamPage from "@/pages/stream";
import { getRoutes } from "@askrjs/askr/router";
import {
  Card,
  EmptyState,
  NavBrand,
  NavGroup,
  NavItem,
  NavLink,
  Navbar,
  Section,
  SidebarLayout,
} from "@askrjs/themes/components";
import { domainLinks, shellLinks } from "@/shared/navigation/domains";
import QueueInflightTable from "@/components/shared/queue-inflight-table";
import "@/routes";

describe("Admin UI", () => {
  it("defines the shared admin shell", () => {
    expect(App).toBeDefined();
    expect(typeof App).toBe("function");
    expect(SidebarLayout).toBeDefined();
    expect(typeof SidebarLayout).toBe("function");
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
    expect(AdminLogin).toBeDefined();
    expect(typeof AdminLogin).toBe("function");
  });

  it("defines the landing and domain pages", () => {
    expect(Home).toBeDefined();
    expect(typeof Home).toBe("function");
    expect(QueuePage).toBeDefined();
    expect(typeof QueuePage).toBe("function");
    expect(QueueResourcePage).toBeDefined();
    expect(typeof QueueResourcePage).toBe("function");
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
    expect(StreamPage).toBeDefined();
    expect(typeof StreamPage).toBe("function");
  });

  it("defines the admin home page", () => {
    expect(AdminHome).toBeDefined();
    expect(typeof AdminHome).toBe("function");
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
    expect(DomainState).toBeDefined();
    expect(typeof DomainState).toBe("function");
    expect(DomainSidebar).toBeDefined();
    expect(typeof DomainSidebar).toBe("function");
    expect(PageShell).toBeDefined();
    expect(typeof PageShell).toBe("function");
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
        "/login",
        "/queue",
        "/queue/{realm}/{area}/{resource}",
        "/kv",
        "/lease",
        "/notice",
        "/rpc",
        "/schedule",
        "/stream",
      ]),
    );
  });
});
