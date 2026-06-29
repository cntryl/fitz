import { state } from "@askrjs/askr";
import { For } from "@askrjs/askr/control";
import { currentRoute, Link, navigate } from "@askrjs/askr/router";
import {
  ChevronDownIcon,
  LogOutIcon,
  MenuIcon,
  MoonIcon,
  ShieldIcon,
  SunIcon,
  UserIcon,
} from "@askrjs/lucide";
import { Container } from "@askrjs/themes/components";
import {
  Dropdown,
  DropdownContent,
  DropdownItem,
  DropdownLabel,
  DropdownPortal,
  DropdownTrigger,
} from "@askrjs/themes/components";
import { Header, NavBrand, NavGroup, NavLink, Navbar, Sidebar } from "@askrjs/themes/components";
import { Badge } from "@askrjs/themes/components";
import { ThemeToggle } from "@askrjs/themes/theme";
import { createCurrentSessionQuery } from "@/features/session/session-query";
import { createMessagingTopologyQuery } from "@/features/topology/topology-query";
import { appConfig } from "@/shared/config";
import {
  adminChildHref,
  adminHref,
  domainLinks,
  pathWithRouteFamily,
  routeFamilyFromPath,
  shellLinks,
} from "@/shared/navigation/domains";
import {
  createOperatorContextSnapshot,
  OperatorContext,
  readInitialRouteFamily,
} from "@/shared/operator-context";

const overviewLinks = shellLinks.filter((link) => link.title === "Overview");
const utilityLinks = shellLinks.filter((link) => link.title !== "Overview");
const sidebarDomainLinks = [...domainLinks].sort((first, second) =>
  first.title.localeCompare(second.title),
);

export default function Layout({ children }: { children?: unknown }) {
  const route = currentRoute();
  const currentSession = createCurrentSessionQuery();
  const topology = createMessagingTopologyQuery();
  const [selectedRouteFamilyId, setSelectedRouteFamilyId] = state(
    routeFamilyFromPath(route.path) ?? readInitialRouteFamily(),
  );
  const [mobileNavOpen, setMobileNavOpen] = state(false);
  const activeRouteFamilyId = routeFamilyFromPath(route.path) ?? selectedRouteFamilyId();
  const operator = createOperatorContextSnapshot(
    topology.data,
    currentSession.data,
    activeRouteFamilyId,
    setSelectedRouteFamilyId,
  );
  const scopedFamily = operator.selectedRouteFamilyId;
  const username = currentSession.data?.username ?? "admin";
  const showUserBadge = currentSession.data?.authenticated === true;
  const isMobileNavOpen = mobileNavOpen();

  function onLogout() {
    navigate("/logout");
  }

  function selectRouteFamily(routeFamilyId: string) {
    const pathname = typeof window === "undefined" ? route.path : window.location.pathname;
    const search = typeof window === "undefined" ? "" : window.location.search;

    operator.setRouteFamily(routeFamilyId);
    navigate(`${pathWithRouteFamily(pathname, routeFamilyId)}${search}`);
  }

  function toggleMobileNavigation() {
    setMobileNavOpen(!mobileNavOpen());
  }

  function closeMobileNavigation() {
    setMobileNavOpen(false);
  }

  return (
    <OperatorContext.Scope value={operator}>
      <div class="operator-context-root">
        <a class="skip-link" href="#main-content">
          Skip to main content
        </a>

        <Header position="sticky" class="operator-shell-header">
          <Container size="xl" class="operator-shell-container">
            <Navbar class="operator-topbar" aria-label="Operator context">
              <NavBrand class="operator-shell-brand">
                <Link href={adminHref(scopedFamily)} aria-label="Fitz admin home">
                  <ShieldIcon size={18} />
                  <span>Fitz Admin</span>
                </Link>
              </NavBrand>

              <NavGroup class="operator-shell-context" aria-label="Route Family" role="group">
                <Dropdown>
                  <div class="route-family-selector">
                    <DropdownTrigger aria-label="Route Family selector">
                      <span class="route-family-selector-label">Route Family</span>
                      <strong>{operator.selectedRouteFamily.label}</strong>
                      <ChevronDownIcon size={14} />
                    </DropdownTrigger>
                    <DropdownPortal>
                      <DropdownContent align="start" sideOffset={8}>
                        <For each={operator.routeFamilies} by={(family) => family.id}>
                          {(family) => (
                            <DropdownItem onSelect={() => selectRouteFamily(family.id)}>
                              {family.label}
                            </DropdownItem>
                          )}
                        </For>
                      </DropdownContent>
                    </DropdownPortal>
                  </div>
                </Dropdown>

                <Badge variant="outline">{appConfig.environmentLabel}</Badge>
              </NavGroup>

              <NavGroup class="operator-shell-actions" aria-label="Account" role="group">
                <ThemeToggle
                  aria-label="Toggle color theme"
                  variant="ghost"
                  size="icon"
                  lightIcon={<SunIcon size={16} />}
                  darkIcon={<MoonIcon size={16} />}
                />
                <Dropdown>
                  <div class="user-menu">
                    <DropdownTrigger aria-label="User menu">
                      <UserIcon size={16} />
                      <span>{showUserBadge ? username : "Admin"}</span>
                      <ChevronDownIcon size={14} />
                    </DropdownTrigger>
                    <DropdownPortal>
                      <DropdownContent align="end" sideOffset={8}>
                        <DropdownLabel>Signed in as {username}</DropdownLabel>
                        <DropdownItem asChild>
                          <Link href={adminChildHref("settings", scopedFamily)}>Settings</Link>
                        </DropdownItem>
                        <DropdownItem asChild>
                          <Link href={adminChildHref("sessions", scopedFamily)}>Sessions</Link>
                        </DropdownItem>
                        <DropdownItem onSelect={onLogout}>
                          <LogOutIcon size={16} />
                          Sign out
                        </DropdownItem>
                      </DropdownContent>
                    </DropdownPortal>
                  </div>
                </Dropdown>
              </NavGroup>
            </Navbar>
          </Container>
        </Header>

        <Container size="xl" class="operator-shell-workspace">
          <div class="operator-shell-layout">
            <Sidebar
              class="operator-sidebar"
              collapsible="none"
              data-mobile-open={isMobileNavOpen ? "true" : undefined}
              onKeyDown={(event: KeyboardEvent) => {
                if (event.key === "Escape") {
                  closeMobileNavigation();
                }
              }}
              aria-label="Primary navigation"
              role="navigation"
            >
              <button
                type="button"
                class="operator-sidebar-toggle"
                aria-controls="operator-sidebar-panel"
                aria-expanded={isMobileNavOpen}
                aria-label="Navigation menu"
                onClick={toggleMobileNavigation}
              >
                <MenuIcon size={16} />
                <span>Navigation</span>
              </button>

              <div class="operator-sidebar-panel" id="operator-sidebar-panel">
                <NavBrand class="operator-sidebar-brand">
                  <Link
                    href={adminHref(scopedFamily)}
                    aria-label="Fitz admin home"
                    onClick={closeMobileNavigation}
                  >
                    <ShieldIcon size={18} />
                    <span>Fitz Admin</span>
                  </Link>
                </NavBrand>

                <NavGroup label="Workspace">
                  <For each={overviewLinks} by={(link) => link.href}>
                    {(link) => (
                      <NavLink
                        href={pathWithRouteFamily(link.href, scopedFamily)}
                        match="exact"
                        onClick={closeMobileNavigation}
                      >
                        <link.icon size={16} />
                        {link.title}
                      </NavLink>
                    )}
                  </For>
                </NavGroup>

                <NavGroup label="Domains">
                  <For each={sidebarDomainLinks} by={(link) => link.href}>
                    {(link) => (
                      <NavLink
                        href={pathWithRouteFamily(link.href, scopedFamily)}
                        match="prefix"
                        onClick={closeMobileNavigation}
                      >
                        <link.icon size={16} />
                        {link.title}
                      </NavLink>
                    )}
                  </For>
                </NavGroup>

                <NavGroup label="Operate">
                  <For each={utilityLinks} by={(link) => link.href}>
                    {(link) => (
                      <NavLink
                        href={pathWithRouteFamily(link.href, scopedFamily)}
                        match="prefix"
                        onClick={closeMobileNavigation}
                      >
                        <link.icon size={16} />
                        {link.title}
                      </NavLink>
                    )}
                  </For>
                </NavGroup>
              </div>
            </Sidebar>

            <Container size="xl" class="route-transition-surface">
              {children}
            </Container>
          </div>
        </Container>
      </div>
    </OperatorContext.Scope>
  );
}
