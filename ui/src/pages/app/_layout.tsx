import { state } from "@askrjs/askr";
import { For } from "@askrjs/askr/control";
import { currentRoute, Link, navigate } from "@askrjs/askr/router";
import {
  ChevronDownIcon,
  LogOutIcon,
  MoonIcon,
  ShieldIcon,
  SunIcon,
  UserIcon,
} from "@askrjs/lucide";
import { Container } from "@askrjs/themes/layouts";
import {
  Dropdown,
  DropdownContent,
  DropdownItem,
  DropdownLabel,
  DropdownPortal,
  DropdownTrigger,
} from "@askrjs/themes/overlays";
import { NavBrand, NavGroup, NavLink, Header, Navbar } from "@askrjs/themes/shells";
import { Badge } from "@askrjs/themes/surfaces";
import { ThemeToggle } from "@askrjs/themes/theme";
import OperatorSearch from "@/components/shared/operator-search";
import { createCurrentSessionQuery } from "@/features/session/session-query";
import { createMessagingTopologyQuery } from "@/features/topology/topology-query";
import { appConfig } from "@/shared/config";
import { domainLinks, shellLinks } from "@/shared/navigation/domains";
import {
  createOperatorContextSnapshot,
  OperatorContext,
  readInitialRouteFamily,
} from "@/shared/operator-context";

export default function Layout({ children }: { children?: unknown }) {
  const route = currentRoute();
  const routeKey = route.path || "/";
  const overviewLinks = shellLinks.filter((link) => link.href === "/");
  const utilityLinks = shellLinks.filter((link) => link.href !== "/");
  const currentSession = createCurrentSessionQuery();
  const topology = createMessagingTopologyQuery();
  const [selectedRouteFamilyId, setSelectedRouteFamilyId] = state(readInitialRouteFamily());
  const operator = createOperatorContextSnapshot(
    topology.data,
    currentSession.data,
    selectedRouteFamilyId(),
    setSelectedRouteFamilyId,
  );
  const username = currentSession.data?.username ?? "admin";
  const showUserBadge = currentSession.data?.authenticated === true;

  function onLogout() {
    navigate("/logout");
  }

  return (
    <OperatorContext.Scope value={operator}>
      <div class="operator-context-root">
        <a class="skip-link" href="#main-content">
          Skip to main content
        </a>

        <Header>
          <Container size="xl">
            <Navbar breakpoint="md" aria-label="Primary navigation">
              <NavBrand>
                <Link href="/" aria-label="Fitz admin home">
                  <ShieldIcon size={18} />
                  <span>Fitz Admin</span>
                </Link>
              </NavBrand>

              <NavGroup aria-label="Operator context" role="group">
                <Dropdown>
                  <div class="route-family-selector">
                    <DropdownTrigger aria-label="Route Family selector">
                      <span class="route-family-selector-label">Route Family</span>
                      <strong>{operator.selectedRouteFamily.label}</strong>
                      <ChevronDownIcon size={14} />
                    </DropdownTrigger>
                    <DropdownPortal>
                      <DropdownContent align="start" sideOffset={8}>
                        <DropdownLabel>Route Family context</DropdownLabel>
                        <For each={operator.routeFamilies} by={(family) => family.id}>
                          {(family) => (
                            <DropdownItem onSelect={() => operator.setRouteFamily(family.id)}>
                              <span class="navbar-domain-menu-copy">
                                <span class="navbar-domain-menu-title">{family.label}</span>
                                <span class="navbar-domain-menu-description">
                                  {family.description}
                                </span>
                              </span>
                            </DropdownItem>
                          )}
                        </For>
                      </DropdownContent>
                    </DropdownPortal>
                  </div>
                </Dropdown>

                <Badge variant="outline">{appConfig.environmentLabel}</Badge>
              </NavGroup>

              <NavGroup aria-label="Workspace" role="group">
                <For each={overviewLinks} by={(link) => link.href}>
                  {(link) => (
                    <NavLink href={link.href} match={link.href === "/" ? "exact" : "prefix"}>
                      <link.icon size={16} />
                      {link.title}
                    </NavLink>
                  )}
                </For>

                <For each={domainLinks} by={(link) => link.href}>
                  {(link) => (
                    <NavLink href={link.href} match="prefix">
                      <link.icon size={16} />
                      {link.title}
                    </NavLink>
                  )}
                </For>

                <For each={utilityLinks} by={(link) => link.href}>
                  {(link) => (
                    <NavLink href={link.href} match="prefix">
                      <link.icon size={16} />
                      {link.title}
                    </NavLink>
                  )}
                </For>
              </NavGroup>

              <NavGroup align="end" aria-label="Account" role="group">
                <OperatorSearch />
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
                          <Link href="/settings">Settings</Link>
                        </DropdownItem>
                        <DropdownItem asChild>
                          <Link href="/sessions">Sessions</Link>
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
            <div class="mobile-context-strip" aria-label="Current operator context">
              <span>Route Family</span>
              <strong>{operator.selectedRouteFamily.label}</strong>
              <Badge variant="outline">{appConfig.environmentLabel}</Badge>
            </div>
          </Container>
        </Header>

        <div key={routeKey} class="route-transition-surface">
          {children}
        </div>
      </div>
    </OperatorContext.Scope>
  );
}
