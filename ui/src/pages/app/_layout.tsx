import { state } from "@askrjs/askr";
import { For } from "@askrjs/askr/control";
import { currentRoute, Link, navigate } from "@askrjs/askr/router";
import {
  ChevronDownIcon,
  LogOutIcon,
  MenuIcon,
  MoonIcon,
  NetworkIcon,
  SunIcon,
} from "@askrjs/lucide";
import { Block, Brand, BrandLabel, Button, Container, Grid, Text } from "@askrjs/themes/components";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuPortal,
  DropdownMenuTrigger,
} from "@askrjs/themes/components";
import {
  Header,
  NavBrand,
  NavGroup,
  Navbar,
  Sidebar,
  SidebarContent,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
} from "@askrjs/themes/components";
import { ThemeToggle } from "@askrjs/themes/theme";
import AppFooter from "@/components/shared/app-footer";
import { createCurrentSessionQuery } from "@/features/session/session-query";
import { createMessagingTopologyQuery } from "@/features/topology/topology-query";
import {
  adminHref,
  contentPathFromRouteFamilyPath,
  type DomainLink,
  domainLinks,
  pathWithRouteFamily,
  routeFamilyFromPath,
  type ShellLink,
  shellLinks,
} from "@/shared/navigation/domains";
import {
  createOperatorScopeSnapshot,
  OperatorScope,
  type RouteFamilyOption,
} from "@/shared/operator-scope";
import { routeFamilyIconColor } from "@/shared/route-family-appearance";
import RouteFamilySelectorPage from "./route-family";

const workspaceLinks = shellLinks.filter(
  (link) =>
    link.title === "Overview" ||
    link.title === "Sessions" ||
    link.title === "Diagnostics" ||
    link.title === "Metrics",
);
const sidebarDomainLinks = [...domainLinks].sort((first, second) =>
  first.title.localeCompare(second.title),
);

function contentPathForRoute(path: string) {
  return contentPathFromRouteFamilyPath(path);
}

function contentPathForHref(href: string) {
  return contentPathForRoute(href);
}

function isActiveShellLink(currentPath: string, href: string, exact: boolean) {
  const routePath = contentPathForRoute(currentPath);
  const linkPath = contentPathForHref(href);

  return exact
    ? routePath === linkPath
    : routePath === linkPath || routePath.startsWith(`${linkPath}/`);
}

function RouteFamilyMenuItem({
  family,
  onNavigate,
}: {
  family: RouteFamilyOption;
  onNavigate: () => void;
}) {
  const route = currentRoute();
  const search = typeof window === "undefined" ? "" : window.location.search;
  const href = `${pathWithRouteFamily(route.path, family.id)}${search}`;

  return (
    <DropdownMenuItem
      class="operator-route-family-option"
      data-route-family={family.id}
      onSelect={() => {
        onNavigate();
        navigate(href);
      }}
    >
      <NetworkIcon
        class="route-family-icon"
        size={16}
        style={{ color: routeFamilyIconColor(family.id) }}
        aria-hidden="true"
      />
      <span>{family.label}</span>
    </DropdownMenuItem>
  );
}

function ScopedSidebarMenuItem({
  exact,
  link,
  onNavigate,
}: {
  exact: boolean;
  link: DomainLink | ShellLink;
  onNavigate: () => void;
}) {
  const route = currentRoute();
  const family = routeFamilyFromPath(route.path) ?? "";
  const active = isActiveShellLink(route.path, link.href, exact);

  return (
    <SidebarMenuItem>
      <SidebarMenuButton active={active} asChild>
        <Link
          href={pathWithRouteFamily(link.href, family)}
          onClick={onNavigate}
          aria-current={active ? "page" : undefined}
        >
          <link.icon size={16} aria-hidden="true" />
          <Text as="span" size="sm" weight="medium">
            {link.title}
          </Text>
        </Link>
      </SidebarMenuButton>
    </SidebarMenuItem>
  );
}

function OperatorNavigation({
  operator,
}: {
  operator: ReturnType<typeof createOperatorScopeSnapshot>;
}) {
  const [navigationOpen, setNavigationOpen] = state(false);
  const [routeFamilyMenuOpen, setRouteFamilyMenuOpen] = state(false);
  const route = currentRoute();
  const routeFamilyId = routeFamilyFromPath(route.path) ?? operator.selectedRouteFamily.id;
  const selectedRouteFamily =
    operator.routeFamilies.find((family) => family.id === routeFamilyId) ??
    operator.selectedRouteFamily;

  function closeMobileNavigation() {
    setNavigationOpen(false);
  }

  function selectRouteFamily() {
    setRouteFamilyMenuOpen(false);
    closeMobileNavigation();
  }

  return (
    <Sidebar
      class="operator-sidebar"
      collapsible="none"
      minHeight="auto"
      padding="md"
      borderRight={false}
      shrink={false}
      width="full"
      aria-label="Primary navigation"
      role="navigation"
      onKeyDown={(event: KeyboardEvent) => {
        if (event.key === "Escape") closeMobileNavigation();
      }}
    >
      <Button
        class="operator-sidebar-toggle"
        variant="outline"
        aria-controls="operator-sidebar-panel"
        aria-expanded={navigationOpen() ? "true" : "false"}
        aria-label="Navigation menu"
        onPress={() => setNavigationOpen((open) => !open)}
      >
        <MenuIcon size={16} />
        <span>Navigation</span>
      </Button>

      <div
        class="operator-sidebar-panel"
        id="operator-sidebar-panel"
        data-open={navigationOpen() ? "true" : undefined}
      >
        <SidebarContent>
          <SidebarGroup>
            <SidebarGroupLabel>Route Family</SidebarGroupLabel>
            <SidebarGroupContent>
              <DropdownMenu open={routeFamilyMenuOpen()} onOpenChange={setRouteFamilyMenuOpen}>
                <DropdownMenuTrigger
                  class="operator-route-family-trigger"
                  aria-label="Route Family selector"
                >
                  <NetworkIcon
                    class="route-family-icon"
                    size={16}
                    style={{ color: routeFamilyIconColor(selectedRouteFamily.id) }}
                    aria-hidden="true"
                  />
                  <Text as="span" size="sm" weight="medium">
                    {selectedRouteFamily.label}
                  </Text>
                  <ChevronDownIcon size={14} aria-hidden="true" />
                </DropdownMenuTrigger>
                <DropdownMenuPortal>
                  <DropdownMenuContent align="start" sideOffset={8}>
                    <DropdownMenuLabel>Route Family</DropdownMenuLabel>
                    <For each={operator.routeFamilies} by={(family) => family.id}>
                      {(family) => (
                        <RouteFamilyMenuItem family={family} onNavigate={selectRouteFamily} />
                      )}
                    </For>
                    {operator.routeFamilyState === "loading" ? (
                      <DropdownMenuItem disabled>Loading Route Families…</DropdownMenuItem>
                    ) : null}
                    {operator.routeFamilyState === "error" ? (
                      <DropdownMenuItem onSelect={operator.retryRouteFamilies}>
                        Retry Route Families
                      </DropdownMenuItem>
                    ) : null}
                    {operator.routeFamilyState === "empty" ? (
                      <DropdownMenuItem disabled>No Route Families available</DropdownMenuItem>
                    ) : null}
                  </DropdownMenuContent>
                </DropdownMenuPortal>
              </DropdownMenu>
            </SidebarGroupContent>
          </SidebarGroup>

          <SidebarGroup>
            <SidebarGroupLabel>Workspace</SidebarGroupLabel>
            <SidebarGroupContent>
              <SidebarMenu>
                <For each={() => workspaceLinks} by={(link) => link.href}>
                  {(link) => (
                    <ScopedSidebarMenuItem
                      exact={link.title === "Overview"}
                      link={link}
                      onNavigate={closeMobileNavigation}
                    />
                  )}
                </For>
              </SidebarMenu>
            </SidebarGroupContent>
          </SidebarGroup>

          <SidebarGroup>
            <SidebarGroupLabel>Domains</SidebarGroupLabel>
            <SidebarGroupContent>
              <SidebarMenu>
                <For each={() => sidebarDomainLinks} by={(link) => link.href}>
                  {(link) => (
                    <ScopedSidebarMenuItem
                      exact={false}
                      link={link}
                      onNavigate={closeMobileNavigation}
                    />
                  )}
                </For>
              </SidebarMenu>
            </SidebarGroupContent>
          </SidebarGroup>
        </SidebarContent>
      </div>
    </Sidebar>
  );
}

function WorkspaceShell({
  children,
  operator,
}: {
  children?: unknown;
  operator: ReturnType<typeof createOperatorScopeSnapshot>;
}) {
  return (
    <Grid class="operator-shell-layout" gap="md" align="start">
      <OperatorNavigation operator={operator} />
      <div class="route-transition-surface">{children}</div>
    </Grid>
  );
}

export default function Layout({ children }: { children?: unknown }) {
  const route = currentRoute();
  const currentSession = createCurrentSessionQuery();
  const activeRouteFamilyId = routeFamilyFromPath(route.path) ?? "";
  const sessionRouteFamilyId =
    currentSession.data?.routeFamilies.find((family) => /^\d+$/.test(family)) ?? "1";
  const topology = createMessagingTopologyQuery(activeRouteFamilyId || sessionRouteFamilyId);
  const operator = createOperatorScopeSnapshot(
    topology.data,
    currentSession.data,
    activeRouteFamilyId,
    {
      error: currentSession.error ?? topology.error,
      loading: currentSession.loading || topology.loading,
      retry: () => {
        void currentSession.refresh();
        void topology.refresh();
      },
    },
  );
  const scopedFamily = operator.selectedRouteFamilyId;
  const hasRouteFamilyScope =
    activeRouteFamilyId !== "" && operator.selectedRouteFamilyId === activeRouteFamilyId;

  return (
    <OperatorScope value={operator}>
      <Block class="operator-context-root" minHeight="screen" direction="column">
        <a class="skip-link" href="#main-content">
          Skip to main content
        </a>

        <Header sticky>
          <Container paddingY="sm">
            <Navbar class="operator-shell-navbar" aria-label="Operator context">
              <NavBrand>
                <Brand asChild>
                  <Link
                    href={hasRouteFamilyScope ? adminHref(scopedFamily) : "/admin"}
                    aria-label="Fitz admin home"
                  >
                    <img
                      class="fitz-brand-logo"
                      src="/assets/logos/fitz-logo-128x128.png"
                      alt=""
                      width={28}
                      height={28}
                      aria-hidden="true"
                    />
                    <BrandLabel>Fitz Admin</BrandLabel>
                  </Link>
                </Brand>
              </NavBrand>

              <NavGroup align="end" aria-label="View controls" role="group">
                <ThemeToggle
                  aria-label="Toggle color theme"
                  variant="ghost"
                  size="icon"
                  lightIcon={<SunIcon size={16} />}
                  darkIcon={<MoonIcon size={16} />}
                />
                {currentSession.data?.authenticated &&
                currentSession.data.authRequired !== false ? (
                  <Button asChild variant="ghost" size="icon">
                    <Link href="/logout" aria-label="Sign out" title="Sign out">
                      <LogOutIcon size={16} aria-hidden="true" />
                    </Link>
                  </Button>
                ) : null}
              </NavGroup>
            </Navbar>
          </Container>
        </Header>

        <Container class="operator-shell-workspace" paddingY="0" grow>
          {hasRouteFamilyScope ? (
            <WorkspaceShell operator={operator}>{children}</WorkspaceShell>
          ) : (
            <RouteFamilySelectorPage />
          )}
        </Container>

        <AppFooter />
      </Block>
    </OperatorScope>
  );
}
