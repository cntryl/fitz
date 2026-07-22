import { state } from "@askrjs/askr";
import { For } from "@askrjs/askr/control";
import { currentRoute, Link } from "@askrjs/askr/router";
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
import fitzLogo from "@/assets/fitz-logo.png";
import AppFooter from "@/components/shared/app-footer";
import { createCurrentSessionQuery } from "@/features/session/session-query";
import { createMessagingTopologyQuery } from "@/features/topology/topology-query";
import {
  adminHref,
  contentPathFromRouteFamilyPath,
  domainLinks,
  pathWithRouteFamily,
  routeFamilyFromPath,
  shellLinks,
} from "@/shared/navigation/domains";
import { createOperatorScopeSnapshot, OperatorScope } from "@/shared/operator-scope";
import RouteFamilySelectorPage from "./route-family";

const workspaceLinks = shellLinks.filter(
  (link) =>
    link.title === "Overview" ||
    link.title === "Sessions" ||
    link.title === "Diagnostics" ||
    link.title === "Metrics",
);
const settingsLink = shellLinks.find((link) => link.href === "/settings");
const settingsSectionLinks = [
  { href: "#operator-context", label: "Context" },
  { href: "#route-family", label: "Route Family" },
  { href: "#session-access", label: "Access" },
];
const sidebarDomainLinks = [...domainLinks].sort((first, second) =>
  first.title.localeCompare(second.title),
);

function SettingsSectionNavigation({ onNavigate }: { onNavigate: () => void }) {
  return (
    <SidebarGroup>
      <SidebarGroupLabel>Workspace &amp; account</SidebarGroupLabel>
      <SidebarGroupContent>
        <SidebarMenu>
          <For each={settingsSectionLinks} by={(link) => link.href}>
            {(link) => (
              <SidebarMenuItem>
                <SidebarMenuButton asChild>
                  <a href={link.href} onClick={onNavigate}>
                    <Text as="span" size="sm">
                      {link.label}
                    </Text>
                  </a>
                </SidebarMenuButton>
              </SidebarMenuItem>
            )}
          </For>
        </SidebarMenu>
      </SidebarGroupContent>
    </SidebarGroup>
  );
}

function contentPathForRoute(path: string) {
  return contentPathFromRouteFamilyPath(path);
}

function contentPathForHref(href: string) {
  return contentPathForRoute(href);
}

function OperatorNavigation({
  currentPath,
  isActiveShellLink,
  operator,
  scopedFamily,
}: {
  currentPath: string;
  isActiveShellLink: (href: string, exact: boolean) => boolean;
  operator: ReturnType<typeof createOperatorScopeSnapshot>;
  scopedFamily: string;
}) {
  const [navigationOpen, setNavigationOpen] = state(false);

  function closeMobileNavigation() {
    setNavigationOpen(false);
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
              <DropdownMenu>
                <DropdownMenuTrigger
                  class="operator-route-family-trigger"
                  aria-label="Route Family selector"
                >
                  <NetworkIcon size={16} aria-hidden="true" />
                  <Text as="span" size="sm" weight="medium">
                    {operator.selectedRouteFamily.label}
                  </Text>
                  <ChevronDownIcon size={14} aria-hidden="true" />
                </DropdownMenuTrigger>
                <DropdownMenuPortal>
                  <DropdownMenuContent align="start" sideOffset={8}>
                    <DropdownMenuLabel>Route Family</DropdownMenuLabel>
                    <For each={operator.routeFamilies} by={(family) => family.id}>
                      {(family) => (
                        <DropdownMenuItem asChild>
                          <Link
                            href={`${pathWithRouteFamily(currentPath, family.id)}${
                              typeof window === "undefined" ? "" : window.location.search
                            }`}
                            onClick={closeMobileNavigation}
                          >
                            {family.label}
                          </Link>
                        </DropdownMenuItem>
                      )}
                    </For>
                    {operator.routeFamilyState === "loading" ? (
                      <DropdownMenuItem disabled>Loading route families…</DropdownMenuItem>
                    ) : null}
                    {operator.routeFamilyState === "error" ? (
                      <DropdownMenuItem onSelect={operator.retryRouteFamilies}>
                        Retry route families
                      </DropdownMenuItem>
                    ) : null}
                    {operator.routeFamilyState === "empty" ? (
                      <DropdownMenuItem disabled>No route families available</DropdownMenuItem>
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
                <For each={workspaceLinks} by={(link) => link.href}>
                  {(link) => (
                    <SidebarMenuItem>
                      <SidebarMenuButton
                        active={isActiveShellLink(link.href, link.title === "Overview")}
                        asChild
                      >
                        <Link
                          href={pathWithRouteFamily(link.href, scopedFamily)}
                          onClick={closeMobileNavigation}
                          aria-current={
                            isActiveShellLink(link.href, link.title === "Overview")
                              ? "page"
                              : undefined
                          }
                        >
                          <link.icon size={16} aria-hidden="true" />
                          <Text as="span" size="sm" weight="medium">
                            {link.title}
                          </Text>
                        </Link>
                      </SidebarMenuButton>
                    </SidebarMenuItem>
                  )}
                </For>
              </SidebarMenu>
            </SidebarGroupContent>
          </SidebarGroup>

          <SidebarGroup>
            <SidebarGroupLabel>Domains</SidebarGroupLabel>
            <SidebarGroupContent>
              <SidebarMenu>
                <For each={sidebarDomainLinks} by={(link) => link.href}>
                  {(link) => (
                    <SidebarMenuItem>
                      <SidebarMenuButton active={isActiveShellLink(link.href, false)} asChild>
                        <Link
                          href={pathWithRouteFamily(link.href, scopedFamily)}
                          onClick={closeMobileNavigation}
                          aria-current={isActiveShellLink(link.href, false) ? "page" : undefined}
                        >
                          <link.icon size={16} aria-hidden="true" />
                          <Text as="span" size="sm" weight="medium">
                            {link.title}
                          </Text>
                        </Link>
                      </SidebarMenuButton>
                    </SidebarMenuItem>
                  )}
                </For>
              </SidebarMenu>
            </SidebarGroupContent>
          </SidebarGroup>

          {settingsLink ? (
            <SidebarGroup>
              <SidebarGroupLabel>Administration</SidebarGroupLabel>
              <SidebarGroupContent>
                <SidebarMenu>
                  <SidebarMenuItem>
                    <SidebarMenuButton active={isActiveShellLink(settingsLink.href, false)} asChild>
                      <Link
                        href={pathWithRouteFamily(settingsLink.href, scopedFamily)}
                        onClick={closeMobileNavigation}
                        aria-current={
                          isActiveShellLink(settingsLink.href, false) ? "page" : undefined
                        }
                      >
                        <settingsLink.icon size={16} aria-hidden="true" />
                        <Text as="span" size="sm" weight="medium">
                          {settingsLink.title}
                        </Text>
                      </Link>
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                </SidebarMenu>
              </SidebarGroupContent>
            </SidebarGroup>
          ) : null}

          {contentPathForRoute(currentPath).startsWith("/settings") ? (
            <SettingsSectionNavigation onNavigate={closeMobileNavigation} />
          ) : null}
        </SidebarContent>
      </div>
    </Sidebar>
  );
}

function WorkspaceShell({
  children,
  currentPath,
  isActiveShellLink,
  operator,
  scopedFamily,
}: {
  children?: unknown;
  currentPath: string;
  isActiveShellLink: (href: string, exact: boolean) => boolean;
  operator: ReturnType<typeof createOperatorScopeSnapshot>;
  scopedFamily: string;
}) {
  return (
    <Grid
      class="operator-shell-layout"
      columns={{ base: 1, md: "13rem minmax(0, 1fr)" }}
      gap="md"
      align="start"
    >
      <OperatorNavigation
        currentPath={currentPath}
        isActiveShellLink={isActiveShellLink}
        operator={operator}
        scopedFamily={scopedFamily}
      />
      <div class="route-transition-surface">{children}</div>
    </Grid>
  );
}

export default function Layout({ children }: { children?: unknown }) {
  const route = currentRoute();
  const currentPath = typeof window === "undefined" ? route.path : window.location.pathname;
  const currentSession = createCurrentSessionQuery();
  const activeRouteFamilyId = routeFamilyFromPath(currentPath) ?? "";
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

  function isActiveShellLink(href: string, exact: boolean) {
    const currentPath = contentPathForRoute(route.path);
    const linkPath = contentPathForHref(href);

    if (exact) {
      return currentPath === linkPath;
    }

    return currentPath === linkPath || currentPath.startsWith(`${linkPath}/`);
  }

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
                      src={fitzLogo}
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
            <WorkspaceShell
              currentPath={currentPath}
              isActiveShellLink={isActiveShellLink}
              operator={operator}
              scopedFamily={scopedFamily}
            >
              {children}
            </WorkspaceShell>
          ) : (
            <RouteFamilySelectorPage />
          )}
        </Container>

        <AppFooter />
      </Block>
    </OperatorScope>
  );
}
