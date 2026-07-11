import { state } from "@askrjs/askr";
import { For } from "@askrjs/askr/control";
import { currentRoute, Link, navigate } from "@askrjs/askr/router";
import { ChevronDownIcon, MenuIcon, MoonIcon, NetworkIcon, SunIcon } from "@askrjs/lucide";
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
  domainLinks,
  pathWithRouteFamily,
  routeFamilyFromPath,
  shellLinks,
} from "@/shared/navigation/domains";
import { createOperatorContextSnapshot, OperatorContext } from "@/shared/operator-context";
import RouteFamilySelectorPage from "./route-family";

const workspaceLinks = shellLinks.filter(
  (link) => link.title === "Overview" || link.title === "Diagnostics" || link.title === "Metrics",
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

function WorkspaceShell({
  children,
  closeMobileNavigation,
  isActiveShellLink,
  isMobileNavOpen,
  scopedFamily,
  toggleMobileNavigation,
}: {
  children?: unknown;
  closeMobileNavigation: () => void;
  isActiveShellLink: (href: string, exact: boolean) => boolean;
  isMobileNavOpen: boolean;
  scopedFamily: string;
  toggleMobileNavigation: () => void;
}) {
  return (
    <Grid
      class="operator-shell-layout"
      columns={{ base: 1, md: "13rem minmax(0, 1fr)" }}
      gap="md"
      align="start"
    >
      <Sidebar
        class="operator-sidebar"
        collapsible="none"
        minHeight="auto"
        padding="md"
        borderRight={false}
        shrink={false}
        width="full"
        data-mobile-open={isMobileNavOpen ? "true" : undefined}
        onKeyDown={(event: KeyboardEvent) => {
          if (event.key === "Escape") {
            closeMobileNavigation();
          }
        }}
        aria-label="Primary navigation"
        role="navigation"
      >
        <Button
          type="button"
          class="operator-sidebar-toggle"
          variant="outline"
          aria-controls="operator-sidebar-panel"
          aria-expanded={isMobileNavOpen}
          aria-label="Navigation menu"
          onClick={toggleMobileNavigation}
        >
          <MenuIcon size={16} />
          <span>Navigation</span>
        </Button>

        <div class="operator-sidebar-panel" id="operator-sidebar-panel">
          <SidebarContent>
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
          </SidebarContent>
        </div>
      </Sidebar>

      <div class="route-transition-surface">{children}</div>
    </Grid>
  );
}

export default function Layout({ children }: { children?: unknown }) {
  const route = currentRoute();
  const currentPath = typeof window === "undefined" ? route.path : window.location.pathname;
  const currentSession = createCurrentSessionQuery();
  const [mobileNavOpen, setMobileNavOpen] = state(false);
  const activeRouteFamilyId = routeFamilyFromPath(currentPath) ?? "";
  const sessionRouteFamilyId =
    currentSession.data?.routeFamilies.find((family) => /^\d+$/.test(family)) ?? "1";
  const topology = createMessagingTopologyQuery(activeRouteFamilyId || sessionRouteFamilyId);
  const operator = createOperatorContextSnapshot(
    topology.data,
    currentSession.data,
    activeRouteFamilyId,
  );
  const scopedFamily = operator.selectedRouteFamilyId;
  const hasRouteFamilyScope =
    activeRouteFamilyId !== "" && operator.selectedRouteFamilyId === activeRouteFamilyId;
  const isMobileNavOpen = mobileNavOpen();

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

  function isActiveShellLink(href: string, exact: boolean) {
    const currentPath = contentPathForRoute(route.path);
    const linkPath = contentPathForHref(href);

    if (exact) {
      return currentPath === linkPath;
    }

    return currentPath === linkPath || currentPath.startsWith(`${linkPath}/`);
  }

  return (
    <OperatorContext.Scope value={operator}>
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
                <DropdownMenu>
                  <DropdownMenuTrigger aria-label="Route Family selector">
                    <NetworkIcon size={16} aria-hidden="true" />
                    <Block as="span" hide={{ base: true, sm: false }}>
                      <Text as="span" size="sm" weight="medium">
                        {operator.selectedRouteFamily.label}
                      </Text>
                    </Block>
                    <ChevronDownIcon size={14} aria-hidden="true" />
                  </DropdownMenuTrigger>
                  <DropdownMenuPortal>
                    <DropdownMenuContent align="end" sideOffset={8}>
                      <DropdownMenuLabel>Route Family</DropdownMenuLabel>
                      <For each={operator.routeFamilies} by={(family) => family.id}>
                        {(family) => (
                          <DropdownMenuItem onSelect={() => selectRouteFamily(family.id)}>
                            {family.label}
                          </DropdownMenuItem>
                        )}
                      </For>
                    </DropdownMenuContent>
                  </DropdownMenuPortal>
                </DropdownMenu>
              </NavGroup>
            </Navbar>
          </Container>
        </Header>

        <Container class="operator-shell-workspace" paddingY="0" grow>
          {hasRouteFamilyScope ? (
            <WorkspaceShell
              closeMobileNavigation={closeMobileNavigation}
              isActiveShellLink={isActiveShellLink}
              isMobileNavOpen={isMobileNavOpen}
              scopedFamily={scopedFamily}
              toggleMobileNavigation={toggleMobileNavigation}
            >
              {children}
            </WorkspaceShell>
          ) : (
            <RouteFamilySelectorPage />
          )}
        </Container>

        <AppFooter />
      </Block>
    </OperatorContext.Scope>
  );
}
