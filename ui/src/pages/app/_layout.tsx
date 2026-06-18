import { For } from "@askrjs/askr/control";
import { currentRoute, Link, navigate } from "@askrjs/askr/router";
import { ChevronDownIcon, LogOutIcon, MoonIcon, ShieldIcon, SunIcon } from "@askrjs/lucide";
import { Button } from "@askrjs/themes/controls";
import { Container } from "@askrjs/themes/layouts";
import {
  Dropdown,
  DropdownContent,
  DropdownItem,
  DropdownLabel,
  DropdownPortal,
  DropdownTrigger,
} from "@askrjs/themes/overlays";
import {
  NavBrand,
  NavGroup,
  NavLink,
  Header,
  Navbar,
} from "@askrjs/themes/shells";
import { Badge } from "@askrjs/themes/surfaces";
import { ThemeToggle } from "@askrjs/themes/theme";
import { createCurrentSessionQuery } from "@/features/session/session-query";
import { domainLinks, shellLinks, type DomainLink } from "@/shared/navigation/domains";

function DomainMenuItem({ link }: { link: DomainLink }) {
  return (
    <DropdownItem asChild>
      <Link class="navbar-domain-menu-item" href={link.href}>
        <span class="navbar-domain-menu-icon" aria-hidden="true">
          <link.icon size={14} />
        </span>
        <span class="navbar-domain-menu-copy">
          <span class="navbar-domain-menu-title">{link.title}</span>
          <span class="navbar-domain-menu-description">{link.description}</span>
        </span>
      </Link>
    </DropdownItem>
  );
}

export default function Layout({ children }: { children?: unknown }) {
  const route = currentRoute();
  const routeKey = route.path || "/";
  const currentSession = createCurrentSessionQuery();
  const username = currentSession.data?.username ?? "admin";
  const showUserBadge = currentSession.data?.authenticated !== false;

  function onLogout() {
    navigate("/logout");
  }

  return (
    <Dropdown aria-label="Domain pages">
      <>
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

              <NavGroup aria-label="Workspace" role="group">
                {shellLinks.map((link) => (
                  <NavLink
                    key={link.href}
                    href={link.href}
                    match={link.href === "/" ? "exact" : "prefix"}
                  >
                    <link.icon size={16} />
                    {link.title}
                  </NavLink>
                ))}

                <div class="navbar-domain-menu">
                  <DropdownTrigger>
                    <span>Domains</span>
                    <ChevronDownIcon size={14} />
                  </DropdownTrigger>
                  <DropdownPortal>
                    <DropdownContent align="start" sideOffset={8}>
                      <DropdownLabel>Domain pages</DropdownLabel>
                      <For each={domainLinks} by={(link) => link.href}>
                        {(link) => <DomainMenuItem link={link} />}
                      </For>
                    </DropdownContent>
                  </DropdownPortal>
                </div>
              </NavGroup>

              <NavGroup align="end" aria-label="Account" role="group">
                {showUserBadge ? (
                  <Badge
                    variant="outline"
                    style={{
                      maxInlineSize: "12rem",
                      overflow: "hidden",
                      textOverflow: "ellipsis",
                      whiteSpace: "nowrap",
                    }}
                    >
                      {username}
                  </Badge>
                ) : null}
                <ThemeToggle
                  aria-label="Toggle color theme"
                  variant="ghost"
                  size="icon"
                  lightIcon={<SunIcon size={16} />}
                  darkIcon={<MoonIcon size={16} />}
                />
                <Button variant="outline" onPress={onLogout}>
                  <LogOutIcon size={16} />
                  Sign out
                </Button>
              </NavGroup>
            </Navbar>
          </Container>
        </Header>

        <div key={routeKey} class="route-transition-surface">
          {children}
        </div>
      </>
    </Dropdown>
  );
}
