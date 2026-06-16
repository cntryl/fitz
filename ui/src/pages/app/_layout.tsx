import { currentRoute, Link, navigate } from "@askrjs/askr/router";
import { MoonIcon, ShieldIcon, SunIcon } from "@askrjs/lucide";
import { Button } from "@askrjs/themes/controls";
import { Container } from "@askrjs/themes/layouts";
import {
  NavBrand,
  NavGroup,
  NavLink,
  Navbar,
  Shell,
  ShellMain,
  ShellNav,
} from "@askrjs/themes/shells";
import { Badge } from "@askrjs/themes/surfaces";
import { ThemeToggle, useTheme } from "@askrjs/themes/theme";
import { createCurrentSessionQuery } from "@/features/session/session-query";
import { shellLinks } from "@/shared/navigation/domains";

export default function Layout({ children }: { children?: unknown }) {
  const route = currentRoute();
  const routeKey = route.path || "/";
  const currentSession = createCurrentSessionQuery();
  const theme = useTheme();
  const username = currentSession.data?.username ?? "admin";
  const showUserBadge = currentSession.data?.authenticated !== false;

  function onLogout() {
    navigate("/logout");
  }

  return (
    <Shell variant="topbar">
      <ShellNav class="app-shell-nav">
        <Container maxWidth="var(--ak-layout-content-max-width)">
          <Navbar breakpoint="md" aria-label="Primary navigation">
            <NavBrand>
              <Link href="/" aria-label="Fitz admin home" class="app-brand">
                <ShieldIcon size={18} />
                <strong>Fitz Admin</strong>
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
            </NavGroup>

            <NavGroup align="end" aria-label="Account" role="group">
              {showUserBadge ? <Badge variant="default">{username}</Badge> : null}
              <ThemeToggle
                key={theme.theme()}
                aria-label="Toggle color theme"
                lightIcon={<SunIcon size={16} />}
                darkIcon={<MoonIcon size={16} />}
              />
              <Button variant="outline" onPress={onLogout}>
                Sign out
              </Button>
            </NavGroup>
          </Navbar>
        </Container>
      </ShellNav>

      <ShellMain>
        <div key={routeKey} class="route-transition-surface">
          {children}
        </div>
      </ShellMain>
    </Shell>
  );
}
