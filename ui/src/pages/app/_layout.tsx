import { currentRoute, Link, navigate } from "@askrjs/askr/router";
import { MoonIcon, ShieldIcon, SunIcon } from "@askrjs/lucide";
import { Button } from "@askrjs/themes/controls";
import { Stack } from "@askrjs/themes/layouts";
import {
  NavBrand,
  NavGroup,
  NavLink,
  Navbar,
  Shell,
  ShellMain,
  ShellNav,
} from "@askrjs/themes/shells";
import { ThemeToggle } from "@askrjs/themes/theme";
import { shellLinks } from "@/shared/navigation/domains";

export default function Layout({ children }: { children?: unknown }) {
  const route = currentRoute();
  const routeKey = route.path || "/";

  function onLogout() {
    navigate("/logout");
  }

  return (
    <Shell variant="topbar">
      <ShellNav class="app-shell-nav">
        <Navbar breakpoint="md" aria-label="Primary navigation">
          <NavBrand>
            <Link href="/" aria-label="Fitz admin home" class="app-brand">
              <ShieldIcon size={18} />
              <Stack gap="0" class="app-brand-copy">
                <strong>Fitz Admin</strong>
                <span>Broker operations console</span>
              </Stack>
            </Link>
          </NavBrand>

          <NavGroup
            class="shell-nav-group shell-nav-workspace"
            label="Workspace"
            aria-label="Workspace"
          >
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

          <NavGroup align="end">
            <ThemeToggle
              aria-label="Toggle color theme"
              lightIcon={<SunIcon size={16} />}
              darkIcon={<MoonIcon size={16} />}
            />
            <Button variant="outline" onPress={onLogout}>
              Sign out
            </Button>
          </NavGroup>
        </Navbar>
      </ShellNav>

      <ShellMain>
        <div key={routeKey} class="route-transition-surface">
          {children}
        </div>
      </ShellMain>
    </Shell>
  );
}
