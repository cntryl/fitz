import { currentRoute, Link, navigate } from "@askrjs/askr/router";
import { MoonIcon, ShieldIcon, SunIcon } from "@askrjs/lucide";
import { Button } from "@askrjs/themes/controls";
import {
  NavBrand,
  NavGroup,
  NavLink,
  Shell,
  ShellMain,
  ShellNav,
  Sidebar,
} from "@askrjs/themes/shells";
import { ThemeToggle } from "@askrjs/themes/theme";
import { domainLinks, shellLinks } from "@/shared/navigation/domains";

export default function Layout({ children }: { children?: unknown }) {
  const route = currentRoute();
  const routeKey = route.path || "/";

  function onLogout() {
    navigate("/logout");
  }

  return (
    <Shell variant="sidebar">
      <ShellNav>
        <Sidebar breakpoint="md" aria-label="Admin navigation">
          <NavBrand>
            <Link href="/" aria-label="Fitz admin home">
              <ShieldIcon size={18} />
              Fitz Admin
            </Link>
          </NavBrand>

          <NavGroup label="Workspace" aria-label="Workspace">
            {shellLinks.map((link) => (
              <NavLink key={link.href} href={link.href}>
                <link.icon size={16} />
                {link.title}
              </NavLink>
            ))}
          </NavGroup>

          <NavGroup label="Domains" aria-label="Domains">
            {domainLinks.map((link) => (
              <NavLink key={link.href} href={link.href}>
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
            <Button onPress={onLogout}>Sign out</Button>
          </NavGroup>
        </Sidebar>
      </ShellNav>

      <ShellMain>
        <div key={routeKey} class="route-transition-surface">
          {children}
        </div>
      </ShellMain>
    </Shell>
  );
}
