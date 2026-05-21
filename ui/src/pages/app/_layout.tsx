import { For } from "@askrjs/askr/control";
import { Link, navigate } from "@askrjs/askr/router";
import { MoonIcon, ShieldIcon, SunIcon } from "@askrjs/lucide";
import { Button } from "@askrjs/themes/controls";
import {
  Navbar,
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

          <NavGroup aria-label="Workspace">
            <For each={shellLinks} by={(link) => link.href}>
              {(link) => (
                <NavLink href={link.href}>
                  <link.icon size={16} />
                  {link.title}
                </NavLink>
              )}
            </For>
          </NavGroup>

          <NavGroup aria-label="Domains">
            <For each={domainLinks} by={(link) => link.href}>
              {(link) => (
                <NavLink href={link.href}>
                  <link.icon size={16} />
                  {link.title}
                </NavLink>
              )}
            </For>
          </NavGroup>

          <NavGroup align="end">
            <Button onPress={onLogout}>Sign Out</Button>
          </NavGroup>
        </Sidebar>
      </ShellNav>

      <ShellMain>
        <Navbar>
          <NavGroup align="end">
            <ThemeToggle
              aria-label="Toggle color theme"
              lightIcon={<SunIcon size={16} />}
              darkIcon={<MoonIcon size={16} />}
            />
          </NavGroup>
        </Navbar>
        {children}
      </ShellMain>
    </Shell>
  );
}
