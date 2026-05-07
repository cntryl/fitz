import "./styles.css";
import { For } from "@askrjs/askr";
import { Link } from "@askrjs/askr/router";
import { ActivityIcon, MoonIcon, ShieldIcon, SunIcon } from "@askrjs/lucide";
import {
  Card,
  CardContent,
  Badge,
  NavBrand,
  NavGroup,
  Navbar,
  NavLink,
  SidebarLayout,
  ThemeProvider,
  ThemeToggle,
} from "@askrjs/themes/components";
import { domainLinks, shellLinks } from "@/shared/navigation/domains";

const shellSections = [
  {
    label: "Workspace",
    links: shellLinks,
  },
  {
    label: "Domains",
    links: domainLinks,
  },
];

export default function App({ children }: { children?: unknown }) {
  return (
    <ThemeProvider defaultTheme="system" storageKey="fitz-admin-theme">
      <div class="app-shell-frame">
        <div class="app-shell-theme-toggle">
          <ThemeToggle
            class="app-shell-theme-toggle-button"
            aria-label="Toggle color theme"
            lightIcon={<SunIcon size={16} />}
            darkIcon={<MoonIcon size={16} />}
          />
        </div>

        <SidebarLayout
          class="app-shell"
          sidebar={
            <div class="app-shell-sidebar">
              <NavBrand>
                <Link href="/" class="app-shell-brand" aria-label="Fitz admin home">
                  <span class="app-shell-brand-icon">
                    <ShieldIcon size={18} />
                  </span>
                  <span>
                    <strong>Fitz Admin</strong>
                  </span>
                </Link>
              </NavBrand>

              <Navbar aria-label="Primary navigation">
                <For each={shellSections} by={(section) => section.label}>
                  {(section) => (
                    <NavGroup class="app-shell-nav-group">
                      <p class="app-shell-nav-label">{section.label}</p>
                      <For each={section.links} by={(link) => link.href}>
                        {(link) => (
                          <NavLink href={link.href}>
                            <span class="app-shell-nav-link">
                              <link.icon size={16} />
                              <span>{link.title}</span>
                            </span>
                          </NavLink>
                        )}
                      </For>
                    </NavGroup>
                  )}
                </For>
              </Navbar>
            </div>
          }
          sidebarPosition="start"
          sidebarWidth="18rem"
          gap="1.5rem"
          collapseBelow="md"
        >
          <div class="app-shell-main">
            <div class="app-shell-main-inner">
              <Card class="app-shell-banner" variant="raised">
                <CardContent class="app-shell-banner-content">
                  <Badge>Operational console</Badge>
                  <span class="app-shell-banner-copy">
                    <ActivityIcon size={16} />
                    Root-mounted admin UI wired to the live Fitz admin API.
                  </span>
                </CardContent>
              </Card>
              {children}
            </div>
          </div>
        </SidebarLayout>
      </div>
    </ThemeProvider>
  );
}
