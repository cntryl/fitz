import "./styles.css";
import { For } from "@askrjs/askr";
import { Link } from "@askrjs/askr/router";
import { MoonIcon, ShieldIcon, SunIcon } from "@askrjs/lucide";
import { Button } from "@askrjs/ui";
import { NavLink, SidebarLayout, ThemeProvider, ThemeToggle } from "@askrjs/themes/components";
import { domainLinks, shellLinks } from "@/shared/navigation/domains";
import { createSignOutMutation } from "@/features/session/session-mutation";

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
  const signOut = createSignOutMutation();

  async function onSignOut() {
    try {
      await signOut.execute(undefined);
    } finally {
      if (typeof window !== "undefined") {
        window.location.replace("/login");
      }
    }
  }

  return (
    <ThemeProvider defaultTheme="system" storageKey="fitz-admin-theme">
      <div>
        <div class="app-shell-theme-toggle">
          <ThemeToggle
            aria-label="Toggle color theme"
            lightIcon={<SunIcon size={16} />}
            darkIcon={<MoonIcon size={16} />}
          />
        </div>

        <SidebarLayout
          sidebar={
            <div class="app-shell-sidebar">
              <Link href="/admin" class="app-shell-brand" aria-label="Fitz admin home">
                <ShieldIcon size={18} /> Fitz Admin
              </Link>

              <nav class="app-shell-nav" aria-label="Primary navigation">
                <For each={shellSections} by={(section) => section.label}>
                  {(section) => (
                    <section class="app-shell-nav-group">
                      <p class="app-shell-nav-title">{section.label}</p>
                      <div class="app-shell-nav-links">
                        <For each={section.links} by={(link) => link.href}>
                          {(link) => (
                            <NavLink href={link.href} class="app-shell-nav-item">
                              <link.icon size={16} />
                              <span>{link.title}</span>
                            </NavLink>
                          )}
                        </For>
                      </div>
                    </section>
                  )}
                </For>
              </nav>

              <div class="app-shell-sidebar-footer">
                <Button onPress={onSignOut}>
                  Sign Out
                </Button>
              </div>
            </div>
          }
          sidebarPosition="start"
          sidebarWidth="18rem"
          collapseBelow="md"
        >
          {children}
        </SidebarLayout>
      </div>
    </ThemeProvider>
  );
}
