import { state } from "@askrjs/askr";
import { For } from "@askrjs/askr/control";
import { Link } from "@askrjs/askr/router";
import { MoonIcon, SearchIcon, ShieldIcon, SunIcon } from "@askrjs/lucide";
import { Input, Label } from "@askrjs/ui";
import { Button } from "@askrjs/themes/controls";
import { NavLink } from "@askrjs/themes/navs";
import { Badge } from "@askrjs/themes/surfaces";
import { ThemeToggle, type ThemeToggleRenderContext } from "@askrjs/themes/theme";
import SidebarLayout from "@/components/shared/sidebar-layout";
import RootLayout from "../_layout";
import { domainLinks, shellLinks } from "@/shared/navigation/domains";
import { createSignOutMutation } from "@/features/session/session-mutation";
import { createCurrentSessionQuery } from "@/features/session/session-query";
import { createHealthSummaryQuery } from "@/features/system/health-query";

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

export default function AppLayout({ children }: { children?: unknown }) {
  const signOut = createSignOutMutation();
  const session = createCurrentSessionQuery();
  const health = createHealthSummaryQuery();
  const command = state("");
  const realm = state("");

  async function onSignOut() {
    try {
      await signOut.execute(undefined);
    } finally {
      if (typeof window !== "undefined") {
        window.location.replace("/login");
      }
    }
  }

  function onCommandSubmit(event: Event) {
    event.preventDefault();

    if (typeof window === "undefined") return;

    const value = command().trim().toLowerCase();
    const domain = domainLinks.find((link) => value.includes(link.title.toLowerCase()));
    const workspace = shellLinks.find((link) => value.includes(link.title.toLowerCase()));

    if (value.includes("dead")) {
      window.location.assign("/queue");
      return;
    }

    if (value.includes("pending") && value.includes("rpc")) {
      window.location.assign("/rpc");
      return;
    }

    if (domain) {
      window.location.assign(domain.href);
      return;
    }

    if (workspace) {
      window.location.assign(workspace.href);
      return;
    }
  }

  return (
    <RootLayout>
      <div class="app-shell-frame">
        <div class="app-shell-theme-toggle">
          <ThemeToggle aria-label="Toggle color theme">
            {({ theme }: ThemeToggleRenderContext) =>
              theme === "dark" ? <MoonIcon size={16} /> : <SunIcon size={16} />
            }
          </ThemeToggle>
        </div>

        <SidebarLayout
          sidebar={
            <div class="app-shell-sidebar">
              <Link href="/admin" class="app-shell-brand" aria-label="Fitz admin home">
                <ShieldIcon size={18} /> Fitz Admin
              </Link>

              <div class="app-shell-status">
                <Badge>{health.data?.readiness ?? "checking"}</Badge>
                <span>{session.data?.username ?? "admin"}</span>
              </div>

              <form class="app-shell-command" onSubmit={onCommandSubmit}>
                <Label for="app-command">Command</Label>
                <div class="app-shell-command-row">
                  <SearchIcon size={15} />
                  <Input
                    id="app-command"
                    placeholder="Search domains, routes, actions"
                    value={command()}
                    onInput={(event: Event) =>
                      command.set((event.target as HTMLInputElement).value)
                    }
                  />
                </div>
              </form>

              <div class="auth-field">
                <Label for="app-realm">Realm filter</Label>
                <Input
                  id="app-realm"
                  placeholder="Any realm"
                  value={realm()}
                  onInput={(event: Event) => realm.set((event.target as HTMLInputElement).value)}
                />
              </div>

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
                <Button class="secondary-action" onPress={() => window.location.reload()}>
                  Refresh
                </Button>
                <Button onPress={onSignOut}>Sign Out</Button>
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
    </RootLayout>
  );
}
