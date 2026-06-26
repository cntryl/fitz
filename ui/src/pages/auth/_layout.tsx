import { Link } from "@askrjs/askr/router";
import { MoonIcon, ShieldIcon, SunIcon } from "@askrjs/lucide";
import { Badge } from "@askrjs/themes/surfaces";
import { Container } from "@askrjs/themes/layouts";
import { Header, Navbar, NavBrand, NavGroup } from "@askrjs/themes/shells";
import { ThemeToggle } from "@askrjs/themes/theme";
import { createCurrentSessionQuery } from "@/features/session/session-query";

export default function Layout({ children }: { children?: unknown }) {
  const currentSession = createCurrentSessionQuery();
  const accountLabel = currentSession.data?.authenticated ? currentSession.data.username : "Guest";

  return (
    <>
      <a class="skip-link" href="#main-content">
        Skip to main content
      </a>

      <Header class="auth-header">
        <Container size="xl">
          <Navbar class="auth-navbar" aria-label="Primary navigation">
            <NavBrand class="auth-brand">
              <Link href="/" aria-label="Fitz admin home">
                <ShieldIcon size={18} />
                <span>Fitz Admin</span>
              </Link>
            </NavBrand>
            <NavGroup align="end" aria-label="Account" role="group">
              <Badge
                variant="outline"
                style={{
                  maxInlineSize: "12rem",
                  overflow: "hidden",
                  textOverflow: "ellipsis",
                  whiteSpace: "nowrap",
                }}
              >
                {accountLabel}
              </Badge>
              <ThemeToggle
                aria-label="Toggle color theme"
                variant="ghost"
                size="icon"
                lightIcon={<SunIcon size={16} />}
                darkIcon={<MoonIcon size={16} />}
              />
            </NavGroup>
          </Navbar>
        </Container>
      </Header>

      <Container size="sm" class="auth-shell-container">
        <main id="main-content" class="auth-shell route-transition-surface">
          {children}
        </main>
      </Container>
    </>
  );
}
