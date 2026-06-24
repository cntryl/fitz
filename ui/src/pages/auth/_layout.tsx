import { Link } from "@askrjs/askr/router";
import { MoonIcon, ShieldIcon, SunIcon } from "@askrjs/lucide";
import { Badge } from "@askrjs/themes/surfaces";
import { Container, Flex } from "@askrjs/themes/layouts";
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

      <Header>
        <Container size="xl">
          <Navbar breakpoint="md" aria-label="Primary navigation">
            <NavBrand>
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

      <Container size="sm" p="4">
        <Flex
          align="center"
          justify="center"
          style={{ minHeight: "calc(100dvh - var(--ak-layout-navbar-height, 60px))" }}
        >
          <main id="main-content" class="route-transition-surface">
            {children}
          </main>
        </Flex>
      </Container>
    </>
  );
}
