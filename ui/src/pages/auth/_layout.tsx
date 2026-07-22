import { Link } from "@askrjs/askr/router";
import { MoonIcon, SunIcon } from "@askrjs/lucide";
import { Badge, Block } from "@askrjs/themes/components";
import { Container } from "@askrjs/themes/components";
import { Header, Navbar, NavBrand, NavGroup } from "@askrjs/themes/components";
import { ThemeToggle } from "@askrjs/themes/theme";
import fitzLogo from "@/assets/fitz-logo.png";
import AppFooter from "@/components/shared/app-footer";
import { createCurrentSessionQuery } from "@/features/session/session-query";

export function authAccountLabel(
  session: { authenticated: boolean; username: string } | null | undefined,
) {
  return session?.authenticated ? session.username || "Open access" : "Guest";
}

export default function Layout({ children }: { children?: unknown }) {
  const currentSession = createCurrentSessionQuery();
  const accountLabel = authAccountLabel(currentSession.data);

  return (
    <Block minHeight="screen" direction="column">
      <a class="skip-link" href="#main-content">
        Skip to main content
      </a>

      <Header class="auth-header">
        <Container size="xl">
          <Navbar class="auth-navbar" aria-label="Primary navigation">
            <NavBrand class="auth-brand">
              <Link href="/" aria-label="Fitz admin home">
                <img
                  class="fitz-brand-logo"
                  src={fitzLogo}
                  alt=""
                  width={28}
                  height={28}
                  aria-hidden="true"
                />
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

      <Container size="sm" grow align="center" justify="center" paddingY="2xl">
        <Block
          as="main"
          id="main-content"
          class="route-transition-surface"
          width="full"
          center
          tabIndex={-1}
        >
          {children}
        </Block>
      </Container>

      <AppFooter />
    </Block>
  );
}
