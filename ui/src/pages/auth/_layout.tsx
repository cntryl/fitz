import { currentRoute } from "@askrjs/askr/router";
import { MoonIcon, SunIcon } from "@askrjs/lucide";
import { Container, Flex } from "@askrjs/themes/layouts";
import { Header, Navbar, NavBrand, NavGroup } from "@askrjs/themes/shells";
import { ThemeToggle } from "@askrjs/themes/theme";

export default function Layout({ children }: { children?: unknown }) {
  const route = currentRoute();
  const routeKey = route.path || "/";

  return (
    <>
      <Header>
        <Container>
          <Navbar>
            <NavBrand>
              <strong>Fitz</strong>
            </NavBrand>
            <NavGroup align="end">
              <ThemeToggle
                aria-label="Toggle color theme"
              >
                {({ nextTheme }) => (
                  <span key={nextTheme} aria-hidden="true">
                    {nextTheme === "dark" ? <MoonIcon size={16} /> : <SunIcon size={16} />}
                  </span>
                )}
              </ThemeToggle>
            </NavGroup>
          </Navbar>
        </Container>
      </Header>
      <Container size="sm" p="4">
        <Flex class="auth-shell" align="center" justify="center">
          <div key={routeKey} class="route-transition-surface">
            {children}
          </div>
        </Flex>
      </Container>
    </>
  );
}
