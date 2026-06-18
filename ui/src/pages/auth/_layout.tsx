import { currentRoute } from "@askrjs/askr/router";
import { MoonIcon, ShieldIcon, SunIcon } from "@askrjs/lucide";
import { Container, Flex } from "@askrjs/themes/layouts";
import { Header, Navbar, NavBrand, NavGroup } from "@askrjs/themes/shells";
import { ThemeToggle } from "@askrjs/themes/theme";

export default function Layout({ children }: { children?: unknown }) {
  const route = currentRoute();
  const routeKey = route.path || "/";

  return (
    <>
      <Header>
        <Container size="xl">
          <Navbar>
            <NavBrand>
              <ShieldIcon size={18} />
              <span>Fitz Admin</span>
            </NavBrand>
            <NavGroup align="end">
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
          style={{ minHeight: "min(42rem, calc(100dvh - 6rem))" }}
        >
          <div key={routeKey} class="route-transition-surface">
            {children}
          </div>
        </Flex>
      </Container>
    </>
  );
}
