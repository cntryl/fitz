import { MoonIcon, SunIcon } from "@askrjs/lucide";
import { Container, Flex } from "@askrjs/themes/layouts";
import { Header, Navbar, NavBrand, NavGroup } from "@askrjs/themes/shells";
import { ThemeToggle } from "@askrjs/themes/theme";

export default function Layout({ children }: { children?: unknown }) {
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
                lightIcon={<SunIcon size={16} />}
                darkIcon={<MoonIcon size={16} />}
              />
            </NavGroup>
          </Navbar>
        </Container>
      </Header>
      <Container size="sm" p="4">
        <Flex class="auth-shell" align="center" justify="center">
          {children}
        </Flex>
      </Container>
    </>
  );
}
