import { MoonIcon, SunIcon } from "@askrjs/lucide";
import { Block, Container } from "@askrjs/themes/layouts";
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
      <Container>
        <Block size="sm" space="lg" align="center" justify="center">
          {children}
        </Block>
      </Container>
    </>
  );
}
