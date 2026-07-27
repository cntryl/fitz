import { Container, Footer, FooterLink, Inline, Text } from "@askrjs/themes/components";

export default function AppFooter() {
  return (
    <Footer>
      <Container paddingY="md">
        <Inline align="center" justify="between" gap="2" wrap="wrap">
          <Text size="sm" tone="muted">
            Fitz operator console
          </Text>
          <Inline as="nav" aria-label="Related repositories" align="center" gap="3">
            <FooterLink href="https://github.com/cntryl/fitz">Fitz broker</FooterLink>
            <FooterLink href="https://github.com/cntryl/fitz-ts">fitz-ts</FooterLink>
            <FooterLink href="https://github.com/cntryl/fitz-go">fitz-go</FooterLink>
          </Inline>
        </Inline>
      </Container>
    </Footer>
  );
}
