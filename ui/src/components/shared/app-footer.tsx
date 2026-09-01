import { Container, Footer, FooterLink, Text, Block } from "@askrjs/themes/components";

export default function AppFooter() {
  return (
    <Footer>
      <Container paddingY="md">
        <Block direction="row" align="center" justify="between" gap="xs" wrap={true}>
          <Text size="sm" tone="muted">
            Fitz operator console
          </Text>
          <Block direction="row" as="nav" aria-label="Related repositories" align="center" gap="sm">
            <FooterLink href="https://github.com/cntryl/fitz">Fitz broker</FooterLink>
            <FooterLink href="https://github.com/cntryl/fitz-ts">fitz-ts</FooterLink>
            <FooterLink href="https://github.com/cntryl/fitz-go">fitz-go</FooterLink>
          </Block>
        </Block>
      </Container>
    </Footer>
  );
}
