import {
  Brand,
  Container,
  Footer,
  FooterContent,
  FooterDescription,
  FooterLink,
  FooterLinks,
  FooterSection,
  FooterTitle,
} from "@askrjs/themes/components";

export default function AppFooter() {
  return (
    <Footer>
      <Container paddingY="2xl">
        <FooterContent>
          <FooterSection>
            <Brand>
              <FooterTitle>Fitz</FooterTitle>
            </Brand>
            <FooterDescription>
              Fitz operator console for live domains, Route Families, and operational diagnostics.
            </FooterDescription>
          </FooterSection>

          <FooterSection>
            <FooterTitle>Repository</FooterTitle>
            <FooterLinks aria-label="Repository">
              <FooterLink href="https://github.com/cntryl/fitz">Fitz broker</FooterLink>
            </FooterLinks>
          </FooterSection>

          <FooterSection>
            <FooterTitle>Clients</FooterTitle>
            <FooterLinks aria-label="Client repositories">
              <FooterLink href="https://github.com/cntryl/fitz-ts">fitz-ts</FooterLink>
              <FooterLink href="https://github.com/cntryl/fitz-go">fitz-go</FooterLink>
            </FooterLinks>
          </FooterSection>
        </FooterContent>
      </Container>
    </Footer>
  );
}
