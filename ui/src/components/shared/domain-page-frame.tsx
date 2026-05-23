import { Container } from "@askrjs/themes/layouts";

export interface DomainPageFrameProps {
  children?: unknown;
  sidebar?: unknown;
}

export default function DomainPageFrame({ children, sidebar: _sidebar }: DomainPageFrameProps) {
  return (
    <Container class="page-frame" fluid p="4">
      <main>{children}</main>
    </Container>
  );
}
