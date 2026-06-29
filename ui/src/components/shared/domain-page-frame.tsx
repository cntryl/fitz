import { Container, Inline, Stack } from "@askrjs/themes/components";
import OperatorBreadcrumbs from "./operator-breadcrumbs";

export interface DomainPageFrameProps {
  children?: unknown;
  sidebar?: unknown;
}

export default function DomainPageFrame({ children, sidebar }: DomainPageFrameProps) {
  const hasSidebar = sidebar !== undefined && sidebar !== null;

  return (
    <Container class="domain-page-frame" size="xl">
      <Inline
        class="page-frame-layout"
        direction={{ base: "column", md: "row" }}
        gap="3"
        align={{ base: "stretch", md: "start" }}
        wrap="nowrap"
      >
        <main id="main-content" class="page-frame-main" tabIndex={-1}>
          <Stack gap="3">
            <OperatorBreadcrumbs />
            {children}
          </Stack>
        </main>

        {hasSidebar ? <aside class="page-frame-sidebar">{sidebar}</aside> : null}
      </Inline>
    </Container>
  );
}
