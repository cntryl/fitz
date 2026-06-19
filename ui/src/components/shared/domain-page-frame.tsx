import { Container, Flex, Stack } from "@askrjs/themes/layouts";

export interface DomainPageFrameProps {
  children?: unknown;
  sidebar?: unknown;
}

export default function DomainPageFrame({ children, sidebar }: DomainPageFrameProps) {
  const hasSidebar = sidebar !== undefined && sidebar !== null;

  return (
    <Container class="domain-page-frame" size="xl">
      <Flex direction={{ initial: "column", md: "row" }} gap="4" align="start" wrap="nowrap">
        <main id="main-content" class="page-frame-main" tabIndex={-1}>
          <Stack gap="4">{children}</Stack>
        </main>

        {hasSidebar ? <aside class="page-frame-sidebar">{sidebar}</aside> : null}
      </Flex>
    </Container>
  );
}
