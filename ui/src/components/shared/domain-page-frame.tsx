import { Container, Stack } from "@askrjs/themes/layouts";

export interface DomainPageFrameProps {
  children?: unknown;
  sidebar?: unknown;
}

export default function DomainPageFrame({ children, sidebar }: DomainPageFrameProps) {
  const hasSidebar = sidebar !== undefined && sidebar !== null;

  return (
    <Container class="page-frame" fluid padding="4">
      <div
        class={
          hasSidebar ? "page-frame-layout page-frame-layout-with-sidebar" : "page-frame-layout"
        }
      >
        {hasSidebar ? <aside class="page-frame-sidebar">{sidebar}</aside> : null}
        <main class="page-frame-main">
          <Stack gap="4">{children}</Stack>
        </main>
      </div>
    </Container>
  );
}
