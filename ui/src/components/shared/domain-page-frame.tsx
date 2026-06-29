import { Block, Grid, Main, Stack } from "@askrjs/themes/components";
import OperatorBreadcrumbs from "./operator-breadcrumbs";

export interface DomainPageFrameProps {
  children?: unknown;
  sidebar?: unknown;
}

export default function DomainPageFrame({ children, sidebar }: DomainPageFrameProps) {
  const hasSidebar = sidebar !== undefined && sidebar !== null;

  return (
    <Main
      id="main-content"
      class="domain-page-frame route-transition-surface"
      paddingY="xl"
      tabIndex={-1}
    >
      <Stack gap="3">
        <OperatorBreadcrumbs />

        {hasSidebar ? (
          <Grid
            class="page-frame-layout"
            columns={{ base: 1, lg: "minmax(0, 1fr) minmax(17rem, 21rem)" }}
            gap="lg"
            align="start"
          >
            <Block class="page-frame-main">
              <Stack gap="3">{children}</Stack>
            </Block>
            <aside class="page-frame-sidebar">{sidebar}</aside>
          </Grid>
        ) : (
          <Block class="page-frame-main">
            <Stack gap="3">{children}</Stack>
          </Block>
        )}
      </Stack>
    </Main>
  );
}
