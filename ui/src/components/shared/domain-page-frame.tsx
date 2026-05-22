import { Box, Container, Flex } from "@askrjs/themes/layouts";

export interface DomainPageFrameProps {
  children?: unknown;
  sidebar?: unknown;
}

export default function DomainPageFrame({ children, sidebar }: DomainPageFrameProps) {
  return (
    <Container fluid p="4">
      <Flex direction={{ initial: "column", lg: "row" }} gap="4" align="start">
        <Box asChild flexGrow={1} minWidth="0">
          <main>{children}</main>
        </Box>
        {sidebar ? (
          <Box
            asChild
            flexBasis="20rem"
            flexShrink={0}
            minWidth="0"
            position={{ initial: "static", lg: "sticky" }}
            top="var(--ak-space-4)"
          >
            <aside>{sidebar}</aside>
          </Box>
        ) : null}
      </Flex>
    </Container>
  );
}
