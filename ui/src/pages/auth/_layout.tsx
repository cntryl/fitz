import { Block } from "@askrjs/themes/components";

export default function Layout({ children }: { children?: unknown }) {
  return (
    <Block
      as="main"
      id="main-content"
      class="auth-page route-transition-surface"
      background="canvas"
      tabIndex={-1}
    >
      <Block class="auth-panel" width="full">
        {children}
      </Block>
    </Block>
  );
}
