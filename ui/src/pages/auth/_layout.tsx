import { MoonIcon, SunIcon } from "@askrjs/lucide";
import { Block } from "@askrjs/themes/components";
import { ThemeToggle } from "@askrjs/themes/theme";

export default function Layout({ children }: { children?: unknown }) {
  return (
    <Block
      as="main"
      id="main-content"
      class="auth-page route-transition-surface"
      background="canvas"
      tabIndex={-1}
    >
      {/* Not a <header>: the auth routes deliberately render no page chrome. */}
      <div class="auth-theme-toggle">
        <ThemeToggle
          aria-label="Toggle color theme"
          variant="ghost"
          size="icon"
          lightIcon={<SunIcon size={16} />}
          darkIcon={<MoonIcon size={16} />}
        />
      </div>

      <Block class="auth-panel" width="full">
        {children}
      </Block>
    </Block>
  );
}
