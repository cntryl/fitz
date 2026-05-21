import { Shell, ShellMain, ShellNav, Sidebar } from "@askrjs/themes/shells";
import type { CollapseBreakpoint } from "@askrjs/themes/navs";

export interface SidebarLayoutProps {
  children?: unknown;
  sidebar?: unknown;
  sidebarPosition?: "start" | "end";
  sidebarWidth?: string;
  gap?: string;
  collapseBelow?: CollapseBreakpoint;
}

export default function SidebarLayout({
  children,
  sidebar,
  sidebarPosition = "start",
  sidebarWidth = "18rem",
  gap = "1rem",
  collapseBelow,
}: SidebarLayoutProps) {
  return (
    <Shell
      class="fitz-sidebar-layout"
      variant="sidebar"
      data-sidebar-position={sidebarPosition}
      style={`--fitz-sidebar-width: ${sidebarWidth}; --fitz-sidebar-gap: ${gap};`}
    >
      {sidebarPosition === "start" ? (
        <ShellNav class="fitz-sidebar-layout-nav">
          <Sidebar breakpoint={collapseBelow} aria-label="Sidebar">
            {sidebar}
          </Sidebar>
        </ShellNav>
      ) : null}

      <ShellMain class="fitz-sidebar-layout-main">{children}</ShellMain>

      {sidebarPosition === "end" ? (
        <ShellNav class="fitz-sidebar-layout-nav">
          <Sidebar breakpoint={collapseBelow} aria-label="Sidebar">
            {sidebar}
          </Sidebar>
        </ShellNav>
      ) : null}
    </Shell>
  );
}
