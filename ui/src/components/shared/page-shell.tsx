import { SidebarLayout } from "@askrjs/themes/components";

export interface PageShellProps {
  children: unknown;
  sidebar?: unknown;
}

export default function PageShell({ children, sidebar }: PageShellProps) {
  if (sidebar == null) {
    return <>{children}</>;
  }

  return (
    <SidebarLayout
      class="page-shell"
      sidebar={sidebar}
      sidebarPosition="end"
      sidebarWidth="18rem"
      gap="1.5rem"
      collapseBelow="md"
    >
      {children}
    </SidebarLayout>
  );
}
