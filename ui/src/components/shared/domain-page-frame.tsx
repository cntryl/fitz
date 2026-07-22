import { task } from "@askrjs/askr/resources";
import { Block, Main, Stack } from "@askrjs/themes/components";
import OperatorBreadcrumbs from "./operator-breadcrumbs";

export interface DomainPageFrameProps {
  children?: unknown;
}

function routePageTitle(main: HTMLElement, explicitTitle?: string) {
  const titleSource =
    explicitTitle === undefined
      ? (main.querySelector(".domain-header-title-row > span:first-child") ??
        main.querySelector("h1"))
      : null;
  const pageTitle = explicitTitle?.trim() || titleSource?.textContent?.trim();

  document.title = pageTitle ? `${pageTitle} · Fitz Admin` : "Fitz Admin";
}

export function manageRoutePageContext(explicitTitle?: string) {
  if (typeof document === "undefined") return;

  let cancelled = false;
  let observer: MutationObserver | null = null;

  queueMicrotask(() => {
    if (cancelled) return;

    const main = document.getElementById("main-content");
    if (!(main instanceof HTMLElement)) return;

    routePageTitle(main, explicitTitle);
    main.focus({ preventScroll: true });

    if (explicitTitle === undefined) {
      observer = new MutationObserver(() => routePageTitle(main));
      observer.observe(main, { childList: true, characterData: true, subtree: true });
    }
  });

  return () => {
    cancelled = true;
    observer?.disconnect();
  };
}

export default function DomainPageFrame({ children }: DomainPageFrameProps) {
  task(() => manageRoutePageContext());

  return (
    <Main
      id="main-content"
      class="domain-page-frame route-transition-surface"
      paddingY="xl"
      tabIndex={-1}
    >
      <Stack gap="3">
        <OperatorBreadcrumbs />

        <Block class="page-frame-main">
          <Stack gap="3">{children}</Stack>
        </Block>
      </Stack>
    </Main>
  );
}
