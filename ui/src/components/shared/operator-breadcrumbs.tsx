import { For } from "@askrjs/askr/control";
import { currentRoute, Link } from "@askrjs/askr/router";
import {
  Breadcrumb,
  BreadcrumbPage,
  BreadcrumbItem,
  BreadcrumbLink,
  BreadcrumbList,
  BreadcrumbSeparator,
} from "@askrjs/themes/components";
import { useOperatorContext } from "@/shared/operator-context";
import {
  adminChildHref,
  adminHref,
  domainHref,
  domainScopeHref,
  domainTitleForSegment,
  isDomainSegment,
  isRouteFamilyPathSegment,
  routeFamilyFromPath,
  shellLinks,
} from "@/shared/navigation/domains";

const shellTitles = new Map(shellLinks.map((link) => [link.href, link.title]));

function decodeSegment(value: string | undefined) {
  if (!value) {
    return null;
  }

  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

function contentPathForRoute(path: string) {
  const parts = path.split("/").filter(Boolean);

  if (parts[0] !== "admin") {
    return path;
  }

  if (isRouteFamilyPathSegment(parts[1])) {
    const child = parts.slice(2).join("/");

    return child ? `/${child}` : "/";
  }

  const legacyChild = parts.slice(1).join("/");

  return legacyChild ? `/${legacyChild}` : "/";
}

function routeCrumbs(path: string, params: Record<string, string | undefined>, family: string) {
  const parts = path.split("/").filter(Boolean);
  const first = parts[0] ?? "";

  if (path === "/" || path === "/admin") {
    return [{ href: adminHref(family), label: "Overview" }];
  }

  if (path === "/diagnostics" || path === "/metrics") {
    return [{ href: adminChildHref("diagnostics", family), label: "Diagnostics" }];
  }

  if (path === "/settings") {
    return [{ href: adminChildHref("settings", family), label: "Settings" }];
  }

  if (path === "/sessions") {
    return [
      { href: adminChildHref("settings", family), label: "Settings" },
      { href: adminChildHref("sessions", family), label: "Sessions" },
    ];
  }

  if (isDomainSegment(first)) {
    const crumbs = [{ href: domainHref(first, family), label: domainTitleForSegment(first) }];
    const realm = decodeSegment(params.realm ?? parts[1]);
    const area = decodeSegment(params.area ?? parts[2]);
    const resource = decodeSegment(params.resource ?? parts[3]);
    const operation = decodeSegment(params.operation ?? parts[4]);

    if (realm) {
      crumbs.push({ href: domainScopeHref(first, { realm }, family), label: realm });
    }
    if (area) {
      crumbs.push({
        href: domainScopeHref(first, { area, realm: realm ?? undefined }, family),
        label: area,
      });
    }
    if (resource) {
      crumbs.push({
        href: domainScopeHref(
          first,
          {
            area: area ?? undefined,
            realm: realm ?? undefined,
            resource,
          },
          family,
        ),
        label: resource,
      });
    }
    if (operation) crumbs.push({ href: path, label: operation });

    return crumbs;
  }

  return [{ href: path, label: shellTitles.get(path) ?? "Workspace" }];
}

export default function OperatorBreadcrumbs() {
  const route = currentRoute();
  const operator = useOperatorContext();
  const family = routeFamilyFromPath(route.path) ?? operator.selectedRouteFamilyId;
  const contentPath = contentPathForRoute(route.path);
  const crumbs = [
    { href: adminHref(family), label: operator.selectedRouteFamily.label },
    ...routeCrumbs(contentPath, route.params, family),
  ];

  return (
    <Breadcrumb class="operator-breadcrumbs" aria-label="Resource hierarchy">
      <BreadcrumbList>
        <For each={crumbs} by={(crumb, index) => `${crumb.label}:${index}`}>
          {(crumb, index) => {
            const isLast = index() === crumbs.length - 1;

            return (
              <BreadcrumbItem>
                {isLast ? (
                  <BreadcrumbPage>{crumb.label}</BreadcrumbPage>
                ) : (
                  <BreadcrumbLink asChild>
                    <Link href={crumb.href}>{crumb.label}</Link>
                  </BreadcrumbLink>
                )}
                {!isLast ? <BreadcrumbSeparator /> : null}
              </BreadcrumbItem>
            );
          }}
        </For>
      </BreadcrumbList>
    </Breadcrumb>
  );
}
