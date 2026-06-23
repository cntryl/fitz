import { For } from "@askrjs/askr/control";
import { currentRoute, Link } from "@askrjs/askr/router";
import {
  Breadcrumb,
  BreadcrumbCurrent,
  BreadcrumbItem,
  BreadcrumbLink,
  BreadcrumbList,
  BreadcrumbSeparator,
} from "@askrjs/themes/navs";
import { useOperatorContext } from "@/shared/operator-context";
import {
  domainHref,
  domainScopeHref,
  domainTitleForSegment,
  isDomainSegment,
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

function routeCrumbs(path: string, params: Record<string, string | undefined>) {
  const parts = path.split("/").filter(Boolean);
  const first = parts[0] ?? "";

  if (path === "/" || path === "/admin") {
    return [{ href: "/", label: "Overview" }];
  }

  if (path === "/diagnostics" || path === "/admin/metrics") {
    return [{ href: "/diagnostics", label: "Diagnostics" }];
  }

  if (path === "/settings") {
    return [{ href: "/settings", label: "Settings" }];
  }

  if (path === "/sessions") {
    return [
      { href: "/settings", label: "Settings" },
      { href: "/sessions", label: "Sessions" },
    ];
  }

  if (isDomainSegment(first)) {
    const crumbs = [{ href: domainHref(first), label: domainTitleForSegment(first) }];
    const realm = decodeSegment(params.realm ?? parts[1]);
    const area = decodeSegment(params.area ?? parts[2]);
    const resource = decodeSegment(params.resource ?? parts[3]);
    const operation = decodeSegment(params.operation ?? parts[4]);

    if (realm) {
      crumbs.push({ href: domainScopeHref(first, { realm }), label: realm });
    }
    if (area) {
      crumbs.push({ href: domainScopeHref(first, { area, realm: realm ?? undefined }), label: area });
    }
    if (resource) {
      crumbs.push({
        href: domainScopeHref(first, {
          area: area ?? undefined,
          realm: realm ?? undefined,
          resource,
        }),
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
  const crumbs = [
    { href: route.path, label: operator.selectedRouteFamily.label },
    ...routeCrumbs(route.path, route.params),
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
                  <BreadcrumbCurrent>{crumb.label}</BreadcrumbCurrent>
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
