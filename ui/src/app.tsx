import "./styles.css";
import { For } from "@askrjs/askr";
import { Link } from "@askrjs/askr/router";
import { ActivityIcon, ShieldIcon } from "@askrjs/lucide";
import { domainLinks } from "@/shared/navigation/domains";

const shellLinks = [
  { href: "/admin", label: "Dashboard" },
  { href: "/login", label: "Sign in" },
  ...domainLinks.map((link) => ({
    href: link.href,
    label: link.title,
  })),
];

export default function App({ children }: { children?: unknown }) {
  return (
    <div class="shell">
      <header class="shell-header">
        <div>
          <div class="shell-bar">
            <Link href="/" class="brand-mark" aria-label="Fitz admin home">
              <span class="brand-icon">
                <ShieldIcon size={18} />
              </span>
              <span>
                <strong>Fitz Admin</strong>
              </span>
            </Link>

            <nav class="shell-nav" aria-label="Primary">
              <For each={shellLinks} by={(link) => link.href}>
                {(link) => (
                  <Link href={link.href} class="shell-nav-link">
                    {link.label}
                  </Link>
                )}
              </For>
            </nav>
          </div>
        </div>
      </header>

      <main class="shell-main">
        <div>
          <div class="shell-banner">
            <span class="shell-badge">Read-only scaffold</span>
            <span class="shell-banner-copy">
              <ActivityIcon size={16} />
              Root-mounted admin UI wired to the live Fitz admin API.
            </span>
          </div>
          {children}
        </div>
      </main>
    </div>
  );
}
