import "./styles.css";
import { Link } from "@askrjs/askr/router";
import { ActivityIcon, ShieldIcon } from "@askrjs/lucide";

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
              <Link href="/admin" class="shell-nav-link">
                Dashboard
              </Link>
              <Link href="/login" class="shell-nav-link">
                Sign in
              </Link>
            </nav>
          </div>
        </div>
      </header>

      <main class="shell-main">
        <div>
          <div class="shell-banner">
            <span class="shell-badge">Askr SPA Baseline</span>
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
