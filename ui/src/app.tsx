import "./styles.css";
import { Link } from "@askrjs/askr/router";
import { Badge } from "@askrjs/askr-ui/badge";
import { Container } from "@askrjs/askr-ui/container";
import { Stack } from "@askrjs/askr-ui/stack";
import { Activity, Shield } from "@askrjs/icons-lucide";

export default function App({ children }: { children?: unknown }) {
  return (
    <div class="shell">
      <header class="shell-header">
        <Container maxWidth="1120px" padding="1.25rem">
          <div class="shell-bar">
            <Link href="/" class="brand-mark" aria-label="Fitz admin home">
              <span class="brand-icon">
                <Shield size={18} />
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
        </Container>
      </header>

      <main class="shell-main">
        <Container maxWidth="1120px" padding="1.25rem">
          <Stack gap="1.5rem">
            <div class="shell-banner">
              <Badge class="shell-badge">Askr SPA Baseline</Badge>
              <span class="shell-banner-copy">
                <Activity size={16} />
                Root-mounted admin UI wired to the live Fitz admin API.
              </span>
            </div>
            {children}
          </Stack>
        </Container>
      </main>
    </div>
  );
}
