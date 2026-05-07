import { Link } from "@askrjs/askr/router";
import { ShieldIcon, MoonIcon, SunIcon } from "@askrjs/lucide";
import { ThemeProvider, ThemeToggle } from "@askrjs/themes/components";

export default function GuestLayout({ children }: { children?: unknown }) {
  return (
    <ThemeProvider defaultTheme="system" storageKey="fitz-admin-theme">
      <div class="guest-shell">
        <header class="guest-shell-topbar">
          <div class="guest-shell-topbar-inner">
            <Link href="/" class="guest-shell-brand" aria-label="Fitz guest home">
              <span class="guest-shell-brand-icon">
                <ShieldIcon size={18} />
              </span>
              <span class="guest-shell-brand-copy">
                <strong>Fitz Admin</strong>
                <span>Guest access</span>
              </span>
            </Link>

            <ThemeToggle
              class="guest-shell-theme-toggle-button"
              aria-label="Toggle color theme"
              lightIcon={<SunIcon size={16} />}
              darkIcon={<MoonIcon size={16} />}
            />
          </div>
        </header>

        <main class="guest-shell-main">
          <div class="guest-shell-main-inner">{children}</div>
        </main>
      </div>
    </ThemeProvider>
  );
}
