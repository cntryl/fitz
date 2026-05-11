import { Link } from "@askrjs/askr/router";
import { ShieldIcon, MoonIcon, SunIcon } from "@askrjs/lucide";
import { ThemeProvider, ThemeToggle } from "@askrjs/themes/components";

export default function GuestLayout({ children }: { children?: unknown }) {
  return (
    <ThemeProvider defaultTheme="system" storageKey="fitz-admin-theme">
      <div class="guest-shell">
        <div class="guest-shell-theme-toggle">
          <ThemeToggle
            aria-label="Toggle color theme"
            lightIcon={<SunIcon size={16} />}
            darkIcon={<MoonIcon size={16} />}
          />
        </div>
        <main class="guest-shell-main">
          <Link href="/" class="guest-shell-brand" aria-label="Fitz guest home">
            <ShieldIcon size={18} /> Fitz Admin
          </Link>
          {children}
        </main>
      </div>
    </ThemeProvider>
  );
}
