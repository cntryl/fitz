import { Link } from "@askrjs/askr/router";
import { ShieldIcon, MoonIcon, SunIcon } from "@askrjs/lucide";
import { ThemeToggle } from "@askrjs/themes/theme";
import RootLayout from "../_layout";

export default function PublicLayout({ children }: { children?: unknown }) {
  return (
    <RootLayout>
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
    </RootLayout>
  );
}
