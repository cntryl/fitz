import "../styles.css";
import { ThemeProvider } from "@askrjs/themes/components";

export default function RootLayout({ children }: { children?: unknown }) {
  return (
    <ThemeProvider defaultTheme="system" storageKey="fitz-admin-theme">
      {children}
    </ThemeProvider>
  );
}