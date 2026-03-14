import "./styles.css";
import { Link } from "@askrjs/askr/router";

export default function App({ children }: { children?: unknown }) {
  return (
    <div class="shell">
      <header class="shell-header">
        <nav class="container">
          <ul>
            <li>
              <Link href="/admin">
                <strong>Fitz Admin</strong>
              </Link>
            </li>
          </ul>
          <ul>
            <li>
              <Link href="/admin/login">Login</Link>
            </li>
          </ul>
        </nav>
      </header>
      <main class="container shell-main">{children}</main>
    </div>
  );
}
