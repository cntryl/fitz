import "./styles.css";
import { Link } from "@askrjs/askr/router";

export default function App({ children }: { children?: unknown }) {
  return (
    <div>
      <header>
        <nav class="container">
          <ul>
            <li>
              <Link href="/">
                <strong>test-app</strong>
              </Link>
            </li>
          </ul>
          <ul>
            <li>
              <Link href="/example">Example</Link>
            </li>
            <li>
              <Link href="/about">About</Link>
            </li>
          </ul>
        </nav>
      </header>
      <main class="container" id="app-routes">
        {children}
      </main>
    </div>
  );
}
