import { Link } from "@askrjs/askr/router";

export default function Home() {
  return (
    <section class="hero">
      <hgroup>
        <h1>Fitz Admin Console</h1>
        <p>Operate Fitz with a lightweight admin UI built on Askr and typed end-to-end APIs.</p>
      </hgroup>
      <div class="hero-buttons">
        <Link href="/admin" class="contrast">
          Open Admin
        </Link>
        <Link href="/admin/login" class="secondary">
          Sign In
        </Link>
      </div>
    </section>
  );
}
