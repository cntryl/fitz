import { Link } from "@askrjs/askr/router";
import DomainIndex from "@/components/shared/domain-index";
import { domainLinks } from "@/shared/navigation/domains";

export default function Home() {
  return (
    <section class="home-page">
      <div class="hero">
        <hgroup>
          <h1>Fitz Admin Console</h1>
          <p>Operate Fitz with a lightweight admin UI built on Askr and typed end-to-end APIs.</p>
        </hgroup>
        <div class="hero-buttons">
          <Link href="/admin" class="contrast">
            Open Admin
          </Link>
          <Link href="/login" class="secondary">
            Sign In
          </Link>
        </div>
      </div>

      <DomainIndex
        title="Browse the admin domains"
        description="Start with a read-only overview and expand each domain as the UI grows."
        links={domainLinks}
      />
    </section>
  );
}
