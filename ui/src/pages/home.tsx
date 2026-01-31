import { Link } from "@askrjs/askr/router";

export default function Home() {
  return (
    <section class="hero">
      <hgroup>
        <h1>Minimal Framework for Modern SPAs</h1>
        <p>
          A minimalist and lightweight starter kit that prioritizes reactive
          simplicity, making every component elegant and performant by default.
        </p>
      </hgroup>
      <div class="hero-buttons">
        <Link href="/about" role="button">
          Learn More
        </Link>
        <Link href="/example" role="button" class="secondary">
          View Example
        </Link>
      </div>
      <div class="hero-stats">
        <div>
          <strong>Fine-grained</strong>
          <small>Reactive primitives</small>
        </div>
        <div>
          <strong>Zero-config</strong>
          <small>Vite powered</small>
        </div>
        <div>
          <strong>TypeScript</strong>
          <small>First-class support</small>
        </div>
      </div>
    </section>
  );
}
