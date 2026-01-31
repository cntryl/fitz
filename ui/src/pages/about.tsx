export default function About() {
  return (
    <>
      <hgroup>
        <h1>About Askr</h1>
        <p>
          A modern reactive framework for building fast, maintainable web
          applications
        </p>
      </hgroup>

      <h2>Why Askr Exists</h2>
      <p>
        Modern web development has become unnecessarily complex. Frameworks have
        grown bloated with abstractions, requiring developers to learn extensive
        APIs and fight against the framework rather than work with it.
      </p>
      <p>
        Askr was created to bring simplicity back to web development. It
        provides just enough structure to build sophisticated applications while
        staying out of your way. No virtual DOM, no complex state management
        libraries, no excessive re-renders—just clean, efficient reactivity.
      </p>

      <h2>What Askr Does</h2>
      <div class="grid">
        <div>
          <h3>Fine-grained Reactivity</h3>
          <p>
            Askr uses <code>state()</code> for reactive values with automatic
            dependency tracking. Updates are surgical—only the specific DOM
            nodes that need to change are updated, nothing more.
          </p>
        </div>
        <div>
          <h3>Simple Async Data</h3>
          <p>
            The <code>resource()</code> primitive handles async data fetching
            with built-in loading states, error handling, and automatic
            refetching when dependencies change.
          </p>
        </div>
        <div>
          <h3>Declarative Routing</h3>
          <p>
            Routes are declared at module-load time with <code>route()</code>{" "}
            and composed with <code>layout()</code>. Clean, type-safe, and no
            configuration needed.
          </p>
        </div>
      </div>

      <h2>Built for Modern Development</h2>
      <p>
        Askr embraces modern tools and standards. It's built with TypeScript for
        excellent type safety, powered by Vite for lightning-fast development,
        and uses standard JSX/TSX syntax that feels familiar.
      </p>
      <p>
        Whether you're building a simple dashboard or a complex single-page
        application, Askr provides the perfect foundation without the bloat.
      </p>
    </>
  );
}
