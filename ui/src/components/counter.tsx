import { state } from "@askrjs/askr";

export default function Counter() {
  const count = state(0);

  return (
    <div class="hero-counter">
      <h2>Interactive Counter</h2>
      <p>
        Built with Askr's reactive <code>state()</code> primitive
      </p>
      <div class="count-display">{count()}</div>
      <div class="button-group">
        <button onClick={() => count.set((c) => Math.max(0, c - 1))}>-</button>
        <button onClick={() => count.set((c) => c + 1)}>+</button>
      </div>
    </div>
  );
}
