import { state } from "@askrjs/askr";

export default function Counter() {
  const count = state(0);

  return (
    <section>
      <p>Current count: {count()}</p>
      <button type="button" onClick={() => count.set(count() + 1)}>
        Increment
      </button>
    </section>
  );
}
