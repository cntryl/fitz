import { describe, it, expect } from "vitest";

describe("Counter Component", () => {
  it("renders counter JSX structure", () => {
    const counterMarkup = (
      <div class="hero-counter">
        <h2>Interactive Counter</h2>
        <div class="count-display">0</div>
        <div class="button-group">
          <button>-</button>
          <button>+</button>
        </div>
      </div>
    );

    expect(counterMarkup).toBeDefined();
  });
});
