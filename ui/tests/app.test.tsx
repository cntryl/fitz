import { describe, it, expect } from "vitest";

describe("App Component", () => {
  it("renders with JSX", () => {
    const app = (
      <div>
        <header>
          <nav class="container">
            <ul>
              <li>
                <strong>Askr</strong>
              </li>
            </ul>
          </nav>
        </header>
      </div>
    );

    expect(app).toBeDefined();
  });
});
