import { describe, expect, it } from "vitest";
import App from "../src/app";
import AdminHome from "../src/pages/admin-home";
import AdminLogin from "../src/pages/admin-login";

describe("Admin UI", () => {
  it("defines the shared admin shell", () => {
    expect(App).toBeDefined();
    expect(typeof App).toBe("function");
  });

  it("defines the admin login page", () => {
    expect(AdminLogin).toBeDefined();
    expect(typeof AdminLogin).toBe("function");
  });

  it("defines the admin home page", () => {
    expect(AdminHome).toBeDefined();
    expect(typeof AdminHome).toBe("function");
  });
});
