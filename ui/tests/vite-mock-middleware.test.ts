// @vitest-environment node
import { once } from "node:events";
import { createServer } from "node:http";
import { afterAll, beforeAll, describe, expect, it } from "vite-plus/test";
import { fitzMockApiPlugin } from "../dev/mock-api";

describe("Vite mock API middleware", () => {
  const server = createServer();
  let origin: string;

  beforeAll(async () => {
    fitzMockApiPlugin().configureServer({
      config: { logger: { info() {} } },
      middlewares: {
        use(handle) {
          server.on("request", (request, response) => {
            handle(request, response, () => {
              response.statusCode = 404;
              response.end("downstream middleware");
            });
          });
        },
      },
    });
    const listening = once(server, "listening");
    server.listen(0, "127.0.0.1");
    await listening;
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("Expected an HTTP listening port");
    origin = `http://127.0.0.1:${address.port}`;
  });

  afterAll(async () => {
    await new Promise<void>((resolve, reject) => {
      server.close((error) => (error ? reject(error) : resolve()));
    });
  });

  it("parses the session POST body and returns the mock response", async () => {
    const response = await fetch(`${origin}/api/v1/session?from=test`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ password: "pwd123", username: "root" }),
    });

    expect(response.status).toBe(204);
    expect(await response.text()).toBe("");
  });

  it("returns the mock JSON error response for an invalid session body", async () => {
    const response = await fetch(`${origin}/api/v1/session`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: "{",
    });

    expect(response.status).toBe(401);
    expect(response.headers.get("content-type")).toContain("application/json");
    expect(await response.json()).toEqual({ error: "Invalid username or password" });
  });

  it("forwards requests outside the mock API to the next handler", async () => {
    const response = await fetch(`${origin}/admin`);

    expect(response.status).toBe(404);
    expect(await response.text()).toBe("downstream middleware");
  });
});
