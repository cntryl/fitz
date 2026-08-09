import { mockFitzResponse } from "./mock-api/responses.ts";

export { mockFitzResponse };

type MockRequest = {
  method?: string;
  url?: string;
  on(event: "data", listener: (chunk: unknown) => void): void;
  on(event: "end" | "error", listener: () => void): void;
};

type MockServerResponse = {
  end(body?: string): void;
  setHeader(name: string, value: string): void;
  statusCode: number;
};

function sendMockResponse(
  mock: ReturnType<typeof mockFitzResponse>,
  response: MockServerResponse,
  next: () => void,
) {
  if (!mock) {
    next();
    return;
  }

  response.statusCode = mock.status;
  for (const [name, value] of Object.entries(mock.headers)) {
    response.setHeader(name, value);
  }
  response.end(mock.body);
}

export function fitzMockApiPlugin() {
  return {
    name: "fitz-mock-api",
    configureServer(server: {
      config: { logger: { info(message: string): void } };
      middlewares: {
        use(
          handler: (request: MockRequest, response: MockServerResponse, next: () => void) => void,
        ): void;
      };
    }) {
      server.config.logger.info("Fitz Vite mock API enabled");
      server.middlewares.use((request, response, next) => {
        if (request.method === "POST" && request.url?.split("?")[0] === "/api/v1/session") {
          let rawBody = "";
          request.on("data", (chunk) => {
            rawBody += String(chunk);
          });
          request.on("end", () => {
            let requestBody: unknown;

            try {
              requestBody = JSON.parse(rawBody);
            } catch {
              requestBody = null;
            }

            sendMockResponse(
              mockFitzResponse(request.method, request.url, requestBody),
              response,
              next,
            );
          });
          request.on("error", next);
          return;
        }

        sendMockResponse(mockFitzResponse(request.method, request.url), response, next);
      });
    },
  };
}
