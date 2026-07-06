import { mockFitzResponse } from "./mock-api/responses";

export { mockFitzResponse };

export function fitzMockApiPlugin() {
  return {
    name: "fitz-mock-api",
    configureServer(server: {
      config: { logger: { info(message: string): void } };
      middlewares: {
        use(
          handler: (
            request: { method?: string; url?: string },
            response: {
              end(body?: string): void;
              setHeader(name: string, value: string): void;
              statusCode: number;
            },
            next: () => void,
          ) => void,
        ): void;
      };
    }) {
      server.config.logger.info("Fitz Vite mock API enabled");
      server.middlewares.use((request, response, next) => {
        const mock = mockFitzResponse(request.method, request.url);
        if (!mock) {
          next();
          return;
        }

        response.statusCode = mock.status;
        for (const [name, value] of Object.entries(mock.headers)) {
          response.setHeader(name, value);
        }
        response.end(mock.body);
      });
    },
  };
}
