/**
 * WebSocket transport implementation for Node.js and browser
 */

import { Transport, TransportOptions } from "./types";
import { TransportError, TimeoutError } from "../core/errors";

// Detect environment and set up WebSocket
let WS: any;

const isNodeEnv = (): boolean => {
  try {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return (
      typeof (globalThis as any).process !== "undefined" &&
      (globalThis as any).process.versions &&
      (globalThis as any).process.versions.node
    );
  } catch {
    return false;
  }
};

if (isNodeEnv()) {
  // Node.js: use ws package
  try {
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    WS = require("ws");
  } catch {
    throw new Error(
      "ws package is required for Node.js. Install with: npm install ws",
    );
  }
} else {
  // Browser: use native WebSocket
  WS = (globalThis as any).WebSocket;
}

export class WebSocketTransport implements Transport {
  private ws: any;
  private url: string;
  private connected: boolean = false;
  private receiveQueue: Uint8Array[] = [];
  private receiverResolve: ((data: Uint8Array) => void) | null = null;
  private timeout: number;

  constructor(url: string, options: TransportOptions = {}) {
    this.url = url;
    this.timeout = options.timeout ?? 30000;
  }

  async connect(): Promise<void> {
    return new Promise((resolve, reject) => {
      try {
        // Ensure URL has proper protocol
        let wsUrl = this.url;
        if (!wsUrl.startsWith("ws://") && !wsUrl.startsWith("wss://")) {
          if (wsUrl.startsWith("https://")) {
            wsUrl = wsUrl.replace("https://", "wss://");
          } else if (wsUrl.startsWith("http://")) {
            wsUrl = wsUrl.replace("http://", "ws://");
          } else {
            wsUrl = `ws://${wsUrl}`;
          }
        }

        this.ws = new WS(wsUrl);

        if (isNodeEnv()) {
          // Node.js WebSocket setup
          this.ws.binaryType = "arraybuffer";
        } else {
          // Browser WebSocket setup
          this.ws.binaryType = "arraybuffer";
        }

        const connectTimeout = setTimeout(() => {
          this.ws?.close?.();
          reject(
            new TimeoutError(
              `WebSocket connection timeout after ${this.timeout}ms`,
            ),
          );
        }, this.timeout);

        this.ws.onopen = () => {
          clearTimeout(connectTimeout);
          this.connected = true;
          resolve();
        };

        this.ws.onmessage = (event: any) => {
          let data: Uint8Array;
          if (event.data instanceof ArrayBuffer) {
            data = new Uint8Array(event.data);
          } else if (event.data instanceof Uint8Array) {
            data = event.data;
          } else {
            // Browser might send Blob
            if (event.data instanceof Blob) {
              event.data.arrayBuffer().then((ab: ArrayBuffer) => {
                this.enqueueMessage(new Uint8Array(ab));
              });
              return;
            }
            console.warn(
              "Unexpected WebSocket message type:",
              typeof event.data,
            );
            return;
          }
          this.enqueueMessage(data);
        };

        this.ws.onerror = (event: any) => {
          clearTimeout(connectTimeout);
          const error = new TransportError(
            `WebSocket error: ${event.message || "unknown error"}`,
          );
          reject(error);
        };

        this.ws.onclose = () => {
          this.connected = false;
          if (this.receiverResolve) {
            this.receiverResolve(new Uint8Array(0)); // Signal EOF
          }
        };
      } catch (err) {
        reject(
          new TransportError(
            `Failed to create WebSocket: ${err instanceof Error ? err.message : String(err)}`,
          ),
        );
      }
    });
  }

  private enqueueMessage(data: Uint8Array) {
    if (this.receiverResolve) {
      this.receiverResolve(data);
      this.receiverResolve = null;
    } else {
      this.receiveQueue.push(data);
    }
  }

  async send(data: Uint8Array): Promise<void> {
    if (!this.connected) {
      throw new TransportError("WebSocket is not connected");
    }

    return new Promise((resolve, reject) => {
      try {
        const timeout = setTimeout(() => {
          reject(
            new TimeoutError(`WebSocket send timeout after ${this.timeout}ms`),
          );
        }, this.timeout);

        this.ws.send(data, (err: any) => {
          clearTimeout(timeout);
          if (err) {
            reject(new TransportError(`WebSocket send failed: ${err.message}`));
          } else {
            resolve();
          }
        });
      } catch (err) {
        reject(
          new TransportError(
            `WebSocket send error: ${err instanceof Error ? err.message : String(err)}`,
          ),
        );
      }
    });
  }

  async receive(): Promise<Uint8Array> {
    // Return queued message if available
    if (this.receiveQueue.length > 0) {
      return this.receiveQueue.shift()!;
    }

    // Create new receiver promise
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.receiverResolve = null;
        reject(
          new TimeoutError(`WebSocket receive timeout after ${this.timeout}ms`),
        );
      }, this.timeout);

      this.receiverResolve = (data: Uint8Array) => {
        clearTimeout(timeout);
        this.receiverResolve = null;
        resolve(data);
      };
    });
  }

  async close(): Promise<void> {
    if (this.ws) {
      return new Promise<void>((resolve) => {
        const timeout = setTimeout(() => {
          this.ws?.terminate?.(); // For Node.js ws
          this.connected = false;
          resolve();
        }, 5000);

        this.ws.onclose = () => {
          clearTimeout(timeout);
          this.connected = false;
          resolve();
        };

        this.ws.close(1000, "Normal closure");
      });
    }
  }

  getUrl(): string {
    return this.url;
  }

  isConnected(): boolean {
    return this.connected;
  }
}
