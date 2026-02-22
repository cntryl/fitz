/**
 * RPC domain client
 * Per fitz-go/internal/domains/rpc/rpc.go
 */

import { DomainClient } from "../base";
import { RpcCodec } from "./codec";
import {
  SendOptions,
  ResponseFrame,
  InboundRequest,
  RpcHandler,
  RpcSubscription,
  ResponseWriter,
  RpcStatus,
} from "./types";
import {
  MSG_RPC_REQUEST,
  MSG_RPC_RESPONSE,
  MSG_RPC_SUBSCRIBE_WORKER,
  MSG_RPC_UNSUBSCRIBE_WORKER,
} from "../../frame/types";
import { RpcError } from "../../core/errors";

/**
 * ResponseWriter implementation for workers
 */
class RpcResponseWriter implements ResponseWriter {
  private sequence = 0n;

  constructor(
    private readonly connection: any, // Connection from base client
    private readonly correlationId: Uint8Array,
  ) {}

  async send(body: Uint8Array, isEnd: boolean): Promise<void> {
    const payload = RpcCodec.encodeResponse(
      this.correlationId,
      this.sequence++,
      body,
      isEnd,
    );

    await this.connection.send(MSG_RPC_RESPONSE, payload);
  }
}

/**
 * Async iterator for streaming RPC responses
 */
class RpcIterator implements AsyncIterableIterator<ResponseFrame> {
  private buffer: ResponseFrame[] = [];
  private done = false;
  private resolveNext: ((frame: ResponseFrame | null) => void) | null = null;

  constructor(
    private readonly correlationId: Uint8Array,
    private readonly client: RpcClient,
    private readonly timeoutMs: number,
  ) {}

  /**
   * Push a response frame to the iterator
   */
  push(frame: ResponseFrame): void {
    if (this.resolveNext) {
      const resolve = this.resolveNext;
      this.resolveNext = null;
      resolve(frame);
    } else {
      this.buffer.push(frame);
    }
  }

  /**
   * Mark the iterator as done (end of stream)
   */
  end(): void {
    this.done = true;
    if (this.resolveNext) {
      const resolve = this.resolveNext;
      this.resolveNext = null;
      resolve(null);
    }
  }

  async next(): Promise<IteratorResult<ResponseFrame>> {
    if (this.buffer.length > 0) {
      return { value: this.buffer.shift()!, done: false };
    }

    if (this.done) {
      return { value: undefined!, done: true };
    }

    // Wait for next frame with timeout
    const frame = await new Promise<ResponseFrame | null>((resolve, reject) => {
      this.resolveNext = resolve;

      const timer = setTimeout(() => {
        this.resolveNext = null;
        this.done = true;
        reject(new RpcError("RPC call timeout", "TIMEOUT", RpcStatus.Timeout));
      }, this.timeoutMs);

      // Clear timeout when resolved
      const originalResolve = this.resolveNext;
      this.resolveNext = (f) => {
        clearTimeout(timer);
        originalResolve(f);
      };
    });

    if (frame === null) {
      return { value: undefined!, done: true };
    }

    return { value: frame, done: false };
  }

  async return(): Promise<IteratorResult<ResponseFrame>> {
    this.done = true;
    this.client.cleanupPendingRpc(this.correlationId);
    return { value: undefined!, done: true };
  }

  [Symbol.asyncIterator](): AsyncIterableIterator<ResponseFrame> {
    return this;
  }
}

/**
 * RPC client with streaming call support and worker mode
 */
export class RpcClient extends DomainClient {
  private pendingRpcs: Map<string, RpcIterator> = new Map();
  private workers: Map<string, RpcHandler> = new Map();
  private initialized = false;

  /**
   * Send a remote procedure call, returning an async iterator over response frames
   * @param route RPC route (e.g., "rpc://realm/area/method")
   * @param body Request body
   * @param options Send options (timeout)
   * @returns AsyncIterableIterator over response frames
   */
  async send(
    route: string,
    body: Uint8Array,
    options?: SendOptions,
  ): Promise<AsyncIterableIterator<ResponseFrame>> {
    this.initRpcHandler();

    const timeoutMs = options?.timeoutMs ?? 30000;
    const correlationId = RpcCodec.generateCorrelationId();
    const correlationKey = this.correlationIdToKey(correlationId);

    // Create iterator and register it
    const iterator = new RpcIterator(correlationId, this, timeoutMs);
    this.pendingRpcs.set(correlationKey, iterator);

    try {
      // Send request
      const payload = RpcCodec.encodeRequest(correlationId, route, "", body);
      const response = await this.request(MSG_RPC_REQUEST, payload);

      // Validate ack response
      const decoded = RpcCodec.decodeRequestResponse(response);
      if (decoded.status !== RpcStatus.Ok) {
        this.pendingRpcs.delete(correlationKey);
        throw new RpcError(
          `RPC REQUEST failed: status ${decoded.status}`,
          "REQUEST_FAILED",
          decoded.status,
        );
      }

      return iterator;
    } catch (error) {
      this.pendingRpcs.delete(correlationKey);
      throw error;
    }
  }

  /**
   * Subscribe as a worker to handle incoming RPC requests
   * @param route Worker route pattern
   * @param handler Handler function to process requests
   * @returns Subscription object with unsubscribe() method
   */
  async subscribe(
    route: string,
    handler: RpcHandler,
  ): Promise<RpcSubscription> {
    this.initRpcHandler();

    const payload = RpcCodec.encodeSubscribeWorker(route);
    const response = await this.request(MSG_RPC_SUBSCRIBE_WORKER, payload);
    const decoded = RpcCodec.decodeSubscribeWorkerResponse(response);

    if (decoded.status !== RpcStatus.Ok) {
      throw new RpcError(
        `RPC SUBSCRIBE_WORKER failed: status ${decoded.status}`,
        "SUBSCRIBE_FAILED",
        decoded.status,
      );
    }

    this.workers.set(route, handler);

    const unsubscribeFn = async (r: string) => {
      await this.unsubscribe(r);
    };

    return new RpcSubscription(route, unsubscribeFn);
  }

  /**
   * Internal: unsubscribe from worker route
   */
  private async unsubscribe(route: string): Promise<void> {
    this.workers.delete(route);

    try {
      const payload = RpcCodec.encodeUnsubscribeWorker(route);
      const response = await this.request(MSG_RPC_UNSUBSCRIBE_WORKER, payload);
      const decoded = RpcCodec.decodeUnsubscribeWorkerResponse(response);

      if (decoded.status !== RpcStatus.Ok) {
        console.warn(
          `RPC UNSUBSCRIBE_WORKER warning: status ${decoded.status}`,
        );
      }
    } catch (error) {
      console.warn("RPC UNSUBSCRIBE_WORKER failed:", error);
    }
  }

  /**
   * Internal: cleanup pending RPC when iterator is closed
   */
  cleanupPendingRpc(correlationId: Uint8Array): void {
    const key = this.correlationIdToKey(correlationId);
    this.pendingRpcs.delete(key);
  }

  /**
   * Initialize RPC handlers (lazy, on first use)
   */
  private initRpcHandler(): void {
    if (this.initialized) {
      return;
    }
    this.initialized = true;

    // Register RPC_RESPONSE handler
    this.connection.registerNotificationHandler(
      MSG_RPC_RESPONSE,
      (payload: Uint8Array) => {
        try {
          const { correlationId, sequence, body, streamEnd } =
            RpcCodec.decodeResponse(payload);
          this.handleRpcResponse(correlationId, sequence, body, streamEnd);
        } catch (error) {
          console.error("RPC response decode error:", error);
        }
      },
    );

    // Register RPC_REQUEST handler (worker mode)
    this.connection.registerNotificationHandler(
      MSG_RPC_REQUEST,
      (payload: Uint8Array) => {
        try {
          const request = RpcCodec.decodeInboundRequest(payload);
          this.handleRpcRequest(request);
        } catch (error) {
          console.error("RPC request decode error:", error);
        }
      },
    );
  }

  /**
   * Handle incoming RPC_RESPONSE (303)
   */
  private handleRpcResponse(
    correlationId: Uint8Array,
    sequence: bigint,
    body: Uint8Array,
    streamEnd: boolean,
  ): void {
    const key = this.correlationIdToKey(correlationId);
    const iterator = this.pendingRpcs.get(key);

    if (!iterator) {
      console.warn(`No pending RPC for correlation ID ${key}`);
      return;
    }

    if (streamEnd) {
      // End of stream: mark iterator as done
      this.pendingRpcs.delete(key);
      iterator.end();
    } else {
      // Push response frame to iterator
      iterator.push({ body, sequence });
    }
  }

  /**
   * Handle incoming RPC_REQUEST (302) for worker mode
   */
  private handleRpcRequest(req: InboundRequest): void {
    const handler = this.workers.get(req.route);

    if (!handler) {
      console.warn(`No worker registered for route ${req.route}`);
      return;
    }

    // Create response writer
    const writer = new RpcResponseWriter(this.connection, req.correlationId);

    // Call handler asynchronously
    Promise.resolve(handler(req, writer)).catch((error) => {
      console.error(`Worker handler error for ${req.route}:`, error);
      // Send error response
      writer
        .send(new TextEncoder().encode(`Handler error: ${error.message}`), true)
        .catch((e) => console.error("Failed to send error response:", e));
    });
  }

  /**
   * Convert correlation ID to string key for Map
   */
  private correlationIdToKey(correlationId: Uint8Array): string {
    return Array.from(correlationId)
      .map((b) => b.toString(16).padStart(2, "0"))
      .join("");
  }
}

export * from "./types";
