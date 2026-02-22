/**
 * Multiplexer for correlating in-flight requests with responses
 * Per CLIENT_SPEC.md and fitz-go/internal/core/connection/mux.go
 *
 * Key Pattern: FIFO ordering per MessageType
 * - Responses matched to requests in order received per MessageType
 * - Matches server's sequential processing model per actor/route
 * - No correlation IDs for most operations (except RPC streaming)
 */

import { Deferred, ConnectionState } from "../core/types";
import { TimeoutError } from "../core/errors";
import { NOTIFICATION_TYPES } from "../frame/types";

export interface PendingRequest {
  deferred: Deferred<Uint8Array>;
  timeout: ReturnType<typeof setTimeout>;
  sentAt: Date;
}

/**
 * Notification handler signature
 * @param payload Raw notification payload
 */
export type NotificationHandler = (payload: Uint8Array) => void;

/**
 * RPC correlation handler for streamed responses
 * @param correlationId 16-byte correlation ID
 * @param payload Response payload
 */
export type RpcCorrelationHandler = (
  correlationId: Uint8Array,
  payload: Uint8Array,
) => void;

export class Multiplexer {
  // FIFO queue of pending requests per MessageType
  private pending: Map<number, PendingRequest[]> = new Map();

  // Notification handlers for push messages (209, 409, 504, 609, 705)
  private notificationHandlers: Map<number, NotificationHandler> = new Map();

  // RPC correlation handler for streaming responses (future use)
  // private rpcCorrelationHandler?: RpcCorrelationHandler;

  private state: ConnectionState = ConnectionState.Disconnected;

  // Metrics
  private requestsInFlight = 0;
  private requestsTotal = 0;
  private responsesTotal = 0;
  private responsesDropped = 0;

  setConnected(): void {
    this.state = ConnectionState.Authenticated;
  }

  setDisconnected(): void {
    this.state = ConnectionState.Disconnected;
    this.cancelAll();
  }

  /**
   * Register notification handler for server push messages
   * @param messageType Notification message type (e.g., 209, 504, 609, 705)
   * @param handler Handler function to call when notification arrives
   */
  registerNotificationHandler(
    messageType: number,
    handler: NotificationHandler,
  ): void {
    if (!NOTIFICATION_TYPES.has(messageType)) {
      throw new Error(`Invalid notification type: ${messageType}`);
    }
    this.notificationHandlers.set(messageType, handler);
  }

  /**
   * Unregister notification handler
   */
  unregisterNotificationHandler(messageType: number): void {
    this.notificationHandlers.delete(messageType);
  }

  /**
   * Send a request and wait for the response (FIFO matching)
   */
  async request(
    messageType: number,
    frameData: Uint8Array,
    send: (data: Uint8Array) => Promise<void>,
    timeoutMs: number,
  ): Promise<Uint8Array> {
    const deferred = new Deferred<Uint8Array>();

    const timeout = setTimeout(() => {
      this.unregisterRequest(messageType, deferred);
      deferred.reject(
        new TimeoutError(
          `Request timeout for message type ${messageType} after ${timeoutMs}ms`,
        ),
      );
    }, timeoutMs);

    const request: PendingRequest = {
      deferred,
      timeout,
      sentAt: new Date(),
    };

    // Add to FIFO queue for this MessageType
    if (!this.pending.has(messageType)) {
      this.pending.set(messageType, []);
    }
    this.pending.get(messageType)!.push(request);

    this.requestsInFlight++;
    this.requestsTotal++;

    try {
      await send(frameData);
      return await deferred.promise;
    } catch (err) {
      clearTimeout(timeout);
      this.unregisterRequest(messageType, deferred);
      throw err;
    }
  }

  /**
   * Unregister a pending request (on cancel/timeout)
   */
  private unregisterRequest(
    messageType: number,
    deferred: Deferred<Uint8Array>,
  ): void {
    const queue = this.pending.get(messageType);
    if (!queue) return;

    const index = queue.findIndex((r) => r.deferred === deferred);
    if (index >= 0) {
      queue.splice(index, 1);
      this.requestsInFlight--;
      if (queue.length === 0) {
        this.pending.delete(messageType);
      }
    }
  }

  /**
   * Dispatch incoming frame to appropriate handler
   */
  dispatch(messageType: number, payload: Uint8Array): void {
    // Handle notification push messages
    if (NOTIFICATION_TYPES.has(messageType)) {
      const handler = this.notificationHandlers.get(messageType);
      if (handler) {
        try {
          handler(payload);
        } catch (err) {
          console.error(
            `Notification handler error for type ${messageType}:`,
            err,
          );
        }
      } else {
        this.responsesDropped++;
        console.warn(
          `No handler registered for notification type ${messageType}`,
        );
      }
      return;
    }

    // Handle synchronous request/response - FIFO matching
    const queue = this.pending.get(messageType);
    if (!queue || queue.length === 0) {
      this.responsesDropped++;
      console.warn(
        `No pending request for message type ${messageType}, dropping response`,
      );
      return;
    }

    // Match to oldest (FIFO) pending request
    const request = queue.shift()!;
    if (queue.length === 0) {
      this.pending.delete(messageType);
    }

    clearTimeout(request.timeout);
    this.requestsInFlight--;
    this.responsesTotal++;

    request.deferred.resolve(payload);
  }

  /**
   * Cancel all in-flight requests
   */
  cancelAll(): void {
    for (const [, queue] of this.pending) {
      for (const request of queue) {
        clearTimeout(request.timeout);
        request.deferred.reject(new Error("Connection closed or reset"));
      }
    }
    this.pending.clear();
    this.requestsInFlight = 0;
  }

  /**
   * Get metrics
   */
  getMetrics() {
    return {
      requestsInFlight: this.requestsInFlight,
      requestsTotal: this.requestsTotal,
      responsesTotal: this.responsesTotal,
      responsesDropped: this.responsesDropped,
    };
  }

  /**
   * Get number of in-flight requests
   */
  getInFlightCount(): number {
    return this.requestsInFlight;
  }

  /**
   * Check if there are pending requests
   */
  hasPending(): boolean {
    return this.requestsInFlight > 0;
  }

  /**
   * Get current state
   */
  getState(): ConnectionState {
    return this.state;
  }
}
