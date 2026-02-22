/**
 * Mock connection for testing domain clients
 */

import {
  Connection,
  NotificationHandler,
} from "../../../src/client/connection";

type RequestHandler = (msgType: number, payload: Uint8Array) => Uint8Array;

/**
 * Mock implementation of Connection for unit tests
 * Allows mocking request/response pairs and notification handlers
 */
export class MockConnection implements Connection {
  private handlers = new Map<number, RequestHandler>();
  private notificationHandlers = new Map<number, NotificationHandler>();

  /**
   * Register a handler for a specific message type
   */
  mockRequest(msgType: number, handler: RequestHandler): void {
    this.handlers.set(msgType, handler);
  }

  /**
   * Simulate receiving a notification
   */
  simulateNotification(msgType: number, payload: Uint8Array): void {
    const handler = this.notificationHandlers.get(msgType);
    if (handler) {
      handler(payload);
    }
  }

  // Connection interface implementation

  /**
   * Execute a request
   */
  async request(msgType: number, payload: Uint8Array): Promise<Uint8Array> {
    const handler = this.handlers.get(msgType);
    if (!handler) {
      throw new Error(`No mock handler for message type ${msgType}`);
    }
    return Promise.resolve(handler(msgType, payload));
  }

  /**
   * Send fire-and-forget message (no response)
   */
  async sendFireAndForget(
    _msgType: number,
    _payload: Uint8Array,
  ): Promise<void> {
    // No-op for testing
    return Promise.resolve();
  }

  /**
   * Register a notification handler
   */
  registerNotificationHandler(
    msgType: number,
    handler: NotificationHandler,
  ): void {
    this.notificationHandlers.set(msgType, handler);
  }

  /**
   * Unregister a notification handler
   */
  unregisterNotificationHandler(msgType: number): void {
    this.notificationHandlers.delete(msgType);
  }

  /**
   * Connect to broker (mock - no-op)
   */
  async connect(): Promise<void> {
    return Promise.resolve();
  }

  /**
   * Disconnect from broker (mock - no-op)
   */
  async disconnect(): Promise<void> {
    return Promise.resolve();
  }

  /**
   * Check if connected
   */
  isConnected(): boolean {
    return true;
  }

  /**
   * Clear all mocked handlers
   */
  clearHandlers(): void {
    this.handlers.clear();
    this.notificationHandlers.clear();
  }
}

/**
 * Create a success response (status = 0)
 */
export function createSuccessResponse(
  data: Uint8Array = new Uint8Array(0),
): Uint8Array {
  const response = new Uint8Array(1 + data.length);
  response[0] = 0; // success status
  response.set(data, 1);
  return response;
}

/**
 * Create an error response
 */
export function createErrorResponse(
  status: number = 1,
  errorMsg?: string,
): Uint8Array {
  if (!errorMsg) {
    return new Uint8Array([status]);
  }

  const encoder = new TextEncoder();
  const msgBytes = encoder.encode(errorMsg);
  const response = new Uint8Array(1 + 4 + msgBytes.length);
  response[0] = status;

  // [u32 len][message]
  const view = new DataView(response.buffer, 1, 4);
  view.setUint32(0, msgBytes.length, false); // big-endian
  response.set(msgBytes, 5);

  return response;
}
