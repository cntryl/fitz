/**
 * RPC domain types
 * Per fitz-go/internal/domains/rpc/rpc.go
 */

/**
 * Single response frame from a streaming RPC call
 */
export interface ResponseFrame {
  body: Uint8Array;
  sequence: bigint;
}

/**
 * Inbound RPC request received by a worker
 */
export interface InboundRequest {
  correlationId: Uint8Array; // 16 bytes
  route: string;
  replyRoute: string;
  body: Uint8Array;
}

/**
 * Allows a worker to send responses back to the caller
 */
export interface ResponseWriter {
  send(body: Uint8Array, isEnd: boolean): Promise<void>;
}

/**
 * Handler for incoming RPC requests (worker mode)
 */
export type RpcHandler = (
  req: InboundRequest,
  writer: ResponseWriter,
) => Promise<void>;

/**
 * Active worker registration
 */
export class RpcSubscription {
  constructor(
    public readonly route: string,
    private readonly unsubscribeFn: (route: string) => Promise<void>,
  ) {}

  async unsubscribe(): Promise<void> {
    await this.unsubscribeFn(this.route);
  }
}

/**
 * RPC send options
 */
export interface SendOptions {
  timeoutMs?: number;
}

/**
 * RPC status codes (from server responses)
 */
export enum RpcStatus {
  Ok = 0,
  Timeout = 1,
  HandlerNotFound = 2,
  HandlerError = 3,
  InvalidRequest = 4,
}

/**
 * Response to RPC_SUBSCRIBE request
 */
export interface SubscribeResponse {
  status: number;
}

/**
 * Response to RPC_UNSUBSCRIBE request
 */
export interface UnsubscribeResponse {
  status: number;
}
