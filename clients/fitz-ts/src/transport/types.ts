/**
 * Transport abstraction for WebSocket and TCP
 */

export interface Transport {
  connect(): Promise<void>;
  send(data: Uint8Array): Promise<void>;
  receive(): Promise<Uint8Array>;
  close(): Promise<void>;
  getUrl(): string;
  isConnected(): boolean;
}

export type TransportConstructor = new (
  url: string,
  options?: TransportOptions,
) => Transport;

export interface TransportOptions {
  timeout?: number;
  retryAttempts?: number;
  retryDelayMs?: number;
}
