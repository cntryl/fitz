/**
 * Common type definitions for Fitz client
 */

export type TransportType = "ws" | "tcp" | "auto";

export interface ClientConfig {
  url: string;
  jwt: string;
  timeout?: number;
  transport?: TransportType;
  retryAttempts?: number;
  retryDelayMs?: number;
}

export interface Route {
  scheme: string;
  realm: string;
  area: string;
  resource: string;
  operation?: string;
}

export type TxMode = "ReadOnly" | "ReadWrite";
export type DurabilityMode = "None" | "Async" | "Sync";

export interface WriteOptions {
  durability: DurabilityMode;
  buffered: boolean;
}

export const DefaultWriteOptions: WriteOptions = {
  durability: "Async",
  buffered: true,
};

/**
 * Deferred is a Promise wrapper that exposes resolve/reject
 */
export class Deferred<T = any> {
  promise: Promise<T>;
  resolve!: (value: T | PromiseLike<T>) => void;
  reject!: (reason?: any) => void;

  constructor() {
    this.promise = new Promise((resolve, reject) => {
      this.resolve = resolve;
      this.reject = reject;
    });
  }
}

/**
 * Connection state machine
 */
export enum ConnectionState {
  Disconnected = "DISCONNECTED",
  Connecting = "CONNECTING",
  Authenticating = "AUTHENTICATING",
  Authenticated = "AUTHENTICATED",
  Closed = "CLOSED",
}
