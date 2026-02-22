/**
 * KV Transaction wrapper
 */

import { Connection } from "../../client/connection";
import { KvCodec } from "./codec";
import { KvStatus } from "./types";
import {
  MSG_KV_PUT,
  MSG_KV_GET,
  MSG_KV_DELETE,
  MSG_KV_COMMIT,
  MSG_KV_ROLLBACK,
  MSG_KV_SCAN,
} from "../../frame/types";
import { KvError } from "../../core/errors";
import { SliceIterator, AsyncIterableIterator } from "../../core/iterator";

export class KvTransaction {
  private connection: Connection;
  private route: string;
  private txId: bigint;
  private closed: boolean = false;

  constructor(connection: Connection, route: string, txId: bigint) {
    this.connection = connection;
    this.route = route;
    this.txId = txId;
  }

  /**
   * Put a key-value pair
   */
  async put(key: Uint8Array, value: Uint8Array): Promise<void> {
    this.ensureOpen();
    const payload = KvCodec.encodePut(this.txId, this.route, key, value);
    const response = await this.connection.request(MSG_KV_PUT, payload);
    const decoded = KvCodec.decodePutResponse(response);
    this.checkStatus(decoded.status);
  }

  /**
   * Get a value by key
   */
  async get(key: Uint8Array): Promise<Uint8Array | null> {
    this.ensureOpen();
    const payload = KvCodec.encodeGet(this.txId, this.route, key);
    const response = await this.connection.request(MSG_KV_GET, payload);
    const decoded = KvCodec.decodeGetResponse(response);
    this.checkStatus(decoded.status);
    return decoded.value ?? null;
  }

  /**
   * Delete a key
   */
  async delete(key: Uint8Array): Promise<void> {
    this.ensureOpen();
    const payload = KvCodec.encodeDelete(this.txId, this.route, key);
    const response = await this.connection.request(MSG_KV_DELETE, payload);
    const decoded = KvCodec.decodeDeleteResponse(response);
    this.checkStatus(decoded.status);
  }

  /**
   * Scan keys as an async iterator
   * Returns an AsyncIterable that can be used with for-await-of loops
   */
  async scan(cursor?: Uint8Array): Promise<AsyncIterable<Uint8Array>> {
    this.ensureOpen();
    const payload = KvCodec.encodeScan(this.txId, this.route, cursor);
    const response = await this.connection.request(MSG_KV_SCAN, payload);
    const decoded = KvCodec.decodeScanResponse(response);
    this.checkStatus(decoded.status);

    const keys = decoded.keys ?? [];
    const iterator = new SliceIterator(keys);
    return new AsyncIterableIterator(iterator);
  }

  /**
   * Commit the transaction
   */
  async commit(): Promise<void> {
    this.ensureOpen();
    this.closed = true;
    const payload = KvCodec.encodeCommit(this.txId, this.route);
    const response = await this.connection.request(MSG_KV_COMMIT, payload);
    const decoded = KvCodec.decodeCommitResponse(response);
    this.checkStatus(decoded.status);
  }

  /**
   * Rollback the transaction
   */
  async rollback(): Promise<void> {
    if (this.closed) {
      return; // Already closed
    }
    this.closed = true;
    const payload = KvCodec.encodeRollback(this.txId, this.route);
    try {
      const response = await this.connection.request(MSG_KV_ROLLBACK, payload);
      const decoded = KvCodec.decodeRollbackResponse(response);
      this.checkStatus(decoded.status);
    } catch (err) {
      // Ignore rollback errors
      console.warn("Rollback error:", err);
    }
  }

  /**
   * Get transaction ID
   */
  getTxId(): bigint {
    return this.txId;
  }

  /**
   * Check if transaction is open
   */
  isOpen(): boolean {
    return !this.closed;
  }

  private ensureOpen(): void {
    if (this.closed) {
      throw new KvError("Transaction already closed", "TX_CLOSED");
    }
  }

  private checkStatus(status: number): void {
    if (status === KvStatus.Ok) {
      return;
    }

    const statusNames: Record<number, string> = {
      [KvStatus.TransactionAborted]: "TransactionAborted",
      [KvStatus.LeaseExpired]: "LeaseExpired",
      [KvStatus.ConflictingWrite]: "ConflictingWrite",
      [KvStatus.KeyNotFound]: "KeyNotFound",
      [KvStatus.OperationNotAllowed]: "OperationNotAllowed",
    };

    const statusName = statusNames[status] || `Unknown(${status})`;
    throw new KvError(`KV operation failed: ${statusName}`, statusName, status);
  }
}
