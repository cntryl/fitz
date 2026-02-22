/**
 * Stream session implementation
 * Per fitz-go/internal/domains/stream/stream.go
 */

import { Connection } from "../../client/connection";
import { StreamCodec } from "./codec";
import { StreamSession, StreamStatus } from "./types";
import { StreamError } from "../../core/errors";
import {
  MSG_STREAM_APPEND,
  MSG_STREAM_COMMIT,
  MSG_STREAM_ROLLBACK,
} from "../../frame/types";

export class StreamSessionImpl implements StreamSession {
  private connection: Connection;
  private sessionId: bigint;
  private closed: boolean = false;

  constructor(connection: Connection, _route: string, sessionId: bigint) {
    this.connection = connection;
    this.sessionId = sessionId;
  }

  /**
   * Send a record to the stream
   * Returns the assigned offset
   */
  async send(body: Uint8Array): Promise<bigint> {
    this.ensureOpen();

    const payload = StreamCodec.encodeAppend(this.sessionId, body);
    const response = await this.connection.request(MSG_STREAM_APPEND, payload);
    const decoded = StreamCodec.decodeAppendResponse(response);

    this.checkStatus(decoded.status, "SEND");

    if (decoded.offset === undefined) {
      throw new StreamError("SEND response missing offset", "MISSING_OFFSET");
    }

    return decoded.offset;
  }

  /**
   * Commit the write session and make appends durable
   */
  async commit(): Promise<void> {
    this.ensureOpen();
    this.closed = true;

    const payload = StreamCodec.encodeCommit(this.sessionId);
    const response = await this.connection.request(MSG_STREAM_COMMIT, payload);
    const decoded = StreamCodec.decodeCommitResponse(response);

    this.checkStatus(decoded.status, "COMMIT");
  }

  /**
   * Rollback and discard uncommitted appends
   */
  async rollback(): Promise<void> {
    if (this.closed) {
      return; // Already closed
    }
    this.closed = true;

    const payload = StreamCodec.encodeRollback(this.sessionId);
    try {
      const response = await this.connection.request(
        MSG_STREAM_ROLLBACK,
        payload,
      );
      const decoded = StreamCodec.decodeRollbackResponse(response);
      this.checkStatus(decoded.status, "ROLLBACK");
    } catch (err) {
      // Ignore rollback errors
      console.warn("Stream rollback error:", err);
    }
  }

  /**
   * Check if session is still open
   */
  isOpen(): boolean {
    return !this.closed;
  }

  /**
   * Get the session ID
   */
  getSessionId(): bigint {
    return this.sessionId;
  }

  private ensureOpen(): void {
    if (this.closed) {
      throw new StreamError("Stream session already closed", "SESSION_CLOSED");
    }
  }

  private checkStatus(status: number, operation: string): void {
    if (status === StreamStatus.Ok) {
      return;
    }

    const statusNames: Record<number, string> = {
      [StreamStatus.StreamNotFound]: "StreamNotFound",
      [StreamStatus.OffsetOutOfRange]: "OffsetOutOfRange",
      [StreamStatus.InvalidOffset]: "InvalidOffset",
      [StreamStatus.StreamFull]: "StreamFull",
      [StreamStatus.SessionNotFound]: "SessionNotFound",
      [StreamStatus.SessionClosed]: "SessionClosed",
      [StreamStatus.ExpectedOffsetMismatch]: "ExpectedOffsetMismatch",
    };

    const statusName = statusNames[status] || `Unknown(${status})`;
    throw new StreamError(
      `${operation} failed: ${statusName}`,
      statusName,
      status,
    );
  }
}
