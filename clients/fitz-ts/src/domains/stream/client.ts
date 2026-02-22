/**
 * Stream domain client for append-only log operations
 * Per fitz-go/internal/domains/stream/stream.go
 *
 * Stream uses session-based transactional semantics:
 * 1. Begin() starts a write session with expectedOffset (OCC)
 * 2. Send() on session adds records
 * 3. Commit() or Rollback() finalizes the session
 */

import { DomainClient } from "../base";
import { StreamCodec } from "./codec";
import {
  StreamSession,
  StreamRecord,
  StreamMetadata,
  StreamStatus,
} from "./types";
import { StreamSessionImpl } from "./session";
import { StreamError } from "../../core/errors";
import { SliceIterator, AsyncIterableIterator } from "../../core/iterator";
import {
  MSG_STREAM_BEGIN,
  MSG_STREAM_READ,
  MSG_STREAM_LAST,
  MSG_STREAM_GET_METADATA,
} from "../../frame/types";

export class StreamClient extends DomainClient {
  /**
   * Begin a write session on the stream
   * @param route Stream route (e.g., "stream://realm/area/events")
   * @param expectedOffset Client's view of the next offset (OCC - optimistic concurrency control)
   * @returns StreamSession for send/commit/rollback
   */
  async begin(route: string, expectedOffset: bigint): Promise<StreamSession> {
    const payload = StreamCodec.encodeBegin(route, expectedOffset);
    const response = await this.request(MSG_STREAM_BEGIN, payload);
    const decoded = StreamCodec.decodeBeginResponse(response);

    this.checkStatus(decoded.status, "BEGIN");

    if (decoded.sessionId === undefined) {
      throw new StreamError(
        "BEGIN response missing sessionId",
        "MISSING_SESSION_ID",
      );
    }

    return new StreamSessionImpl(this.connection, route, decoded.sessionId);
  }

  /**
   * Read records from the stream
   * @param route Stream route
   * @param startOffset Offset to start reading from (0 for beginning)
   * @param limit Maximum number of records to read (default: 100)
   * @returns Array of stream records
   */
  async read(
    route: string,
    startOffset: bigint,
    limit: number = 100,
  ): Promise<StreamRecord[]> {
    const payload = StreamCodec.encodeRead(route, startOffset, limit);
    const response = await this.request(MSG_STREAM_READ, payload);
    const decoded = StreamCodec.decodeReadResponse(response);

    this.checkStatus(decoded.status, "READ");

    return decoded.records;
  }

  /**
   * Consume records from the stream as an async iterator
   * @param route Stream route
   * @param startOffset Offset to start reading from (0 for beginning)
   * @param limit Maximum number of records to read (default: 100)
   * @returns AsyncIterable of stream records
   */
  async consume(
    route: string,
    startOffset: bigint,
    limit: number = 100,
  ): Promise<AsyncIterable<StreamRecord>> {
    const records = await this.read(route, startOffset, limit);
    const iterator = new SliceIterator(records);
    return new AsyncIterableIterator(iterator);
  }

  /**
   * Get the last record in the stream
   * @param route Stream route
   * @returns The most recent record, or null if stream is empty
   */
  async last(route: string): Promise<StreamRecord | null> {
    const payload = StreamCodec.encodeLast(route);
    const response = await this.request(MSG_STREAM_LAST, payload);
    const decoded = StreamCodec.decodeLastResponse(response);

    this.checkStatus(decoded.status, "LAST");

    return decoded.record ?? null;
  }

  /**
   * Get stream metadata
   * @param route Stream route
   * @returns Stream metadata (offsets and record count)
   */
  async getMetadata(route: string): Promise<StreamMetadata> {
    const payload = StreamCodec.encodeGetMetadata(route);
    const response = await this.request(MSG_STREAM_GET_METADATA, payload);
    const decoded = StreamCodec.decodeGetMetadataResponse(response);

    this.checkStatus(decoded.status, "GET_METADATA");

    if (decoded.metadata === undefined) {
      throw new StreamError(
        "GET_METADATA response missing metadata",
        "MISSING_METADATA",
      );
    }

    return decoded.metadata;
  }

  /**
   * Check status and throw error if not OK
   */
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
