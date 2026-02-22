/**
 * Stream domain codec for encoding/decoding messages
 * Per CLIENT_SPEC.md and fitz-go/internal/domains/stream/protocol.go
 */

import { BufferWriter, BufferReader } from "../../core/buffer";
import { StreamRecord, StreamMetadata } from "./types";

export class StreamCodec {
  /**
   * Encode BEGIN request
   * Payload: [route: string][expected_offset: u64][options: u8]
   */
  static encodeBegin(
    route: string,
    expectedOffset: bigint,
    options: number = 0,
  ): Uint8Array {
    const writer = new BufferWriter(256);
    writer.writeRoute(route);
    writer.writeU64BE(expectedOffset);
    writer.writeU8(options);
    return writer.getBuffer();
  }

  /**
   * Decode BEGIN response
   * Payload: [status: u8][session_id: u64]
   */
  static decodeBeginResponse(payload: Uint8Array): {
    status: number;
    sessionId?: bigint;
  } {
    const reader = new BufferReader(payload);
    const status = reader.readU8();
    if (reader.isEOF()) {
      return { status };
    }
    const sessionId = reader.readU64BE();
    return { status, sessionId };
  }

  /**
   * Encode APPEND request
   * Payload: [session_id: u64][body_len: u32][body: bytes]
   */
  static encodeAppend(sessionId: bigint, body: Uint8Array): Uint8Array {
    const writer = new BufferWriter(512);
    writer.writeU64BE(sessionId);
    writer.writeU32BE(body.length);
    writer.writeBytes(body);
    return writer.getBuffer();
  }

  /**
   * Decode APPEND response
   * Payload: [status: u8][offset: u64]
   */
  static decodeAppendResponse(payload: Uint8Array): {
    status: number;
    offset?: bigint;
  } {
    const reader = new BufferReader(payload);
    const status = reader.readU8();
    if (reader.isEOF()) {
      return { status };
    }
    const offset = reader.readU64BE();
    return { status, offset };
  }

  /**
   * Encode COMMIT request
   * Payload: [session_id: u64]
   */
  static encodeCommit(sessionId: bigint): Uint8Array {
    const writer = new BufferWriter(64);
    writer.writeU64BE(sessionId);
    return writer.getBuffer();
  }

  /**
   * Decode COMMIT response
   * Payload: [status: u8]
   */
  static decodeCommitResponse(payload: Uint8Array): { status: number } {
    const reader = new BufferReader(payload);
    const status = reader.readU8();
    return { status };
  }

  /**
   * Encode ROLLBACK request
   * Payload: [session_id: u64]
   */
  static encodeRollback(sessionId: bigint): Uint8Array {
    const writer = new BufferWriter(64);
    writer.writeU64BE(sessionId);
    return writer.getBuffer();
  }

  /**
   * Decode ROLLBACK response
   * Payload: [status: u8]
   */
  static decodeRollbackResponse(payload: Uint8Array): { status: number } {
    const reader = new BufferReader(payload);
    const status = reader.readU8();
    return { status };
  }

  /**
   * Encode READ request
   * Payload: [route: string][start_offset: u64][limit: u32]
   */
  static encodeRead(
    route: string,
    startOffset: bigint,
    limit: number,
  ): Uint8Array {
    const writer = new BufferWriter(256);
    writer.writeRoute(route);
    writer.writeU64BE(startOffset);
    writer.writeU32BE(limit);
    return writer.getBuffer();
  }

  /**
   * Decode READ response
   * Payload: [status: u8][count: u32]([offset: u64][timestamp: u64][body_len: u32][body: bytes] ...)
   */
  static decodeReadResponse(payload: Uint8Array): {
    status: number;
    records: StreamRecord[];
  } {
    const reader = new BufferReader(payload);
    const status = reader.readU8();

    if (reader.isEOF()) {
      return { status, records: [] };
    }

    const count = reader.readU32BE();
    const records: StreamRecord[] = [];

    for (let i = 0; i < count; i++) {
      const offset = reader.readU64BE();
      const timestamp = reader.readU64BE();
      const bodyLen = reader.readU32BE();
      const body = reader.readBytes(bodyLen);

      records.push({ offset, timestamp, body });
    }

    return { status, records };
  }

  /**
   * Encode LAST request
   * Payload: [route: string]
   */
  static encodeLast(route: string): Uint8Array {
    const writer = new BufferWriter(128);
    writer.writeRoute(route);
    return writer.getBuffer();
  }

  /**
   * Decode LAST response
   * Payload: [status: u8][offset: u64][timestamp: u64][body_len: u32][body: bytes]
   */
  static decodeLastResponse(payload: Uint8Array): {
    status: number;
    record?: StreamRecord;
  } {
    const reader = new BufferReader(payload);
    const status = reader.readU8();

    if (reader.isEOF()) {
      return { status };
    }

    const offset = reader.readU64BE();
    const timestamp = reader.readU64BE();
    const bodyLen = reader.readU32BE();
    const body = reader.readBytes(bodyLen);

    return { status, record: { offset, timestamp, body } };
  }

  /**
   * Encode GET_METADATA request
   * Payload: [route: string]
   */
  static encodeGetMetadata(route: string): Uint8Array {
    const writer = new BufferWriter(128);
    writer.writeRoute(route);
    return writer.getBuffer();
  }

  /**
   * Decode GET_METADATA response
   * Payload: [status: u8][first_offset: u64][last_offset: u64][record_count: u64]
   */
  static decodeGetMetadataResponse(payload: Uint8Array): {
    status: number;
    metadata?: StreamMetadata;
  } {
    const reader = new BufferReader(payload);
    const status = reader.readU8();

    if (reader.isEOF()) {
      return { status };
    }

    const firstOffset = reader.readU64BE();
    const lastOffset = reader.readU64BE();
    const recordCount = reader.readU64BE();

    return { status, metadata: { firstOffset, lastOffset, recordCount } };
  }
}
