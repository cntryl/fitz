/**
 * KV domain codec for encoding/decoding messages
 */

import { BufferWriter, BufferReader } from "../../core/buffer";
import { CodecError } from "../../core/errors";
import {
  KvBeginResponse,
  KvPutResponse,
  KvGetResponse,
  KvDeleteResponse,
  KvCommitResponse,
  KvRollbackResponse,
  KvScanResponse,
  TxMode,
  DurabilityMode,
} from "./types";

export class KvCodec {
  /**
   * Encode BEGIN request
   * Payload: [route: string][mode: u8][durability: u8]
   * Note: RouteFamily is derived by the server from the route, not sent by client
   */
  static encodeBegin(
    route: string,
    mode: TxMode,
    durability: DurabilityMode,
  ): Uint8Array {
    const writer = new BufferWriter(256);
    writer.writeRoute(route);
    writer.writeU8(mode === "ReadWrite" ? 1 : 0);
    writer.writeU8(this.encodeDurability(durability));
    return writer.getBuffer();
  }

  /**
   * Decode BEGIN response
   * Payload: [status: u8][tx_id: u64][...]
   */
  static decodeBeginResponse(payload: Uint8Array): KvBeginResponse {
    const reader = new BufferReader(payload);
    const status = reader.readU8();
    const txId = reader.readU64BE();
    return { txId, status };
  }

  /**
   * Encode PUT request
   * Payload: [tx_id: u64][route: string][key: bytes][value: bytes]
   */
  static encodePut(
    txId: bigint,
    route: string,
    key: Uint8Array,
    value: Uint8Array,
  ): Uint8Array {
    const writer = new BufferWriter(512);
    writer.writeU64BE(txId);
    writer.writeRoute(route);
    writer.writeBytes(key);
    writer.writeBytes(value);
    return writer.getBuffer();
  }

  /**
   * Decode PUT response
   * Payload: [status: u8]
   */
  static decodePutResponse(payload: Uint8Array): KvPutResponse {
    const reader = new BufferReader(payload);
    const status = reader.readU8();
    return { status };
  }

  /**
   * Encode GET request
   * Payload: [tx_id: u64][route: string][key: bytes]
   */
  static encodeGet(txId: bigint, route: string, key: Uint8Array): Uint8Array {
    const writer = new BufferWriter(256);
    writer.writeU64BE(txId);
    writer.writeRoute(route);
    writer.writeBytes(key);
    return writer.getBuffer();
  }

  /**
   * Decode GET response
   * Payload: [status: u8][found: u8][value_len: u32][value: bytes]
   */
  static decodeGetResponse(payload: Uint8Array): KvGetResponse {
    const reader = new BufferReader(payload);
    const status = reader.readU8();
    const found = reader.readU8();

    if (found === 0 || reader.isEOF()) {
      return { status, value: undefined };
    }

    const value = reader.readBytes(reader.remainingBytes());
    return { status, value };
  }

  /**
   * Encode DELETE request
   * Payload: [tx_id: u64][route: string][key: bytes]
   */
  static encodeDelete(
    txId: bigint,
    route: string,
    key: Uint8Array,
  ): Uint8Array {
    const writer = new BufferWriter(256);
    writer.writeU64BE(txId);
    writer.writeRoute(route);
    writer.writeBytes(key);
    return writer.getBuffer();
  }

  /**
   * Decode DELETE response
   * Payload: [status: u8]
   */
  static decodeDeleteResponse(payload: Uint8Array): KvDeleteResponse {
    const reader = new BufferReader(payload);
    const status = reader.readU8();
    return { status };
  }

  /**
   * Encode COMMIT request
   * Payload: [tx_id: u64][route: string]
   */
  static encodeCommit(txId: bigint, route: string): Uint8Array {
    const writer = new BufferWriter(256);
    writer.writeU64BE(txId);
    writer.writeRoute(route);
    return writer.getBuffer();
  }

  /**
   * Decode COMMIT response
   * Payload: [status: u8]
   */
  static decodeCommitResponse(payload: Uint8Array): KvCommitResponse {
    const reader = new BufferReader(payload);
    const status = reader.readU8();
    return { status };
  }

  /**
   * Encode ROLLBACK request
   * Payload: [tx_id: u64][route: string]
   */
  static encodeRollback(txId: bigint, route: string): Uint8Array {
    const writer = new BufferWriter(256);
    writer.writeU64BE(txId);
    writer.writeRoute(route);
    return writer.getBuffer();
  }

  /**
   * Decode ROLLBACK response
   * Payload: [status: u8]
   */
  static decodeRollbackResponse(payload: Uint8Array): KvRollbackResponse {
    const reader = new BufferReader(payload);
    const status = reader.readU8();
    return { status };
  }

  /**
   * Encode SCAN request
   * Payload: [tx_id: u64][route: string][cursor: bytes]
   */
  static encodeScan(
    txId: bigint,
    route: string,
    cursor?: Uint8Array,
  ): Uint8Array {
    const writer = new BufferWriter(512);
    writer.writeU64BE(txId);
    writer.writeRoute(route);
    if (cursor) {
      writer.writeBytes(cursor);
    }
    return writer.getBuffer();
  }

  /**
   * Decode SCAN response
   * Payload: [status: u8][count: u32]([key_len: u32][key: bytes] ...)[next_cursor_len: u32][cursor: bytes]
   */
  static decodeScanResponse(payload: Uint8Array): KvScanResponse {
    const reader = new BufferReader(payload);
    const status = reader.readU8();

    if (reader.isEOF()) {
      return { status, keys: [] };
    }

    const count = reader.readU32BE();
    const keys: Uint8Array[] = [];

    for (let i = 0; i < count; i++) {
      const keyLen = reader.readU32BE();
      keys.push(reader.readBytes(keyLen));
    }

    let nextCursor: Uint8Array | undefined;
    if (!reader.isEOF()) {
      nextCursor = reader.readBytes(reader.remainingBytes());
    }

    return { status, keys, nextCursor };
  }

  private static encodeDurability(durability: DurabilityMode): number {
    switch (durability) {
      case "None":
        return 0;
      case "Async":
        return 1;
      case "Sync":
        return 2;
      default:
        throw new CodecError(`Unknown durability mode: ${durability}`);
    }
  }
}
