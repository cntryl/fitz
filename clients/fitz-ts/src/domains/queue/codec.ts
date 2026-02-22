/**
 * Queue domain codec for encoding/decoding messages
 * Per fitz-go/internal/domains/queue/protocol.go
 */

import { BufferWriter, BufferReader } from "../../core/buffer";
import {
  QueueSendResponse,
  QueueReceiveResponse,
  QueueAckResponse,
  QueueExtendResponse,
  QueueSubscribeResponse,
  QueueUnsubscribeResponse,
  SendOptions,
} from "./types";

export class QueueCodec {
  /**
   * Encode SEND request (wire protocol: ENQUEUE)
   * Payload: [route: string][body_len: u32][body: bytes][has_delay: u8][delay_seconds: u64 if has_delay]
   */
  static encodeSend(
    route: string,
    body: Uint8Array,
    options?: SendOptions,
  ): Uint8Array {
    const writer = new BufferWriter(512);
    writer.writeRoute(route);
    writer.writeU32BE(body.length);
    writer.writeBytes(body);

    const delaySeconds = options?.delayMs
      ? Math.floor(options.delayMs / 1000)
      : 0;
    const hasDelay = delaySeconds > 0 ? 1 : 0;
    writer.writeU8(hasDelay);
    if (hasDelay) {
      writer.writeU64BE(BigInt(delaySeconds));
    }

    return writer.getBuffer();
  }

  /**
   * Decode SEND response (wire protocol: ENQUEUE)
   * Payload: [status: u8][message_id: u64]
   */
  static decodeSendResponse(payload: Uint8Array): QueueSendResponse {
    const reader = new BufferReader(payload);
    const status = reader.readU8();
    let messageId: bigint | undefined;
    if (!reader.isEOF()) {
      messageId = reader.readU64BE();
    }
    return { status, messageId };
  }

  /**
   * Encode RECEIVE request (wire protocol: RESERVE)
   * Payload: [route: string][lease_seconds: u64][has_batch_size: u8][batch_size: u32][has_wait_seconds: u8][wait_seconds: u64]
   */
  static encodeReceive(
    route: string,
    leaseSeconds: number,
    batchSize?: number,
    waitSeconds?: number,
  ): Uint8Array {
    const writer = new BufferWriter(256);
    writer.writeRoute(route);
    writer.writeU64BE(BigInt(leaseSeconds));

    const hasBatchSize = batchSize !== undefined && batchSize > 0 ? 1 : 0;
    writer.writeU8(hasBatchSize);
    if (hasBatchSize) {
      writer.writeU32BE(batchSize!);
    }

    const hasWaitSeconds = waitSeconds !== undefined && waitSeconds > 0 ? 1 : 0;
    writer.writeU8(hasWaitSeconds);
    if (hasWaitSeconds) {
      writer.writeU64BE(BigInt(waitSeconds!));
    }

    return writer.getBuffer();
  }

  /**
   * Decode RECEIVE response (wire protocol: RESERVE)
   * Payload: [status: u8][lease_count: u32]([message_id: u64][lease_token: u64][body_len: u32][body: bytes] ...)
   */
  static decodeReceiveResponse(payload: Uint8Array): QueueReceiveResponse {
    const reader = new BufferReader(payload);
    const status = reader.readU8();

    if (reader.isEOF()) {
      return { status, items: [] };
    }

    const leaseCount = reader.readU32BE();
    const items: Array<{ id: bigint; token: bigint; body: Uint8Array }> = [];

    for (let i = 0; i < leaseCount; i++) {
      const id = reader.readU64BE();
      const token = reader.readU64BE();
      const bodyLen = reader.readU32BE();
      const body = reader.readBytes(bodyLen);

      items.push({ id, token, body });
    }

    return { status, items };
  }

  /**
   * Encode ACK request (wire protocol: COMPLETE)
   * Payload: [route: string][message_id: u64][lease_token: u64]
   */
  static encodeAck(
    route: string,
    messageId: bigint,
    leaseToken: bigint,
  ): Uint8Array {
    const writer = new BufferWriter(128);
    writer.writeRoute(route);
    writer.writeU64BE(messageId);
    writer.writeU64BE(leaseToken);
    return writer.getBuffer();
  }

  /**
   * Decode ACK response
   * Payload: [status: u8]
   */
  static decodeAckResponse(payload: Uint8Array): QueueAckResponse {
    const reader = new BufferReader(payload);
    const status = reader.readU8();
    return { status };
  }

  /**
   * Encode EXTEND request
   * Payload: [route: string][message_id: u64][lease_token: u64][lease_seconds: u64]
   */
  static encodeExtend(
    route: string,
    messageId: bigint,
    leaseToken: bigint,
    leaseSeconds: number,
  ): Uint8Array {
    const writer = new BufferWriter(128);
    writer.writeRoute(route);
    writer.writeU64BE(messageId);
    writer.writeU64BE(leaseToken);
    writer.writeU64BE(BigInt(leaseSeconds));
    return writer.getBuffer();
  }

  /**
   * Decode EXTEND response
   * Payload: [status: u8]
   */
  static decodeExtendResponse(payload: Uint8Array): QueueExtendResponse {
    const reader = new BufferReader(payload);
    const status = reader.readU8();
    return { status };
  }

  /**
   * Encode SUBSCRIBE request
   * Payload: [pattern: string]
   */
  static encodeSubscribe(pattern: string): Uint8Array {
    const writer = new BufferWriter(128);
    writer.writeRoute(pattern);
    return writer.getBuffer();
  }

  /**
   * Decode SUBSCRIBE response
   * Payload: [status: u8][sub_id: u64]
   */
  static decodeSubscribeResponse(payload: Uint8Array): QueueSubscribeResponse {
    const reader = new BufferReader(payload);
    const status = reader.readU8();
    let subId: bigint | undefined;
    if (!reader.isEOF()) {
      subId = reader.readU64BE();
    }
    return { status, subId };
  }

  /**
   * Encode UNSUBSCRIBE request
   * Payload: [sub_id: u64]
   */
  static encodeUnsubscribe(subId: bigint): Uint8Array {
    const writer = new BufferWriter(64);
    writer.writeU64BE(subId);
    return writer.getBuffer();
  }

  /**
   * Decode UNSUBSCRIBE response
   * Payload: [status: u8]
   */
  static decodeUnsubscribeResponse(
    payload: Uint8Array,
  ): QueueUnsubscribeResponse {
    const reader = new BufferReader(payload);
    const status = reader.readU8();
    return { status };
  }

  /**
   * Decode notification payload
   * Payload: [sub_id: u64][route: string]
   */
  static decodeNotification(payload: Uint8Array): {
    subId: bigint;
    route: string;
  } {
    const reader = new BufferReader(payload);
    const subId = reader.readU64BE();
    const route = reader.readString();
    return { subId, route };
  }
}
