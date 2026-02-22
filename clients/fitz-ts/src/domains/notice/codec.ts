/**
 * Notice domain codec for encoding/decoding messages
 * Per fitz-go/internal/domains/notice/protocol.go
 */

import { BufferWriter, BufferReader } from "../../core/buffer";
import { SubscribeResponse, UnsubscribeResponse } from "./types";

export class NoticeCodec {
  /**
   * Encode PUBLISH request (fire-and-forget, no response)
   * Payload: [string route][bytes body]
   */
  static encodePublish(route: string, body: Uint8Array): Uint8Array {
    const writer = new BufferWriter(256);
    writer.writeRoute(route);
    writer.writeBytes(body);
    return writer.getBuffer();
  }

  /**
   * Encode SUBSCRIBE request
   * Payload: [string pattern]
   */
  static encodeSubscribe(pattern: string): Uint8Array {
    const writer = new BufferWriter(64);
    writer.writeRoute(pattern);
    return writer.getBuffer();
  }

  /**
   * Decode SUBSCRIBE response
   * Standard response: [u8 status=0][u8 has_sub_id][u64 sub_id if has=1]
   */
  static decodeSubscribeResponse(payload: Uint8Array): SubscribeResponse {
    if (payload.length < 1) {
      throw new Error("SUBSCRIBE response too short");
    }

    const reader = new BufferReader(payload);
    const hasSubId = reader.readU8();

    if (hasSubId !== 1) {
      throw new Error("SUBSCRIBE response missing subscription_id");
    }

    if (reader.remainingBytes() < 8) {
      throw new Error("SUBSCRIBE response too short for subscription_id");
    }

    const subId = reader.readU64BE();
    return { status: 0, subId };
  }

  /**
   * Encode UNSUBSCRIBE request
   * Payload: [string pattern]
   */
  static encodeUnsubscribe(pattern: string): Uint8Array {
    const writer = new BufferWriter(64);
    writer.writeRoute(pattern);
    return writer.getBuffer();
  }

  /**
   * Decode UNSUBSCRIBE response
   * Standard response: [u8 status=0]
   */
  static decodeUnsubscribeResponse(): UnsubscribeResponse {
    return { status: 0 };
  }

  /**
   * Decode NOTIFY (504) message
   * Payload: [u64 subscription_id][string route][bytes body]
   */
  static decodeNotification(payload: Uint8Array): {
    subId: bigint;
    route: string;
    body: Uint8Array;
  } {
    const reader = new BufferReader(payload);
    const subId = reader.readU64BE();
    const route = reader.readRoute();
    const bodyLen = reader.readU32BE();
    const body = reader.readBytes(bodyLen);

    return { subId, route, body };
  }
}
