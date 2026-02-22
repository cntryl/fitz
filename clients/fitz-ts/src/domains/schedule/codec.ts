/**
 * Schedule domain codec for encoding/decoding messages
 * Per fitz-go/internal/domains/schedule
 */

import { BufferWriter, BufferReader } from "../../core/buffer";
import {
  ScheduleEntry,
  ScheduleCreateResponse,
  ScheduleCancelResponse,
  ScheduleListResponse,
  ScheduleSubscribeResponse,
  ScheduleUnsubscribeResponse,
} from "./types";

export class ScheduleCodec {
  /**
   * Encode CREATE request
   * Payload: [route: string][cron: string][payload: bytes]
   */
  static encodeCreate(
    route: string,
    cronExpr: string,
    payload: Uint8Array,
  ): Uint8Array {
    const writer = new BufferWriter(512);
    writer.writeRoute(route);
    writer.writeString(cronExpr);
    writer.writeBytes(payload);
    return writer.getBuffer();
  }

  /**
   * Decode CREATE response
   * Payload: [status: u8][optional has_schedule_id: u8][schedule_id: string if has=1]
   */
  static decodeCreateResponse(payload: Uint8Array): ScheduleCreateResponse {
    const reader = new BufferReader(payload);
    const status = reader.readU8();

    let scheduleId: string | undefined;
    if (!reader.isEOF() && reader.readU8() === 1) {
      scheduleId = reader.readString();
    }

    return { status, scheduleId };
  }

  /**
   * Encode CANCEL request
   * Payload: [route: string]
   */
  static encodeCancel(route: string): Uint8Array {
    const writer = new BufferWriter(256);
    writer.writeRoute(route);
    return writer.getBuffer();
  }

  /**
   * Decode CANCEL response
   * Payload: [status: u8]
   */
  static decodeCancelResponse(payload: Uint8Array): ScheduleCancelResponse {
    const reader = new BufferReader(payload);
    const status = reader.readU8();
    return { status };
  }

  /**
   * Encode LIST request
   * Payload: [optional offset: u64][optional limit: u64]
   */
  static encodeList(offset: bigint = 0n, limit: bigint = 0n): Uint8Array {
    const writer = new BufferWriter(32);
    writer.writeOptionalU64(offset);
    writer.writeOptionalU64(limit);
    return writer.getBuffer();
  }

  /**
   * Decode LIST response
   * Payload: [status: u8][total_count: u64][has_entry: u8]...[route: string][cron: string][payload: bytes when has_entry=1]
   */
  static decodeListResponse(payload: Uint8Array): ScheduleListResponse {
    const reader = new BufferReader(payload);
    const status = reader.readU8();

    if (reader.isEOF()) {
      return { status, totalCount: 0n, entries: [] };
    }

    const totalCount = reader.readU64BE();
    const entries: ScheduleEntry[] = [];

    while (!reader.isEOF()) {
      const hasEntry = reader.readU8();
      if (hasEntry === 0) {
        break;
      }

      const route = reader.readString();
      const cron = reader.readString();
      const payloadBytes = reader.readBytes(reader.remainingBytes());

      entries.push({
        id: route, // Route as identity
        route,
        cron,
        payload: payloadBytes,
      });
    }

    return { status, totalCount, entries };
  }

  /**
   * Encode SUBSCRIBE request
   * Payload: [pattern: string]
   */
  static encodeSubscribe(pattern: string): Uint8Array {
    const writer = new BufferWriter(256);
    writer.writeString(pattern);
    return writer.getBuffer();
  }

  /**
   * Decode SUBSCRIBE response
   * Payload: [status: u8][optional has_sub_id: u8][sub_id: u64 if has=1]
   */
  static decodeSubscribeResponse(
    payload: Uint8Array,
  ): ScheduleSubscribeResponse {
    const reader = new BufferReader(payload);
    const status = reader.readU8();

    let subId: bigint | undefined;
    if (!reader.isEOF() && reader.readU8() === 1) {
      subId = reader.readU64BE();
    }

    return { status, subId };
  }

  /**
   * Encode UNSUBSCRIBE request
   * Payload: [pattern: string]
   */
  static encodeUnsubscribe(pattern: string): Uint8Array {
    const writer = new BufferWriter(256);
    writer.writeString(pattern);
    return writer.getBuffer();
  }

  /**
   * Decode UNSUBSCRIBE response
   * Payload: [status: u8]
   */
  static decodeUnsubscribeResponse(
    payload: Uint8Array,
  ): ScheduleUnsubscribeResponse {
    const reader = new BufferReader(payload);
    const status = reader.readU8();
    return { status };
  }

  /**
   * Decode NOTIFY notification (MSG_SCHEDULE_NOTIFY 705)
   * Payload: [subscription_id: u64][payload: bytes]
   */
  static decodeNotification(payload: Uint8Array): {
    subId: bigint;
    payload: Uint8Array;
  } {
    const reader = new BufferReader(payload);
    const subId = reader.readU64BE();
    const notificationPayload =
      reader.remainingBytes() > 0
        ? reader.readBytes(reader.remainingBytes())
        : new Uint8Array(0);
    return { subId, payload: notificationPayload };
  }
}
