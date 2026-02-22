/**
 * Lease domain codec for encoding/decoding messages
 * Per fitz-go/internal/domains/lease/protocol.go
 */

import { BufferWriter, BufferReader } from "../../core/buffer";
import {
  AcquireResponse,
  QueryResponse,
  SubscribeResponse,
  UnsubscribeResponse,
} from "./types";

export class LeaseCodec {
  /**
   * Encode ACQUIRE request
   * Payload: [string route][string client_id (empty)][u64 ttl_seconds]
   */
  static encodeAcquire(route: string, ttlSecs: number): Uint8Array {
    const writer = new BufferWriter(128);
    writer.writeRoute(route);
    writer.writeRoute(""); // client_id (empty = server assigns)
    writer.writeU64BE(BigInt(ttlSecs));
    return writer.getBuffer();
  }

  /**
   * Decode ACQUIRE response
   * Standard response: [u8 status=0][u8 response_type][u64 fencing_token]
   * response_type: 0=Acquired, 1=AlreadyHeld (idempotent)
   */
  static decodeAcquireResponse(payload: Uint8Array): AcquireResponse {
    if (payload.length < 9) {
      throw new Error(
        `ACQUIRE response too short: got ${payload.length} bytes, expected >= 9`,
      );
    }

    const reader = new BufferReader(payload);
    reader.readU8(); // responseType: 0=Acquired, 1=AlreadyHeld
    const fencingToken = reader.readU64BE();

    // response_type: 0=Acquired, 1=AlreadyHeld
    // For now, treat both as success
    return { token: fencingToken, expiresAt: 0n }; // expiresAt computed by client
  }

  /**
   * Encode EXTEND request
   * Payload: [string route][string client_id (empty)][u64 fencing_token][u64 ttl_seconds]
   */
  static encodeExtend(
    route: string,
    token: bigint,
    ttlSecs: number,
  ): Uint8Array {
    const writer = new BufferWriter(128);
    writer.writeRoute(route);
    writer.writeRoute(""); // client_id (empty = use existing)
    writer.writeU64BE(token);
    writer.writeU64BE(BigInt(ttlSecs));
    return writer.getBuffer();
  }

  /**
   * Encode RELEASE request
   * Payload: [string route][string client_id (empty)][u64 fencing_token]
   */
  static encodeRelease(route: string, token: bigint): Uint8Array {
    const writer = new BufferWriter(128);
    writer.writeRoute(route);
    writer.writeRoute(""); // client_id (empty = use existing)
    writer.writeU64BE(token);
    return writer.getBuffer();
  }

  /**
   * Encode QUERY request
   * Payload: [string route]
   */
  static encodeQuery(route: string): Uint8Array {
    const writer = new BufferWriter(64);
    writer.writeRoute(route);
    return writer.getBuffer();
  }

  /**
   * Decode QUERY response
   * Free: [u8 has_holder=0][u32 pending_waiters]
   * Held: [u8 has_holder=1][string owner_id][u64 ttl_remaining_secs][u32 pending_waiters]
   */
  static decodeQueryResponse(payload: Uint8Array): QueryResponse {
    const reader = new BufferReader(payload);
    const hasHolder = reader.readU8();

    if (hasHolder === 0) {
      // Free
      reader.readU32BE(); // pendingWaiters
      return { status: 0, isHeld: false };
    }

    // Held
    const owner = reader.readRoute();
    const ttlRemainingSecs = reader.readU64BE();
    reader.readU32BE(); // pendingWaiters

    // Note: token not returned in QUERY response
    return {
      status: 0,
      isHeld: true,
      owner,
      expiresAt: BigInt(Math.floor(Date.now() / 1000)) + ttlRemainingSecs,
    };
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
   * Standard response: [u8 status=0][u64 subscription_id]
   */
  static decodeSubscribeResponse(payload: Uint8Array): SubscribeResponse {
    if (payload.length < 8) {
      throw new Error(
        `SUBSCRIBE response too short: got ${payload.length} bytes, expected >= 8`,
      );
    }

    const reader = new BufferReader(payload);
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
  static decodeUnsubscribeResponse(_payload: Uint8Array): UnsubscribeResponse {
    return { status: 0 };
  }

  /**
   * Decode NOTIFY (409) message
   * Payload: [u64 subscription_id][string route][bytes payload]
   */
  static decodeNotification(payload: Uint8Array): {
    subId: bigint;
    route: string;
  } {
    const reader = new BufferReader(payload);
    const subId = reader.readU64BE();
    const route = reader.readRoute();

    return { subId, route };
  }
}
