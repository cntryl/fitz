/**
 * RPC domain codec for encoding/decoding messages
 * Per fitz-go/internal/domains/rpc/protocol.go
 */

import { BufferWriter, BufferReader } from "../../core/buffer";
import {
  InboundRequest,
  SubscribeResponse,
  UnsubscribeResponse,
} from "./types";

export class RpcCodec {
  /**
   * Generate a random 16-byte correlation ID
   * Per fitz-go rpc.go: crypto/rand.Read(correlationID[:])
   */
  static generateCorrelationId(): Uint8Array {
    const correlationId = new Uint8Array(16);
    if (typeof window !== "undefined" && window.crypto) {
      window.crypto.getRandomValues(correlationId);
    } else if (typeof globalThis !== "undefined" && globalThis.crypto) {
      globalThis.crypto.getRandomValues(correlationId);
    } else {
      // Fallback for older environments
      for (let i = 0; i < 16; i++) {
        correlationId[i] = Math.floor(Math.random() * 256);
      }
    }
    return correlationId;
  }

  /**
   * Encode RPC_REQUEST (302)
   * Payload: [bytes correlation_id][string route][string reply_route][bytes body]
   * Where bytes/string = [u32 BE len][data]
   */
  static encodeRequest(
    correlationId: Uint8Array,
    route: string,
    replyRoute: string,
    body: Uint8Array,
  ): Uint8Array {
    const writer = new BufferWriter(256);
    writer.writeBytes(correlationId); // [u32 len=16][16 bytes]
    writer.writeRoute(route);
    writer.writeRoute(replyRoute);
    writer.writeBytes(body);
    return writer.getBuffer();
  }

  /**
   * Decode RPC_REQUEST response (standard [u8 status][payload])
   * Just validates OK status; no additional payload expected
   */
  static decodeRequestResponse(payload: Uint8Array): { status: number } {
    if (payload.length === 0) {
      throw new Error("Empty REQUEST response");
    }
    const reader = new BufferReader(payload);
    const status = reader.readU8();
    return { status };
  }

  /**
   * Encode RPC_RESPONSE (303)
   * Payload: [bytes correlation_id][u64 sequence][bytes body][u8 stream_end]
   */
  static encodeResponse(
    correlationId: Uint8Array,
    sequence: bigint,
    body: Uint8Array,
    streamEnd: boolean,
  ): Uint8Array {
    const writer = new BufferWriter(256);
    writer.writeBytes(correlationId);
    writer.writeU64BE(sequence);
    writer.writeBytes(body);
    writer.writeU8(streamEnd ? 1 : 0);
    return writer.getBuffer();
  }

  /**
   * Decode RPC_RESPONSE (303)
   * Payload: [bytes correlation_id][u64 sequence][bytes body][u8 stream_end]
   * Returns: { correlationId, sequence, body, streamEnd }
   */
  static decodeResponse(payload: Uint8Array): {
    correlationId: Uint8Array;
    sequence: bigint;
    body: Uint8Array;
    streamEnd: boolean;
  } {
    const reader = new BufferReader(payload);

    // Read correlation ID (bytes = [u32 len][data])
    const corrLen = reader.readU32BE();
    if (corrLen !== 16) {
      throw new Error(`Invalid correlation ID length: ${corrLen}, expected 16`);
    }
    const correlationId = reader.readBytes(corrLen);

    // Read sequence
    const sequence = reader.readU64BE();

    // Read body (bytes = [u32 len][data])
    const bodyLen = reader.readU32BE();
    const body = reader.readBytes(bodyLen);

    // Read stream_end flag
    let streamEnd = false;
    if (!reader.isEOF()) {
      streamEnd = reader.readU8() === 1;
    }

    return { correlationId, sequence, body, streamEnd };
  }

  /**
   * Encode RPC_SUBSCRIBE_WORKER (304)
   * Payload: [string worker_route]
   */
  static encodeSubscribeWorker(route: string): Uint8Array {
    const writer = new BufferWriter(128);
    writer.writeRoute(route);
    return writer.getBuffer();
  }

  /**
   * Decode SUBSCRIBE_WORKER response (standard [u8 status])
   */
  static decodeSubscribeWorkerResponse(payload: Uint8Array): SubscribeResponse {
    if (payload.length === 0) {
      throw new Error("Empty SUBSCRIBE_WORKER response");
    }
    const reader = new BufferReader(payload);
    const status = reader.readU8();
    return { status };
  }

  /**
   * Encode RPC_UNSUBSCRIBE_WORKER (305)
   * Payload: [string worker_route]
   */
  static encodeUnsubscribeWorker(route: string): Uint8Array {
    const writer = new BufferWriter(128);
    writer.writeRoute(route);
    return writer.getBuffer();
  }

  /**
   * Decode UNSUBSCRIBE_WORKER response (standard [u8 status])
   */
  static decodeUnsubscribeWorkerResponse(
    payload: Uint8Array,
  ): UnsubscribeResponse {
    if (payload.length === 0) {
      throw new Error("Empty UNSUBSCRIBE_WORKER response");
    }
    const reader = new BufferReader(payload);
    const status = reader.readU8();
    return { status };
  }

  /**
   * Decode incoming RPC_REQUEST (302) for worker mode
   * Payload: [u32 corrLen=16][16 bytes correlation_id][string route][string reply_route][bytes body]
   */
  static decodeInboundRequest(payload: Uint8Array): InboundRequest {
    const reader = new BufferReader(payload);

    // Read correlation ID length prefix
    const corrLen = reader.readU32BE();
    if (corrLen !== 16) {
      throw new Error(`Invalid correlation ID length: ${corrLen}, expected 16`);
    }

    // Read correlation ID
    const correlationId = reader.readBytes(16);

    // Read route and reply route (TLV strings)
    const route = reader.readRoute();
    const replyRoute = reader.readRoute();

    // Read body (TLV bytes)
    const bodyLen = reader.readU32BE();
    const body = reader.readBytes(bodyLen);

    return { correlationId, route, replyRoute, body };
  }
}
