/**
 * Test utilities and helpers
 */

import { BufferWriter, BufferReader } from "../../../src/core/buffer";

/**
 * Build a simple success response with u64 value
 */
export function buildU64Response(value: bigint): Uint8Array {
  const writer = new BufferWriter(16);
  writer.writeU8(0); // status = success
  writer.writeU64BE(value);
  return writer.getBuffer();
}

/**
 * Build a simple success response with string value
 */
export function buildStringResponse(value: string): Uint8Array {
  const writer = new BufferWriter(256);
  writer.writeU8(0); // status = success
  writer.writeString(value);
  return writer.getBuffer();
}

/**
 * Build a simple success response with bytes value
 */
export function buildBytesResponse(value: Uint8Array): Uint8Array {
  const writer = new BufferWriter(value.length + 10);
  writer.writeU8(0); // status = success
  writer.writeBytes(value);
  return writer.getBuffer();
}

/**
 * Parse response status (first byte)
 */
export function getResponseStatus(response: Uint8Array): number {
  if (response.length === 0) throw new Error("Empty response");
  return response[0];
}

/**
 * Parse response data (everything after status byte)
 */
export function getResponseData(response: Uint8Array): Uint8Array {
  if (response.length <= 1) return new Uint8Array(0);
  return response.slice(1);
}

/**
 * Assert response is success
 */
export function expectSuccess(response: Uint8Array): Uint8Array {
  const status = getResponseStatus(response);
  if (status !== 0) throw new Error(`Expected success status 0, got ${status}`);
  return getResponseData(response);
}

/**
 * Encode a message frame: [msgType u16][length u16][payload]
 */
export function encodeMessageFrame(
  msgType: number,
  payload: Uint8Array,
): Uint8Array {
  const writer = new BufferWriter(4 + payload.length);
  writer.writeU16BE(msgType);
  writer.writeU16BE(payload.length);
  writer.writeBytes(payload);
  return writer.getBuffer();
}

/**
 * Decode a message frame
 */
export function decodeMessageFrame(frame: Uint8Array): [number, Uint8Array] {
  if (frame.length < 4) throw new Error("Frame too short");
  const reader = new BufferReader(frame);
  const msgType = reader.readU16BE();
  const length = reader.readU16BE();
  if (4 + length !== frame.length) {
    throw new Error(
      `Frame length mismatch: expected ${4 + length}, got ${frame.length}`,
    );
  }
  const payload = reader.readBytes(length);
  return [msgType, payload];
}

/**
 * Create test data (UTF-8 encoded string as Uint8Array)
 */
export function testData(str: string): Uint8Array {
  return new TextEncoder().encode(str);
}

/**
 * Create large test data
 */
export function largeTestData(sizeBytes: number): Uint8Array {
  return new Uint8Array(sizeBytes).fill(0x42); // Fill with 'B' (0x42)
}

/**
 * Compare two Uint8Arrays
 */
export function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return false;
  }
  return true;
}
