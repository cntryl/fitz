/**
 * Standard response parsing utilities
 * Per CLIENT_SPEC.md and fitz-go/internal/core/connection/connection.go
 *
 * Standard Response Format:
 * [u8 status][payload...]
 *
 * status=0: success, remaining payload follows
 * status=1: error, followed by [u32 len][error message]
 */

import { BufferReader } from "../core/buffer";

export interface ParsedResponse {
  success: boolean;
  data: Uint8Array;
  error?: string;
}

/**
 * Parse standard response format
 * Returns {success: true, data: remaining} on status=0
 * Returns {success: false, error: message} on status=1
 */
export function parseStandardResponse(payload: Uint8Array): ParsedResponse {
  if (payload.length === 0) {
    throw new Error("Response payload is empty");
  }

  const reader = new BufferReader(payload);
  const status = reader.readU8();

  if (status === 0) {
    // Success - return remaining payload
    const remaining = reader.remaining();
    return { success: true, data: remaining };
  }

  if (status === 1) {
    // Error - read error message
    if (reader.isEOF()) {
      return {
        success: false,
        data: new Uint8Array(0),
        error: "Unknown error (no message)",
      };
    }
    const errorMsg = reader.readString();
    return { success: false, data: new Uint8Array(0), error: errorMsg };
  }

  // Unknown status code
  throw new Error(`Unknown response status: ${status}`);
}

/**
 * Check if response is success, throw error if not
 * Returns remaining data on success
 */
export function assertSuccess(
  payload: Uint8Array,
  operation: string,
): Uint8Array {
  const result = parseStandardResponse(payload);
  if (!result.success) {
    throw new Error(`${operation} failed: ${result.error || "Unknown error"}`);
  }
  return result.data;
}
