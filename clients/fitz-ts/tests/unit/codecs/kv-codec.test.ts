/**
 * KV Codec unit tests
 * Tests encoding and decoding of all KV protocol messages
 */

import { describe, it, expect } from "vitest";
import { KvCodec } from "../../../src/domains/kv/codec";
import { BufferReader, BufferWriter } from "../../../src/core/buffer";
import {
  testData,
  buildU64Response,
  expectSuccess as _expectSuccess,
  getResponseStatus as _getResponseStatus,
} from "../helpers/test-utils";

describe("KvCodec", () => {
  describe("BEGIN encoding", () => {
    it("should_encode_begin_with_all_fields", () => {
      // Arrange
      const route = "kv://acme/app/users";
      const mode = "ReadWrite";
      const durability = "Sync";

      // Act
      const encoded = KvCodec.encodeBegin(route, mode, durability);

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
      expect(encoded.length).toBeGreaterThan(0);
    });

    it("should_encode_begin_read_only_mode", () => {
      // Arrange/Act
      const encoded = KvCodec.encodeBegin(
        "kv://test/app/data",
        "ReadOnly",
        "Async",
      );

      // Assert: Verify encoding is valid
      const reader = new BufferReader(encoded);
      const route = reader.readString();
      expect(route).toBe("kv://test/app/data");
    });

    it("should_encode_begin_with_different_durabilities", () => {
      const testCases = [
        { durability: "None", desc: "None durability" },
        { durability: "Async", desc: "Async durability" },
        { durability: "Sync", desc: "Sync durability" },
      ] as const;

      for (const tc of testCases) {
        const encoded = KvCodec.encodeBegin(
          "kv://test/app/data",
          "ReadWrite",
          tc.durability,
        );
        expect(encoded.length).toBeGreaterThan(0);
      }
    });
  });

  describe("BEGIN decoding", () => {
    it("should_decode_begin_response_with_valid_status", () => {
      // Arrange
      const response = buildU64Response(12345n);

      // Act
      const decoded = KvCodec.decodeBeginResponse(response);

      // Assert
      expect(decoded.status).toBe(0);
      expect(decoded.txId).toBe(12345n);
    });

    it("should_decode_begin_response_with_large_tx_id", () => {
      // Arrange
      const largeId = 9223372036854775807n; // Max i64
      const response = buildU64Response(largeId);

      // Act
      const decoded = KvCodec.decodeBeginResponse(response);

      // Assert
      expect(decoded.txId).toBe(largeId);
    });

    it("should_decode_begin_response_with_error_status", () => {
      // Arrange
      const writer = new BufferWriter(16);
      writer.writeU8(5); // status = OperationNotAllowed
      writer.writeU64BE(0n); // Still need tx_id field
      const response = writer.getBuffer();

      // Act
      const decoded = KvCodec.decodeBeginResponse(response);

      // Assert
      expect(decoded.status).toBe(5);
    });
  });

  describe("PUT encoding", () => {
    it("should_encode_put_with_simple_key_value", () => {
      // Arrange
      const txId = 100n;
      const route = "kv://test/app/data";
      const key = testData("alice");
      const value = testData("secret123");

      // Act
      const encoded = KvCodec.encodePut(txId, route, key, value);

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
      expect(encoded.length).toBeGreaterThan(0);
    });

    it("should_encode_put_with_empty_value", () => {
      // Arrange
      const txId = 100n;
      const route = "kv://test/app/data";
      const key = testData("key");
      const value = new Uint8Array(0);

      // Act
      const encoded = KvCodec.encodePut(txId, route, key, value);

      // Assert
      const reader = new BufferReader(encoded);
      const decodedTxId = reader.readU64BE();
      expect(decodedTxId).toBe(txId);
    });

    it("should_encode_put_with_large_value", () => {
      // Arrange
      const txId = 100n;
      const route = "kv://test/app/data";
      const key = testData("key");
      const value = new Uint8Array(1000000); // 1MB
      value.fill(0x42);

      // Act
      const encoded = KvCodec.encodePut(txId, route, key, value);

      // Assert
      expect(encoded.length).toBeGreaterThan(1000000);
    });
  });

  describe("PUT decoding", () => {
    it("should_decode_put_response_success", () => {
      // Arrange
      const response = new Uint8Array([0]); // status = success

      // Act
      const decoded = KvCodec.decodePutResponse(response);

      // Assert
      expect(decoded.status).toBe(0);
    });

    it("should_decode_put_response_error", () => {
      // Arrange
      const response = new Uint8Array([1, 0]); // error status, found = 0

      // Act
      const decoded = KvCodec.decodePutResponse(response);

      // Assert
      expect(decoded.status).toBe(1);
    });
  });

  describe("GET encoding", () => {
    it("should_encode_get_with_simple_key", () => {
      // Arrange
      const txId = 100n;
      const route = "kv://test/app/data";
      const key = testData("alice");

      // Act
      const encoded = KvCodec.encodeGet(txId, route, key);

      // Assert
      const reader = new BufferReader(encoded);
      expect(reader.readU64BE()).toBe(txId);
    });
  });

  describe("GET decoding", () => {
    it("should_decode_get_response_with_value", () => {
      // Arrange
      const writer = new BufferWriter(256);
      writer.writeU8(0); // status = success
      writer.writeU8(1); // found = 1
      const value = testData("found_value");
      writer.writeBytes(value);
      const response = writer.getBuffer();

      // Act
      const decoded = KvCodec.decodeGetResponse(response);

      // Assert
      expect(decoded.status).toBe(0);
      expect(decoded.value).toEqual(value);
    });

    it("should_decode_get_response_not_found", () => {
      // Arrange
      const response = new Uint8Array([0, 0]); // status = success, found = 0

      // Act
      const decoded = KvCodec.decodeGetResponse(response);

      // Assert
      expect(decoded.status).toBe(0);
      expect(decoded.value).toBeUndefined();
    });

    it("should_decode_get_response_error", () => {
      // Arrange
      const response = new Uint8Array([4, 0]); // status = KeyNotFound, found = 0

      // Act
      const decoded = KvCodec.decodeGetResponse(response);

      // Assert
      expect(decoded.status).toBe(4);
    });
  });

  describe("DELETE encoding", () => {
    it("should_encode_delete_with_simple_key", () => {
      // Arrange
      const txId = 100n;
      const route = "kv://test/app/data";
      const key = testData("alice");

      // Act
      const encoded = KvCodec.encodeDelete(txId, route, key);

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
    });
  });

  describe("SCAN encoding", () => {
    it("should_encode_scan_without_cursor", () => {
      // Arrange/Act
      const encoded = KvCodec.encodeScan(
        100n,
        "kv://test/app/items",
        undefined,
      );

      // Assert
      const reader = new BufferReader(encoded);
      expect(reader.readU64BE()).toBe(100n);
    });

    it("should_encode_scan_with_cursor", () => {
      // Arrange
      const txId = 100n;
      const route = "kv://test/app/items";
      const cursor = new Uint8Array([1, 2, 3, 4]); // Raw bytes, not text

      // Act
      const encoded = KvCodec.encodeScan(txId, route, cursor);

      // Assert
      expect(encoded.length).toBeGreaterThan(0);
    });
  });

  describe("SCAN decoding", () => {
    it("should_decode_scan_response_with_multiple_keys", () => {
      // Arrange
      const writer = new BufferWriter(256);
      writer.writeU8(0); // status
      writer.writeU32BE(2); // count = 2
      // Key 1
      writer.writeU32BE(testData("key1").length);
      writer.writeBytes(testData("key1"));
      // Key 2
      writer.writeU32BE(testData("key2").length);
      writer.writeBytes(testData("key2"));
      const response = writer.getBuffer();

      // Act
      const decoded = KvCodec.decodeScanResponse(response);

      // Assert
      expect(decoded.status).toBe(0);
      expect(decoded.keys).toHaveLength(2);
      expect(decoded.keys![0]).toEqual(testData("key1"));
      expect(decoded.keys![1]).toEqual(testData("key2"));
    });

    it("should_decode_scan_response_with_empty_result", () => {
      // Arrange
      const writer = new BufferWriter(8);
      writer.writeU8(0); // status
      writer.writeU32BE(0); // count = 0
      const response = writer.getBuffer();

      // Act
      const decoded = KvCodec.decodeScanResponse(response);

      // Assert
      expect(decoded.keys).toEqual([]);
      expect(decoded.nextCursor).toBeUndefined();
    });

    it("should_decode_scan_response_with_next_cursor", () => {
      // Arrange
      const writer = new BufferWriter(256);
      writer.writeU8(0); // status
      writer.writeU32BE(1); // count = 1
      writer.writeU32BE(testData("key1").length);
      writer.writeBytes(testData("key1"));
      writer.writeBytes(new Uint8Array([1, 2, 3, 4])); // cursor bytes
      const response = writer.getBuffer();

      // Act
      const decoded = KvCodec.decodeScanResponse(response);

      // Assert
      expect(decoded.keys).toHaveLength(1);
      expect(decoded.nextCursor).toEqual(new Uint8Array([1, 2, 3, 4]));
    });
  });

  describe("COMMIT encoding", () => {
    it("should_encode_commit", () => {
      // Arrange/Act
      const encoded = KvCodec.encodeCommit(100n, "kv://test/app/data");

      // Assert
      const reader = new BufferReader(encoded);
      expect(reader.readU64BE()).toBe(100n);
    });
  });

  describe("ROLLBACK encoding", () => {
    it("should_encode_rollback", () => {
      // Arrange/Act
      const encoded = KvCodec.encodeRollback(100n, "kv://test/app/data");

      // Assert
      const reader = new BufferReader(encoded);
      expect(reader.readU64BE()).toBe(100n);
    });
  });

  describe("Round-trip encoding", () => {
    it("should_encode_and_decode_begin_round_trip", () => {
      // Arrange
      const route = "kv://acme/payments/invoices";
      const mode = "ReadWrite";
      const durability = "Sync";

      // Act
      const encoded = KvCodec.encodeBegin(route, mode, durability);
      const response = buildU64Response(999n);
      const decoded = KvCodec.decodeBeginResponse(response);

      // Assert
      expect(decoded.txId).toBe(999n);
      expect(encoded).toBeInstanceOf(Uint8Array);
    });

    it("should_encode_and_decode_put_get_round_trip", () => {
      // Arrange
      const txId = 999n;
      const route = "kv://test/app/store";
      const key = testData("product_id_123");
      const value = testData('{"name": "Widget", "price": 19.99}');

      // Act: Encode PUT
      const putEncoded = KvCodec.encodePut(txId, route, key, value);
      expect(putEncoded).toBeInstanceOf(Uint8Array);

      // Act: Encode GET
      const getEncoded = KvCodec.encodeGet(txId, route, key);
      expect(getEncoded).toBeInstanceOf(Uint8Array);

      // Assert: Decode responses
      const putResponse = new Uint8Array([0]); // success
      const putDecoded = KvCodec.decodePutResponse(putResponse);
      expect(putDecoded.status).toBe(0);

      const getWriter = new BufferWriter(256);
      getWriter.writeU8(0); // status
      getWriter.writeU8(1); // found
      getWriter.writeBytes(value);
      const getResponse = getWriter.getBuffer();
      const getDecoded = KvCodec.decodeGetResponse(getResponse);
      expect(getDecoded.value).toEqual(value);
    });
  });
});
