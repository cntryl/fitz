/**
 * Stream Codec unit tests
 */

import { describe, it, expect } from "vitest";
import { StreamCodec } from "../../../src/domains/stream/codec";
import { BufferWriter } from "../../../src/core/buffer";
import { testData } from "../helpers/test-utils";

describe("StreamCodec", () => {
  describe("BEGIN encoding", () => {
    it("should_encode_begin_with_route_and_offset", () => {
      // Arrange
      const route = "stream://acme/events/orders";
      const expectedOffset = 100n;

      // Act
      const encoded = StreamCodec.encodeBegin(route, expectedOffset);

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
      expect(encoded.length).toBeGreaterThan(0);
    });

    it("should_encode_begin_with_zero_offset", () => {
      // Arrange/Act
      const encoded = StreamCodec.encodeBegin("stream://test/data", 0n);

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
    });
  });

  describe("BEGIN decoding", () => {
    it("should_decode_begin_response_with_session_id", () => {
      // Arrange
      const writer = new BufferWriter(16);
      writer.writeU8(0); // status = success
      writer.writeU64BE(456n); // sessionId
      const response = writer.getBuffer();

      // Act
      const decoded = StreamCodec.decodeBeginResponse(response);

      // Assert
      expect(decoded.status).toBe(0);
      expect(decoded.sessionId).toBe(456n);
    });

    it("should_decode_begin_response_error", () => {
      // Arrange
      const response = new Uint8Array([1]); // error status

      // Act
      const decoded = StreamCodec.decodeBeginResponse(response);

      // Assert
      expect(decoded.status).toBe(1);
    });
  });

  describe("APPEND encoding", () => {
    it("should_encode_append_with_records", () => {
      // Arrange
      const sessionId = 456n;
      const route = "stream://test/events";
      const records = [testData("record1"), testData("record2")];

      // Act
      const encoded = StreamCodec.encodeAppend(sessionId, route, records);

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
      expect(encoded.length).toBeGreaterThan(0);
    });

    it("should_encode_append_with_empty_records", () => {
      // Arrange/Act
      const encoded = StreamCodec.encodeAppend(
        456n,
        "stream://test/events",
        [],
      );

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
    });
  });

  describe("READ encoding", () => {
    it("should_encode_read_with_offset_and_limit", () => {
      // Arrange/Act
      const encoded = StreamCodec.encodeRead("stream://test/events", 0n, 100);

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
    });

    it("should_encode_read_from_middle_with_custom_limit", () => {
      // Arrange/Act
      const encoded = StreamCodec.encodeRead("stream://test/events", 50n, 25);

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
    });
  });

  describe("READ decoding", () => {
    it("should_decode_read_response_with_records", () => {
      // Arrange
      const writer = new BufferWriter(256);
      writer.writeU8(0); // status
      writer.writeU32BE(2); // count
      // Record 1
      writer.writeU64BE(100n); // offset
      writer.writeU64BE(1234567890n); // timestamp
      writer.writeU32BE(testData("record1").length);
      writer.writeBytes(testData("record1"));
      // Record 2
      writer.writeU64BE(101n); // offset
      writer.writeU64BE(1234567891n); // timestamp
      writer.writeU32BE(testData("record2").length);
      writer.writeBytes(testData("record2"));
      const response = writer.getBuffer();

      // Act
      const decoded = StreamCodec.decodeReadResponse(response);

      // Assert
      expect(decoded.status).toBe(0);
      expect(decoded.records).toHaveLength(2);
      expect(decoded.records[0].offset).toBe(100n);
      expect(decoded.records[1].offset).toBe(101n);
    });

    it("should_decode_read_response_empty", () => {
      // Arrange
      const writer = new BufferWriter(8);
      writer.writeU8(0); // status
      writer.writeU32BE(0); // count
      const response = writer.getBuffer();

      // Act
      const decoded = StreamCodec.decodeReadResponse(response);

      // Assert
      expect(decoded.records).toHaveLength(0);
    });
  });

  describe("LAST encoding", () => {
    it("should_encode_last", () => {
      // Arrange/Act
      const encoded = StreamCodec.encodeLast("stream://test/events");

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
    });
  });
});
