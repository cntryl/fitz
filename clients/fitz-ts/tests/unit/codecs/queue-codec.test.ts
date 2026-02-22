/**
 * Queue Codec unit tests
 */

import { describe, it, expect } from "vitest";
import { QueueCodec } from "../../../src/domains/queue/codec";
import { BufferWriter } from "../../../src/core/buffer";
import { testData } from "../helpers/test-utils";

describe("QueueCodec", () => {
  describe("ENQUEUE encoding", () => {
    it("should_encode_enqueue_with_body", () => {
      // Arrange
      const route = "queue://acme/tasks/inbox";
      const body = testData('{"task": "process_order"}');

      // Act
      const encoded = QueueCodec.encodeEnqueue(route, body);

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
      expect(encoded.length).toBeGreaterThan(0);
    });

    it("should_encode_enqueue_with_empty_body", () => {
      // Arrange/Act
      const encoded = QueueCodec.encodeEnqueue(
        "queue://test/tasks",
        new Uint8Array(0),
      );

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
    });
  });

  describe("ENQUEUE decoding", () => {
    it("should_decode_enqueue_response_with_message_id", () => {
      // Arrange
      const writer = new BufferWriter(16);
      writer.writeU8(0); // status = success
      writer.writeU64BE(999n); // messageId
      const response = writer.getBuffer();

      // Act
      const decoded = QueueCodec.decodeEnqueueResponse(response);

      // Assert
      expect(decoded.status).toBe(0);
      expect(decoded.messageId).toBe(999n);
    });
  });

  describe("RECEIVE encoding", () => {
    it("should_encode_receive_with_route_and_ttl", () => {
      // Arrange
      const route = "queue://acme/jobs/tasks";
      const ttlSecs = 30;

      // Act
      const encoded = QueueCodec.encodeReceive(route, ttlSecs);

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
    });
  });

  describe("RECEIVE decoding", () => {
    it("should_decode_receive_response_with_item", () => {
      // Arrange
      const writer = new BufferWriter(256);
      writer.writeU8(0); // status
      writer.writeU32BE(1); // leaseCount = 1
      writer.writeU64BE(100n); // itemId
      writer.writeU64BE(777n); // token
      writer.writeU32BE(testData('{"msg": "hello"}').length);
      writer.writeBytes(testData('{"msg": "hello"}'));
      const response = writer.getBuffer();

      // Act
      const decoded = QueueCodec.decodeReserveResponse(response);

      // Assert
      expect(decoded.status).toBe(0);
      expect(decoded.items).toHaveLength(1);
      expect(decoded.items[0].id).toBe(100n);
      expect(decoded.items[0].token).toBe(777n);
    });

    it("should_decode_reserve_response_no_item", () => {
      // Arrange
      const response = new Uint8Array([0]); // status, no items

      // Act
      const decoded = QueueCodec.decodeReserveResponse(response);

      // Assert
      expect(decoded.status).toBe(0);
      expect(decoded.items).toHaveLength(0);
    });
  });

  describe("EXTEND encoding", () => {
    it("should_encode_extend_with_token_and_ttl", () => {
      // Arrange
      const route = "queue://test/tasks";
      const messageId = 100n;
      const token = 777n;
      const ttlSecs = 600;

      // Act
      const encoded = QueueCodec.encodeExtend(route, messageId, token, ttlSecs);

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
    });
  });

  describe("COMPLETE encoding", () => {
    it("should_encode_complete_with_token", () => {
      // Arrange
      const route = "queue://test/tasks";
      const messageId = 100n;
      const token = 777n;

      // Act
      const encoded = QueueCodec.encodeComplete(route, messageId, token);

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
    });
  });

  describe("SUBSCRIBE encoding", () => {
    it("should_encode_subscribe_with_pattern", () => {
      // Arrange
      const pattern = "queue://acme/tasks/*";

      // Act
      const encoded = QueueCodec.encodeSubscribe(pattern);

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
    });
  });

  describe("SUBSCRIBE decoding", () => {
    it("should_decode_subscribe_response_with_sub_id", () => {
      // Arrange
      const writer = new BufferWriter(16);
      writer.writeU8(0); // status
      writer.writeU64BE(555n); // subId (no has_sub_id flag)
      const response = writer.getBuffer();

      // Act
      const decoded = QueueCodec.decodeSubscribeResponse(response);

      // Assert
      expect(decoded.status).toBe(0);
      expect(decoded.subId).toBe(555n);
    });
  });
});
