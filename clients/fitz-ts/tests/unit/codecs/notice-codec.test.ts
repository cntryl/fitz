/**
 * Notice Codec unit tests
 */

import { describe, it, expect } from "vitest";
import { NoticeCodec } from "../../../src/domains/notice/codec";
import { BufferWriter } from "../../../src/core/buffer";
import { testData } from "../helpers/test-utils";

describe("NoticeCodec", () => {
  describe("PUBLISH encoding", () => {
    it("should_encode_publish_with_route_and_body", () => {
      // Arrange
      const route = "notice://acme/alerts/system";
      const body = testData('{"level": "warning", "msg": "high cpu"}');

      // Act
      const encoded = NoticeCodec.encodePublish(route, body);

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
      expect(encoded.length).toBeGreaterThan(0);
    });

    it("should_encode_publish_with_empty_body", () => {
      // Arrange/Act
      const encoded = NoticeCodec.encodePublish(
        "notice://test/events",
        new Uint8Array(0),
      );

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
    });
  });

  describe("SUBSCRIBE encoding", () => {
    it("should_encode_subscribe_with_pattern", () => {
      // Arrange
      const pattern = "notice://acme/alerts/*";

      // Act
      const encoded = NoticeCodec.encodeSubscribe(pattern);

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
    });

    it("should_encode_subscribe_with_exact_route", () => {
      // Arrange/Act
      const encoded = NoticeCodec.encodeSubscribe(
        "notice://acme/alerts/critical",
      );

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
    });
  });

  describe("SUBSCRIBE decoding", () => {
    it("should_decode_subscribe_response_with_sub_id", () => {
      // Arrange
      const writer = new BufferWriter(16);
      writer.writeU8(1); // has_sub_id = 1
      writer.writeU64BE(333n); // subId
      const response = writer.getBuffer();

      // Act
      const decoded = NoticeCodec.decodeSubscribeResponse(response);

      // Assert
      expect(decoded.status).toBe(0);
      expect(decoded.subId).toBe(333n);
    });

    it("should_decode_subscribe_response_without_sub_id", () => {
      // Arrange
      const response = new Uint8Array([0]); // has_sub_id = 0

      // Act & Assert - should throw error per codec implementation
      expect(() => NoticeCodec.decodeSubscribeResponse(response)).toThrow(
        "missing subscription_id",
      );
    });
  });

  describe("UNSUBSCRIBE encoding", () => {
    it("should_encode_unsubscribe_with_pattern", () => {
      // Arrange
      const pattern = "notice://acme/alerts/*";

      // Act
      const encoded = NoticeCodec.encodeUnsubscribe(pattern);

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
    });
  });

  describe("NOTIFY decoding", () => {
    it("should_decode_notification_with_route_and_body", () => {
      // Arrange
      const writer = new BufferWriter(256);
      writer.writeU64BE(333n); // subId
      writer.writeString("notice://acme/alerts/cpu");
      writer.writeU32BE(testData('{"cpu": 95.5}').length);
      writer.writeBytes(testData('{"cpu": 95.5}'));
      const payload = writer.getBuffer();

      // Act
      const decoded = NoticeCodec.decodeNotification(payload);

      // Assert
      expect(decoded.subId).toBe(333n);
      expect(decoded.route).toBe("notice://acme/alerts/cpu");
      expect(decoded.body).toEqual(testData('{"cpu": 95.5}'));
    });

    it("should_decode_notification_with_empty_body", () => {
      // Arrange
      const writer = new BufferWriter(64);
      writer.writeU64BE(333n);
      writer.writeString("notice://test/event");
      writer.writeU32BE(0);
      writer.writeBytes(new Uint8Array(0));
      const payload = writer.getBuffer();

      // Act
      const decoded = NoticeCodec.decodeNotification(payload);

      // Assert
      expect(decoded.subId).toBe(333n);
      expect(decoded.body).toEqual(new Uint8Array(0));
    });
  });
});
