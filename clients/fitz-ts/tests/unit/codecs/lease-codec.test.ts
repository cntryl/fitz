/**
 * Lease Codec unit tests
 */

import { describe, it, expect } from "vitest";
import { LeaseCodec } from "../../../src/domains/lease/codec";
import { BufferWriter } from "../../../src/core/buffer";
import { testData as _testData } from "../helpers/test-utils";

describe("LeaseCodec", () => {
  describe("ACQUIRE encoding", () => {
    it("should_encode_acquire_with_route_and_ttl", () => {
      // Arrange
      const route = "lease://acme/resources/db_connection";
      const ttlSecs = 60;

      // Act
      const encoded = LeaseCodec.encodeAcquire(route, ttlSecs);

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
      expect(encoded.length).toBeGreaterThan(0);
    });

    it("should_encode_acquire_with_different_ttl_values", () => {
      const testCases = [1, 60, 3600, 86400];
      for (const ttl of testCases) {
        const encoded = LeaseCodec.encodeAcquire("lease://test/resource", ttl);
        expect(encoded).toBeInstanceOf(Uint8Array);
      }
    });
  });

  describe("ACQUIRE decoding", () => {
    it("should_decode_acquire_response_with_token", () => {
      // Arrange
      const writer = new BufferWriter(32);
      writer.writeU8(0); // response_type = Acquired
      writer.writeU64BE(888n); // token
      const response = writer.getBuffer();

      // Act
      const decoded = LeaseCodec.decodeAcquireResponse(response);

      // Assert
      expect(decoded.token).toBe(888n);
      expect(decoded.expiresAt).toBe(0n); // Computed by client
    });
  });

  describe("RENEW encoding", () => {
    it("should_encode_renew_with_token_and_new_ttl", () => {
      // Arrange
      const route = "lease://test/resource";
      const token = 888n;
      const newTtl = 120;

      // Act
      const encoded = LeaseCodec.encodeRenew(route, token, newTtl);

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
    });
  });

  describe("RELEASE encoding", () => {
    it("should_encode_release_with_route_and_token", () => {
      // Arrange
      const route = "lease://test/resource";
      const token = 888n;

      // Act
      const encoded = LeaseCodec.encodeRelease(route, token);

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
    });
  });

  describe("SUBSCRIBE encoding", () => {
    it("should_encode_subscribe_with_pattern", () => {
      // Arrange
      const pattern = "lease://acme/resources/*";

      // Act
      const encoded = LeaseCodec.encodeSubscribe(pattern);

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
    });
  });

  describe("SUBSCRIBE decoding", () => {
    it("should_decode_subscribe_response_with_sub_id", () => {
      // Arrange
      const writer = new BufferWriter(16);
      writer.writeU64BE(222n); // subId (no has_sub_id flag)
      const response = writer.getBuffer();

      // Act
      const decoded = LeaseCodec.decodeSubscribeResponse(response);

      // Assert
      expect(decoded.status).toBe(0);
      expect(decoded.subId).toBe(222n);
    });
  });

  describe("NOTIFY decoding", () => {
    it("should_decode_notification_payload", () => {
      // Arrange
      const writer = new BufferWriter(32);
      writer.writeU64BE(222n); // subId
      writer.writeU8(0); // change type = released
      writer.writeU64BE(888n); // token
      const payload = writer.getBuffer();

      // Act
      const decoded = LeaseCodec.decodeNotification(payload);

      // Assert
      expect(decoded.subId).toBe(222n);
    });
  });
});
