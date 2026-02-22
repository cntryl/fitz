/**
 * Schedule Codec unit tests
 */

import { describe, it, expect } from "vitest";
import { ScheduleCodec } from "../../../src/domains/schedule/codec";
import { BufferWriter } from "../../../src/core/buffer";
import { testData } from "../helpers/test-utils";

describe("ScheduleCodec", () => {
  describe("CREATE encoding", () => {
    it("should_encode_create_with_route_cron_payload", () => {
      // Arrange
      const route = "schedule://acme/tasks/nightly_backup";
      const cronExpr = "0 0 * * *"; // midnight daily
      const payload = testData('{"bucket": "s3://backups"}');

      // Act
      const encoded = ScheduleCodec.encodeCreate(route, cronExpr, payload);

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
      expect(encoded.length).toBeGreaterThan(0);
    });

    it("should_encode_create_with_empty_payload", () => {
      // Arrange/Act
      const encoded = ScheduleCodec.encodeCreate(
        "schedule://test/job",
        "*/5 * * * *",
        new Uint8Array(0),
      );

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
    });
  });

  describe("CREATE decoding", () => {
    it("should_decode_create_response_with_schedule_id", () => {
      // Arrange
      const writer = new BufferWriter(64);
      writer.writeU8(0); // status = success
      writer.writeU8(1); // has_schedule_id
      writer.writeString("schedule_id_12345");
      const response = writer.getBuffer();

      // Act
      const decoded = ScheduleCodec.decodeCreateResponse(response);

      // Assert
      expect(decoded.status).toBe(0);
      expect(decoded.scheduleId).toBe("schedule_id_12345");
    });

    it("should_decode_create_response_success_without_id", () => {
      // Arrange
      const response = new Uint8Array([0]); // status = success, no id

      // Act
      const decoded = ScheduleCodec.decodeCreateResponse(response);

      // Assert
      expect(decoded.status).toBe(0);
      expect(decoded.scheduleId).toBeUndefined();
    });
  });

  describe("CANCEL encoding", () => {
    it("should_encode_cancel_with_route", () => {
      // Arrange
      const route = "schedule://acme/tasks/nightly_backup";

      // Act
      const encoded = ScheduleCodec.encodeCancel(route);

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
    });
  });

  describe("LIST encoding", () => {
    it("should_encode_list_with_offset_and_limit", () => {
      // Arrange/Act
      const encoded = ScheduleCodec.encodeList(10n, 50n);

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
    });

    it("should_encode_list_with_defaults", () => {
      // Arrange/Act
      const encoded = ScheduleCodec.encodeList();

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
    });
  });

  describe("LIST decoding", () => {
    it("should_decode_list_response_with_single_entry", () => {
      // Arrange
      const writer = new BufferWriter(128);
      writer.writeU8(0); // status
      writer.writeU64BE(1n); // totalCount = 1
      // Entry 1
      writer.writeU8(1); // hasEntry = 1
      writer.writeString("schedule://acme/job1");
      writer.writeString("0 0 * * *");
      writer.writeBytes(testData("payload1"));
      // End marker
      writer.writeU8(0); // hasEntry = 0

      const response = writer.getBuffer();

      // Act
      const decoded = ScheduleCodec.decodeListResponse(response);

      // Assert
      expect(decoded.status).toBe(0);
      expect(decoded.totalCount).toBe(1n);
      expect(decoded.entries).toHaveLength(1);
      expect(decoded.entries[0].route).toBe("schedule://acme/job1");
      expect(decoded.entries[0].cron).toBe("0 0 * * *");
    });

    it("should_decode_list_response_empty", () => {
      // Arrange
      const writer = new BufferWriter(16);
      writer.writeU8(0); // status
      writer.writeU64BE(0n); // totalCount
      writer.writeU8(0); // hasEntry = 0 (no entries)
      const response = writer.getBuffer();

      // Act
      const decoded = ScheduleCodec.decodeListResponse(response);

      // Assert
      expect(decoded.entries).toHaveLength(0);
      expect(decoded.totalCount).toBe(0n);
    });
  });

  describe("SUBSCRIBE encoding", () => {
    it("should_encode_subscribe_with_pattern", () => {
      // Arrange
      const pattern = "schedule://acme/tasks/*";

      // Act
      const encoded = ScheduleCodec.encodeSubscribe(pattern);

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
    });
  });

  describe("SUBSCRIBE decoding", () => {
    it("should_decode_subscribe_response_with_sub_id", () => {
      // Arrange
      const writer = new BufferWriter(16);
      writer.writeU8(0); // status
      writer.writeU8(1); // has_sub_id
      writer.writeU64BE(444n); // subId
      const response = writer.getBuffer();

      // Act
      const decoded = ScheduleCodec.decodeSubscribeResponse(response);

      // Assert
      expect(decoded.status).toBe(0);
      expect(decoded.subId).toBe(444n);
    });
  });

  describe("NOTIFY decoding", () => {
    it("should_decode_schedule_fire_notification", () => {
      // Arrange
      const writer = new BufferWriter(256);
      writer.writeU64BE(444n); // subId
      writer.writeBytes(testData('{"execution_id": "exec_123"}'));
      const payload = writer.getBuffer();

      // Act
      const decoded = ScheduleCodec.decodeNotification(payload);

      // Assert
      expect(decoded.subId).toBe(444n);
      expect(decoded.payload).toEqual(testData('{"execution_id": "exec_123"}'));
    });
  });

  describe("Cron expression handling", () => {
    it("should_encode_various_cron_expressions", () => {
      const cronExpressions = [
        "0 0 * * *", // Daily
        "*/5 * * * *", // Every 5 minutes
        "0 */2 * * *", // Every 2 hours
        "0 0 * * 0", // Weekly
        "0 0 1 * *", // Monthly
      ];

      for (const cron of cronExpressions) {
        const encoded = ScheduleCodec.encodeCreate(
          "schedule://test/job",
          cron,
          new Uint8Array(0),
        );
        expect(encoded).toBeInstanceOf(Uint8Array);
      }
    });
  });
});
