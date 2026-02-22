/**
 * Frame encoding/decoding for Fitz protocol
 * Format: [MessageType (variable)][Length (u16 BE)][Payload]
 * MessageType: 0-254 = single byte, 255+ = escape 0xFF + u16 BE
 */

import { CodecError } from "../core/errors";
import { BufferWriter, BufferReader } from "../core/buffer";

export interface Frame {
  messageType: number;
  payload: Uint8Array;
}

export class FrameCodec {
  /**
   * Encode a message type (variable-length: 1-3 bytes)
   * 0-254: single byte
   * 255+: 0xFF followed by u16 BE
   */
  private static encodeMessageType(
    messageType: number,
    writer: BufferWriter,
  ): void {
    if (messageType < 0) {
      throw new CodecError(`Invalid message type: ${messageType}`);
    }
    if (messageType <= 254) {
      writer.writeU8(messageType);
    } else {
      writer.writeU8(0xff);
      writer.writeU16BE(messageType);
    }
  }

  /**
   * Decode a message type (variable-length)
   */
  private static decodeMessageType(reader: BufferReader): number {
    const firstByte = reader.readU8();
    if (firstByte === 0xff) {
      return reader.readU16BE();
    }
    return firstByte;
  }

  /**
   * Encode frame: [MessageType][Length (u16 BE)][Payload]
   */
  static encodeFrame(messageType: number, payload: Uint8Array): Uint8Array {
    const writer = new BufferWriter(payload.length + 10);

    // Encode message type
    this.encodeMessageType(messageType, writer);

    // Encode length
    writer.writeU16BE(payload.length);

    // Write payload
    writer.writeBytes(payload);

    return writer.getBuffer();
  }

  /**
   * Decode frame from buffer
   */
  static decodeFrame(buffer: Uint8Array): Frame {
    const reader = new BufferReader(buffer);

    try {
      const messageType = this.decodeMessageType(reader);
      const length = reader.readU16BE();

      if (reader.remainingBytes() < length) {
        throw new CodecError(
          `Frame incomplete: expected ${length} bytes, got ${reader.remainingBytes()}`,
        );
      }

      const payload = reader.readBytes(length);

      return { messageType, payload };
    } catch (err) {
      if (err instanceof CodecError) {
        throw err;
      }
      throw new CodecError(
        `Failed to decode frame: ${err instanceof Error ? err.message : String(err)}`,
      );
    }
  }

  /**
   * Calculate encoded size of a message type
   */
  static getMessageTypeSize(messageType: number): number {
    if (messageType <= 254) {
      return 1;
    }
    return 3; // 0xFF + u16 BE
  }

  /**
   * Calculate total frame size
   */
  static calculateFrameSize(
    messageType: number,
    payloadLength: number,
  ): number {
    return this.getMessageTypeSize(messageType) + 2 + payloadLength; // +2 for length field
  }
}

/**
 * Helper for streaming frame parsing
 */
export class FrameParser {
  private buffer: Uint8Array = new Uint8Array(0);
  private offset: number = 0;
  private messageType: number | null = null;
  private payloadLength: number | null = null;
  private state: "reading_type" | "reading_length" | "reading_payload" =
    "reading_type";

  /**
   * Feed data into the parser and try to extract complete frames
   */
  parseFrames(data: Uint8Array): Frame[] {
    this.appendData(data);

    const frames: Frame[] = [];

    while (true) {
      if (this.state === "reading_type") {
        if (!this.tryReadMessageType()) break;
        this.state = "reading_length";
      }

      if (this.state === "reading_length") {
        if (!this.tryReadLength()) break;
        this.state = "reading_payload";
      }

      if (this.state === "reading_payload") {
        if (!this.tryReadPayload(frames)) break;
        this.state = "reading_type";
      }
    }

    // Compact buffer by removing consumed data
    this.compactBuffer();

    return frames;
  }

  private appendData(data: Uint8Array): void {
    const newBuffer = new Uint8Array(this.buffer.length + data.length);
    newBuffer.set(this.buffer);
    newBuffer.set(data, this.buffer.length);
    this.buffer = newBuffer;
    this.offset = 0;
  }

  private tryReadMessageType(): boolean {
    if (this.offset >= this.buffer.length) return false;

    const firstByte = this.buffer[this.offset];

    if (firstByte === 0xff) {
      // Extended message type (3 bytes total)
      if (this.offset + 3 > this.buffer.length) return false;

      const high = this.buffer[this.offset + 1];
      const low = this.buffer[this.offset + 2];
      this.messageType = (high << 8) | low;
      this.offset += 3;
    } else {
      // Short message type (1 byte)
      this.messageType = firstByte;
      this.offset += 1;
    }

    return true;
  }

  private tryReadLength(): boolean {
    if (this.offset + 2 > this.buffer.length) return false;

    const high = this.buffer[this.offset];
    const low = this.buffer[this.offset + 1];
    this.payloadLength = (high << 8) | low;
    this.offset += 2;

    return true;
  }

  private tryReadPayload(frames: Frame[]): boolean {
    if (!this.messageType || this.payloadLength === null) return false;

    if (this.offset + this.payloadLength > this.buffer.length) return false;

    const payload = this.buffer.slice(
      this.offset,
      this.offset + this.payloadLength,
    );
    this.offset += this.payloadLength;

    frames.push({
      messageType: this.messageType,
      payload,
    });

    this.messageType = null;
    this.payloadLength = null;

    return true;
  }

  private compactBuffer(): void {
    if (this.offset > 0) {
      this.buffer = this.buffer.slice(this.offset);
      this.offset = 0;
    }
  }

  hasCompleteFrame(): boolean {
    if (this.state === "reading_type") {
      return false;
    }
    if (this.state === "reading_length") {
      return false;
    }
    if (this.state === "reading_payload" && this.payloadLength !== null) {
      return this.offset + this.payloadLength <= this.buffer.length;
    }
    return false;
  }
}
