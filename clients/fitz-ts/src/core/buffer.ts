/**
 * Buffer utilities for big-endian encoding
 */

export class BufferWriter {
  private buffer: Uint8Array;
  private offset: number = 0;

  constructor(capacity: number = 4096) {
    this.buffer = new Uint8Array(capacity);
  }

  private ensureCapacity(needed: number) {
    if (this.offset + needed > this.buffer.length) {
      const newCapacity = Math.max(
        this.buffer.length * 2,
        this.offset + needed,
      );
      const newBuffer = new Uint8Array(newCapacity);
      newBuffer.set(this.buffer);
      this.buffer = newBuffer;
    }
  }

  writeU8(value: number): void {
    this.ensureCapacity(1);
    this.buffer[this.offset++] = value & 0xff;
  }

  writeU16BE(value: number): void {
    this.ensureCapacity(2);
    this.buffer[this.offset++] = (value >> 8) & 0xff;
    this.buffer[this.offset++] = value & 0xff;
  }

  writeU32BE(value: number): void {
    this.ensureCapacity(4);
    this.buffer[this.offset++] = (value >> 24) & 0xff;
    this.buffer[this.offset++] = (value >> 16) & 0xff;
    this.buffer[this.offset++] = (value >> 8) & 0xff;
    this.buffer[this.offset++] = value & 0xff;
  }

  writeU64BE(value: bigint): void {
    this.ensureCapacity(8);
    this.buffer[this.offset++] = Number((value >> 56n) & 0xffn);
    this.buffer[this.offset++] = Number((value >> 48n) & 0xffn);
    this.buffer[this.offset++] = Number((value >> 40n) & 0xffn);
    this.buffer[this.offset++] = Number((value >> 32n) & 0xffn);
    this.buffer[this.offset++] = Number((value >> 24n) & 0xffn);
    this.buffer[this.offset++] = Number((value >> 16n) & 0xffn);
    this.buffer[this.offset++] = Number((value >> 8n) & 0xffn);
    this.buffer[this.offset++] = Number(value & 0xffn);
  }

  writeBytes(data: Uint8Array): void {
    this.ensureCapacity(data.length);
    this.buffer.set(data, this.offset);
    this.offset += data.length;
  }

  writeString(str: string): void {
    const encoder = new TextEncoder();
    const encoded = encoder.encode(str);
    this.writeU32BE(encoded.length);
    this.writeBytes(encoded);
  }

  writeRoute(route: string): void {
    this.writeString(route);
  }

  getBuffer(): Uint8Array {
    return this.buffer.slice(0, this.offset);
  }

  getLength(): number {
    return this.offset;
  }

  reset(): void {
    this.offset = 0;
  }

  /**
   * Write optional U64BE value
   * Format: [u8 hasValue][u64 value if hasValue=1]
   */
  writeOptionalU64(value: bigint | undefined): void {
    if (value === undefined) {
      this.writeU8(0);
    } else {
      this.writeU8(1);
      this.writeU64BE(value);
    }
  }

  /**
   * Write optional string value
   * Format: [u8 hasValue][u32 len][string if hasValue=1]
   */
  writeOptionalString(value: string | undefined): void {
    if (value === undefined) {
      this.writeU8(0);
    } else {
      this.writeU8(1);
      this.writeString(value);
    }
  }

  /**
   * Write optional bytes value
   * Format: [u8 hasValue][u32 len][bytes if hasValue=1]
   */
  writeOptionalBytes(value: Uint8Array | undefined): void {
    if (value === undefined) {
      this.writeU8(0);
    } else {
      this.writeU8(1);
      this.writeU32BE(value.length);
      this.writeBytes(value);
    }
  }
}

export class BufferReader {
  private buffer: Uint8Array;
  private offset: number = 0;

  constructor(buffer: Uint8Array) {
    this.buffer = buffer;
  }

  readU8(): number {
    if (this.offset >= this.buffer.length) {
      throw new Error("Buffer overflow: cannot read U8");
    }
    return this.buffer[this.offset++];
  }

  readU16BE(): number {
    if (this.offset + 2 > this.buffer.length) {
      throw new Error("Buffer overflow: cannot read U16BE");
    }
    const value =
      (this.buffer[this.offset] << 8) | this.buffer[this.offset + 1];
    this.offset += 2;
    return value;
  }

  readU32BE(): number {
    if (this.offset + 4 > this.buffer.length) {
      throw new Error("Buffer overflow: cannot read U32BE");
    }
    const value =
      (this.buffer[this.offset] << 24) |
      (this.buffer[this.offset + 1] << 16) |
      (this.buffer[this.offset + 2] << 8) |
      this.buffer[this.offset + 3];
    this.offset += 4;
    return value >>> 0; // Ensure unsigned
  }

  readU64BE(): bigint {
    if (this.offset + 8 > this.buffer.length) {
      throw new Error("Buffer overflow: cannot read U64BE");
    }
    const high =
      ((BigInt(this.buffer[this.offset]) << 24n) |
        (BigInt(this.buffer[this.offset + 1]) << 16n) |
        (BigInt(this.buffer[this.offset + 2]) << 8n) |
        BigInt(this.buffer[this.offset + 3])) &
      0xffffffffn;

    const low =
      ((BigInt(this.buffer[this.offset + 4]) << 24n) |
        (BigInt(this.buffer[this.offset + 5]) << 16n) |
        (BigInt(this.buffer[this.offset + 6]) << 8n) |
        BigInt(this.buffer[this.offset + 7])) &
      0xffffffffn;

    this.offset += 8;
    return (high << 32n) | low;
  }

  readBytes(count: number): Uint8Array {
    if (this.offset + count > this.buffer.length) {
      throw new Error("Buffer overflow: cannot read bytes");
    }
    const data = this.buffer.slice(this.offset, this.offset + count);
    this.offset += count;
    return data;
  }

  readString(): string {
    const length = this.readU32BE();
    const bytes = this.readBytes(length);
    const decoder = new TextDecoder();
    return decoder.decode(bytes);
  }

  readRoute(): string {
    return this.readString();
  }

  remainingBytes(): number {
    return this.buffer.length - this.offset;
  }

  isEOF(): boolean {
    return this.offset >= this.buffer.length;
  }

  getOffset(): number {
    return this.offset;
  }

  setOffset(offset: number): void {
    if (offset < 0 || offset > this.buffer.length) {
      throw new Error("Invalid offset");
    }
    this.offset = offset;
  }

  peekU8(): number {
    if (this.offset >= this.buffer.length) {
      throw new Error("Buffer overflow: cannot peek U8");
    }
    return this.buffer[this.offset];
  }

  /**
   * Read optional U64BE value
   * Format: [u8 hasValue][u64 value if hasValue=1]
   */
  readOptionalU64(): bigint | undefined {
    const hasValue = this.readU8();
    if (hasValue === 0) {
      return undefined;
    }
    return this.readU64BE();
  }

  /**
   * Read optional string value
   * Format: [u8 hasValue][u32 len][string if hasValue=1]
   */
  readOptionalString(): string | undefined {
    const hasValue = this.readU8();
    if (hasValue === 0) {
      return undefined;
    }
    return this.readString();
  }

  /**
   * Read optional bytes value
   * Format: [u8 hasValue][u32 len][bytes if hasValue=1]
   */
  readOptionalBytes(): Uint8Array | undefined {
    const hasValue = this.readU8();
    if (hasValue === 0) {
      return undefined;
    }
    const length = this.readU32BE();
    return this.readBytes(length);
  }

  /**
   * Get remaining unread bytes
   */
  remaining(): Uint8Array {
    return this.buffer.slice(this.offset);
  }
}
