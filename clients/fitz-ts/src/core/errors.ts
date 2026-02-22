/**
 * Error types for Fitz client
 */

export class FitzError extends Error {
  code: string;
  domainCode?: number;

  constructor(message: string, code: string, domainCode?: number) {
    super(message);
    this.name = "FitzError";
    this.code = code;
    this.domainCode = domainCode;
    Object.setPrototypeOf(this, FitzError.prototype);
  }
}

export class TransportError extends FitzError {
  constructor(message: string) {
    super(message, "TRANSPORT_ERROR");
    this.name = "TransportError";
    Object.setPrototypeOf(this, TransportError.prototype);
  }
}

export class ConnectionError extends FitzError {
  constructor(message: string) {
    super(message, "CONNECTION_ERROR");
    this.name = "ConnectionError";
    Object.setPrototypeOf(this, ConnectionError.prototype);
  }
}

export class AuthenticationError extends FitzError {
  constructor(message: string) {
    super(message, "AUTH_ERROR");
    this.name = "AuthenticationError";
    Object.setPrototypeOf(this, AuthenticationError.prototype);
  }
}

export class TimeoutError extends FitzError {
  constructor(message: string) {
    super(message, "TIMEOUT");
    this.name = "TimeoutError";
    Object.setPrototypeOf(this, TimeoutError.prototype);
  }
}

export class ProtocolError extends FitzError {
  constructor(message: string, domainCode?: number) {
    super(message, "PROTOCOL_ERROR", domainCode);
    this.name = "ProtocolError";
    Object.setPrototypeOf(this, ProtocolError.prototype);
  }
}

export class CodecError extends FitzError {
  constructor(message: string) {
    super(message, "CODEC_ERROR");
    this.name = "CodecError";
    Object.setPrototypeOf(this, CodecError.prototype);
  }
}

// Domain-specific errors
export class KvError extends FitzError {
  constructor(message: string, code: string, domainCode?: number) {
    super(message, `KV_${code}`, domainCode);
    this.name = "KvError";
    Object.setPrototypeOf(this, KvError.prototype);
  }
}

export class QueueError extends FitzError {
  constructor(message: string, code: string, domainCode?: number) {
    super(message, `QUEUE_${code}`, domainCode);
    this.name = "QueueError";
    Object.setPrototypeOf(this, QueueError.prototype);
  }
}

export class NoticeError extends FitzError {
  constructor(message: string, code: string, domainCode?: number) {
    super(message, `NOTICE_${code}`, domainCode);
    this.name = "NoticeError";
    Object.setPrototypeOf(this, NoticeError.prototype);
  }
}

export class RpcError extends FitzError {
  constructor(message: string, code: string, domainCode?: number) {
    super(message, `RPC_${code}`, domainCode);
    this.name = "RpcError";
    Object.setPrototypeOf(this, RpcError.prototype);
  }
}

export class LeaseError extends FitzError {
  constructor(message: string, code: string, domainCode?: number) {
    super(message, `LEASE_${code}`, domainCode);
    this.name = "LeaseError";
    Object.setPrototypeOf(this, LeaseError.prototype);
  }
}

export class StreamError extends FitzError {
  constructor(message: string, code: string, domainCode?: number) {
    super(message, `STREAM_${code}`, domainCode);
    this.name = "StreamError";
    Object.setPrototypeOf(this, StreamError.prototype);
  }
}

export class ScheduleError extends FitzError {
  constructor(message: string, code: string, domainCode?: number) {
    super(message, `SCHEDULE_${code}`, domainCode);
    this.name = "ScheduleError";
    Object.setPrototypeOf(this, ScheduleError.prototype);
  }
}
