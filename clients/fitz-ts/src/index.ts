/**
 * Fitz TypeScript Client - Main Entry Point
 */

// Core exports
export { Client } from "./client/client";
export { Connection } from "./client/connection";

// Types and errors
export type { ClientConfig, TransportType, WriteOptions } from "./core/types";
export {
  FitzError,
  TransportError,
  ConnectionError,
  AuthenticationError,
  TimeoutError,
  ProtocolError,
  CodecError,
  KvError,
  QueueError,
  NoticeError,
  RpcError,
  LeaseError,
  StreamError,
  ScheduleError,
} from "./core/errors";

// Domain clients
export { KvClient, KvTransaction } from "./domains/kv/client";
export type {
  TxMode,
  DurabilityMode,
  WriteOptions as KvWriteOptions,
} from "./domains/kv/types";

export { QueueClient } from "./domains/queue/client";
export { RpcClient } from "./domains/rpc/client";
export { LeaseClient } from "./domains/lease/client";
export { NoticeClient } from "./domains/notice/client";
export { StreamClient } from "./domains/stream/client";
export { ScheduleClient } from "./domains/schedule/client";

// Buffer utilities
export { BufferWriter, BufferReader } from "./core/buffer";

// Frame utilities
export { FrameCodec, FrameParser } from "./frame/codec";
export * from "./frame/types";

// Transport
export { createTransport } from "./transport/factory";
export type { Transport, TransportOptions } from "./transport/types";
