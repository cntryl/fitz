/**
 * Stream domain type definitions
 * Per CLIENT_SPEC.md and fitz-go/internal/domains/stream/stream.go
 *
 * Stream uses session-based transactional semantics:
 * 1. Begin() returns a session with server-assigned sessionID
 * 2. Append() on session (no offset tracking needed)
 * 3. Commit() or Rollback() to finalize
 */

/**
 * Stream record with offset, timestamp, and payload
 */
export interface StreamRecord {
  offset: bigint;
  timestamp: bigint;
  body: Uint8Array;
}

/**
 * Stream metadata
 */
export interface StreamMetadata {
  firstOffset: bigint;
  lastOffset: bigint;
  recordCount: bigint;
}

/**
 * Stream session for write operations
 * Obtained from StreamClient.begin()
 */
export interface StreamSession {
  /**
   * Send a record to the stream
   * Returns the assigned offset
   */
  send(body: Uint8Array): Promise<bigint>;

  /**
   * Commit the write session and make sends durable
   */
  commit(): Promise<void>;

  /**
   * Rollback and discard uncommitted sends
   */
  rollback(): Promise<void>;

  /**
   * Check if session is still open
   */
  isOpen(): boolean;

  /**
   * Get the session ID
   */
  getSessionId(): bigint;
}

/**
 * Stream operation status codes
 */
export enum StreamStatus {
  Ok = 0,
  StreamNotFound = 1,
  OffsetOutOfRange = 2,
  InvalidOffset = 3,
  StreamFull = 4,
  SessionNotFound = 5,
  SessionClosed = 6,
  ExpectedOffsetMismatch = 7,
}
