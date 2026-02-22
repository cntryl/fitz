/**
 * Fitz message type constants
 * Per CLIENT_SPEC.md and fitz-go/internal/protocol/message_types.go
 *
 * Format: [MessageType (variable 1-3 bytes)][Length (u16 BE)][Payload]
 * MessageType 0-254: single byte
 * MessageType 255+: escape byte 0xFF + u16 BE
 *
 * Key Pattern: Single MessageType per operation (request AND response use same type)
 * Responses matched via FIFO ordering in multiplexer, NOT separate message types
 */

// Control messages (0-99)
export const MSG_CONNECT = 1;

// KV Domain (100-199)
export const MSG_KV_BEGIN = 100;
export const MSG_KV_COMMIT = 101;
export const MSG_KV_ROLLBACK = 102;
export const MSG_KV_GET = 103;
export const MSG_KV_PUT = 104;
export const MSG_KV_INSERT = 105;
export const MSG_KV_DELETE = 106;
export const MSG_KV_DELETE_RANGE = 107;
export const MSG_KV_SCAN = 108;

// Queue Domain (200-299)
// Note: 201 = ENQUEUE_BATCH is reserved per CLIENT_SPEC; do not use
export const MSG_QUEUE_ENQUEUE = 200;
export const MSG_QUEUE_RESERVE = 202;
export const MSG_QUEUE_EXTEND = 203;
export const MSG_QUEUE_COMPLETE = 204;
export const MSG_QUEUE_SUBSCRIBE = 207;
export const MSG_QUEUE_UNSUBSCRIBE = 208;
export const MSG_QUEUE_NOTIFY = 209; // Server -> Client only

// RPC Domain (300-399)
export const MSG_RPC_SUBSCRIBE_WORKER = 300;
export const MSG_RPC_UNSUBSCRIBE_WORKER = 301;
export const MSG_RPC_REQUEST = 302;
export const MSG_RPC_RESPONSE = 303;
export const MSG_RPC_ACK = 304;

// Lease Domain (400-499)
export const MSG_LEASE_ACQUIRE = 400;
export const MSG_LEASE_RENEW = 401;
export const MSG_LEASE_RELEASE = 402;
export const MSG_LEASE_QUERY = 403;
export const MSG_LEASE_SUBSCRIBE = 407;
export const MSG_LEASE_UNSUBSCRIBE = 408;
export const MSG_LEASE_NOTIFY = 409; // Server -> Client only

// Notice Domain (500-599)
export const MSG_NOTICE_PUBLISH = 500;
export const MSG_NOTICE_SUBSCRIBE = 501;
export const MSG_NOTICE_UNSUBSCRIBE = 502;
export const MSG_NOTICE_UNSUBSCRIBE_ALL = 503;
export const MSG_NOTICE_NOTIFY = 504; // Server -> Client only

// Stream Domain (600-699)
export const MSG_STREAM_BEGIN = 600;
export const MSG_STREAM_APPEND = 601;
export const MSG_STREAM_COMMIT = 602;
export const MSG_STREAM_ROLLBACK = 603;
export const MSG_STREAM_READ = 604;
export const MSG_STREAM_LAST = 605;
export const MSG_STREAM_GET_METADATA = 606;
export const MSG_STREAM_SUBSCRIBE = 607;
export const MSG_STREAM_UNSUBSCRIBE = 608;
export const MSG_STREAM_NOTIFY = 609; // Server -> Client only

// Schedule Domain (700-799)
export const MSG_SCHEDULE_CREATE = 700;
export const MSG_SCHEDULE_CANCEL = 701;
export const MSG_SCHEDULE_LIST = 702;
export const MSG_SCHEDULE_SUBSCRIBE = 703;
export const MSG_SCHEDULE_UNSUBSCRIBE = 704;
export const MSG_SCHEDULE_NOTIFY = 705; // Server -> Client only

/**
 * Returns the domain name for a given MessageType
 * Used for routing frames to domain handlers
 */
export function routeDomain(msgType: number): string {
  if (msgType >= 100 && msgType <= 199) return "kv";
  if (msgType >= 200 && msgType <= 299) return "queue";
  if (msgType >= 300 && msgType <= 399) return "rpc";
  if (msgType >= 400 && msgType <= 499) return "lease";
  if (msgType >= 500 && msgType <= 599) return "notice";
  if (msgType >= 600 && msgType <= 699) return "stream";
  if (msgType >= 700 && msgType <= 799) return "schedule";
  return "unknown";
}

/**
 * Notification message types (server -> client push)
 */
export const NOTIFICATION_TYPES = new Set([
  MSG_QUEUE_NOTIFY,
  MSG_LEASE_NOTIFY,
  MSG_NOTICE_NOTIFY,
  MSG_STREAM_NOTIFY,
  MSG_SCHEDULE_NOTIFY,
]);
