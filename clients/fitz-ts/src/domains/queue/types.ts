/**
 * Queue domain types
 * Per fitz-go/internal/domains/queue/queue.go
 */

import { Connection } from "../../client/connection";
import { QueueCodec } from "./codec";
import { QueueError } from "../../core/errors";
import { MSG_QUEUE_EXTEND, MSG_QUEUE_COMPLETE } from "../../frame/types";

/**
 * Queue item represents a received (reserved) queue message
 * Encapsulates route and token for extend/ack operations
 */
export class QueueItem {
  readonly id: bigint;
  readonly token: bigint;
  readonly body: Uint8Array;

  private route: string;
  private connection: Connection;

  constructor(
    id: bigint,
    token: bigint,
    body: Uint8Array,
    route: string,
    connection: Connection,
  ) {
    this.id = id;
    this.token = token;
    this.body = body;
    this.route = route;
    this.connection = connection;
  }

  /**
   * Extend the lease on this queue item
   * @param leaseSecs Lease duration in seconds
   */
  async extend(leaseSecs: number): Promise<void> {
    const payload = QueueCodec.encodeExtend(
      this.route,
      this.id,
      this.token,
      leaseSecs,
    );
    const response = await this.connection.request(MSG_QUEUE_EXTEND, payload);
    const decoded = QueueCodec.decodeExtendResponse(response);

    if (decoded.status !== QueueStatus.Ok) {
      const statusName =
        QueueStatus[decoded.status] || `Unknown(${decoded.status})`;
      throw new QueueError(
        `EXTEND failed: ${statusName}`,
        statusName,
        decoded.status,
      );
    }
  }

  /**
   * Acknowledge (complete) processing of this queue item
   * Removes the message from the queue
   */
  async ack(): Promise<void> {
    const payload = QueueCodec.encodeAck(this.route, this.id, this.token);
    const response = await this.connection.request(MSG_QUEUE_COMPLETE, payload);
    const decoded = QueueCodec.decodeAckResponse(response);

    if (decoded.status !== QueueStatus.Ok) {
      const statusName =
        QueueStatus[decoded.status] || `Unknown(${decoded.status})`;
      throw new QueueError(
        `ACK failed: ${statusName}`,
        statusName,
        decoded.status,
      );
    }
  }
}

/**
 * Availability notification from queue
 */
export interface AvailabilityNotification {
  route: string;
}

/**
 * Handler for availability notifications
 */
export type AvailabilityHandler = (
  notification: AvailabilityNotification,
) => void | Promise<void>;

/**
 * Queue availability subscription
 */
export class QueueSubscription {
  private subId: bigint;
  private pattern: string;
  private unsubscribeFn: (subId: bigint) => Promise<void>;

  constructor(
    subId: bigint,
    pattern: string,
    unsubscribeFn: (subId: bigint) => Promise<void>,
  ) {
    this.subId = subId;
    this.pattern = pattern;
    this.unsubscribeFn = unsubscribeFn;
  }

  /**
   * Unsubscribe from availability notifications
   */
  async unsubscribe(): Promise<void> {
    await this.unsubscribeFn(this.subId);
  }

  getSubId(): bigint {
    return this.subId;
  }

  getPattern(): string {
    return this.pattern;
  }
}

/**
 * Queue operation status codes
 */
export enum QueueStatus {
  Ok = 0,
  QueueNotFound = 1,
  MessageNotFound = 2,
  InvalidToken = 3,
  QueueFull = 4,
  InvalidDelay = 5,
}

/**
 * Options for send operation
 */
export interface SendOptions {
  priority?: number;
  delayMs?: number;
  ttlMs?: number;
}

/**
 * Internal codec response types
 */
export interface QueueSendResponse {
  status: number;
  messageId?: bigint;
}

export interface QueueReceiveResponse {
  status: number;
  items?: Array<{
    id: bigint;
    token: bigint;
    body: Uint8Array;
  }>;
  cursor?: Uint8Array;
}

export interface QueueExtendResponse {
  status: number;
}

export interface QueueAckResponse {
  status: number;
}

export interface QueueSubscribeResponse {
  status: number;
  subId?: bigint;
}

export interface QueueUnsubscribeResponse {
  status: number;
}
