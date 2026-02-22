/**
 * Queue domain client
 * Per fitz-go/internal/domains/queue/queue.go
 */

import { DomainClient } from "../base";
import { QueueCodec } from "./codec";
import {
  QueueItem,
  QueueStatus,
  SendOptions,
  AvailabilityHandler,
  AvailabilityNotification,
  QueueSubscription,
} from "./types";
import {
  MSG_QUEUE_ENQUEUE,
  MSG_QUEUE_RESERVE,
  MSG_QUEUE_SUBSCRIBE,
  MSG_QUEUE_UNSUBSCRIBE,
  MSG_QUEUE_NOTIFY,
} from "../../frame/types";
import { QueueError } from "../../core/errors";

export class QueueClient extends DomainClient {
  private subscriptions: Map<
    bigint,
    { handler: AvailabilityHandler; pattern: string }
  > = new Map();
  private notificationHandlerRegistered = false;

  /**
   * Send a message to the queue
   * @param route Queue route (e.g., "queue://realm/area/tasks")
   * @param body Message body
   * @param options Send options (delay, priority, TTL)
   * @returns Server-assigned message ID
   */
  async send(
    route: string,
    body: Uint8Array,
    options?: SendOptions,
  ): Promise<bigint> {
    const payload = QueueCodec.encodeSend(route, body, options);
    const response = await this.request(MSG_QUEUE_ENQUEUE, payload);
    const decoded = QueueCodec.decodeSendResponse(response);

    this.checkStatus(decoded.status, "SEND");

    if (decoded.messageId === undefined) {
      throw new QueueError(
        "SEND response missing messageId",
        "MISSING_MESSAGE_ID",
      );
    }

    return decoded.messageId;
  }

  /**
   * Receive messages from the queue with leasing
   * @param route Queue route
   * @param leaseSeconds Lease duration in seconds
   * @param batchSize Maximum number of messages to receive (default: 1)
   * @param waitSeconds How long to wait for messages (default: 0 for immediate)
   * @returns Array of QueueItem objects with extend() and ack() methods
   */
  async receive(
    route: string,
    leaseSeconds: number,
    batchSize: number = 1,
    waitSeconds: number = 0,
  ): Promise<QueueItem[]> {
    const payload = QueueCodec.encodeReceive(
      route,
      leaseSeconds,
      batchSize,
      waitSeconds,
    );
    const response = await this.request(MSG_QUEUE_RESERVE, payload);
    const decoded = QueueCodec.decodeReceiveResponse(response);

    this.checkStatus(decoded.status, "RECEIVE");

    if (!decoded.items || decoded.items.length === 0) {
      return [];
    }

    return decoded.items.map(
      (item) =>
        new QueueItem(item.id, item.token, item.body, route, this.connection),
    );
  }

  /**
   * Subscribe to availability notifications
   * @param pattern Pattern to match (e.g., "queue://realm/area/*")
   * @param handler Handler to call when messages become available
   * @returns Subscription object with unsubscribe() method
   */
  async subscribe(
    pattern: string,
    handler: AvailabilityHandler,
  ): Promise<QueueSubscription> {
    this.initNotificationHandler();

    const payload = QueueCodec.encodeSubscribe(pattern);
    const response = await this.request(MSG_QUEUE_SUBSCRIBE, payload);
    const decoded = QueueCodec.decodeSubscribeResponse(response);

    this.checkStatus(decoded.status, "SUBSCRIBE");

    if (decoded.subId === undefined) {
      throw new QueueError(
        "SUBSCRIBE response missing subId",
        "MISSING_SUB_ID",
      );
    }

    const subId = decoded.subId;
    this.subscriptions.set(subId, { handler, pattern });

    const unsubscribeFn = async (id: bigint) => {
      await this.unsubscribe(id);
    };

    return new QueueSubscription(subId, pattern, unsubscribeFn);
  }

  /**
   * Internal method to unsubscribe from notifications
   */
  private async unsubscribe(subId: bigint): Promise<void> {
    this.subscriptions.delete(subId);

    const payload = QueueCodec.encodeUnsubscribe(subId);
    const response = await this.request(MSG_QUEUE_UNSUBSCRIBE, payload);
    const decoded = QueueCodec.decodeUnsubscribeResponse(response);

    this.checkStatus(decoded.status, "UNSUBSCRIBE");
  }

  /**
   * Initialize notification handler (lazy, on first subscribe)
   */
  private initNotificationHandler(): void {
    if (this.notificationHandlerRegistered) {
      return;
    }
    this.notificationHandlerRegistered = true;

    this.connection.registerNotificationHandler(
      MSG_QUEUE_NOTIFY,
      (payload: Uint8Array) => {
        try {
          const { subId, route } = QueueCodec.decodeNotification(payload);
          const subscription = this.subscriptions.get(subId);

          if (!subscription) {
            console.warn(
              `No handler registered for queue subscription ${subId}`,
            );
            return;
          }

          const notification: AvailabilityNotification = { route };

          // Call handler asynchronously to avoid blocking dispatch loop
          Promise.resolve(subscription.handler(notification)).catch((err) => {
            console.error("Queue availability handler error:", err);
          });
        } catch (err) {
          console.error("Queue notification decode error:", err);
        }
      },
    );
  }

  /**
   * Check status and throw error if not OK
   */
  private checkStatus(status: number, operation: string): void {
    if (status === QueueStatus.Ok) {
      return;
    }

    const statusNames: Record<number, string> = {
      [QueueStatus.QueueNotFound]: "QueueNotFound",
      [QueueStatus.MessageNotFound]: "MessageNotFound",
      [QueueStatus.InvalidToken]: "InvalidToken",
      [QueueStatus.QueueFull]: "QueueFull",
      [QueueStatus.InvalidDelay]: "InvalidDelay",
    };

    const statusName = statusNames[status] || `Unknown(${status})`;
    throw new QueueError(
      `${operation} failed: ${statusName}`,
      statusName,
      status,
    );
  }
}

export * from "./types";
