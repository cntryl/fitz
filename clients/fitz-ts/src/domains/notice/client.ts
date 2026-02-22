/**
 * Notice domain client (Pub/Sub)
 * Per fitz-go/internal/domains/notice/notice.go
 */

import { DomainClient } from "../base";
import { NoticeCodec } from "./codec";
import { NoticeMsg, NoticeHandler, NoticeSubscription } from "./types";
import {
  MSG_NOTICE_PUBLISH,
  MSG_NOTICE_SUBSCRIBE,
  MSG_NOTICE_UNSUBSCRIBE,
  MSG_NOTICE_NOTIFY,
} from "../../frame/types";
import { NoticeError } from "../../core/errors";
import { parseStandardResponse, assertSuccess } from "../../protocol/response";

export class NoticeClient extends DomainClient {
  private subscriptions: Map<
    bigint,
    { handler: NoticeHandler; pattern: string }
  > = new Map();
  private initialized = false;

  /**
   * Publish a message to a route (fire-and-forget)
   * @param route Target route (exact route, not pattern)
   * @param body Message body
   */
  async publish(route: string, body: Uint8Array): Promise<void> {
    const payload = NoticeCodec.encodePublish(route, body);
    // Send fire-and-forget (no response expected)
    await this.connection.sendFireAndForget(MSG_NOTICE_PUBLISH, payload);
  }

  /**
   * Subscribe to notification messages matching a pattern
   * @param pattern Pattern to match (e.g., "notice://realm/area/*")
   * @param handler Handler to call when messages arrive
   * @returns Subscription object with unsubscribe() method
   */
  async subscribe(
    pattern: string,
    handler: NoticeHandler,
  ): Promise<NoticeSubscription> {
    this.initNotifyHandler();

    const payload = NoticeCodec.encodeSubscribe(pattern);
    const response = await this.request(MSG_NOTICE_SUBSCRIBE, payload);
    const { success, data } = parseStandardResponse(response);

    assertSuccess(success, data, "SUBSCRIBE");

    const decoded = NoticeCodec.decodeSubscribeResponse(data!);

    if (decoded.subId === undefined) {
      throw new NoticeError(
        "SUBSCRIBE response missing subId",
        "MISSING_SUB_ID",
      );
    }

    const subId = decoded.subId;
    this.subscriptions.set(subId, { handler, pattern });

    const unsubscribeFn = async (id: bigint) => {
      await this.unsubscribe(id);
    };

    return new NoticeSubscription(subId, pattern, unsubscribeFn);
  }

  /**
   * Internal method to unsubscribe from notifications
   */
  private async unsubscribe(subId: bigint): Promise<void> {
    const subscription = this.subscriptions.get(subId);
    if (!subscription) {
      return;
    }

    this.subscriptions.delete(subId);

    try {
      const payload = NoticeCodec.encodeUnsubscribe(subscription.pattern);
      const response = await this.request(MSG_NOTICE_UNSUBSCRIBE, payload);
      const { success, data } = parseStandardResponse(response);

      assertSuccess(success, data, "UNSUBSCRIBE");
    } catch (error) {
      console.warn("UNSUBSCRIBE failed:", error);
    }
  }

  /**
   * Initialize notification handler (lazy, on first subscribe)
   */
  private initNotifyHandler(): void {
    if (this.initialized) {
      return;
    }
    this.initialized = true;

    this.connection.registerNotificationHandler(
      MSG_NOTICE_NOTIFY,
      (payload: Uint8Array) => {
        try {
          const { subId, route, body } =
            NoticeCodec.decodeNotification(payload);
          const subscription = this.subscriptions.get(subId);

          if (!subscription) {
            console.warn(
              `No handler registered for notice subscription ${subId}`,
            );
            return;
          }

          const msg: NoticeMsg = { route, body };

          // Call handler asynchronously to avoid blocking dispatch loop
          Promise.resolve(subscription.handler(msg)).catch((err) => {
            console.error("Notice handler error:", err);
          });
        } catch (err) {
          console.error("Notice notification decode error:", err);
        }
      },
    );
  }
}

export * from "./types";
