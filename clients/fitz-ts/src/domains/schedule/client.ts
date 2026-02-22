/**
 * Schedule domain client for cron-based task scheduling
 * Per fitz-go/internal/domains/schedule
 */

import { DomainClient } from "../base";
import { ScheduleCodec } from "./codec";
import {
  ScheduleEntry,
  ScheduleNotification,
  ScheduleHandler,
  ScheduleSubscription,
} from "./types";
import {
  MSG_SCHEDULE_CREATE,
  MSG_SCHEDULE_CANCEL,
  MSG_SCHEDULE_LIST,
  MSG_SCHEDULE_SUBSCRIBE,
  MSG_SCHEDULE_UNSUBSCRIBE,
  MSG_SCHEDULE_NOTIFY,
} from "../../frame/types";
import { assertSuccess } from "../../protocol/response";

export class ScheduleClient extends DomainClient {
  private subscriptions = new Map<bigint, ScheduleSubscription>();
  private nextClientSubId = 0n;
  private notifyHandlerInitialized = false;

  /**
   * Create a cron-based schedule at the given route (upsert semantics).
   * Returns the schedule route (identity).
   */
  async create(
    route: string,
    cronExpr: string,
    payload: Uint8Array = new Uint8Array(),
  ): Promise<string> {
    const payloadBytes = ScheduleCodec.encodeCreate(route, cronExpr, payload);
    const response = await this.request(MSG_SCHEDULE_CREATE, payloadBytes);
    const data = assertSuccess(response, "SCHEDULE_CREATE");
    const decoded = ScheduleCodec.decodeCreateResponse(data);

    // Return scheduleId if provided, otherwise use route as identity
    return decoded.scheduleId || route;
  }

  /**
   * Cancel a schedule by route (route-based identity).
   */
  async cancel(route: string): Promise<void> {
    const payload = ScheduleCodec.encodeCancel(route);
    const response = await this.request(MSG_SCHEDULE_CANCEL, payload);
    assertSuccess(response, "SCHEDULE_CANCEL");
  }

  /**
   * List schedules with pagination
   * @param offset Starting position (0-based), default 0
   * @param limit Maximum entries to return, default 0 (server uses 100)
   * @returns Tuple of [schedules, totalCount]
   */
  async list(
    offset: bigint = 0n,
    limit: bigint = 0n,
  ): Promise<[ScheduleEntry[], bigint]> {
    const payload = ScheduleCodec.encodeList(offset, limit);
    const response = await this.request(MSG_SCHEDULE_LIST, payload);
    const data = assertSuccess(response, "SCHEDULE_LIST");
    const decoded = ScheduleCodec.decodeListResponse(data);

    return [decoded.entries, decoded.totalCount];
  }

  /**
   * Subscribe to schedule fire notifications for the given route pattern.
   * When a schedule fires, the handler is invoked with the schedule's payload.
   *  Subscriptions are session-scoped and lost on disconnect.
   */
  async subscribe(
    pattern: string,
    handler: ScheduleHandler,
  ): Promise<ScheduleSubscription> {
    this.initNotifyHandler();

    const payload = ScheduleCodec.encodeSubscribe(pattern);
    const response = await this.request(MSG_SCHEDULE_SUBSCRIBE, payload);
    const data = assertSuccess(response, "SCHEDULE_SUBSCRIBE");
    const decoded = ScheduleCodec.decodeSubscribeResponse(data);

    // Use server subId if provided, otherwise assign client subId
    let subId = decoded.subId;
    if (!subId || subId === 0n) {
      this.nextClientSubId++;
      subId = this.nextClientSubId;
    }

    const subscription = new ScheduleSubscription(subId, pattern, handler, () =>
      this.doUnsubscribe(subId!, pattern),
    );

    this.subscriptions.set(subId, subscription);
    return subscription;
  }

  /**
   * Handle SCHEDULE_NOTIFY (705) notifications
   */
  private handleNotify(payload: Uint8Array): void {
    const { subId, payload: notificationPayload } =
      ScheduleCodec.decodeNotification(payload);

    const sub = this.subscriptions.get(subId);
    if (!sub) {
      return; // Subscription already removed
    }

    // Fire-and-forget: invoke handler but don't await
    const notification: ScheduleNotification = {
      payload: notificationPayload,
    };

    sub.handler(notification).catch((err: unknown) => {
      console.error("Schedule notification handler error:", err);
    });
  }

  /**
   * Internal unsubscribe - removes subscription and sends UNSUBSCRIBE request
   */
  private async doUnsubscribe(subId: bigint, pattern: string): Promise<void> {
    this.subscriptions.delete(subId);

    // Best-effort unsubscribe; ignore errors to match notice semantics
    try {
      const payload = ScheduleCodec.encodeUnsubscribe(pattern);
      const response = await this.request(MSG_SCHEDULE_UNSUBSCRIBE, payload);
      assertSuccess(response, "SCHEDULE_UNSUBSCRIBE");
    } catch (err: unknown) {
      console.warn("Schedule unsubscribe error:", err);
    }
  }

  /**
   * Lazy-initialize MSG_SCHEDULE_NOTIFY (705) handler
   */
  private initNotifyHandler(): void {
    if (this.notifyHandlerInitialized) {
      return;
    }

    this.notifyHandlerInitialized = true;
    this.connection.registerNotificationHandler(
      MSG_SCHEDULE_NOTIFY,
      (payload) => {
        this.handleNotify(payload);
      },
    );
  }
}
