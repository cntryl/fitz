/**
 * Lease domain client
 * Per fitz-go/internal/domains/lease/lease.go
 */

import { DomainClient } from "../base";
import { LeaseCodec } from "./codec";
import {
  Lease,
  LeaseInfo,
  ChangeHandler,
  LeaseSubscription,
} from "./types";
import {
  MSG_LEASE_ACQUIRE,
  MSG_LEASE_QUERY,
  MSG_LEASE_SUBSCRIBE,
  MSG_LEASE_UNSUBSCRIBE,
  MSG_LEASE_NOTIFY,
} from "../../frame/types";
import { LeaseError } from "../../core/errors";
import { parseStandardResponse, assertSuccess } from "../../protocol/response";

export class LeaseClient extends DomainClient {
  private subscriptions: Map<
    bigint,
    { handler: ChangeHandler; pattern: string }
  > = new Map();
  private initialized = false;

  /**
   * Acquire a lease on the given route
   * @param route Lease route (e.g., "lease://realm/area/resource")
   * @param ttlSecs Lease TTL in seconds
   * @returns Lease handle with renew() and release() methods
   * @throws LeaseError with code LEASE_HELD if already held by another owner
   */
  async acquire(route: string, ttlSecs: number): Promise<Lease> {
    const payload = LeaseCodec.encodeAcquire(route, ttlSecs);
    const response = await this.request(MSG_LEASE_ACQUIRE, payload);
    const { success, data } = parseStandardResponse(response);

    // Per fitz-go: LeaseHeld returns ErrorResponse, not success
    assertSuccess(success, data, "ACQUIRE");

    const decoded = LeaseCodec.decodeAcquireResponse(data!);
    const expiresAt = BigInt(Math.floor(Date.now() / 1000)) + BigInt(ttlSecs);

    return new Lease(decoded.token, expiresAt, route, this.connection);
  }

  /**
   * Query current lease status for the route
   * @param route Lease route
   * @returns Lease information (held, owner, expires_at)
   */
  async query(route: string): Promise<LeaseInfo> {
    const payload = LeaseCodec.encodeQuery(route);
    const response = await this.request(MSG_LEASE_QUERY, payload);
    const { success, data } = parseStandardResponse(response);

    assertSuccess(success, data, "QUERY");

    const decoded = LeaseCodec.decodeQueryResponse(data!);

    return {
      isHeld: decoded.isHeld ?? false,
      owner: decoded.owner,
      token: decoded.token,
      expiresAt: decoded.expiresAt,
    };
  }

  /**
   * Subscribe to lease change notifications (released or expired)
   * @param pattern Pattern to match (e.g., "lease://realm/area/*")
   * @param handler Handler to call when lease changes occur
   * @returns Subscription object with unsubscribe() method
   */
  async subscribe(
    pattern: string,
    handler: ChangeHandler,
  ): Promise<LeaseSubscription> {
    this.initNotifyHandler();

    const payload = LeaseCodec.encodeSubscribe(pattern);
    const response = await this.request(MSG_LEASE_SUBSCRIBE, payload);
    const { success, data } = parseStandardResponse(response);

    assertSuccess(success, data, "SUBSCRIBE");

    const decoded = LeaseCodec.decodeSubscribeResponse(data!);

    if (decoded.subId === undefined) {
      throw new LeaseError(
        "SUBSCRIBE response missing subId",
        "MISSING_SUB_ID",
      );
    }

    const subId = decoded.subId;
    this.subscriptions.set(subId, { handler, pattern });

    const unsubscribeFn = async (id: bigint) => {
      await this.unsubscribe(id);
    };

    return new LeaseSubscription(subId, pattern, unsubscribeFn);
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
      const payload = LeaseCodec.encodeUnsubscribe(subscription.pattern);
      const response = await this.request(MSG_LEASE_UNSUBSCRIBE, payload);
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
      MSG_LEASE_NOTIFY,
      (payload: Uint8Array) => {
        try {
          const { subId, route } = LeaseCodec.decodeNotification(payload);
          const subscription = this.subscriptions.get(subId);

          if (!subscription) {
            console.warn(
              `No handler registered for lease subscription ${subId}`,
            );
            return;
          }

          const notification: ChangeNotification = { route };

          // Call handler asynchronously to avoid blocking dispatch loop
          Promise.resolve(subscription.handler(notification)).catch((err) => {
            console.error("Lease change handler error:", err);
          });
        } catch (err) {
          console.error("Lease notification decode error:", err);
        }
      },
    );
  }
}

export * from "./types";
