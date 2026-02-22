/**
 * Lease domain types
 * Per fitz-go/internal/domains/lease/lease.go
 */

import type { Connection } from "../../client/connection";

/**
 * Change notification when a lease is released or expires
 */
export interface ChangeNotification {
  route: string;
}

/**
 * Handler for lease change notifications
 */
export type ChangeHandler = (notif: ChangeNotification) => Promise<void>;

/**
 * Active lease change subscription
 */
export class LeaseSubscription {
  constructor(
    public readonly subId: bigint,
    public readonly pattern: string,
    private readonly unsubscribeFn: (subId: bigint) => Promise<void>,
  ) {}

  async unsubscribe(): Promise<void> {
    await this.unsubscribeFn(this.subId);
  }
}

/**
 * Lease handle representing an acquired lease
 * Provides renew() and release() methods
 */
export class Lease {
  public token: bigint;
  public expiresAt: bigint;

  constructor(
    token: bigint,
    expiresAt: bigint,
    private readonly route: string,
    private readonly connection: Connection,
  ) {
    this.token = token;
    this.expiresAt = expiresAt;
  }

  /**
   * Extend the lease TTL
   * @param ttlSecs Lease duration in seconds
   * @returns New expiry timestamp (seconds since epoch)
   */
  async extend(ttlSecs: number): Promise<bigint> {
    const payload = LeaseCodec.encodeExtend(this.route, this.token, ttlSecs);
    const response = await this.connection.request(MSG_LEASE_RENEW, payload);
    const data = assertSuccess(response, "EXTEND");

    // Parse new fencing token: [u64 BE new_fencing_token]
    if (data && data.length >= 8) {
      const reader = new BufferReader(data);
      const newToken = reader.readU64BE();
      this.token = newToken;
    }

    const newExpiry = BigInt(Math.floor(Date.now() / 1000)) + BigInt(ttlSecs);
    this.expiresAt = newExpiry;
    return newExpiry;
  }

  /**
   * Release the lease
   */
  async release(): Promise<void> {
    const payload = LeaseCodec.encodeRelease(this.route, this.token);
    const response = await this.connection.request(MSG_LEASE_RELEASE, payload);
    assertSuccess(response, "RELEASE");
  }
}

/**
 * Response to ACQUIRE request
 */
export interface AcquireResponse {
  token: bigint;
  expiresAt: bigint;
}

/**
 * Lease information from QUERY request
 */
export interface LeaseInfo {
  isHeld: boolean;
  owner?: string;
  token?: bigint;
  expiresAt?: bigint;
}

/**
 * Response to QUERY request
 */
export interface QueryResponse {
  status: number;
  isHeld?: boolean;
  owner?: string;
  token?: bigint;
  expiresAt?: bigint;
}

/**
 * Response to SUBSCRIBE request
 */
export interface SubscribeResponse {
  status: number;
  subId?: bigint;
}

/**
 * Response to UNSUBSCRIBE request
 */
export interface UnsubscribeResponse {
  status: number;
}

/**
 * Lease status codes
 */
export enum LeaseStatus {
  Ok = 0,
  LeaseHeld = 1,
  NotFound = 2,
  InvalidToken = 3,
}

// Import needed types for Lease class methods
import { BufferReader } from "../../core/buffer";
import { assertSuccess } from "../../protocol/response";
import { MSG_LEASE_RENEW, MSG_LEASE_RELEASE } from "../../frame/types";
import { LeaseCodec } from "./codec";
