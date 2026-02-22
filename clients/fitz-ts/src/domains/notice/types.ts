/**
 * Notice domain types (Pub/Sub)
 * Per fitz-go/internal/domains/notice/notice.go
 */

/**
 * Received notification message
 */
export interface NoticeMsg {
  route: string;
  body: Uint8Array;
}

/**
 * Handler for incoming notifications
 */
export type NoticeHandler = (msg: NoticeMsg) => Promise<void> | void;

/**
 * Active notice subscription
 */
export class NoticeSubscription {
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
 * Notice status codes
 */
export enum NoticeStatus {
  Ok = 0,
}
