/**
 * Schedule domain types
 * Per fitz-go/internal/domains/schedule (cron-based task scheduling)
 */

/**
 * ScheduleEntry represents a schedule returned by List
 * Per CLIENT_SPEC: route, cron, payload
 */
export interface ScheduleEntry {
  id: string; // Route as identity
  route: string;
  cron: string;
  payload: Uint8Array;
}

/**
 * Notification is the payload delivered when a schedule fires (SCHEDULE_NOTIFY 705)
 */
export interface ScheduleNotification {
  payload: Uint8Array;
}

/**
 * ScheduleHandler is called when a schedule fires for a subscribed pattern
 * It is fire-and-forget; the return value is not used
 */
export type ScheduleHandler = (
  notification: ScheduleNotification,
) => Promise<void>;

/**
 * ScheduleSubscription represents an active subscription to schedule fire notifications
 */
export class ScheduleSubscription {
  constructor(
    public subId: bigint,
    public pattern: string,
    public handler: ScheduleHandler,
    private unsubscribeFn: () => Promise<void>,
  ) {}

  async unsubscribe(): Promise<void> {
    return this.unsubscribeFn();
  }
}

export interface ScheduleCreateResponse {
  status: number;
  scheduleId?: string;
}

export interface ScheduleCancelResponse {
  status: number;
}

export interface ScheduleListResponse {
  status: number;
  totalCount: bigint;
  entries: ScheduleEntry[];
}

export interface ScheduleSubscribeResponse {
  status: number;
  subId?: bigint;
}

export interface ScheduleUnsubscribeResponse {
  status: number;
}

/**
 * Schedule domain errors
 */
export class ScheduleError extends Error {
  constructor(message: string, _code?: string) {
    super(message);
    this.name = "ScheduleError";
  }
}

/**
 * Schedule operation status codes
 */
export enum ScheduleStatus {
  Ok = 0,
  ScheduleNotFound = 1,
  TaskNotFound = 2,
  InvalidCron = 3,
  InvalidDelay = 4,
  InvalidTimestamp = 5,
}
