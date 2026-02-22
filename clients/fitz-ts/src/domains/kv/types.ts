/**
 * KV domain types
 */

export type TxMode = "ReadOnly" | "ReadWrite";
export type DurabilityMode = "None" | "Async" | "Sync";

export interface WriteOptions {
  durability: DurabilityMode;
  buffered: boolean;
}

export const DefaultWriteOptions: WriteOptions = {
  durability: "Async",
  buffered: true,
};

export interface KvBeginResponse {
  txId: bigint;
  status: number;
}

export interface KvPutResponse {
  status: number;
}

export interface KvGetResponse {
  status: number;
  value?: Uint8Array;
}

export interface KvDeleteResponse {
  status: number;
}

export interface KvCommitResponse {
  status: number;
}

export interface KvRollbackResponse {
  status: number;
}

export interface KvScanResponse {
  status: number;
  keys?: Uint8Array[];
  nextCursor?: Uint8Array;
}

export enum KvStatus {
  Ok = 0,
  TransactionAborted = 1,
  LeaseExpired = 2,
  ConflictingWrite = 3,
  KeyNotFound = 4,
  OperationNotAllowed = 5,
}
