/**
 * Main Fitz client
 */

import { ClientConfig, ConnectionState } from "../core/types";
import { Connection } from "./connection";
import { createTransport } from "../transport/factory";
import { ConnectionError } from "../core/errors";
import { KvClient } from "../domains/kv/client";
import { QueueClient } from "../domains/queue/client";
import { RpcClient } from "../domains/rpc/client";
import { LeaseClient } from "../domains/lease/client";
import { NoticeClient } from "../domains/notice/client";
import { StreamClient } from "../domains/stream/client";
import { ScheduleClient } from "../domains/schedule/client";

export class Client {
  private connection: Connection | null = null;
  private config: ClientConfig;
  private kvClient: KvClient | null = null;
  private queueClient: QueueClient | null = null;
  private rpcClient: RpcClient | null = null;
  private leaseClient: LeaseClient | null = null;
  private noticeClient: NoticeClient | null = null;
  private streamClient: StreamClient | null = null;
  private scheduleClient: ScheduleClient | null = null;

  constructor(config: ClientConfig) {
    this.config = {
      timeout: 30000,
      transport: "auto",
      retryAttempts: 3,
      retryDelayMs: 100,
      ...config,
    };

    if (!this.config.url) {
      throw new Error("URL is required");
    }
    if (!this.config.jwt) {
      throw new Error("JWT token is required");
    }
  }

  /**
   * Connect to Fitz server
   */
  async connect(): Promise<void> {
    try {
      const transport = createTransport(
        this.config.url,
        this.config.transport,
        {
          timeout: this.config.timeout,
          retryAttempts: this.config.retryAttempts,
          retryDelayMs: this.config.retryDelayMs,
        },
      );

      this.connection = new Connection(
        transport,
        this.config.jwt,
        this.config.timeout,
      );

      await this.connection.connect();
    } catch (err) {
      this.connection = null;
      throw new ConnectionError(
        `Failed to connect: ${err instanceof Error ? err.message : String(err)}`,
      );
    }
  }

  /**
   * Disconnect from Fitz server
   */
  async disconnect(): Promise<void> {
    if (this.connection) {
      await this.connection.disconnect();
      this.connection = null;
    }
  }

  /**
   * Check if connected
   */
  isConnected(): boolean {
    return this.connection?.isConnected() ?? false;
  }

  /**
   * Get KV client
   */
  kv(): KvClient {
    this.ensureConnected();
    if (!this.kvClient) {
      this.kvClient = new KvClient(this.connection!);
    }
    return this.kvClient;
  }

  /**
   * Get Queue client
   */
  queue(): QueueClient {
    this.ensureConnected();
    if (!this.queueClient) {
      this.queueClient = new QueueClient(this.connection!);
    }
    return this.queueClient;
  }

  /**
   * Get RPC client
   */
  rpc(): RpcClient {
    this.ensureConnected();
    if (!this.rpcClient) {
      this.rpcClient = new RpcClient(this.connection!);
    }
    return this.rpcClient;
  }

  /**
   * Get Lease client
   */
  lease(): LeaseClient {
    this.ensureConnected();
    if (!this.leaseClient) {
      this.leaseClient = new LeaseClient(this.connection!);
    }
    return this.leaseClient;
  }

  /**
   * Get Notice client
   */
  notice(): NoticeClient {
    this.ensureConnected();
    if (!this.noticeClient) {
      this.noticeClient = new NoticeClient(this.connection!);
    }
    return this.noticeClient;
  }

  /**
   * Get Stream client
   */
  stream(): StreamClient {
    this.ensureConnected();
    if (!this.streamClient) {
      this.streamClient = new StreamClient(this.connection!);
    }
    return this.streamClient;
  }

  /**
   * Get Schedule client
   */
  schedule(): ScheduleClient {
    this.ensureConnected();
    if (!this.scheduleClient) {
      this.scheduleClient = new ScheduleClient(this.connection!);
    }
    return this.scheduleClient;
  }

  /**
   * Get connection URL
   */
  getUrl(): string {
    if (!this.connection) {
      throw new ConnectionError("Not connected");
    }
    return this.connection.getUrl();
  }

  /**
   * Get connection state
   */
  getState(): ConnectionState {
    return this.connection?.getState() ?? ConnectionState.Disconnected;
  }

  private ensureConnected(): void {
    if (!this.connection?.isConnected()) {
      throw new ConnectionError(
        "Not connected to Fitz server. Call connect() first.",
      );
    }
  }
}
