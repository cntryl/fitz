/**
 * Connection manager for Fitz protocol
 */

import { Transport } from "../transport/types";
import { ConnectionState } from "../core/types";
import { FrameCodec, FrameParser } from "../frame/codec";
import { MSG_CONNECT } from "../frame/types";
import { ConnectionError } from "../core/errors";
import { BufferWriter } from "../core/buffer";
import { Multiplexer } from "./multiplexer";

export class Connection {
  private transport: Transport;
  private state: ConnectionState = ConnectionState.Disconnected;
  private jwt: string;
  private timeout: number;
  private multiplexer: Multiplexer;
  private frameParser: FrameParser = new FrameParser();
  private receiveLoop: Promise<void> | null = null;
  private receiveLoopAbort: boolean = false;

  constructor(transport: Transport, jwt: string, timeout: number = 30000) {
    this.transport = transport;
    this.jwt = jwt;
    this.timeout = timeout;
    this.multiplexer = new Multiplexer();
  }

  async connect(): Promise<void> {
    try {
      this.setState(ConnectionState.Connecting);

      // Connect transport
      await this.transport.connect();

      // Start receive loop
      this.receiveLoopAbort = false;
      this.receiveLoop = this.startReceiveLoop();

      // Send CONNECT message
      this.setState(ConnectionState.Authenticating);
      await this.sendConnect();

      this.setState(ConnectionState.Authenticated);
      this.multiplexer.setConnected();
    } catch (err) {
      this.setState(ConnectionState.Closed);
      this.multiplexer.setDisconnected();
      throw err;
    }
  }

  private async sendConnect(): Promise<void> {
    const writer = new BufferWriter(256);
    writer.writeString(this.jwt);

    const payload = writer.getBuffer();
    const frame = FrameCodec.encodeFrame(MSG_CONNECT, payload);

    await this.transport.send(frame);
  }

  private async startReceiveLoop(): Promise<void> {
    while (
      !this.receiveLoopAbort &&
      this.state === ConnectionState.Authenticated
    ) {
      try {
        const data = await this.transport.receive();

        if (data.length === 0) {
          // Connection closed
          break;
        }

        // Parse frames
        const frames = this.frameParser.parseFrames(data);

        for (const frame of frames) {
          this.multiplexer.dispatch(frame.messageType, frame.payload);
        }
      } catch (err) {
        if (!this.receiveLoopAbort) {
          console.error("Receive loop error:", err);
          break;
        }
      }
    }

    this.receiveLoopAbort = true;
  }

  /**
   * Send a request and wait for response
   */
  async request(
    messageType: number,
    requestPayload: Uint8Array,
  ): Promise<Uint8Array> {
    if (this.state !== ConnectionState.Authenticated) {
      throw new ConnectionError(
        `Cannot send request: connection state is ${this.state}`,
      );
    }

    const frame = FrameCodec.encodeFrame(messageType, requestPayload);

    return this.multiplexer.request(
      messageType,
      frame,
      (data) => this.transport.send(data),
      this.timeout,
    );
  }

  /**
   * Send a one-way message (no response expected)
   */
  async send(messageType: number, requestPayload: Uint8Array): Promise<void> {
    if (this.state !== ConnectionState.Authenticated) {
      throw new ConnectionError(
        `Cannot send message: connection state is ${this.state}`,
      );
    }

    const frame = FrameCodec.encodeFrame(messageType, requestPayload);
    await this.transport.send(frame);
  }

  /**
   * Disconnect
   */
  async disconnect(): Promise<void> {
    this.receiveLoopAbort = true;
    this.multiplexer.setDisconnected();

    if (this.receiveLoop) {
      try {
        await Promise.race([
          this.receiveLoop,
          new Promise((_, reject) =>
            setTimeout(
              () => reject(new Error("Receive loop shutdown timeout")),
              5000,
            ),
          ),
        ]);
      } catch {
        // Ignore shutdown errors
      }
    }

    await this.transport.close();
    this.setState(ConnectionState.Closed);
  }

  /**
   * Get connection state
   */
  getState(): ConnectionState {
    return this.state;
  }

  /**
   * Check if connected
   */
  isConnected(): boolean {
    return this.state === ConnectionState.Authenticated;
  }

  /**
   * Get transport URL
   */
  getUrl(): string {
    return this.transport.getUrl();
  }

  /**
   * Register notification handler for server push messages
   */
  registerNotificationHandler(
    messageType: number,
    handler: (payload: Uint8Array) => void,
  ): void {
    this.multiplexer.registerNotificationHandler(messageType, handler);
  }

  /**
   * Unregister notification handler
   */
  unregisterNotificationHandler(messageType: number): void {
    this.multiplexer.unregisterNotificationHandler(messageType);
  }

  /**
   * Get multiplexer (for domain clients that need direct access)
   */
  getMultiplexer(): Multiplexer {
    return this.multiplexer;
  }

  private setState(newState: ConnectionState): void {
    this.state = newState;
    // Could emit events here if needed
  }
}
