/**
 * Base class for domain clients
 */

import { Connection } from "../client/connection";

export class DomainClient {
  protected connection: Connection;

  constructor(connection: Connection) {
    this.connection = connection;
  }

  protected async request(
    messageType: number,
    payload: Uint8Array,
  ): Promise<Uint8Array> {
    return this.connection.request(messageType, payload);
  }

  protected async send(
    messageType: number,
    payload: Uint8Array,
  ): Promise<void> {
    return this.connection.send(messageType, payload);
  }
}
