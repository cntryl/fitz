/**
 * KV domain client
 */

import { DomainClient } from "../base";
import { KvCodec } from "./codec";
import { KvTransaction } from "./transaction";
import { TxMode, DurabilityMode, DefaultWriteOptions } from "./types";
import { MSG_KV_BEGIN } from "../../frame/types";
import { KvError } from "../../core/errors";

export class KvClient extends DomainClient {
  /**
   * Begin a new transaction
   */
  async begin(
    route: string,
    mode: TxMode = "ReadWrite",
    options?: {
      durability?: DurabilityMode;
    },
  ): Promise<KvTransaction> {
    const durability = options?.durability ?? DefaultWriteOptions.durability;

    const payload = KvCodec.encodeBegin(route, mode, durability);
    const response = await this.request(MSG_KV_BEGIN, payload);
    const decoded = KvCodec.decodeBeginResponse(response);

    if (decoded.status !== 0) {
      throw new KvError(
        `Failed to begin transaction with status ${decoded.status}`,
        "BEGIN_FAILED",
        decoded.status,
      );
    }

    return new KvTransaction(this.connection, route, decoded.txId);
  }
}

export { KvTransaction } from "./transaction";
export * from "./types";
