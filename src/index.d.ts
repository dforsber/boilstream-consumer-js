import type { Schema, Table, ExtractionOptions } from "@uwdata/flechette";
import type { EventSource as NodeEventSource } from "eventsource";

/** Opaque server-issued SSE ID. Preserve it verbatim; do not parse or compare it. */
export type ReplayCursor = string;
export interface RefreshRequired {
  reason: "refresh_required";
}
export interface ConsumerOptions {
  /** Defaults to exact BigInt extraction for int64, decimal coefficients and timestamps. */
  arrowOptions?: ExtractionOptions;
  /** Synchronous schema/readiness callback. Cursor may be empty when an event carries no ID. */
  onSchema?: (schema: Schema, cursor: ReplayCursor) => void;
  /** Must finish synchronously. Exceptions and Promise returns close the stream. */
  onBatch?: (table: Table, cursor: ReplayCursor) => void;
  onHeartbeat?: () => void;
  /** The stream is already closed; refresh the snapshot before starting again. */
  onRefreshRequired?: (details: RefreshRequired) => void;
  /** Decode and application callback errors close the stream and require refresh. Network errors may reconnect. */
  onError?: (error: unknown) => void;
  EventSource?: new (url: string) => Pick<NodeEventSource, "addEventListener" | "close">;
}
export declare class BoilStreamConsumer {
  constructor(url: string, options?: ConsumerOptions);
  get schema(): Schema | null;
  close(): void;
}
export declare function decodeArrowIPC(base64Data: string, options?: ExtractionOptions): Table;
