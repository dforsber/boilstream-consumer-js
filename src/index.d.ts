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

/** Experimental v2 client; requires a server explicitly advertising SSE v2. */
export interface SnapshotConsumerOptions {
  getToken: (context: { signal: AbortSignal; unauthorized: boolean }) => string | Promise<string>;
  /** Must correspond to already committed application state, never provisional rows. */
  cursor?: ReplayCursor;
  onSnapshotStart: (context: { signal: AbortSignal }) => void | Promise<void>;
  onSnapshot: (table: Table, context: { signal: AbortSignal }) => void | Promise<void>;
  /** Atomically activate the staged state and persist this cursor. */
  onSnapshotComplete: (cursor: ReplayCursor, context: { signal: AbortSignal }) => void | Promise<void>;
  onSnapshotDiscard: () => void | Promise<void>;
  /** Apply effects and persist cursor together; retries can redeliver unapplied data. */
  onBatch: (table: Table, cursor: ReplayCursor, context: { signal: AbortSignal }) => void | Promise<void>;
  /** Discard/invalidate the previously active state on terminal failure. */
  onRefreshRequired: (details: RefreshRequired) => void | Promise<void>;
  onSchema?: (schema: Schema, cursor: ReplayCursor | undefined) => void | Promise<void>;
  arrowOptions?: ExtractionOptions;
  fetch?: typeof globalThis.fetch;
  maxRetries?: number;
  retryMinMs?: number;
  retryMaxMs?: number;
  ioTimeoutMs?: number;
  maxEventChars?: number;
}
export declare class StreamError extends Error {
  readonly code: string;
  constructor(code: string);
}
export declare class BoilStreamSnapshotConsumer {
  constructor(url: string, options: SnapshotConsumerOptions);
  get cursor(): ReplayCursor | undefined;
  get schema(): Schema | null;
  /** Single use. Resolves on close, rejects with StreamError on terminal failure. */
  run(): Promise<void>;
  /** Aborts network I/O; run waits for the current application callback to settle. */
  close(): void;
}
