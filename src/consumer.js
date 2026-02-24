/**
 * BoilStream SSE Consumer — real-time Arrow data streaming.
 *
 * Connects to a BoilStream SSE endpoint and decodes Arrow IPC events
 * using flechette. Works in browsers (native EventSource) and Node.js
 * (pass the `eventsource` polyfill).
 *
 * Reconnection with Last-Event-ID is handled automatically by EventSource.
 *
 * @example
 * import { BoilStreamConsumer } from "boilstream-consumer";
 * import EventSource from "eventsource"; // Node.js only
 *
 * const consumer = new BoilStreamConsumer("https://host:8443/stream/{token}", {
 *   onSchema: (schema) => console.log("Schema:", schema),
 *   onBatch: (table, batchSeq) => console.log(`Batch ${batchSeq}: ${table.numRows} rows`),
 *   onHeartbeat: () => {},
 *   onError: (err) => console.error(err),
 *   EventSource, // pass polyfill for Node.js; omit in browsers
 * });
 *
 * // Later: disconnect
 * consumer.close();
 */

import { decodeArrowIPC } from "./decoder.js";

export class BoilStreamConsumer {
  #url;
  #callbacks;
  #EventSourceImpl;
  #es;
  #schema;

  /**
   * @param {string} url - SSE endpoint URL (e.g. "https://host/stream/{token}")
   * @param {Object} options
   * @param {Function} [options.onSchema] - Called with flechette schema on schema events
   * @param {Function} [options.onBatch] - Called with (flechette Table, batchSeq) on batch events
   * @param {Function} [options.onHeartbeat] - Called on heartbeat events
   * @param {Function} [options.onError] - Called with error on connection/decode errors
   * @param {typeof EventSource} [options.EventSource] - EventSource implementation (required in Node.js)
   */
  constructor(url, options = {}) {
    this.#url = url;
    this.#callbacks = {
      onSchema: options.onSchema,
      onBatch: options.onBatch,
      onHeartbeat: options.onHeartbeat,
      onError: options.onError,
    };
    this.#EventSourceImpl = options.EventSource || globalThis.EventSource;
    this.#schema = null;

    if (!this.#EventSourceImpl) {
      throw new Error(
        "EventSource is not available. In Node.js, install and pass the `eventsource` package.",
      );
    }

    this.#connect();
  }

  /** Current schema (null until first schema event). */
  get schema() {
    return this.#schema;
  }

  #connect() {
    this.#es = new this.#EventSourceImpl(this.#url);

    this.#es.addEventListener("schema", (event) => {
      try {
        const table = decodeArrowIPC(event.data);
        this.#schema = table.schema;
        this.#callbacks.onSchema?.(table.schema);
      } catch (err) {
        this.#callbacks.onError?.(err);
      }
    });

    this.#es.addEventListener("batch", (event) => {
      try {
        const batchSeq = Number(event.lastEventId);
        const table = decodeArrowIPC(event.data);
        this.#callbacks.onBatch?.(table, batchSeq);
      } catch (err) {
        this.#callbacks.onError?.(err);
      }
    });

    this.#es.addEventListener("heartbeat", () => {
      this.#callbacks.onHeartbeat?.();
    });

    this.#es.onerror = (err) => {
      this.#callbacks.onError?.(err);
      // EventSource auto-reconnects with Last-Event-ID — no manual logic needed
    };
  }

  /** Disconnect from the SSE stream. */
  close() {
    this.#es?.close();
    this.#es = null;
  }
}
