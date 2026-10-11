/**
 * BoilStream SSE Consumer — real-time Arrow data streaming.
 *
 * Connects to a BoilStream SSE endpoint and decodes Arrow IPC events
 * using flechette. Works in browsers (native EventSource) and Node.js
 * (pass the `eventsource` polyfill).
 *
 * Reconnection with Last-Event-ID is handled automatically by EventSource.
 * IDs are opaque strings. Terminal refresh and Arrow decode errors close the
 * stream; the application must refresh its snapshot before starting again.
 *
 * @example
 * import { BoilStreamConsumer } from "boilstream-consumer";
 * import { EventSource } from "eventsource"; // Node.js only
 *
 * const consumer = new BoilStreamConsumer("https://host:8443/stream/{token}", {
 *   onSchema: (schema) => console.log("Schema:", schema),
 *   onBatch: (table, cursor) => console.log(`Cursor ${cursor}: ${table.numRows} rows`),
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
   * @param {import("./index.js").ConsumerOptions} [options]
   */
  constructor(url, options = {}) {
    this.#url = url;
    this.#callbacks = {
      onSchema: options.onSchema,
      onBatch: options.onBatch,
      onHeartbeat: options.onHeartbeat,
      onRefreshRequired: options.onRefreshRequired,
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
    const es = new this.#EventSourceImpl(this.#url);
    this.#es = es;

    es.addEventListener("schema", (event) => {
      if (this.#es !== es) return;
      this.#deliverArrow(event, (table, cursor) => {
        this.#schema = table.schema;
        this.#callbacks.onSchema?.(table.schema, cursor);
      });
    });

    es.addEventListener("batch", (event) => {
      if (this.#es !== es) return;
      this.#deliverArrow(event, (table, cursor) => {
        this.#callbacks.onBatch?.(table, cursor);
      });
    });

    es.addEventListener("heartbeat", () => {
      if (this.#es !== es) return;
      this.#callbacks.onHeartbeat?.();
    });

    es.addEventListener("refresh-required", (event) => {
      if (this.#es !== es) return;
      // Named terminal events are terminal even if their body is malformed.
      // Close before parsing or calling application code, which may throw.
      this.close();
      let malformed;
      try {
        const data = JSON.parse(event.data);
        if (data?.reason !== "refresh_required") {
          throw new Error("Invalid refresh-required event reason");
        }
      } catch (err) {
        malformed = err;
      }
      try {
        this.#callbacks.onRefreshRequired?.({ reason: "refresh_required" });
      } catch (err) {
        this.#callbacks.onError?.(err);
      }
      if (malformed) this.#callbacks.onError?.(malformed);
    });

    es.onerror = (err) => {
      if (this.#es !== es) return;
      this.#callbacks.onError?.(err);
      // Ordinary network errors retain EventSource's Last-Event-ID retry.
    };
  }

  #deliverArrow(event, callback) {
    let table;
    try {
      table = decodeArrowIPC(event.data);
    } catch (err) {
      // EventSource may already have advanced Last-Event-ID. Continuing would
      // silently skip these undecoded rows; refresh the application snapshot.
      this.close();
      this.#callbacks.onError?.(err);
      return;
    }
    try {
      callback(table, event.lastEventId);
    } catch (err) {
      // Application callback failures are distinct from invalid Arrow data.
      this.#callbacks.onError?.(err);
    }
  }

  /** Disconnect from the SSE stream. */
  close() {
    const es = this.#es;
    this.#es = null;
    es?.close();
  }
}
