/**
 * Arrow IPC decoder — base64 SSE data to flechette Table.
 *
 * Works in both Node.js and browsers.
 */

import { tableFromIPC } from "@uwdata/flechette";

/**
 * Decode base64-encoded Arrow IPC stream into a flechette Table.
 * @param {string} base64Data - Base64-encoded Arrow IPC stream bytes
 * @param {import("@uwdata/flechette").ExtractionOptions} [options] Explicit value conversion overrides.
 * @returns {import("@uwdata/flechette").Table} Decoded Arrow table
 */
export function decodeArrowIPC(base64Data, options = {}) {
  const bytes =
    typeof Buffer !== "undefined"
      ? new Uint8Array(Buffer.from(base64Data, "base64"))
      : Uint8Array.from(atob(base64Data), (c) => c.charCodeAt(0));
  // SQL BIGINT, DECIMAL and sub-millisecond timestamps must not silently lose
  // precision. Units and decimal scale remain available in the Arrow schema.
  return tableFromIPC(bytes, {
    useBigInt: true, useDecimalInt: true, useBigIntTimestamp: true, ...options,
  });
}
