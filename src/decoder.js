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
  if (typeof base64Data !== "string" || base64Data.length > 4 * Math.ceil(16 * 1024 * 1024 / 3) ||
      base64Data.length % 4 !== 0 || !/^[A-Za-z0-9+/]+={0,2}$/.test(base64Data)) {
    throw new Error("Invalid or oversized Arrow base64");
  }
  const bytes =
    typeof Buffer !== "undefined"
      ? new Uint8Array(Buffer.from(base64Data, "base64"))
      : Uint8Array.from(atob(base64Data), (c) => c.charCodeAt(0));
  // The server sends one complete modern IPC stream per event. Flechette can
  // interpret arbitrary/truncated bytes as an empty table; do not acknowledge
  // that as a successfully decoded event. Require framing and a real schema.
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  if (bytes.length > 16 * 1024 * 1024 || bytes.length < 16 || view.getInt32(0, true) !== -1 || view.getInt32(4, true) <= 0 ||
      view.getInt32(bytes.length - 8, true) !== -1 || view.getInt32(bytes.length - 4, true) !== 0) {
    throw new Error("Incomplete Arrow IPC stream");
  }
  // SQL BIGINT, DECIMAL and sub-millisecond timestamps must not silently lose
  // precision. Units and decimal scale remain available in the Arrow schema.
  const table = tableFromIPC(bytes, {
    useBigInt: true, useDecimalInt: true, useBigIntTimestamp: true, ...options,
  });
  if (!Number.isInteger(table.schema.version)) throw new Error("Arrow IPC schema missing");
  return table;
}
