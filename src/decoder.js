/**
 * Arrow IPC decoder — base64 SSE data to flechette Table.
 *
 * Works in both Node.js and browsers.
 */

import { tableFromIPC } from "@uwdata/flechette";

/**
 * Decode base64-encoded Arrow IPC stream into a flechette Table.
 * @param {string} base64Data - Base64-encoded Arrow IPC stream bytes
 * @returns {import("@uwdata/flechette").Table} Decoded Arrow table
 */
export function decodeArrowIPC(base64Data) {
  const bytes =
    typeof Buffer !== "undefined"
      ? new Uint8Array(Buffer.from(base64Data, "base64"))
      : Uint8Array.from(atob(base64Data), (c) => c.charCodeAt(0));
  return tableFromIPC(bytes);
}
