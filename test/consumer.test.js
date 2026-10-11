#!/usr/bin/env node

/**
 * BoilStreamConsumer SDK Integration Test
 *
 * Requires a running BoilStream server with valid tokens.
 *
 * Usage:
 *   NODE_TLS_REJECT_UNAUTHORIZED=0 \
 *   STREAM_TOKEN=<hex> INGEST_TOKEN=<hex> \
 *   node test/consumer.test.js
 */

import { BoilStreamConsumer } from "boilstream-consumer";
import { tableFromColumns, columnFromArray, utf8, tableToIPC } from "@uwdata/flechette";
import { EventSource } from "eventsource";

const STREAM_TOKEN = process.env.STREAM_TOKEN;
const INGEST_TOKEN = process.env.INGEST_TOKEN;
const SERVER_URL = process.env.SERVER_URL || "https://localhost:8443";

if (!STREAM_TOKEN || !INGEST_TOKEN) {
  console.error(
    "Usage: STREAM_TOKEN=<hex> INGEST_TOKEN=<hex> node test/consumer.test.js",
  );
  process.exit(1);
}

async function ingestTestData() {
  const table = tableFromColumns({
    id: columnFromArray(Int32Array.from([1, 2, 3])),
    name: columnFromArray(["alice", "bob", "charlie"], utf8()),
    value: columnFromArray(Float64Array.from([1.1, 2.2, 3.3])),
  });
  const ipcBytes = tableToIPC(table, { format: "stream" });

  const url = `${SERVER_URL}/ingest/${INGEST_TOKEN}`;
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/vnd.apache.arrow.stream" },
    body: ipcBytes,
  });

  if (!res.ok) {
    throw new Error(`Ingest failed: ${res.status} ${await res.text()}`);
  }
  console.log("  Ingested 3 rows via HTTP/2 Arrow IPC");
}

async function runTest() {
  console.log("\n=== BoilStreamConsumer SDK Integration Test ===\n");

  let schemaReceived = false;
  let batchCount = 0;
  let totalRows = 0;
  const cursorErrors = [];
  let resolve;
  const done = new Promise((r) => {
    resolve = r;
  });

  const consumer = new BoilStreamConsumer(
    `${SERVER_URL}/stream/${STREAM_TOKEN}`,
    {
      onSchema: (schema, cursor) => {
        if (typeof cursor !== "string" || !cursor.startsWith("r1.")) cursorErrors.push("Schema lacks opaque baseline cursor");
        schemaReceived = true;
        const fieldNames = schema?.fields?.map?.((f) => f.name) ?? schema;
        console.log("  Schema received:", fieldNames);
      },
      onBatch: (table, cursor) => {
        if (typeof cursor !== "string" || !cursor.startsWith("r1.")) cursorErrors.push("Batch lacks opaque cursor");
        batchCount++;
        totalRows += table.numRows;
        console.log(`  Cursor ${cursor}: ${table.numRows} rows`);
        // Print first few values for debugging
        for (const col of table.schema.fields) {
          const arr = table.getChild(col.name);
          if (arr) {
            const vals = [];
            for (let i = 0; i < Math.min(3, table.numRows); i++) {
              vals.push(arr.at(i));
            }
            console.log(`    ${col.name}: [${vals.join(", ")}]`);
          }
        }
        if (totalRows >= 3) resolve();
      },
      onHeartbeat: () => {
        console.log("  Heartbeat");
      },
      onError: (err) => {
        // EventSource fires error on initial connect attempt too; ignore non-fatal
        if (err?.status !== undefined) {
          console.error("  SSE error:", err.status, err.message);
        }
      },
      EventSource,
    },
  );

  // Wait for consumer to register, then ingest data
  console.log("  Waiting for consumer registration...");
  await new Promise((r) => setTimeout(r, 2000));

  console.log("  Ingesting test data...");
  await ingestTestData();

  // Wait for batch events (up to 15s)
  console.log("  Waiting for SSE events...\n");
  const timeout = new Promise((_, reject) =>
    setTimeout(() => reject(new Error("Timeout: no batch events in 15s")), 15000),
  );
  await Promise.race([done, timeout]);

  consumer.close();

  console.log("\n=== Results ===");
  console.log(`  Schema received: ${schemaReceived}`);
  console.log(`  Batch events:    ${batchCount}`);
  console.log(`  Total rows:      ${totalRows}`);

  // Assertions
  const errors = [...cursorErrors];
  if (!schemaReceived) errors.push("No schema event received");
  if (totalRows < 3) errors.push(`Expected >= 3 rows, got ${totalRows}`);

  if (errors.length > 0) {
    console.error("\nFAILED:", errors.join("; "));
    process.exit(1);
  }

  console.log("\nAll assertions passed!");
}

runTest().catch((err) => {
  console.error("\nTest failed:", err.message || err);
  process.exit(1);
});
