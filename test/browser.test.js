#!/usr/bin/env node

/**
 * BoilStream Consumer Browser Test (Playwright)
 *
 * Verifies the SDK works in a real browser environment.
 * Uses a local proxy to handle self-signed TLS certificates.
 *
 * Usage:
 *   NODE_TLS_REJECT_UNAUTHORIZED=0 \
 *   STREAM_TOKEN=<hex> INGEST_TOKEN=<hex> \
 *   node test/browser.test.js
 */

import http from "node:http";
import https from "node:https";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";
import { tableFromColumns, columnFromArray, utf8, tableToIPC } from "@uwdata/flechette";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const STREAM_TOKEN = process.env.STREAM_TOKEN;
const INGEST_TOKEN = process.env.INGEST_TOKEN;
const SERVER_URL = process.env.SERVER_URL || "https://localhost:8443";

if (!STREAM_TOKEN || !INGEST_TOKEN) {
  console.error(
    "Usage: STREAM_TOKEN=<hex> INGEST_TOKEN=<hex> node test/browser.test.js",
  );
  process.exit(1);
}

// Local server that serves the test HTML page AND proxies SSE requests
// This avoids browser self-signed cert issues with EventSource
function startServer() {
  return new Promise((resolve) => {
    const server = http.createServer((req, res) => {
      // Proxy SSE stream requests to the real server
      if (req.url.startsWith("/stream/")) {
        const targetUrl = new URL(req.url, SERVER_URL);
        const options = {
          hostname: targetUrl.hostname,
          port: targetUrl.port,
          path: targetUrl.pathname,
          method: "GET",
          headers: {
            Accept: "text/event-stream",
            "Cache-Control": "no-cache",
          },
          rejectUnauthorized: false, // Accept self-signed certs
        };

        // Forward Last-Event-ID if present
        if (req.headers["last-event-id"]) {
          options.headers["Last-Event-ID"] = req.headers["last-event-id"];
        }

        const proxyReq = https.request(options, (proxyRes) => {
          res.writeHead(proxyRes.statusCode, {
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
          });
          proxyRes.pipe(res);
        });

        proxyReq.on("error", (err) => {
          console.error("  Proxy error:", err.message);
          res.writeHead(502);
          res.end("Proxy error");
        });

        req.on("close", () => proxyReq.destroy());
        proxyReq.end();
        return;
      }

      // Serve the test HTML page
      const filePath = path.join(__dirname, "browser-test.html");
      const content = fs.readFileSync(filePath, "utf-8");
      res.writeHead(200, { "Content-Type": "text/html" });
      res.end(content);
    });

    server.listen(0, "127.0.0.1", () => {
      const port = server.address().port;
      resolve({ server, port });
    });
  });
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
  console.log("  Ingested 3 rows");
}

async function runBrowserTest() {
  console.log("\n=== BoilStream Browser Test (Playwright) ===\n");

  // Start local server (file serving + SSE proxy)
  const { server, port } = await startServer();
  const localUrl = `http://127.0.0.1:${port}`;
  console.log(`  Local server on ${localUrl}`);

  let browser;
  try {
    // Launch headless Chromium
    browser = await chromium.launch({ headless: true });
    const context = await browser.newContext();
    const page = await context.newPage();

    // Listen for console messages from the page
    page.on("console", (msg) => {
      console.log(`  [browser] ${msg.text()}`);
    });

    // Navigate to test page — SSE connects through the local proxy, not directly to HTTPS
    const testUrl = `${localUrl}/?SERVER_URL=${encodeURIComponent(localUrl)}&STREAM_TOKEN=${STREAM_TOKEN}`;
    console.log(`  Opening browser at ${testUrl}`);
    await page.goto(testUrl);

    // Wait for consumer to register, then ingest data
    console.log("  Waiting for SSE connection...");
    await new Promise((r) => setTimeout(r, 3000));

    console.log("  Ingesting test data...");
    await ingestTestData();

    // Wait for #status to become "done" (up to 15s)
    console.log("  Waiting for browser to receive batches...");
    await page.waitForFunction(
      () => {
        const el = document.getElementById("status");
        return el && (el.textContent === "done" || el.textContent === "error");
      },
      { timeout: 15000 },
    );

    // Read results from DOM
    const status = await page.$eval("#status", (el) => el.textContent);
    const schema = await page.$eval("#schema", (el) => el.textContent);
    const rows = await page.$eval("#rows", (el) => el.textContent);

    console.log("\n=== Results ===");
    console.log(`  Status: ${status}`);
    console.log(`  Schema: ${schema}`);
    console.log(`  Rows:   ${rows}`);

    // Assertions
    const errors = [];
    if (status !== "done") errors.push(`Status is '${status}', expected 'done'`);
    if (schema === "-") errors.push("No schema received");
    if (parseInt(rows) < 3) errors.push(`Expected >= 3 rows, got ${rows}`);

    if (errors.length > 0) {
      console.error("\nFAILED:", errors.join("; "));
      process.exit(1);
    }

    console.log("\nAll browser assertions passed!");
  } finally {
    if (browser) await browser.close();
    server.close();
  }
}

runBrowserTest().catch((err) => {
  console.error("\nBrowser test failed:", err.message || err);
  process.exit(1);
});
