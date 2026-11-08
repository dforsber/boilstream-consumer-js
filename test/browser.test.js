#!/usr/bin/env node
// Optional live-server smoke of the actual v2 example. Requires a quiet append-only
// fixture topic, matching tokens and NODE_EXTRA_CA_CERTS for a private test CA.
// Browser -> loopback HTTP proxy -> verified server HTTPS; not deployed-edge qualification.
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import https from 'node:https';
import { once } from 'node:events';
import { chromium } from 'playwright';
import { tableFromColumns, columnFromArray, utf8, tableToIPC } from '@uwdata/flechette';
import { serveExample } from '../examples/serve.mjs';

const { STREAM_TOKEN, INGEST_TOKEN, SERVER_URL = 'https://localhost:8443' } = process.env;
assert.ok(STREAM_TOKEN && INGEST_TOKEN, 'Set STREAM_TOKEN and INGEST_TOKEN for the same quiet fixture topic');
assert.notEqual(process.env.NODE_TLS_REJECT_UNAUTHORIZED, '0', 'TLS verification must remain enabled');
const upstream = new URL(SERVER_URL);
assert.equal(upstream.protocol, 'https:');
const server = createServer((req, res) => {
  if (req.url !== '/stream/v2' || req.method !== 'GET') return serveExample(req, res);
  const headers = { accept: 'text/event-stream' };
  for (const name of ['authorization', 'last-event-id']) if (req.headers[name]) headers[name] = req.headers[name];
  const request = https.request(new URL('/stream/v2', upstream), { headers, rejectUnauthorized: true }, response => {
    res.writeHead(response.statusCode, response.headers); response.pipe(res);
  });
  request.on('error', () => { if (!res.headersSent) res.writeHead(502); res.end(); });
  res.on('close', () => request.destroy());
  request.end();
});
let browser;
try {
  server.listen(0, '127.0.0.1'); await once(server, 'listening');
  const origin = `http://127.0.0.1:${server.address().port}`;
  browser = await chromium.launch();
  const page = await browser.newPage();
  await page.goto(origin);
  await page.locator('#endpoint').fill(`${origin}/stream/v2`);
  await page.locator('#token').fill(STREAM_TOKEN);
  await page.locator('#start').click();
  await page.waitForFunction(() => document.querySelector('#status').textContent === 'live');
  const baseline = Number(await page.locator('#rows').textContent());
  const table = tableFromColumns({
    id: columnFromArray(Int32Array.from([1, 2, 3])),
    name: columnFromArray(['alice', 'bob', 'charlie'], utf8()),
    value: columnFromArray(Float64Array.from([1.1, 2.2, 3.3])),
  });
  const response = await fetch(new URL(`/ingest/${INGEST_TOKEN}`, upstream), {
    method: 'POST', headers: { 'content-type': 'application/vnd.apache.arrow.stream' },
    body: tableToIPC(table, { format: 'stream' }), signal: AbortSignal.timeout(10000),
  });
  assert.ok(response.ok, `Fixture ingestion status ${response.status}`);
  await page.waitForFunction(expected => Number(document.querySelector('#rows').textContent) === expected, baseline + 3);
  assert.match(await page.locator('#cursor').textContent(), /^r1\./);
  await page.locator('#stop').click();
  await page.waitForFunction(() => document.querySelector('#status').textContent === 'stopped');
  assert.equal(new URL(page.url()).search, '');
  console.log('PASS actual v2 browser example: complete snapshot, three live rows, committed cursor, disposal');
} finally {
  await browser?.close();
  server.closeAllConnections();
  await new Promise(resolve => server.close(resolve));
}
