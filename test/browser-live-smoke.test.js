// Exercise the live-smoke command and its verified-TLS proxy using a local wire
// fixture. This validates the example harness, not BoilStream native ingestion.
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { createServer } from 'node:https';
import { once } from 'node:events';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawn, spawnSync } from 'node:child_process';
import { tableFromArrays, tableFromIPC, tableToIPC } from 'apache-arrow';

test('live example command preserves v2 headers through a trusted TLS proxy', { timeout: 20000 }, async t => {
  const dir = mkdtempSync(join(tmpdir(), 'sse-example-tls-'));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  const key = join(dir, 'key.pem'), cert = join(dir, 'cert.pem');
  const generated = spawnSync('openssl', ['req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
    '-subj', '/CN=localhost', '-addext', 'subjectAltName=IP:127.0.0.1,DNS:localhost', '-keyout', key, '-out', cert], { encoding: 'utf8' });
  assert.equal(generated.status, 0, generated.stderr);
  const token = 'b'.repeat(64), ingestToken = 'c'.repeat(64);
  const table = tableFromArrays({ value: [11, null, 11] });
  const ipc = value => Buffer.from(tableToIPC(value, 'stream')).toString('base64');
  const event = (kind, data, id) => `event: ${kind}\n${id ? `id: ${id}\n` : ''}data: ${data}\n\n`;
  let stream, authorization, rows;
  const server = createServer({ key: readFileSync(key), cert: readFileSync(cert) }, async (req, res) => {
    if (req.url === '/stream/v2') {
      authorization = req.headers.authorization;
      stream = res;
      res.writeHead(200, { 'content-type': 'text/event-stream', 'x-boilstream-sse-version': '2' });
      res.write(event('schema', ipc(table.slice(0,0))) + event('snapshot', ipc(table)) + event('snapshot-complete', '{}', 'r1.base'));
    } else if (req.url === `/ingest/${ingestToken}` && req.method === 'POST') {
      const chunks = []; for await (const chunk of req) chunks.push(chunk);
      rows = tableFromIPC(Buffer.concat(chunks)).numRows;
      res.writeHead(200); res.end('{}');
      stream.write(event('batch', ipc(table), 'r1.live'));
    } else { res.writeHead(404); res.end(); }
  });
  server.listen(0, '127.0.0.1'); await once(server, 'listening');
  t.after(async () => { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); });
  const env = { ...process.env, STREAM_TOKEN: token, INGEST_TOKEN: ingestToken,
    SERVER_URL: `https://127.0.0.1:${server.address().port}`, NODE_EXTRA_CA_CERTS: cert };
  delete env.NODE_TLS_REJECT_UNAUTHORIZED;
  const child = spawn(process.execPath, ['test/browser.test.js'], { env, stdio: ['ignore','pipe','pipe'] });
  t.after(() => { if (child.exitCode === null) child.kill(); });
  let output = ''; for (const pipe of [child.stdout, child.stderr]) pipe.on('data', chunk => output += chunk);
  const [code] = await once(child, 'close');
  assert.equal(code, 0, output);
  assert.equal(authorization, `Bearer ${token}`); assert.equal(rows, 3);
  assert.match(output, /PASS actual v2 browser example/);
  assert.ok(!output.includes(token) && !output.includes(ingestToken));
});
