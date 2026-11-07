// Local HTTP browser qualification of the installed SDK/decoder. This is not
// qualification of a deployed BoilStream server, TLS proxy or authorization.
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { createServer } from 'node:http';
import { readFileSync } from 'node:fs';
import { once } from 'node:events';
import { setImmediate as nextTurn } from 'node:timers/promises';
import { chromium, firefox, webkit } from 'playwright';
import { tableFromArrays, tableToIPC } from 'apache-arrow';

// An independent Arrow implementation creates the wire; browser flechette
// decodes nullable duplicates, Unicode and int64 values beyond JS safe integers.
const ipc = Buffer.from(tableToIPC(tableFromArrays({
  value: [11, null, 11],
  label: ['雪\n🌊', null, '雪\n🌊'],
  exact: BigInt64Array.from([9007199254740993n, -9007199254740993n, 0n]),
}), 'stream')).toString('base64');
const frame = (kind, data, id) => `event: ${kind}\r\n${id ? `id: ${id}\r\n` : ''}data: ${data}\r\n\r\n`;
const files = new Map([
  ['/src/index.js', new URL('../src/index.js', import.meta.url)],
  ['/src/consumer.js', new URL('../src/consumer.js', import.meta.url)],
  ['/src/decoder.js', new URL('../src/decoder.js', import.meta.url)],
  ['/flechette.mjs', new URL('../node_modules/@uwdata/flechette/dist/flechette.mjs', import.meta.url)],
]);

for (const [name, engine] of Object.entries({ chromium, firefox, webkit })) {
  test(`${name}: actual SDK resumes exact cursor and decodes independent Arrow`, { timeout: 30000 }, async t => {
    const headers = [];
    const server = createServer((req, res) => {
      if (req.url === '/events') {
        headers.push(req.headers['last-event-id']);
        res.writeHead(200, { 'content-type': 'text/event-stream' });
        const wire = headers.length === 1
          ? 'retry: 10\r\n\r\n' + frame('schema', ipc, 'r1.baseline') + frame('batch', ipc, 'r1.opaque_9007199254740993')
          : frame('batch', ipc, 'r1.next') + frame('refresh-required', '{"reason":"refresh_required"}') + frame('batch', ipc, 'r1.stale');
        void (async () => {
          for (let i = 0; i < wire.length; i += 31) { res.write(wire.slice(i, i + 31)); await nextTurn(); }
          res.end();
        })();
      } else if (files.has(req.url)) {
        res.writeHead(200, { 'content-type': 'text/javascript' });
        res.end(readFileSync(files.get(req.url)));
      } else {
        res.writeHead(200, { 'content-type': 'text/html' });
        res.end('<script type="importmap">{"imports":{"@uwdata/flechette":"/flechette.mjs"}}</script>');
      }
    });
    server.listen(0, '127.0.0.1');
    await once(server, 'listening');
    t.after(async () => {
      server.closeAllConnections();
      await new Promise(resolve => server.close(resolve));
    });
    const browser = await engine.launch();
    t.after(() => browser.close());
    const page = await browser.newPage();
    const errors = [];
    page.on('pageerror', error => errors.push(error.message));
    await page.goto(`http://127.0.0.1:${server.address().port}`);
    const result = await page.evaluate(async () => {
      const { BoilStreamConsumer } = await import('/src/index.js');
      const rows = [], cursors = [];
      await new Promise((resolve, reject) => {
        const consumer = new BoilStreamConsumer('/events', {
          onBatch: (table, cursor) => {
            rows.push({ value: [...table.getChild('value')], label: [...table.getChild('label')],
              exact: [...table.getChild('exact')].map(String) });
            cursors.push(cursor);
          },
          onRefreshRequired: resolve,
          onError: error => { if (error instanceof Error) { consumer.close(); reject(error); } },
        });
      });
      return { rows, cursors };
    });
    assert.deepEqual(errors, []);
    assert.deepEqual(headers, [undefined, 'r1.opaque_9007199254740993']);
    assert.deepEqual(result.cursors, ['r1.opaque_9007199254740993', 'r1.next']);
    const row = { value: [11, null, 11], label: ['雪\n🌊', null, '雪\n🌊'], exact: ['9007199254740993', '-9007199254740993', '0'] };
    assert.deepEqual(result.rows, [row, row]);
    console.log(`${name} ${browser.version()}`);
  });
}
