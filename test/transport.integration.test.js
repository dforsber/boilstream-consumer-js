import assert from 'node:assert/strict';
import { test } from 'node:test';
import { createServer } from 'node:https';
import { readFileSync } from 'node:fs';
import { once } from 'node:events';
import { setImmediate as nextTurn } from 'node:timers/promises';
import { EventSource } from 'eventsource';
import { BoilStreamConsumer } from 'boilstream-consumer';
import { tableFromColumns, columnFromArray, tableToIPC } from '@uwdata/flechette';

const ipc = Buffer.from(tableToIPC(tableFromColumns({
  value: columnFromArray([11, null, 11, -7]),
}), { format: 'stream' })).toString('base64');
const event = (kind, data = '', id) =>
  `event: ${kind}\r\n${id === undefined ? '' : `id: ${id}\r\n`}data: ${data}\r\n\r\n`;

async function fixture(t, handler) {
  const server = createServer({
    cert: readFileSync(process.env.SSE_TEST_CERT),
    key: readFileSync(process.env.SSE_TEST_KEY),
  });
  const sockets = new Set();
  let requests = 0;
  server.on('connection', socket => {
    sockets.add(socket);
    socket.on('close', () => sockets.delete(socket));
  });
  server.on('request', (req, res) => {
    requests++;
    Promise.resolve(handler(req, res, requests)).catch(error => {
      res.destroy(error);
      t.assert.fail(error.message);
    });
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(async () => {
    const closed = new Promise(resolve => server.close(resolve));
    for (const socket of sockets) socket.destroy();
    await closed;
  });
  return { url: `https://127.0.0.1:${server.address().port}/stream/test`, requests: () => requests };
}

if (process.env.SSE_TEST_UNTRUSTED) {
  test('untrusted TLS never delivers an HTTP request or Arrow data', { timeout: 5000 }, async t => {
    const server = await fixture(t, () => assert.fail('untrusted request reached handler'));
    await new Promise(resolve => {
      const consumer = new BoilStreamConsumer(server.url, {
        EventSource,
        onBatch: () => assert.fail('untrusted rows delivered'),
        onError: () => { consumer.close(); resolve(); },
      });
      t.after(() => consumer.close());
    });
    assert.equal(server.requests(), 0);
  });
} else {
  test('real TLS, fragmented Arrow and reconnect preserve exact opaque cursor and rows', { timeout: 10000 }, async t => {
    const id = 'r1.AZ_opaque-9007199254740993';
    const headers = [], delivered = [];
    const server = await fixture(t, async (req, res, attempt) => {
      headers.push(req.headers['last-event-id']);
      res.writeHead(200, { 'content-type': 'text/event-stream' });
      if (attempt === 1) {
        const wire = 'retry: 10\r\n\r\n' + event('schema', ipc, 'r1.baseline') + event('batch', ipc, id);
        // Many socket writes split SSE field names, CRLF and base64 groups.
        for (let offset = 0; offset < wire.length; offset += 17) {
          res.write(wire.slice(offset, offset + 17));
          await nextTurn();
        }
        res.end();
      } else {
        assert.equal(attempt, 2);
        res.end(event('batch', ipc, 'r1.next') + event('refresh-required', '{"reason":"refresh_required"}') + event('batch', ipc, 'r1.stale'));
      }
    });
    await new Promise((resolve, reject) => {
      const consumer = new BoilStreamConsumer(server.url, {
        EventSource,
        onBatch: (table, cursor) => delivered.push([[...table.getChild('value')], cursor]),
        onRefreshRequired: resolve,
        onError: error => { if (error instanceof Error) reject(error); },
      });
      t.after(() => consumer.close());
    });
    assert.deepEqual(headers, [undefined, id]);
    assert.deepEqual(delivered, [[[11, null, 11, -7], id], [[11, null, 11, -7], 'r1.next']]);
  });

  for (const failure of ['decode', 'application', 'async application']) {
    test(`real EventSource closes on ${failure} failure before queued cursor`, { timeout: 5000 }, async t => {
      let delivered = 0, closed = false;
      class ObservedEventSource extends EventSource {
        close() { closed = true; super.close(); }
      }
      const server = await fixture(t, (_req, res) => {
        res.writeHead(200, { 'content-type': 'text/event-stream' });
        res.end('retry: 10\n\n' + event('batch', failure === 'decode' ? 'not-arrow' : ipc, 'r1.failed') + event('batch', ipc, 'r1.must-not-deliver'));
      });
      await new Promise(resolve => {
        const consumer = new BoilStreamConsumer(server.url, {
          EventSource: ObservedEventSource,
          onBatch: () => {
            delivered++;
            if (failure === 'async application') return Promise.reject(new Error('async failed'));
            throw new Error('application failed');
          },
          onError: () => { assert.equal(closed, true); resolve(); },
        });
        t.after(() => consumer.close());
      });
      await nextTurn();
      assert.equal(delivered, failure === 'decode' ? 0 : 1);
      assert.equal(server.requests(), 1);
    });
  }
}
