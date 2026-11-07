import assert from 'node:assert/strict';
import { test } from 'node:test';
import { createServer } from 'node:https';
import { readFileSync } from 'node:fs';
import { once } from 'node:events';
import { setImmediate as turn } from 'node:timers/promises';
import { tableFromArrays, tableToIPC } from 'apache-arrow';
import { BoilStreamSnapshotConsumer, StreamError } from 'boilstream-consumer';

const TOKEN = 'a'.repeat(64), RENEWED = 'b'.repeat(64);
const table = tableFromArrays({ value: [11, null, 11] });
const ipc = Buffer.from(tableToIPC(table, 'stream')).toString('base64');
const schema = Buffer.from(tableToIPC(table.slice(0, 0), 'stream')).toString('base64');
const event = (kind, data = '', id) => `event: ${kind}\r\n${id === undefined ? '' : `id: ${id}\r\n`}data: ${data}\r\n\r\n`;
const initial = event('schema', schema) + event('snapshot', ipc) + event('snapshot-complete', '{}', 'r1.base');
const deferred = () => { let resolve; const promise = new Promise(r => { resolve = r; }); return { promise, resolve }; };
const defaults = overrides => ({
  getToken: () => TOKEN, onSnapshotStart() {}, onSnapshot() {}, onSnapshotComplete() {},
  onSnapshotDiscard() {}, onBatch() {}, onRefreshRequired() {},
  retryMinMs: 1, retryMaxMs: 2, ioTimeoutMs: 1000, maxRetries: 2, ...overrides,
});
async function server(t, handle) {
  const requests = [], failures = [];
  const s = createServer({ cert: readFileSync(process.env.SSE_TEST_CERT), key: readFileSync(process.env.SSE_TEST_KEY) });
  const sockets = new Set();
  s.on('connection', socket => { sockets.add(socket); socket.on('close', () => sockets.delete(socket)); });
  s.on('request', (req, res) => {
    requests.push({ path: req.url, headers: req.headers });
    Promise.resolve().then(() => handle(req, res, requests.length)).catch(error => { failures.push(error); res.destroy(); });
  });
  s.listen(0, '127.0.0.1');
  await once(s, 'listening');
  t.after(async () => {
    for (const socket of sockets) socket.destroy();
    await new Promise(resolve => s.close(resolve));
    assert.deepEqual(failures, []);
  });
  return { url: `https://127.0.0.1:${s.address().port}/stream/v2`, requests };
}
const headers = res => res.writeHead(200, { 'content-type': 'text/event-stream', 'x-boilstream-sse-version': '2' });

if (process.env.SSE_TEST_UNTRUSTED) {
  test('Fetch v2 refuses untrusted TLS without sending credentials', { timeout: 5000 }, async t => {
    const s = await server(t, () => assert.fail('untrusted HTTP reached server'));
    const c = new BoilStreamSnapshotConsumer(s.url, defaults({ maxRetries: 0 }));
    await assert.rejects(c.run(), { code: 'retry_exhausted' });
    assert.equal(s.requests.length, 0);
  });
} else {
  test('snapshot activation and live resume wait for async app commits over real TLS', { timeout: 5000 }, async t => {
    const completing = deferred(), commit = deferred(), applying = deferred(), applied = deferred();
    const staged = [], active = [], callbacks = [];
    const s = await server(t, (_req, res, n) => {
      headers(res);
      res.end(n === 1 ? initial + event('batch', ipc, 'r1.live1')
        : event('schema', schema, 'r1.live1') + event('batch', ipc, 'r1.live2'));
    });
    const c = new BoilStreamSnapshotConsumer(s.url, defaults({
      onSnapshotStart: () => { callbacks.push('start'); },
      onSnapshot: async rows => { staged.push(...rows.getChild('value')); await turn(); },
      onSnapshotComplete: async id => { completing.resolve(); await commit.promise; active.push(...staged); callbacks.push(id); },
      onSnapshotDiscard: () => assert.fail('committed snapshot discarded'),
      onBatch: async (rows, id) => {
        if (id === 'r1.live1') { applying.resolve(); await applied.promise; }
        active.push(...rows.getChild('value')); callbacks.push(id);
        if (id === 'r1.live2') c.close();
      },
      onRefreshRequired: () => assert.fail('healthy subscription refreshed'),
    }));
    t.after(() => c.close());
    const running = c.run();
    await completing.promise;
    assert.equal(c.cursor, undefined);
    assert.deepEqual(active, []);
    commit.resolve();
    await applying.promise;
    assert.equal(c.cursor, 'r1.base');
    assert.equal(s.requests.length, 1, 'reconnected before application completed');
    applied.resolve();
    await running;
    assert.equal(c.cursor, 'r1.live2');
    assert.deepEqual(callbacks, ['start', 'r1.base', 'r1.live1', 'r1.live2']);
    assert.deepEqual(active, [11, null, 11, 11, null, 11, 11, null, 11]);
    assert.deepEqual(s.requests.map(r => r.headers['last-event-id']), [undefined, 'r1.live1']);
    for (const request of s.requests) {
      assert.equal(request.path, '/stream/v2');
      assert.equal(request.headers.authorization, `Bearer ${TOKEN}`);
    }
  });

  test('interrupted provisional snapshot restarts without a cursor and replaces staged rows', { timeout: 5000 }, async t => {
    let staged = [], active = [], starts = 0, discards = 0, commits = 0;
    const s = await server(t, (_req, res, n) => {
      headers(res);
      res.end(n === 1 ? event('schema', schema) + event('snapshot', ipc) + 'event: snapshot-complete\nid: r1.partial\ndata: {' : initial);
    });
    const c = new BoilStreamSnapshotConsumer(s.url, defaults({
      onSnapshotStart: () => { starts++; staged = []; },
      onSnapshot: rows => { staged.push(...rows.getChild('value')); },
      onSnapshotDiscard: () => { discards++; staged = []; },
      onSnapshotComplete: () => { commits++; active = staged; c.close(); },
    }));
    await c.run();
    assert.deepEqual([starts, discards, commits], [2, 1, 1]);
    assert.deepEqual(active, [11, null, 11]);
    assert.equal(c.cursor, 'r1.base');
    assert.deepEqual(s.requests.map(r => r.headers['last-event-id']), [undefined, undefined]);
  });

  test('401 obtains renewed header credentials without changing endpoint or cursor', { timeout: 5000 }, async t => {
    const renewals = [];
    const s = await server(t, (_req, res, n) => {
      if (n === 1) { res.writeHead(401); res.end('private response body'); return; }
      headers(res); res.end(event('schema', schema, 'r1.persisted') + event('batch', ipc, 'r1.next'));
    });
    const c = new BoilStreamSnapshotConsumer(s.url, defaults({
      cursor: 'r1.persisted',
      getToken: ({ unauthorized }) => { renewals.push(unauthorized); return unauthorized ? RENEWED : TOKEN; },
      onSnapshotStart: () => assert.fail('resume started new snapshot'),
      onBatch: () => { c.close(); },
    }));
    await c.run();
    assert.deepEqual(renewals, [false, true]);
    assert.deepEqual(s.requests.map(r => r.headers.authorization), [`Bearer ${TOKEN}`, `Bearer ${RENEWED}`]);
    assert.deepEqual(s.requests.map(r => r.headers['last-event-id']), ['r1.persisted', 'r1.persisted']);
    assert.equal(c.cursor, 'r1.next');
  });

  for (const failure of ['commit', 'discard', 'refresh']) {
    test(`failed refresh ${failure} callback cannot activate or reuse a generation`, { timeout: 5000 }, async t => {
      let staged = [], active = [], discards = 0, refreshes = 0, batches = 0;
      const s = await server(t, (_req, res, n) => {
        headers(res);
        const interrupted = event('schema', schema) + event('snapshot', ipc) +
          event('refresh-required', '{"reason":"refresh_required"}') +
          event('snapshot-complete', '{}', 'r1.invalid');
        res.end((n === 1 && failure !== 'commit' ? interrupted : initial) +
          event('batch', ipc, 'r1.queued'));
      });
      const c = new BoilStreamSnapshotConsumer(s.url, defaults({
        onSnapshot: rows => { staged.push(...rows.getChild('value')); },
        onSnapshotComplete: async () => {
          assert.equal(c.cursor, undefined);
          await turn();
          throw new Error(TOKEN); // The application's transaction never commits.
        },
        onSnapshotDiscard: async () => {
          discards++; staged = [];
          assert.equal(c.cursor, undefined);
          assert.equal(c.schema, null);
          await turn();
          if (failure === 'discard') throw new Error(TOKEN);
        },
        onRefreshRequired: async () => {
          refreshes++;
          assert.equal(c.cursor, undefined);
          assert.equal(c.schema, null);
          await turn();
          if (failure === 'refresh') throw new Error(TOKEN);
        },
        onBatch: () => { batches++; },
      }));
      t.after(() => c.close());
      await assert.rejects(c.run(), error => error instanceof StreamError &&
        error.code === 'application_failed' && !error.message.includes(TOKEN));
      assert.deepEqual([discards, refreshes, batches, s.requests.length], [1, 1, 0, 1]);
      assert.deepEqual(staged, []);
      assert.deepEqual(active, []);
      assert.equal(c.cursor, undefined);
      assert.equal(c.schema, null);
      await assert.rejects(c.run(), { code: 'already_started' });
      assert.equal(s.requests.length, 1, 'failed generation must not reconnect');

      // Recovery is an explicit new generation. It requests a complete snapshot
      // without the failed cursor, retains duplicate/NULL rows and commits once.
      let commits = 0;
      const replacement = new BoilStreamSnapshotConsumer(s.url, defaults({
        onSnapshotStart: () => { staged = []; },
        onSnapshot: rows => { staged.push(...rows.getChild('value')); },
        onSnapshotComplete: async () => {
          assert.equal(replacement.cursor, undefined);
          await turn(); active = staged; commits++; replacement.close();
        },
        onSnapshotDiscard: () => assert.fail('replacement commit was discarded'),
        onBatch: () => assert.fail('queued data crossed explicit close'),
        onRefreshRequired: () => assert.fail('valid replacement failed'),
      }));
      t.after(() => replacement.close());
      await replacement.run();
      assert.equal(commits, 1);
      assert.deepEqual(active, [11, null, 11]);
      assert.equal(replacement.cursor, 'r1.base');
      assert.deepEqual(s.requests.map(r => r.headers['last-event-id']), [undefined, undefined]);
    });
  }

  test('persistent 401 has one renewal attempt and terminal state invalidation', { timeout: 5000 }, async t => {
    let refreshed = 0;
    const s = await server(t, (_req, res) => { res.writeHead(401); res.end(TOKEN); });
    const c = new BoilStreamSnapshotConsumer(s.url, defaults({ cursor: 'r1.old', maxRetries: 8, onRefreshRequired: () => { refreshed++; } }));
    await assert.rejects(c.run(), error => error instanceof StreamError && error.code === 'authentication_failed' && !error.message.includes(TOKEN));
    assert.equal(s.requests.length, 2);
    assert.equal(refreshed, 1);
    assert.equal(c.cursor, undefined);
  });

  const failures = [
    ['protocol_mismatch', res => { res.writeHead(200, { 'content-type': 'text/event-stream' }); res.end(initial); }],
    ['http_refused', res => { res.writeHead(403); res.end(TOKEN); }],
    ['invalid_snapshot', res => { headers(res); res.end(event('schema', schema) + event('snapshot', ipc, 'r1.early')); }],
    ['invalid_schema', res => { headers(res); res.end(event('schema', ipc)); }],
    ['invalid_arrow', res => { headers(res); res.end(event('schema', schema) + event('snapshot', 'invalid-arrow')); }],
    ['invalid_batch', res => { headers(res); res.end(event('schema', schema) + event('batch', ipc, 'r1.early')); }],
    ['refresh_required', res => { headers(res); res.end(event('refresh-required', 'malformed') + initial); }],
  ];
  for (const [code, respond] of failures) {
    test(`v2 ${code} fails closed without retry or activation`, { timeout: 5000 }, async t => {
      let refreshed = 0;
      const s = await server(t, (_req, res) => respond(res));
      const c = new BoilStreamSnapshotConsumer(s.url, defaults({
        onSnapshotComplete: () => assert.fail('invalid stream activated'),
        onBatch: () => assert.fail('invalid stream applied data'),
        onRefreshRequired: () => { refreshed++; },
      }));
      await assert.rejects(c.run(), { code });
      assert.equal(s.requests.length, 1);
      assert.equal(refreshed, 1);
      assert.equal(c.cursor, undefined);
    });
  }

  test('application failure suppresses already queued batches and clears resume state', { timeout: 5000 }, async t => {
    let calls = 0, refreshed = 0;
    const s = await server(t, (_req, res) => { headers(res); res.end(initial + event('batch', ipc, 'r1.first') + event('batch', ipc, 'r1.second')); });
    const c = new BoilStreamSnapshotConsumer(s.url, defaults({
      onBatch: async () => { calls++; await turn(); throw new Error(TOKEN); },
      onRefreshRequired: () => { refreshed++; },
    }));
    await assert.rejects(c.run(), error => error.code === 'application_failed' && !error.message.includes(TOKEN));
    assert.deepEqual([calls, refreshed, s.requests.length], [1, 1, 1]);
    assert.equal(c.cursor, undefined);
  });

  test('closing inside a failing commit does not hide application failure', { timeout: 5000 }, async t => {
    let refreshed = 0;
    const s = await server(t, (_req, res) => { headers(res); res.end(initial + event('batch', ipc, 'r1.failed')); });
    const c = new BoilStreamSnapshotConsumer(s.url, defaults({
      onBatch: async () => { c.close(); throw new Error(TOKEN); },
      onRefreshRequired: () => { refreshed++; },
    }));
    await assert.rejects(c.run(), { code: 'application_failed' });
    assert.equal(c.cursor, undefined);
    assert.equal(refreshed, 1);
  });

  test('close waits for the in-flight commit and suppresses queued data', { timeout: 5000 }, async t => {
    const applying = deferred(), committed = deferred();
    let calls = 0, settled = false;
    const s = await server(t, (_req, res) => { headers(res); res.end(initial + event('batch', ipc, 'r1.first') + event('batch', ipc, 'r1.second')); });
    const c = new BoilStreamSnapshotConsumer(s.url, defaults({
      onBatch: async (_table, _cursor, { signal }) => {
        calls++; applying.resolve(); await committed.promise;
        assert.equal(signal.aborted, true);
      },
      onRefreshRequired: () => assert.fail('explicit close invalidated committed data'),
    }));
    const running = c.run().finally(() => { settled = true; });
    await applying.promise;
    c.close(); await turn();
    assert.equal(settled, false);
    assert.equal(c.cursor, 'r1.base');
    committed.resolve(); await running;
    assert.equal(c.cursor, 'r1.first');
    assert.equal(calls, 1);
    assert.equal(s.requests.length, 1);
  });

  test('503 retries preserve committed state and bounded hints without activating a snapshot', { timeout: 5000 }, async t => {
    const s = await server(t, (_req, res, n) => {
      if (n === 1) { res.writeHead(503, { 'retry-after': '99999999999' }); res.end(TOKEN); return; }
      headers(res); res.end(event('schema', schema, 'r1.old') + event('batch', ipc, 'r1.new'));
    });
    const c = new BoilStreamSnapshotConsumer(s.url, defaults({
      cursor: 'r1.old', onSnapshotStart: () => assert.fail('resume activated snapshot'),
      onBatch: () => { c.close(); },
    }));
    await c.run();
    assert.equal(c.cursor, 'r1.new');
    assert.deepEqual(s.requests.map(r => r.headers['last-event-id']), ['r1.old', 'r1.old']);
  });

  test('partial-frame limit terminates without buffering an unlimited data line', { timeout: 5000 }, async t => {
    const s = await server(t, (_req, res) => { headers(res); res.end('data: ' + 'x'.repeat(1024)); });
    const c = new BoilStreamSnapshotConsumer(s.url, defaults({ maxEventChars: 128 }));
    await assert.rejects(c.run(), { code: 'invalid_frame' });
    assert.equal(s.requests.length, 1);
  });

  test('close aborts an unresolved token provider without making an HTTP request', { timeout: 5000 }, async t => {
    const started = deferred();
    let signal;
    const s = await server(t, () => assert.fail('closed request reached server'));
    const c = new BoilStreamSnapshotConsumer(s.url, defaults({ getToken: context => { signal = context.signal; started.resolve(); return new Promise(() => {}); } }));
    const running = c.run();
    await started.promise;
    c.close();
    await running;
    assert.equal(signal.aborted, true);
    assert.equal(s.requests.length, 0);
  });

  test('token-provider deadline and retry budget are bounded even if callback never settles', { timeout: 5000 }, async t => {
    let calls = 0;
    const s = await server(t, () => assert.fail('missing credentials reached server'));
    const c = new BoilStreamSnapshotConsumer(s.url, defaults({
      ioTimeoutMs: 10, maxRetries: 1, getToken: () => { calls++; return new Promise(() => {}); },
    }));
    await assert.rejects(c.run(), { code: 'retry_exhausted' });
    assert.equal(calls, 2);
    assert.equal(s.requests.length, 0);
  });

  test('redirects never forward the bearer credential to another endpoint', { timeout: 5000 }, async t => {
    const destination = await server(t, () => assert.fail('redirect followed'));
    const s = await server(t, (_req, res) => { res.writeHead(307, { location: destination.url }); res.end(); });
    const c = new BoilStreamSnapshotConsumer(s.url, defaults({ maxRetries: 0 }));
    await assert.rejects(c.run(), { code: 'retry_exhausted' });
    assert.equal(s.requests.length, 1);
    assert.equal(destination.requests.length, 0);
  });
}
