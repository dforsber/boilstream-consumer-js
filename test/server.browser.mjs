// Actual SDK -> loopback HTTP proxy -> verified TLS/HTTP2 BoilStream.
// Invoked by the server's test-sse-browser target; stdin carries private fixture
// credentials, stdout is bounded fixture control. Never fabricate SSE frames.
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { connect } from 'node:http2';
import { once } from 'node:events';
import { readFileSync } from 'node:fs';
import { createInterface } from 'node:readline';
import { chromium, firefox, webkit } from 'playwright';

const lines = createInterface({ input: process.stdin });
const input = lines[Symbol.asyncIterator]();
const config = JSON.parse((await input.next()).value);
assert.equal(new URL(config.endpoint).hostname, 'localhost');
assert.equal(new URL(config.endpoint).protocol, 'https:');
assert.notEqual(process.env.NODE_TLS_REJECT_UNAUTHORIZED, '0', 'TLS verification must remain enabled');
async function command(command, value) {
  process.stdout.write(JSON.stringify({ command, value, suite: config.suite ?? 'snapshot' }) + '\n');
  const reply = await input.next();
  assert.ok(!reply.done && JSON.parse(reply.value).ok, 'native fixture command failed');
}
const ca = readFileSync(config.ca);
const files = new Map([
  ...['index', 'consumer', 'decoder', 'snapshot-consumer'].map(name => [`/src/${name}.js`, new URL(`../src/${name}.js`, import.meta.url)]),
  ...['index', 'parse', 'errors'].map(name => [`/parser/${name}.js`, new URL(`../node_modules/eventsource-parser/dist/${name}.js`, import.meta.url)]),
  ['/flechette.mjs', new URL('../node_modules/@uwdata/flechette/dist/flechette.mjs', import.meta.url)],
]);
const app = createServer((req, res) => {
  if (files.has(req.url)) {
    res.writeHead(200, { 'content-type': 'text/javascript' });
    res.end(readFileSync(files.get(req.url)));
  } else if (req.url === '/') {
    res.writeHead(200, { 'content-type': 'text/html', 'cache-control': 'no-store' });
    res.end('<script type="importmap">{"imports":{"@uwdata/flechette":"/flechette.mjs","eventsource-parser":"/parser/index.js"}}</script>');
  } else { res.writeHead(404); res.end(); }
});
let upstream, browser, interruptSnapshot = false, interrupted = 0, fault = "none";
const setFault = value => {
  assert.ok(["none", "disconnect-schema", "disconnect-schema-always", "stall-body"].includes(value));
  fault = value;
};
const requests = [], preflights = [], responses = [], sessions = new Set();
// Leave time for child/native cleanup inside the server fixture's 45-second bound.
const deadline = setTimeout(() => { void browser?.close(); upstream?.destroy(); }, 25000);
const proxy = createServer((req, res) => {
  if (req.url !== '/stream/v2' || !['GET', 'OPTIONS'].includes(req.method)) { res.writeHead(404); res.end(); return; }
  const headers = { ':method': req.method, ':path': req.url };
  for (const name of ['origin', 'authorization', 'last-event-id', 'accept', 'access-control-request-method', 'access-control-request-headers']) {
    if (req.headers[name] !== undefined) headers[name] = req.headers[name];
  }
  if (req.method === 'GET') requests.push({ cursor: headers['last-event-id'], credential: headers.authorization === `Bearer ${config.good}` ? 'good' : 'bad', origin: headers.origin });
  else preflights.push(headers['access-control-request-headers']);
  const stream = upstream.request(headers);
  sessions.add(stream);
  stream.on('close', () => sessions.delete(stream));
  stream.on('error', () => { if (!res.headersSent) res.writeHead(502); res.end(); });
  res.on('close', () => stream.close());
  stream.on('response', response => {
    const { ':status': status, ...headers } = response;
    responses.push({ method: req.method, status, origin: headers['access-control-allow-origin'], allowed: headers['access-control-allow-headers'] });
    res.writeHead(status, headers);
    res.flushHeaders();
    const cutKind = interruptSnapshot ? 'snapshot' : fault.startsWith('disconnect-schema') ? 'schema' : undefined;
    if (req.method === 'GET' && status === 200 && cutKind) {
      interruptSnapshot = false;
      if (fault === 'disconnect-schema') fault = 'none';
      let pending = Buffer.alloc(0), cut = false;
      stream.on('data', chunk => {
        if (cut) return;
        pending = Buffer.concat([pending, chunk]);
        if (pending.length > 1024 * 1024) { res.destroy(new Error('fixture frame limit')); stream.close(); return; }
        let end;
        while ((end = pending.indexOf('\n\n')) !== -1) {
          const frame = pending.subarray(0, end + 2);
          pending = pending.subarray(end + 2);
          res.write(frame);
          if (frame.toString().startsWith(`event: ${cutKind}\n`)) {
            cut = true; interrupted++; res.end(); stream.close(); return;
          }
        }
      });
      stream.on('end', () => { if (!cut) { res.write(pending); res.end(); } });
    } else if (req.method === 'GET' && status === 200 && fault === 'stall-body') {
      stream.pause(); // Model a proxy that sends headers but never flushes its SSE body.
    } else stream.pipe(res); // Ordinary proxy path preserves transport backpressure.
  });
  stream.end();
});
async function listen(server, port = 0) { server.listen(port, '127.0.0.1'); await once(server, 'listening'); return `http://127.0.0.1:${server.address().port}`; }
async function shutdown(server) { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); }
try {
  // Negative control: the self-signed fixture chain must fail without its CA.
  await new Promise((resolve, reject) => {
    const untrusted = connect(config.endpoint, { ca: [] });
    const timer = setTimeout(() => { untrusted.destroy(); reject(new Error('TLS negative control timed out')); }, 5000);
    untrusted.once('connect', () => { clearTimeout(timer); untrusted.destroy(); reject(new Error('untrusted TLS accepted')); });
    untrusted.once('error', error => { clearTimeout(timer); untrusted.destroy();
      if (['UNABLE_TO_VERIFY_LEAF_SIGNATURE', 'UNABLE_TO_GET_ISSUER_CERT_LOCALLY', 'SELF_SIGNED_CERT_IN_CHAIN', 'DEPTH_ZERO_SELF_SIGNED_CERT'].includes(error.code)) resolve(); else reject(error); });
  });
  upstream = connect(config.endpoint, { ca, rejectUnauthorized: true });
  await once(upstream, 'connect');
  assert.equal(upstream.socket.authorized, true);
  upstream.on('error', () => {}); // Individual streams surface failures to the SDK.
  const origin = await listen(app, config.appPort), endpoint = (await listen(proxy)) + '/stream/v2';
  const expected = [null, '11', '11', '9007199254740993'];
  let checks = 0;
  assert.ok(['chromium', 'firefox', 'webkit'].includes(config.browser));
  assert.ok(['snapshot', 'recovery'].includes(config.suite ?? 'snapshot'));
  for (const [name, engine] of Object.entries({ chromium, firefox, webkit }).filter(([name]) => name === config.browser)) {
    browser = await engine.launch();
    const page = await browser.newPage();
    const errors = [];
    page.on('pageerror', error => errors.push(error.message));
    const value = 23;
    await page.exposeFunction('appendNative', () => command('append', value));
    await page.goto(origin);
    if (config.suite === 'recovery') {
      const { recovery } = await import('./server.recovery.mjs');
      checks = await recovery(page, endpoint, config.good, requests, setFault);
      assert.deepEqual(errors, []);
      process.stderr.write(`${name} ${browser.version()}: disconnect replay, retry exhaustion, stalled proxy, disposal PASS\n`);
      await browser.close(); browser = undefined;
      continue;
    }
    interruptSnapshot = true;
    const requestStart = requests.length;
    const result = await page.evaluate(async ({ endpoint, bad, good }) => {
      const { BoilStreamSnapshotConsumer } = await import('/src/index.js');
      const rows = table => [...table.getChild('v')].map(value => value === null ? null : String(value));
      let staged = [], active = [], starts = 0, discards = 0, refreshed = 0, renewed = false;
      const renewals = [];
      const defaults = {
        getToken: () => good, onSnapshotStart() {}, onSnapshot() {}, onSnapshotComplete() {},
        onSnapshotDiscard() {}, onBatch() {}, onRefreshRequired() { refreshed++; },
        retryMinMs: 5, retryMaxMs: 20, maxRetries: 3, ioTimeoutMs: 10000,
      };
      const snapshot = new BoilStreamSnapshotConsumer(endpoint, { ...defaults,
        getToken: ({ unauthorized }) => { renewals.push(unauthorized); renewed ||= unauthorized; return renewed ? good : bad; },
        onSnapshotStart() { starts++; staged = []; },
        async onSnapshot(table) { await Promise.resolve(); staged.push(...rows(table)); },
        async onSnapshotComplete() { await Promise.resolve(); active = [...staged]; snapshot.close(); },
        onSnapshotDiscard() { discards++; staged = []; },
      });
      await snapshot.run();
      const cut = snapshot.cursor;
      let inserted = false, liveRows = [], cursorDuringCommit;
      const live = new BoilStreamSnapshotConsumer(endpoint, { ...defaults, cursor: cut,
        async onSchema() { if (!inserted) { inserted = true; await window.appendNative(); } },
        async onBatch(table) { cursorDuringCommit = live.cursor; await Promise.resolve(); liveRows.push(...rows(table)); live.close(); },
      });
      await live.run();
      let applicationError, rejectedRows = [], failures = 0;
      const rejected = new BoilStreamSnapshotConsumer(endpoint, { ...defaults, cursor: cut,
        async onBatch(table) { rejectedRows.push(...rows(table)); await Promise.resolve(); throw new Error('fixture application failure'); },
        onRefreshRequired() { failures++; },
      });
      try { await rejected.run(); } catch (error) { applicationError = error.code; }
      let replayRows = [], snapshotOnResume = 0;
      const resume = new BoilStreamSnapshotConsumer(endpoint, { ...defaults, cursor: cut,
        onSnapshotStart() { snapshotOnResume++; },
        async onBatch(table) { await Promise.resolve(); replayRows.push(...rows(table)); resume.close(); },
      });
      await resume.run();
      return { active, starts, discards, renewals, refreshed, cut, liveRows, liveCursor: live.cursor, cursorDuringCommit,
        applicationError, rejectedRows, rejectedCursor: rejected.cursor, failures, replayRows, resumeCursor: resume.cursor, snapshotOnResume };
    }, { endpoint, bad: config.bad, good: config.good });
    assert.deepEqual(errors, []);
    assert.deepEqual(result.active.slice().sort(), expected.slice().sort());
    assert.equal(result.starts, 2); assert.equal(result.discards, 1);
    assert.deepEqual(result.renewals, [false, true, false]);
    assert.equal(result.refreshed, 0); assert.match(result.cut, /^r1\./);
    assert.deepEqual(result.liveRows, [String(value)]);
    assert.equal(result.cursorDuringCommit, result.cut);
    assert.notEqual(result.liveCursor, result.cut);
    assert.equal(result.applicationError, 'application_failed');
    assert.deepEqual(result.rejectedRows, [String(value)]);
    assert.equal(result.rejectedCursor, undefined); assert.equal(result.failures, 1);
    assert.deepEqual(result.replayRows, [String(value)]);
    assert.equal(result.resumeCursor, result.liveCursor); assert.equal(result.snapshotOnResume, 0);
    const seen = requests.slice(requestStart);
    assert.deepEqual(seen.map(request => request.credential), ['bad', 'good', 'good', 'good', 'good', 'good']);
    assert.deepEqual(seen.map(request => request.cursor), [undefined, undefined, undefined, result.cut, result.cut, result.cut]);
    assert.ok(seen.every(request => request.origin === origin));
    // Browsers may cache every header permitted by the first response (WebKit),
    // or preflight again when Last-Event-ID is first used (Chromium/Firefox).
    assert.ok(preflights.length > 0 && preflights.every(headers => headers.includes('authorization')));
    assert.ok(responses.filter(response => response.method === 'OPTIONS').every(response =>
      response.status === 204 && response.origin === origin &&
      response.allowed.split(',').map(value => value.trim()).includes('last-event-id')));
    checks += 4; expected.push(String(value));
    process.stderr.write(`${name} ${browser.version()}: snapshot restart/renewal, live commit, app failure, replay PASS\n`);
    await browser.close(); browser = undefined;
  }
  assert.equal(interrupted, config.suite === 'recovery' ? 4 : 1);
  assert.equal(checks, 4);
  await command('complete', checks);
} catch (error) {
  process.stderr.write(JSON.stringify({ requests, preflights, responses, interrupted }) + '\n');
  throw error;
} finally {
  clearTimeout(deadline);
  if (browser) await browser.close();
  for (const stream of sessions) stream.close();
  if (upstream) upstream.destroy();
  await Promise.all([shutdown(proxy), shutdown(app)]);
  lines.close(); process.stdin.destroy();
}
