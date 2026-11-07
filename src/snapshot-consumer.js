import { createParser } from 'eventsource-parser';
import { decodeArrowIPC } from './decoder.js';

/** Stable, redacted errors: never include credentials, response bodies or URLs. */
export class StreamError extends Error {
  constructor(code) {
    super(`BoilStream subscription stopped: ${code}`);
    this.name = 'StreamError';
    this.code = code;
  }
}
class Retry extends Error {
  constructor(unauthorized = false, delay = 0) {
    super('Stream interrupted');
    this.unauthorized = unauthorized;
    this.delay = delay;
  }
}
const fail = code => { throw new StreamError(code); };
const cursorValid = value => typeof value === 'string' && value.length <= 8192 && /^r1\.[A-Za-z0-9_-]+$/.test(value);
const positive = (value, max) => Number.isSafeInteger(value) && value > 0 && value <= max;

/** Await external I/O with cancellation and a deadline; do not race app commits. */
function io(operation, controller, milliseconds) {
  const signal = controller.signal;
  return new Promise((resolve, reject) => {
    if (signal.aborted) { reject(new Retry()); return; }
    let timer;
    const finish = (settle, value) => {
      clearTimeout(timer);
      signal.removeEventListener('abort', aborted);
      settle(value);
    };
    const aborted = () => finish(reject, new Retry());
    signal.addEventListener('abort', aborted, { once: true });
    timer = setTimeout(() => controller.abort(), milliseconds);
    Promise.resolve().then(() => {
      if (signal.aborted) throw new Retry();
      return operation();
    }).then(value => finish(resolve, value), () => finish(reject, new Retry()));
  });
}
function delay(milliseconds, signal) {
  return new Promise(resolve => {
    if (signal.aborted) { resolve(); return; }
    const done = () => { clearTimeout(timer); signal.removeEventListener('abort', done); resolve(); };
    const timer = setTimeout(done, milliseconds);
    signal.addEventListener('abort', done, { once: true });
  });
}

/**
 * Explicit v2 snapshot/resume client. Construct, then await run(). Application
 * callbacks may be async; only their successful completion commits a cursor.
 */
export class BoilStreamSnapshotConsumer {
  #url; #options; #fetch; #controller = new AbortController();
  #cursor; #schema = null; #running = false; #provisional = false;
  #progress = 0;

  constructor(url, options) {
    let endpoint;
    try { endpoint = new URL(url, globalThis.location?.href); } catch { fail('invalid_endpoint'); }
    const loopback = ['localhost', '127.0.0.1', '[::1]'].includes(endpoint.hostname);
    if ((endpoint.protocol !== 'https:' && !(endpoint.protocol === 'http:' && loopback)) ||
        endpoint.pathname !== '/stream/v2' || endpoint.username || endpoint.password || endpoint.search || endpoint.hash) {
      fail('invalid_endpoint');
    }
    for (const name of ['getToken', 'onSnapshotStart', 'onSnapshot', 'onSnapshotComplete', 'onSnapshotDiscard', 'onBatch', 'onRefreshRequired']) {
      if (typeof options?.[name] !== 'function') fail('missing_callback');
    }
    this.#options = {
      maxRetries: 8, retryMinMs: 500, retryMaxMs: 30000,
      ioTimeoutMs: 30000, maxEventChars: 24 * 1024 * 1024, ...options,
    };
    const o = this.#options;
    if (!Number.isSafeInteger(o.maxRetries) || o.maxRetries < 0 || o.maxRetries > 100 ||
        !positive(o.retryMinMs, 60000) || !positive(o.retryMaxMs, 60000) || o.retryMinMs > o.retryMaxMs ||
        !positive(o.ioTimeoutMs, 300000) || !positive(o.maxEventChars, 24 * 1024 * 1024)) fail('invalid_limits');
    if (o.cursor !== undefined && !cursorValid(o.cursor)) fail('invalid_cursor');
    this.#cursor = o.cursor;
    this.#url = endpoint.href;
    const fetch = options.fetch ?? globalThis.fetch;
    if (typeof fetch !== 'function') fail('fetch_unavailable');
    // Browser fetch is receiver-sensitive; a stored private field otherwise
    // invokes it with this consumer as the receiver (illegal invocation).
    this.#fetch = fetch.bind(globalThis);
  }
  get cursor() { return this.#cursor; }
  get schema() { return this.#schema; }
  close() { this.#controller.abort(); }

  async #callback(name, ...args) {
    try { return await this.#options[name]?.(...args); }
    catch { fail('application_failed'); }
  }
  async #discard() {
    if (!this.#provisional) return;
    this.#provisional = false;
    await this.#callback('onSnapshotDiscard');
  }

  async run() {
    if (this.#running) fail('already_started');
    this.#running = true;
    let failures = 0, unauthorized = false, authFailures = 0;
    try {
      while (!this.#controller.signal.aborted) {
        const before = this.#progress;
        try { await this.#connection(unauthorized); }
        catch (error) {
          if (error instanceof Retry && this.#controller.signal.aborted) break;
          if (!(error instanceof Retry)) throw error;
          await this.#discard();
          if (this.#progress !== before) failures = 0;
          if (++failures > this.#options.maxRetries) fail('retry_exhausted');
          unauthorized = error.unauthorized;
          authFailures = unauthorized ? authFailures + 1 : 0;
          if (authFailures > 1) fail('authentication_failed');
          const cap = Math.min(this.#options.retryMaxMs, this.#options.retryMinMs * 2 ** Math.min(failures - 1, 16));
          const jitter = this.#options.retryMinMs + Math.random() * (cap - this.#options.retryMinMs);
          await delay(Math.min(this.#options.retryMaxMs, Math.max(jitter, error.delay)), this.#controller.signal);
        }
      }
    } catch (error) {
      this.close();
      // Terminal failure invalidates the application state. Never hand the
      // caller a reusable cursor for that invalidated generation.
      this.#cursor = undefined;
      this.#schema = null;
      try { await this.#discard(); }
      finally { await this.#callback('onRefreshRequired', { reason: 'refresh_required' }); }
      throw error instanceof StreamError ? error : new StreamError('protocol_failed');
    } finally {
      await this.#discard();
    }
  }

  async #connection(unauthorized) {
    const controller = new AbortController();
    const closed = () => controller.abort();
    this.#controller.signal.addEventListener('abort', closed, { once: true });
    if (this.#controller.signal.aborted) controller.abort();
    let reader, response;
    const signal = controller.signal;
    const o = this.#options;
    let retryDelay = 0;
    try {
      const token = await io(() => o.getToken({ signal, unauthorized }), controller, o.ioTimeoutMs);
      if (typeof token !== 'string' || !/^(?:[A-Fa-f0-9]{64}|[A-Fa-f0-9]{128})$/.test(token)) fail('invalid_credential');
      const headers = { Accept: 'text/event-stream', Authorization: `Bearer ${token}` };
      if (this.#cursor !== undefined) headers['Last-Event-ID'] = this.#cursor;
      response = await io(() => this.#fetch(this.#url, {
        method: 'GET', headers, signal, credentials: 'omit', redirect: 'error', cache: 'no-store',
      }), controller, o.ioTimeoutMs);
      if (response.status === 401) throw new Retry(true);
      if ([429, 502, 503, 504].includes(response.status)) {
        const seconds = Number(response.headers.get('retry-after'));
        throw new Retry(false, Number.isFinite(seconds) && seconds > 0 ? seconds * 1000 : 0);
      }
      if (response.status !== 200) fail('http_refused');
      if (response.headers.get('x-boilstream-sse-version') !== '2' ||
          response.headers.get('content-type')?.split(';')[0].trim().toLowerCase() !== 'text/event-stream' || !response.body) fail('protocol_mismatch');
      reader = response.body.getReader();
      let stage = 'schema';
      const initial = this.#cursor === undefined;
      if (initial) {
        this.#provisional = true;
        await this.#callback('onSnapshotStart', { signal: this.#controller.signal });
      }
      const queue = [];
      let queuedChars = 0;
      const parser = createParser({
        maxBufferSize: o.maxEventChars,
        onError: () => fail('invalid_frame'),
        onRetry: ms => { retryDelay = Math.min(o.retryMaxMs, Math.max(o.retryMinMs, ms)); },
        onEvent: event => {
          queuedChars += event.data.length;
          if (event.data.length > o.maxEventChars || queuedChars > o.maxEventChars + 16384 || queue.length >= 256) fail('event_limit');
          queue.push(event);
        },
      });
      const decoder = new TextDecoder('utf-8', { fatal: true });
      for (;;) {
        if (this.#controller.signal.aborted) return;
        const { done, value } = await io(() => reader.read(), controller, o.ioTimeoutMs);
        if (done) throw new Retry(false, retryDelay); // Incomplete frames never commit.
        if (value.byteLength > 32 * 1024 * 1024) fail('event_limit');
        // Feed bounded slices, then await all app work before reading/feeding more.
        for (let offset = 0; offset < value.length; offset += 16384) {
          try { parser.feed(decoder.decode(value.subarray(offset, offset + 16384), { stream: true })); }
          catch (error) { throw error instanceof StreamError ? error : new StreamError('invalid_frame'); }
          for (const event of queue) {
            if (this.#controller.signal.aborted) return;
            const { event: kind, id, data } = event;
            if (kind === 'refresh-required') fail('refresh_required');
            if (kind === 'heartbeat') {
              if (id !== undefined || data !== '') fail('invalid_control');
              continue;
            }
            if (kind === 'snapshot-complete') {
              if (stage !== 'snapshot' || !cursorValid(id) || data !== '{}') fail('invalid_snapshot');
              await this.#callback('onSnapshotComplete', id, { signal: this.#controller.signal });
              this.#cursor = id;
              this.#provisional = false;
              this.#progress++;
              stage = 'live';
              continue;
            }
            if (!['schema', 'snapshot', 'batch'].includes(kind)) fail('unknown_event');
            if (kind === 'schema') {
              if (stage !== 'schema' && stage !== 'live') fail('invalid_schema');
              if (id !== this.#cursor) fail('invalid_cursor');
            } else if (kind === 'snapshot') {
              if (stage !== 'snapshot' || id !== undefined) fail('invalid_snapshot');
            } else if (stage !== 'live' || !cursorValid(id) || id === this.#cursor) fail('invalid_batch');
            let table;
            try { table = decodeArrowIPC(data, o.arrowOptions); } catch { fail('invalid_arrow'); }
            if (kind === 'schema') {
              if (table.numRows !== 0) fail('invalid_schema');
              this.#schema = table.schema;
              await this.#callback('onSchema', table.schema, this.#cursor);
              stage = initial && this.#cursor === undefined ? 'snapshot' : 'live';
            } else {
              if (JSON.stringify(table.schema.fields) !== JSON.stringify(this.#schema.fields)) fail('schema_mismatch');
              if (kind === 'snapshot') await this.#callback('onSnapshot', table, { signal: this.#controller.signal });
              else {
                await this.#callback('onBatch', table, id, { signal: this.#controller.signal });
                this.#cursor = id;
                this.#progress++;
              }
            }
          }
          queue.length = 0;
          queuedChars = 0;
        }
      }
    } finally {
      controller.abort();
      this.#controller.signal.removeEventListener('abort', closed);
      if (reader) { await reader.cancel().catch(() => {}); reader.releaseLock(); }
      else if (response?.body) await response.body.cancel().catch(() => {});
    }
  }
}
