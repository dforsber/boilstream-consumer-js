// Failure/recovery cases over the actual server. The proxy only interrupts or
// withholds real responses; it never invents SSE frames or cursor acknowledgments.
import assert from 'node:assert/strict';

export async function recovery(page, endpoint, token, requests, setFault) {
  await page.exposeFunction('setProxyFault', setFault);
  const baseline = await page.evaluate(async ({ endpoint, token }) => {
    const { BoilStreamSnapshotConsumer: Consumer } = await import('/src/index.js');
    window.Consumer = Consumer;
    window.options = {
      getToken: () => token, onSnapshotStart() {}, onSnapshot() {}, onSnapshotComplete() {},
      onSnapshotDiscard() {}, onBatch() {}, onRefreshRequired() {},
      retryMinMs: 10, retryMaxMs: 20, maxRetries: 2, ioTimeoutMs: 1000,
    };
    const rows = [];
    const c = new Consumer(endpoint, { ...window.options,
      onSnapshot: table => rows.push(...[...table.getChild('v')].map(v => v === null ? null : String(v))),
      onSnapshotComplete: () => c.close(),
    });
    await c.run();
    return { rows, cursor: c.cursor };
  }, { endpoint, token });
  assert.deepEqual(baseline.rows.slice().sort(), [null, '11', '11', '9007199254740993'].sort());
  assert.match(baseline.cursor, /^r1\./);

  // Disconnect after the real resume schema but before any row acknowledgment.
  // Commit a native row while that connection is lost, then require exact replay.
  let start = requests.length;
  setFault('disconnect-schema');
  const recovered = await page.evaluate(async ({ endpoint, cursor }) => {
    let inserted = false, schemas = 0, snapshots = 0, refreshed = 0, duringCommit;
    const rows = [];
    const c = new window.Consumer(endpoint, { ...window.options, cursor,
      onSnapshotStart() { snapshots++; }, onRefreshRequired() { refreshed++; },
      async onSchema() { schemas++; if (!inserted) { inserted = true; await window.appendNative(); } },
      async onBatch(table) {
        duringCommit = c.cursor;
        await Promise.resolve();
        rows.push(...[...table.getChild('v')].map(String)); c.close();
      },
    });
    await c.run();
    return { rows, cursor: c.cursor, schemas, snapshots, refreshed, duringCommit };
  }, { endpoint, cursor: baseline.cursor });
  assert.deepEqual(recovered.rows, ['23']);
  assert.equal(recovered.schemas, 2); assert.equal(recovered.snapshots, 0); assert.equal(recovered.refreshed, 0);
  assert.equal(recovered.duringCommit, baseline.cursor);
  assert.notEqual(recovered.cursor, baseline.cursor);
  assert.deepEqual(requests.slice(start).map(r => r.cursor), [baseline.cursor, baseline.cursor]);

  // Both transport loss and a proxy that flushes headers but buffers the entire
  // body must exhaust a fixed retry budget. Neither may reset it on each schema.
  for (const fault of ['disconnect-schema-always', 'stall-body']) {
    start = requests.length;
    setFault(fault);
    const exhausted = await page.evaluate(async ({ endpoint, cursor, stall }) => {
      let tokens = 0, schemas = 0, snapshots = 0, rows = 0, refreshed = 0, error;
      const c = new window.Consumer(endpoint, { ...window.options, cursor, ioTimeoutMs: stall ? 200 : 2000,
        getToken: args => { tokens++; return window.options.getToken(args); },
        onSchema() { schemas++; }, onSnapshotStart() { snapshots++; }, onBatch(table) { rows += table.numRows; },
        onRefreshRequired() { refreshed++; },
      });
      try { await c.run(); } catch (e) { error = e.code; }
      return { tokens, schemas, snapshots, rows, refreshed, error, cursor: c.cursor };
    }, { endpoint, cursor: recovered.cursor, stall: fault === 'stall-body' });
    assert.deepEqual(exhausted, { tokens: 3, schemas: fault === 'stall-body' ? 0 : 3, snapshots: 0, rows: 0, refreshed: 1, error: 'retry_exhausted', cursor: undefined });
    assert.deepEqual(requests.slice(start).map(r => r.cursor), Array(3).fill(recovered.cursor));
  }

  // Return to a healthy route and dispose an idle resumed consumer inside its
  // awaited callback. Cancellation must settle run(), keep its applied cursor,
  // suppress further work and release the actual server registration.
  start = requests.length;
  setFault('none');
  const disposed = await page.evaluate(async ({ endpoint, cursor }) => {
    let schemas = 0, rows = 0, refreshed = 0, tokens = 0;
    const c = new window.Consumer(endpoint, { ...window.options, cursor,
      getToken: args => { tokens++; return window.options.getToken(args); },
      async onSchema() { schemas++; await Promise.resolve(); c.close(); },
      onBatch(table) { rows += table.numRows; }, onRefreshRequired() { refreshed++; },
    });
    await c.run();
    return { schemas, rows, refreshed, tokens, cursor: c.cursor };
  }, { endpoint, cursor: recovered.cursor });
  assert.deepEqual(disposed, { schemas: 1, rows: 0, refreshed: 0, tokens: 1, cursor: recovered.cursor });
  assert.deepEqual(requests.slice(start).map(r => r.cursor), [recovered.cursor]);
  assert.ok(requests.every(request => request.credential === 'good'));
  return 4;
}
