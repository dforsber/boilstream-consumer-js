import assert from "node:assert/strict";
import { test } from "node:test";
import { BoilStreamConsumer } from "boilstream-consumer";
import { tableFromColumns, columnFromArray, tableToIPC } from "@uwdata/flechette";

const ipc = Buffer.from(tableToIPC(tableFromColumns({
  value: columnFromArray(Int32Array.from([11, 11, 33])),
}), { format: "stream" })).toString("base64");

function fixture(callbacks = {}) {
  let source;
  class Source {
    listeners = new Map();
    closed = false;
    constructor(url) { this.url = url; source = this; }
    addEventListener(type, listener) { this.listeners.set(type, listener); }
    close() { this.closed = true; this.onClose?.(); }
    // Allow events already queued when close() ran to reach the SDK listener.
    emit(type, data = "", lastEventId = "") {
      this.listeners.get(type)?.({ data, lastEventId });
    }
  }
  const consumer = new BoilStreamConsumer("https://example.invalid/stream/capability", {
    ...callbacks, EventSource: Source,
  });
  return { consumer, source };
}

function rows(table) { return [...table.getChild("value")]; }

test("schema baseline and batch IDs remain exact opaque strings with exact decoded rows", () => {
  const schemas = [], batches = [];
  const { consumer, source } = fixture({
    onSchema: (schema, cursor) => schemas.push([schema, cursor]),
    onBatch: (table, cursor) => batches.push([rows(table), cursor]),
  });
  for (const id of ["r1.Abc_123-opaque", "9007199254740993", "0", ""]) {
    source.emit("schema", ipc, id);
    source.emit("batch", ipc, id);
    assert.equal(schemas.at(-1)[1], id);
    assert.equal(consumer.schema, schemas.at(-1)[0]);
    assert.deepEqual(batches.at(-1), [[11, 11, 33], id]);
  }
  consumer.close();
});

test("refresh closes before callback and refuses already queued events", () => {
  const seen = [];
  const { source } = fixture({
    onRefreshRequired: (details) => {
      assert.equal(source.closed, true);
      seen.push(details);
    },
    onSchema: () => assert.fail("schema after terminal"),
    onBatch: () => assert.fail("batch after terminal"),
    onHeartbeat: () => assert.fail("heartbeat after terminal"),
    onError: () => assert.fail("error after terminal"),
  });
  source.emit("refresh-required", '{"reason":"refresh_required"}');
  source.emit("schema", ipc, "r1.later");
  source.emit("batch", ipc, "r1.later");
  source.emit("heartbeat");
  source.onerror(new Error("queued transport error"));
  source.emit("refresh-required", '{"reason":"refresh_required"}');
  assert.deepEqual(seen, [{ reason: "refresh_required" }]);
});

test("malformed terminal bodies still close and request refresh before reporting errors", () => {
  for (const data of ["{", "null", "{}", "[]", '{"reason":"unknown"}']) {
    const seen = [];
    const { source } = fixture({
      onRefreshRequired: (details) => {
        assert.equal(source.closed, true);
        seen.push(details);
      },
      onError: (error) => {
        assert.equal(source.closed, true);
        assert.ok(error instanceof Error);
        seen.push("error");
      },
    });
    source.emit("refresh-required", data);
    assert.deepEqual(seen, [{ reason: "refresh_required" }, "error"]);
  }
});

test("terminal callback exceptions cannot keep the connection alive", () => {
  const failure = new Error("application callback failed");
  const errors = [];
  const { source } = fixture({
    onRefreshRequired: () => { assert.equal(source.closed, true); throw failure; },
    onError: (error) => errors.push(error),
  });
  source.emit("refresh-required", '{"reason":"refresh_required"}');
  assert.equal(source.closed, true);
  assert.deepEqual(errors, [failure]);
});

test("invalid schema or batch IPC closes before error and cannot skip to a later cursor", () => {
  for (const type of ["schema", "batch"]) {
    const errors = [];
    const { consumer, source } = fixture({
      onSchema: () => assert.fail("invalid or post-failure schema delivered"),
      onBatch: () => assert.fail("invalid or post-failure batch delivered"),
      onError: (error) => { assert.equal(source.closed, true); errors.push(error); },
    });
    source.emit(type, Buffer.from("not Arrow IPC").toString("base64"), "r1.failed");
    source.emit("batch", ipc, "r1.later");
    assert.equal(consumer.schema, null);
    assert.equal(errors.length, 1);
  }
});

test("application callback failures are distinct from Arrow decoding failure", () => {
  const failure = new Error("application callback failed");
  const errors = [], delivered = [];
  const { consumer, source } = fixture({
    onSchema: () => { throw failure; },
    onBatch: (table, cursor) => { delivered.push([rows(table), cursor]); throw failure; },
    onError: (error) => errors.push(error),
  });
  source.emit("schema", ipc, "r1.baseline");
  source.emit("batch", ipc, "r1.first");
  source.emit("batch", ipc, "r1.second");
  assert.equal(source.closed, false);
  assert.ok(consumer.schema);
  assert.deepEqual(delivered, [[[11, 11, 33], "r1.first"], [[11, 11, 33], "r1.second"]]);
  assert.deepEqual(errors, [failure, failure, failure]);
  consumer.close();
});

test("ordinary network failures preserve EventSource retry and subsequent delivery", () => {
  const failure = new Error("temporary network loss");
  const errors = [], delivered = [];
  const { consumer, source } = fixture({
    onBatch: (table, cursor) => delivered.push([rows(table), cursor]),
    onError: (error) => errors.push(error),
  });
  source.onerror(failure);
  assert.equal(source.closed, false);
  source.emit("batch", ipc, "r1.resumed");
  assert.deepEqual(errors, [failure]);
  assert.deepEqual(delivered, [[[11, 11, 33], "r1.resumed"]]);
  consumer.close();
});

test("explicit close suppresses synchronous close callbacks and stale events", () => {
  const { consumer, source } = fixture({ onError: () => assert.fail("closed callback") });
  source.onClose = () => source.onerror(new Error("close callback"));
  consumer.close();
  consumer.close();
  source.emit("batch", ipc, "r1.stale");
  assert.equal(source.closed, true);
});
