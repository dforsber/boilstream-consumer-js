# boilstream-consumer

Real-time Arrow data streaming for browsers and Node.js via Server-Sent Events (SSE).

Connects to a [BoilStream](https://boilstream.com) SSE endpoint and delivers decoded [Apache Arrow](https://arrow.apache.org/) tables using [flechette](https://github.com/uwdata/flechette). Automatic reconnection with `Last-Event-ID` is handled by `EventSource` for ordinary network interruptions. Cursor IDs are opaque strings and must be preserved verbatim.

## Install

```bash
npm install boilstream-consumer
```

## Quick Start

### Node.js

```js
import { BoilStreamConsumer } from "boilstream-consumer";
import { EventSource } from "eventsource";

const consumer = new BoilStreamConsumer("https://host:8443/stream/<token>", {
  onSchema: (schema, cursor) => console.log("Ready:", cursor, schema.fields.map(f => f.name)),
  onBatch: (table, cursor) => console.log(`Cursor ${cursor}: ${table.numRows} rows`),
  onError: (err) => console.error(err),
  onRefreshRequired: ({ reason }) => console.log("Refresh the snapshot:", reason),
  EventSource, // required in Node.js
});

// Later: disconnect
consumer.close();
```

### Browser

```js
import { BoilStreamConsumer } from "boilstream-consumer";

const consumer = new BoilStreamConsumer("https://host:8443/stream/<token>", {
  onSchema: (schema, cursor) => console.log("Ready:", cursor, schema.fields.map(f => f.name)),
  onBatch: (table, cursor) => console.log(`Cursor ${cursor}: ${table.numRows} rows`),
  onError: (err) => console.error(err),
  onRefreshRequired: ({ reason }) => console.log("Refresh the snapshot:", reason),
  // Native EventSource is used automatically in browsers
});
```

See [`examples/browser-test.html`](examples/browser-test.html) for a standalone browser demo.

## API

### `new BoilStreamConsumer(url, options)`

| Parameter | Type | Description |
|-----------|------|-------------|
| `url` | `string` | SSE endpoint URL (`https://host/stream/<token>`) |
| `options.onSchema` | `Function` | Called with `(Schema, cursor: string)` on schema/readiness events |
| `options.onBatch` | `Function` | Called with `(Table, cursor: string)` on complete batch events |
| `options.onHeartbeat` | `Function` | Called on heartbeat events |
| `options.onError` | `Function` | Called with connection, decode, malformed-control or callback errors; decode errors close the stream |
| `options.onRefreshRequired` | `Function` | Called with `{ reason: "refresh_required" }` after the stream closes |
| `options.EventSource` | `class` | EventSource implementation (required in Node.js) |

### `consumer.schema`

Current schema (`null` until first schema event).

### `consumer.close()`

Disconnect from the SSE stream.

### `decodeArrowIPC(base64Data)`

Low-level utility: decode a base64-encoded Arrow IPC stream into a flechette `Table`.

```js
import { decodeArrowIPC } from "boilstream-consumer";
const table = decodeArrowIPC(base64String);
```

## SSE Event Types

| Event | Data | Description |
|-------|------|-------------|
| `schema` | Base64 Arrow IPC | Table schema (sent on connect and schema changes) |
| `batch` | Base64 Arrow IPC | Complete logical batch with opaque `lastEventId` for resume |
| `heartbeat` | empty | Keep-alive signal |
| `refresh-required` | `{"reason":"refresh_required"}` | Terminal; no new ID; SDK closes before invoking callbacks |

## Cursor and refresh behavior

`onSchema(schema, cursor)` exposes the initial snapshot baseline; `onBatch(table, cursor)` exposes the cursor for the complete decoded batch. IDs such as `r1.…` are versioned, opaque strings, not sequence numbers. Never use `Number`, compare their order, or manufacture a successor. The SDK preserves the EventSource ID exactly, including an empty string if an event has no ID. Existing one-argument schema callbacks continue to work; applications that expected a numeric second argument to `onBatch` must update it to a string.

On `refresh-required`, the SDK closes **before** parsing or invoking application code, even if the control body is malformed or the callback throws. `onRefreshRequired` still receives the generic refresh reason on malformed input; `onError` also receives the parsing/validation error. Events queued before closure are ignored. Obtain a fresh canonical snapshot and start a new subscription according to your application's reconciliation flow; do not blindly retry the rejected cursor. As with any network protocol, connection loss can prevent delivery of the terminal event.

Arrow decode failures also close before `onError`: EventSource may already have advanced its internal ID, so continuing could silently skip unread rows. These failures require snapshot refresh. Exceptions thrown by your own schema/batch callbacks are reported through `onError` without being mistaken for a decoder failure. Ordinary network errors retain EventSource's automatic retry behavior.

## Focused tests

```sh
npm run test:unit
```

These tests use the actual SDK and Arrow IPC decoder with controlled EventSource delivery. They cover exact IDs, decoded duplicate rows, terminal/malformed controls, queued events, decoder failures and callback errors without a running server. `npm test` remains the separately configured live-server integration test; unit success does not qualify that integration.

TypeScript declarations are included. `test/types.test.ts` checks both browser and Node EventSource constructors and rejects assigning a cursor to a number.

## Dependencies

- [`@uwdata/flechette`](https://github.com/uwdata/flechette) — Arrow IPC decoding
- [`eventsource`](https://github.com/EventSource/eventsource) — Node.js EventSource polyfill

## License

MIT
