# boilstream-consumer

Real-time Arrow data streaming for browsers and Node.js via Server-Sent Events (SSE).

Connects to a [BoilStream](https://boilstream.com) SSE endpoint and delivers decoded [Apache Arrow](https://arrow.apache.org/) tables using [flechette](https://github.com/uwdata/flechette). Automatic reconnection with `Last-Event-ID` is handled by `EventSource`.

## Install

```bash
npm install boilstream-consumer
```

## Quick Start

### Node.js

```js
import { BoilStreamConsumer } from "boilstream-consumer";
import EventSource from "eventsource";

const consumer = new BoilStreamConsumer("https://host:8443/stream/<token>", {
  onSchema: (schema) => console.log("Fields:", schema.fields.map(f => f.name)),
  onBatch: (table, seq) => console.log(`Batch ${seq}: ${table.numRows} rows`),
  onError: (err) => console.error(err),
  EventSource, // required in Node.js
});

// Later: disconnect
consumer.close();
```

### Browser

```js
import { BoilStreamConsumer } from "boilstream-consumer";

const consumer = new BoilStreamConsumer("https://host:8443/stream/<token>", {
  onSchema: (schema) => console.log("Fields:", schema.fields.map(f => f.name)),
  onBatch: (table, seq) => console.log(`Batch ${seq}: ${table.numRows} rows`),
  onError: (err) => console.error(err),
  // Native EventSource is used automatically in browsers
});
```

See [`examples/browser-test.html`](examples/browser-test.html) for a standalone browser demo.

## API

### `new BoilStreamConsumer(url, options)`

| Parameter | Type | Description |
|-----------|------|-------------|
| `url` | `string` | SSE endpoint URL (`https://host/stream/<token>`) |
| `options.onSchema` | `Function` | Called with flechette `Schema` on schema events |
| `options.onBatch` | `Function` | Called with `(Table, batchSeq)` on batch events |
| `options.onHeartbeat` | `Function` | Called on heartbeat events |
| `options.onError` | `Function` | Called with error on connection/decode errors |
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
| `batch` | Base64 Arrow IPC | Record batch with `lastEventId` for resume |
| `heartbeat` | empty | Keep-alive signal |

## Dependencies

- [`@uwdata/flechette`](https://github.com/uwdata/flechette) — Arrow IPC decoding
- [`eventsource`](https://github.com/EventSource/eventsource) — Node.js EventSource polyfill

## License

MIT
