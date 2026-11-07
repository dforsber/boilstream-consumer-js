# boilstream-consumer

Real-time Arrow data streaming for browsers and Node.js via Server-Sent Events (SSE).

Connects to a [BoilStream](https://boilstream.com) SSE endpoint and delivers decoded [Apache Arrow](https://arrow.apache.org/) tables using [flechette](https://github.com/uwdata/flechette). Automatic reconnection with `Last-Event-ID` is handled by `EventSource` for ordinary network interruptions. Cursor IDs are opaque strings and must be preserved verbatim.

## Install

```bash
npm install boilstream-consumer
```

## Quick Start

The repository candidate is 0.2.0 (legacy SSE protocol); this change does not publish it to npm.
Requires Node.js 22.12 or later when using the Node EventSource implementation.

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

Arrow decode failures also close before `onError`: EventSource may already have advanced its internal ID, so continuing could silently skip unread rows. These failures require snapshot refresh. Exceptions thrown by schema/batch callbacks also close the stream before `onError`; the application must refresh. These callbacks must finish synchronously. Returning a Promise closes the stream because EventSource cannot wait for application acknowledgement. Ordinary network errors retain EventSource's automatic retry behavior.

## Focused tests

```sh
npm ci
npm run test:unit
npm run test:integration
npm audit
```

These tests use the actual SDK and Arrow IPC decoder with controlled EventSource delivery. They cover exact IDs, decoded duplicate rows, terminal/malformed controls, queued events, decoder failures and callback errors without a running server. `npm test` remains the separately configured live-server integration test; unit success does not qualify that integration.

TypeScript declarations are included. `test/types.test.ts` checks both browser and Node EventSource constructors and rejects assigning a cursor to a number.

## Dependencies

- [`@uwdata/flechette`](https://github.com/uwdata/flechette) — Arrow IPC decoding
- [`eventsource`](https://github.com/EventSource/eventsource) — Node.js EventSource polyfill

## License

MIT

## Production status and dependency policy

`BoilStreamConsumer` uses the legacy live-only endpoint. A schema cursor is a live
baseline, **not a delivered initial snapshot**. Separately querying a snapshot
and opening this endpoint does not guarantee a gap-free handoff. Do not claim
exactly-once delivery or durable application acknowledgements.

Dependencies are pinned with a committed lockfile: EventSource 5.1.4,
flechette 2.7.1 and Playwright 1.65.0 (checked 2026-11-07). Use `npm ci` for
repeatable qualification. The v2 client directly pins eventsource-parser 4.1.3.
The TLS transport tests use the actual installed EventSource and Arrow decoder,
a temporary trusted test CA, fragmented socket delivery, reconnect IDs and
terminal failures. They also prove an untrusted certificate is rejected. They
do not qualify server authorization, the production proxy, or real browsers.

Keep SSE for one-way browser updates. The experimental v2 client uses Fetch plus
`eventsource-parser`, header credentials, explicit application-committed resume
IDs, bounded parsing/retry and staged snapshot completion. Keep flechette
unless cross-language Arrow fixtures expose unsupported types or compression.
Bulk analytical reads belong on the binary query interfaces; base64 SSE adds
about one third to Arrow payload size before HTTP compression.

### Exact Arrow values

The decoder preserves 64-bit integers as `BigInt`, decimals as unscaled integer
coefficients, and timestamps as integers in the Arrow field's declared unit.
Use the schema's decimal scale and timestamp unit/timezone when formatting.
This changes the previous implicit conversion to JavaScript numbers, which can
fail for large integers or lose decimal/timestamp precision. Pass explicit
`arrowOptions` to the consumer (or a second argument to `decodeArrowIPC`) if your
application intentionally wants flechette's number/date conversion options.

`npm run test:browser:fixture` runs the actual local SDK in Chromium, Firefox and
WebKit, using Apache Arrow JS as an independent wire encoder. It checks nullable
duplicates, Unicode, exact int64 values, fragmented events, reconnect and terminal
closure. This local HTTP fixture does not replace the deployed TLS/browser gate.

`npm run test:types` checks this SDK's public types with dependency declaration
checking disabled: flechette 2.7.1 currently declares an untyped `getFactory`
member. Keep that upstream typing limitation visible when upgrading the decoder.


## Experimental snapshot client (SSE v2)

`BoilStreamSnapshotConsumer` requires the new server `/stream/v2` endpoint and an
explicit `x-boilstream-sse-version: 2` response. It refuses the legacy endpoint.
The server candidate is still undergoing native/security qualification; this
export does not declare SSE GA. No npm publication is included in this change.

```js
import { BoilStreamSnapshotConsumer } from 'boilstream-consumer';

// Supply an application store with the atomic operations described below.
export function subscribe(viewStore, getToken) {
  return new BoilStreamSnapshotConsumer('https://api.example.com/stream/v2', {
    getToken, // async ({ signal, unauthorized }) => currentOrRenewedToken
    cursor: viewStore.committedCursor,
    onSnapshotStart: ({ signal }) => viewStore.beginStaging(signal),
    onSnapshot: (table, { signal }) => viewStore.stage(table, signal),
    onSnapshotComplete: (cursor, { signal }) => viewStore.activateStaging(cursor, signal),
    onSnapshotDiscard: () => viewStore.discardStaging(),
    onBatch: (table, cursor, { signal }) => viewStore.applyAndSaveCursor(table, cursor, signal),
    onRefreshRequired: () => viewStore.invalidate(),
  });
}
// const consumer = subscribe(store, tokenProvider);
// await consumer.run(); // resolves after close(); rejects with redacted StreamError
```

The store must stage rows separately from the active view. `activateStaging`
atomically replaces that view and saves its cursor; `applyAndSaveCursor` applies a
live batch and its cursor in one transaction. Returning successfully acknowledges
that work. If a callback throws, the client stops and invalidates the view. Keep
application staging bounded (for example in IndexedDB) rather than accumulating
an unlimited snapshot in browser memory. Snapshot retries discard staging and
start from scratch. A supplied `cursor` must match already committed application
state; persisting only the cursor can lose data. This is not an exactly-once side
effect guarantee.

Only one callback runs at a time. `close()` aborts network I/O, but waits for the
active application callback to settle; it cannot forcibly cancel arbitrary app
code. Callbacks receive an abort signal and must settle cooperatively. A successful
in-flight commit is retained even if close was requested; later queued events are
suppressed. Observe the `run()` promise and its `StreamError.code`.

Tokens travel only in Authorization headers. The provider is called for each
connection and receives `unauthorized: true` for one renewal attempt after 401.
Keep renewed tokens in the provider's own session state. Two consecutive 401
responses stop the client. TLS verification remains enabled; redirects are refused,
credentials/cookies are omitted, and endpoint queries/userinfo are rejected.
For cross-origin deployments allow Authorization and Last-Event-ID in preflight
and expose `x-boilstream-sse-version` on responses, including CORS on error responses.

Defaults: eight consecutive retries without committed progress, jittered backoff
from 500ms to 30s, 30s per token/network read, 24MiB maximum partial event text,
16MiB decoded IPC payload, and 256 queued events per bounded parser feed. Retry
hints are capped. Successful application commits reset the retry budget. Protocol,
decode and permanent HTTP failures invalidate state instead of reconnecting.
Legacy and v2 accept complete modern Arrow IPC streams; malformed base64, missing
stream framing, oversized payloads or absent schema fail closed. This does not
claim exhaustive validation of hostile FlatBuffers internals or every Arrow type.

Local verification uses independent Apache Arrow JS encoding, actual TLS Fetch and
EventSource transports, and the actual SDK in Chromium/Firefox/WebKit with CORS,
credential renewal and partial snapshot restart. The public type fixture includes
both clients. Actual BoilStream server, deployed proxy/TLS, RLS revocation and soak
remain separate qualification gates.

### Combined browser/server qualification

From the server's `ingestion-agent` directory, with the normal native build paths
selected, run:

```sh
make MAKE_DIRENV=0 test-sse-browser SSE_SDK_DIR=/absolute/path/to/boilstream-consumer-js
```

Install this SDK's locked dependencies and all three Playwright browsers first.
The target fails when prerequisites are missing. It runs Chromium, Firefox and
WebKit against a real native BoilStream fixture, with snapshot interruption,
revoked-token renewal, exact large integers/nulls/duplicates, live application
commits, callback failure and replay from the previous committed cursor.

The browser app and proxy use loopback HTTP; the proxy verifies the real server's
TLS certificate and speaks HTTP/2. An untrusted-CA negative control must fail.
This is local integration evidence, not qualification of browser-facing deployment
TLS, a production proxy configuration, cluster failover or sustained load.

On 2026-11-07, all three engines passed the combined gate on macOS against the
SSE server candidate and its existing native build. Each engine checks snapshot
restart/renewal, live commit, failed application processing and exact replay.
The server's newer native selection has a separate ingestion-fixture failure;
these results do not qualify that selection or promote SSE to GA.

The same server target also runs recovery scenarios. To select only those cases:

```sh
make MAKE_DIRENV=0 test-sse-browser-recovery SSE_SDK_DIR=/absolute/path/to/boilstream-consumer-js
```

They verify exact replay after a lost connection, a finite retry budget across
schema-only reconnects, deadlines when a proxy withholds the response body, and
idle cancellation with server registration cleanup. On macOS, all three engines
passed these cases and the original three snapshot scenarios. A temporary
extra-retry mutation failed the real Chromium test and was restored. These are
bounded failure tests, not sustained-load or cluster-failover qualification.
