# boilstream-consumer

Real-time Arrow data streaming for browsers and Node.js via Server-Sent Events (SSE).

Use `BoilStreamSnapshotConsumer` for the **SSE v2 preview**: an initial Arrow
snapshot, live batches, and reconnects from the last application-committed opaque
cursor. `BoilStreamConsumer` remains available for the legacy live-only endpoint.
Both use the pinned [flechette](https://github.com/uwdata/flechette) Arrow decoder.

## Install the GitHub preview

The compatible client version is **0.2.0**. Install the annotated GitHub release
explicitly. The older npm 0.1.0 package is incompatible with the current cursor
protocol; it must not be used with this server.

```bash
npm install 'github:dforsber/boilstream-consumer-js#v0.2.0'
```

Use Node.js 22.12 or later. For browsers, import the package through your bundler.
Start with the [v2 snapshot client](#experimental-snapshot-client-sse-v2) below;
its callbacks must stage, activate and commit application state with each cursor.

## Run the browser example

```bash
git clone https://github.com/dforsber/boilstream-consumer-js.git
cd boilstream-consumer-js
npm ci
npm run example:browser
```

Open `http://127.0.0.1:8787`, enter your `/stream/v2` endpoint and stream token,
and select **Fresh snapshot**. Allow that origin in the server's CORS settings;
allow `Authorization` and `Last-Event-ID` and expose `X-Boilstream-SSE-Version`.
The browser must trust the server certificate. The example serves only local
lockfile dependencies; it does not fetch a floating decoder from a CDN.

The [example](examples/browser-test.html) uses the actual SDK. It stages snapshot
rows until completion, commits rows and cursor together, preserves duplicate/NULL
values, and invalidates state on refresh or decode/application failure. **Stop**
retains state for **Resume retained state** on the same endpoint. Reloading loses
both state and cursor. Its 10,000-row/one-million-JSON-character limits are demo bounds, not a production store;
use bounded transactional storage such as IndexedDB for a real application.
Tokens stay in memory and Authorization headers, never page URLs or browser storage.
Supply a new token manually when needed; this demo does not implement your login flow.

## Legacy live-only examples

These examples use `/stream/{token}` and do not supply existing snapshot rows.
EventSource callbacks must be synchronous; use v2 for awaited application commits.

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

The browser demo above uses v2; these EventSource snippets are legacy only.

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
The reliable preview is qualified locally against server main `389f80212`; this
export does not declare SSE GA. No npm publication is included.

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
both clients. The actual native-server gate below also passed. Deployed proxy/TLS,
cluster resilience and sustained load remain separate production gates. Governed
streaming RLS/CLS is future work; current streams refuse governed sources.

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

On 2026-11-08, all six snapshot/recovery cases passed on macOS in Chromium,
Firefox and WebKit against the merged native baseline. The previously failing
Quack replay cases also passed. Server main `389f80212` contains this qualified
preview; SDK runtime `7aba755` is the matching client. Each engine checks snapshot
restart/renewal, live commit, failed application processing and exact replay.
These results qualify the local preview, not production GA.

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

### Example validation

`npm run test:example` drives the shipped demo in Chromium, Firefox and WebKit
against independent Apache Arrow JS fixtures. It checks private snapshot staging,
exact duplicate/NULL rows, paired cursor commits, stop/resume, refresh, malformed
Arrow refusal and absence of credentials in URLs/storage. It serves the real SDK
and the installed dependencies, not a mock consumer.

`npm run test:browser` is an optional live-server smoke of this same v2 demo via
a loopback proxy with verified server TLS. Set `SERVER_URL`, matching `STREAM_TOKEN`
and `INGEST_TOKEN` for a quiet fixture topic, and `NODE_EXTRA_CA_CERTS` for a private
CA. It writes three fixture rows. `npm test` remains a legacy EventSource smoke;
neither command replaces `make test-sse-browser` in the server repository.
