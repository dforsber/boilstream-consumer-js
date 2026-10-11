# Opaque replay cursor SDK review

This change preserves SSE IDs as strings in `onBatch(table, cursor)` and exposes the exact readiness/baseline ID through `onSchema(schema, cursor)`. A `refresh-required` event closes the connection before parsing its body or invoking application code. Even malformed terminal data invokes the generic refresh callback and reports its error. Existing callbacks taking only the schema remain compatible; numeric batch-cursor consumers must change to strings.

## Review 1: continuity and error semantics

`Number(lastEventId)` corrupted versioned IDs to `NaN` and could round large numeric IDs. The SDK now forwards the exact string, including empty and zero IDs, without guessing a successor. Named terminal events cannot reconnect automatically, and already queued events are ignored after close.

The review also confirmed an existing continuity bug: a failed Arrow decode merely reported an error while EventSource could already have advanced Last-Event-ID. Continuing could omit unread rows. Decode errors now close before `onError`; applications must refresh their canonical snapshot. Application callback exceptions remain distinct from decoder failures and retain their previous nonterminal error reporting. Ordinary network errors preserve EventSource retry behavior.

## Review 2: API and architecture

The change uses the existing EventSource instance, callbacks and Arrow decoder. It adds no retry loop, cursor parser, timer, payload queue or alternate transport. Closure clears the current instance before calling its `close`, suppressing synchronous close-related errors and stale events. `onRefreshRequired` receives `{ reason: "refresh_required" }`; valid server control events carry no new ID. Schema/batch declarations consistently type the cursor as a string. Browser and Node EventSource constructors both satisfy the exported options type.

## Review 3: focused tests and qualification scope

Eight `node:test` cases pass using actual SDK modules and real flechette Arrow IPC. They check exact cursor strings and duplicate rows, close-before-callback ordering, malformed terminal events, queued events, terminal callback exceptions, decoder refusal, application callback distinction, ordinary network errors and idempotent close. Three semantic mutations fail: reintroducing numeric conversion, omitting terminal close, and continuing after decode failure. TypeScript 5.9.3 validates the declarations and rejects assigning a cursor to a number.

The existing live-server test now requires opaque IDs on both schema and batch events. It was not run in this focused SDK phase. No live server, browser, native replay, hot/cold correctness or release qualification is claimed here; those belong to the combined ingestion-agent integration gate. The in-repository ingestion-agent SDK harness must install the exact tested SDK commit or packed artifact instead of the published `0.1.0` package before claiming compatibility.

Validation used Node 24.18.0, `@uwdata/flechette` 2.3.0, `eventsource` 3.0.7 and `eventsource-parser` 3.0.6, matching the ingestion-agent lockfile. Dependency ranges were not changed. The host's older global TypeScript did not support NodeNext; the recorded successful check used explicitly selected TypeScript 5.9.3 instead.

Commands:

```sh
npm run test:unit
npm exec --yes --package=typescript@5.9.3 -- tsc --noEmit --strict --module nodenext --moduleResolution nodenext --target es2022 --lib es2022,dom test/types.test.ts
node --check src/consumer.js
node --check test/consumer.test.js
git diff --check
```
