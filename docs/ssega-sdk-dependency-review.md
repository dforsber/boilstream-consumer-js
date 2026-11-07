# SSEGA.5 dependency and legacy SDK deliverable — 2026-11-07

This is a usable legacy-protocol maintenance deliverable, not SSE GA or npm
publication. The authoritative task ledger is BoilStream's root TASKS.md.

Dependencies: EventSource 5.1.4, eventsource-parser 4.1.3 (transitive), flechette
2.7.1, Playwright 1.65.0; independent test encoder Apache Arrow JS 21.2.0;
TypeScript 7.0.2. Node >=22.12 is now required. Dependencies and transitive
resolution are locked; `npm audit` reported zero vulnerabilities.

## Inline author review 1 — delivery correctness

Finding: schema/batch application exceptions previously continued after native
EventSource had advanced its cursor. Promise-returning callbacks were also
unobserved. Resolution: close before reporting callback failure, refuse async
callbacks and consume their rejection, suppress stale queued delivery. Keep the
legacy synchronous contract explicit. Verification: controlled callback tests and
actual EventSource over TLS cover decode, sync and async application failure,
fragmentation, Last-Event-ID and terminal closure. Verdict: pass for this legacy
maintenance deliverable; application-acknowledged Fetch transport remains SSEGA.5.

## Inline author review 2 — data and library compatibility

Finding: all three real browsers failed an independent Apache Arrow int64 fixture
above Number.MAX_SAFE_INTEGER with flechette's default conversions. Resolution:
default to exact BigInt integers, decimal coefficients and timestamp units;
allow explicit extraction overrides and document the behavior change in 0.2.0.
Verification: independent Arrow fixtures cover nullable duplicate int64s, decimal
coefficient/scale and nanosecond timezone/unit preservation. Chromium
158.0.8127.18, Firefox 159.0 and WebKit 27.2 pass actual SDK loading, independent
Arrow decoding, fragmented events, exact reconnect IDs and terminal closure.
Verdict: keep flechette; tested conversion settings solve the observed failure.
No exhaustive SQL/Arrow-type or compressed-IPC qualification is claimed.

## Inline author review 3 — security, reproducibility and evidence limits

Finding: old browser smoke disabled TLS verification, loaded a demo instead of
the SDK, and logged credential-bearing page URLs. Resolution: enable normal TLS
verification, correct the example path, redact the URL, and add a separate actual
SDK browser fixture with local pinned modules. TLS integration generates an
isolated temporary CA, proves rejection without trust, and trusts it only in the
positive child process. No certificate bypass is used. A committed npm lockfile
and Node floor make dependency resolution reviewable. Verification: 13 focused
and interop tests, 5 TLS tests, 3 real-browser tests, public TypeScript fixture and
npm audit pass. Upstream flechette getFactory has an implicit-any declaration;
`skipLibCheck` excludes dependency declarations, not this SDK's public API checks.
Verdict: pass maintenance merge. Deployed TLS/CORS/proxy, actual server policy,
snapshot handoff, renewal and soak remain required before SSE GA. No npm release
or server release is asserted by this document.
